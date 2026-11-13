package projectionserver

import (
	"errors"

	"github.com/adiom-data/weave/internal/querysubscription/model"
	reactivev1 "github.com/adiom-data/weave/pkg/gen/reactive/v1"
)

func (session *connectionSession) handleOutbound(raw outboundMessage) error {
	switch item := raw.(type) {
	case transactionMessage:
		return session.publisher.sendTransaction(item)
	case queryEventMessage:
		if item.err != nil {
			return item.err
		}
		session.sourceProgress.observe(item.event)
		if session.service.relationAccess != nil {
			session.publisher.policy.observe(item.event)
			affected, err := session.service.relationAccess.observe(item.event)
			if err != nil {
				return err
			}
			for _, loading := range session.pending {
				if item.event.Progress != nil {
					loading.retry = true
					if loading.direct != nil {
						loading.direct.observe(item.event.Progress.Position, item.event.Progress.Revision)
					}
				}
				if item.event.Delivery != nil {
					if loading.direct != nil {
						if err := loading.direct.apply(*item.event.Delivery); err != nil {
							loading.cancel()
							loading.retry = true
						}
					}
					for effect := range item.event.Delivery.SubscriptionEffects() {
						if loading.owns != nil && loading.owns(effect.SubscriptionID) {
							loading.retry = true
						}
					}
				}
				for _, target := range affected {
					if loading.policyOwns != nil && loading.policyOwns(target.Scope) {
						loading.retry = true
					}
				}
			}
		}
		if item.event.Progress != nil {
			for _, state := range session.projections {
				if _, _, err := state.projection.ApplyEvent(item.event); err != nil {
					return session.closeProjection(state, err)
				}
			}
			for _, state := range session.pending {
				if state.projection != nil {
					if _, _, err := state.projection.ApplyEvent(item.event); err != nil {
						state.cancel()
					}
				}
			}
			if err := session.publisher.outputs.Advance(item.event.Progress.Revision, session.publisher.consumeProjectionOutput); err != nil {
				return err
			}
			if session.service.relationAccess != nil {
				progress := item.event.Progress
				return session.publisher.emitDeliveries([]feedDelivery{{metadata: querysubscription.DeliveryMetadata{Kind: querysubscription.DeliveryHydration, Checkpoint: progress.Position, Revision: progress.Revision, PreviousRevision: progress.PreviousRevision}}})
			}
			return nil
		}
		return handleQueryEvent(item.event, func(deliveries []feedDelivery) error {
			for index := range deliveries {
				deliveries[index].messages = item.messages
			}
			return session.publisher.emitDeliveries(deliveries)
		}, session.closeDirect, session.closeProjection, session.subscriptions, session.projections, session.pending)
	case completionMessage:
		err := item.run()
		if item.finished != nil {
			item.finished <- err
		}
		return err
	case projectionClosedMessage:
		projection := session.projections[item.externalID]
		if projection == nil || (item.generation != "" && item.generation != projection.generation) {
			if loading := session.pending[item.externalID]; loading != nil && loading.generation == item.generation {
				loading.cancel()
				loading.retry = true
			}
			return nil
		}
		return session.closeProjection(projection, item.cause)
	case windowFailedMessage:
		if projection := session.projections[item.externalID]; projection != nil {
			if item.generation != "" && item.generation != projection.generation {
				return nil
			}
			projection.sequence++
			return session.stream.Send(&reactivev1.ServerMessage{SubscriptionId: projection.externalID, RequestId: item.requestID, Sequence: projection.sequence, Event: &reactivev1.ServerMessage_Error{Error: &reactivev1.SubscriptionError{Code: "WINDOW_FAILED", Message: item.cause.Error()}}})
		}
		state := session.subscriptions[item.externalID]
		if state == nil || (item.generation != "" && item.generation != state.internalID) {
			return nil
		}
		state.sequence++
		return session.stream.Send(&reactivev1.ServerMessage{SubscriptionId: state.externalID, RequestId: item.requestID, Sequence: state.sequence, Event: &reactivev1.ServerMessage_Error{Error: &reactivev1.SubscriptionError{Code: "WINDOW_FAILED", Message: item.cause.Error()}}})
	case projectionBatchMessage:
		projection := session.projections[item.externalID]
		if projection == nil || (item.generation != "" && item.generation != projection.generation) {
			return nil
		}
		if output := session.publisher.outputs.Submit(projection.projection, projection, item.batch); output != nil {
			return session.publisher.consumeProjectionOutput(*output)
		}
		return nil
	case subscriptionBatchMessage:
		return session.publisher.sendSubscriptionImmediate(item, true)
	default:
		return errors.New("unknown outbound message")
	}
}
