package projectionserver

import (
	"errors"
	"fmt"

	"github.com/adiom-data/weave/internal/accesscontrol"
	proj "github.com/adiom-data/weave/internal/projection"
	"github.com/adiom-data/weave/internal/querysubscription/model"
	reactivev1 "github.com/adiom-data/weave/pkg/gen/reactive/v1"
)

// connectionPublisher owns the public model, publication scheduler, and
// connection sequence. Its methods run only on the connection consumer, so
// one transaction is authorized and assembled before any later output.
type connectionPublisher struct {
	connection *connectionSession
	model      *connectionModel
	outputs    *proj.OutputScheduler[*sharedProjection]
	sequence   uint64
	policy     *policyPublications
}

type feedDelivery struct {
	// replay restricts historical catch-up to the newly installed projection.
	// It must not rewind other scopes or advance the connection's live clock.
	replay   *sharedProjection
	metadata querysubscription.DeliveryMetadata
	parts    []deliveryPayload
	messages []*reactivev1.TransactionMessage
}

func (publisher *connectionPublisher) sendTransaction(item transactionMessage) error {
	publisher.sequence++
	batch := &reactivev1.ModelBatch{Kind: reactivev1.ModelBatch_KIND_COMMIT, Atomic: true, Checkpoint: checkpointString(item.transaction.Position), Messages: item.transaction.Messages}
	return publisher.connection.stream.Send(&reactivev1.ServerMessage{ConnectionSequence: publisher.sequence, Event: &reactivev1.ServerMessage_ModelBatch{ModelBatch: batch}})
}

func (publisher *connectionPublisher) sendCleanup(subscriptionID string, sequence *uint64) error {
	batch, err := publisher.model.closeScope(subscriptionID)
	if err != nil {
		return err
	}
	if len(batch.Entities) == 0 && len(batch.Queries) == 0 && len(batch.Projections) == 0 && len(batch.Computed) == 0 && len(batch.Branches) == 0 {
		return nil
	}
	*sequence = *sequence + 1
	return publisher.connection.stream.Send(&reactivev1.ServerMessage{SubscriptionId: subscriptionID, Sequence: *sequence, Event: &reactivev1.ServerMessage_ModelBatch{ModelBatch: batch}})
}

func (publisher *connectionPublisher) cleanupFailedScope(subscriptionID string) error {
	batch, err := publisher.model.closeScope(subscriptionID)
	if err != nil {
		return err
	}
	// The failed initial scope was never attached publicly. Its removal may
	// nevertheless render deferred entity changes for surviving references;
	// those must be sent rather than silently marked as already published.
	if len(batch.Entities) == 0 {
		return nil
	}
	publisher.sequence++
	return publisher.connection.stream.Send(&reactivev1.ServerMessage{ConnectionSequence: publisher.sequence, Event: &reactivev1.ServerMessage_ModelBatch{ModelBatch: &reactivev1.ModelBatch{Kind: reactivev1.ModelBatch_KIND_WINDOW, Entities: batch.Entities}}})
}

func (publisher *connectionPublisher) consumeProjectionOutput(output proj.Output[*sharedProjection]) error {
	state := output.Owner
	if publisher.connection.projections[state.externalID] != state {
		return nil
	}
	if output.Err != nil {
		return publisher.connection.closeProjection(state, fmt.Errorf("complete projection publication: %w", output.Err))
	}
	return publisher.sendProjectionImmediate(projectionBatchMessage{
		externalID: state.externalID, generation: state.generation, batch: output.Batch,
	}, true)
}

func (publisher *connectionPublisher) emitDeliveries(deliveries []feedDelivery) error {
	failed := make(map[string]visibilityFailure)
	failureKey := func(failure visibilityFailure) string {
		if failure.projection {
			return "projection:" + failure.externalID
		}
		return "subscription:" + failure.externalID
	}
	addFailure := func(failure visibilityFailure) {
		if failure.cause != nil {
			failed[failureKey(failure)] = failure
		}
	}
	isFailed := func(externalID string, projection bool) bool {
		return failed[failureKey(visibilityFailure{externalID: externalID, projection: projection})].cause != nil
	}
	for _, delivery := range deliveries {
		// Empty commits still synchronize auxiliary state: an authorization-only
		// transaction can produce visibility changes without a data payload.
		var authorizationView accesscontrol.View
		var invalidation accesscontrol.Invalidation
		var err error
		if delivery.metadata.Checkpoint != nil {
			authorizationView, invalidation, err = synchronizeVisibility(publisher.connection.ctx, publisher.connection.service.access, publisher.connection.prepared, delivery.metadata.Checkpoint)
			if err != nil {
				return fmt.Errorf("synchronize access control: %w", err)
			}
		} else {
			authorizationView, err = acquireVisibilityAt(publisher.connection.ctx, publisher.connection.service.access, publisher.connection.prepared, delivery.metadata.Checkpoint, delivery.metadata.Fence)
			if err != nil {
				return fmt.Errorf("synchronize materialized access control: %w", err)
			}
		}
		parts := append([]deliveryPayload(nil), delivery.parts...)
		if (delivery.metadata.Kind == querysubscription.DeliveryCommit || delivery.metadata.Kind == querysubscription.DeliveryHydration) && (invalidation.All || len(invalidation.Candidates) > 0 || len(invalidation.Targets) > 0) {
			// Every scope receives one frame joining raw effects and policy
			// invalidations. Catch-up changes only the target set, not the path.
			seen := make(map[string]bool)
			seenSubscriptions := make(map[string]bool)
			for index, raw := range parts {
				if part, ok := raw.(subscriptionBatchMessage); ok && delivery.replay == nil {
					part.invalidation = invalidation
					parts[index] = part
					seenSubscriptions[part.externalID] = true
				}
				if part, ok := raw.(projectionBatchMessage); ok {
					part.invalidation = invalidation
					parts[index] = part
					seen[part.externalID] = true
				}
			}
			appendFrame := func(state *sharedProjection) {
				if !seen[state.externalID] {
					parts = append(parts, projectionBatchMessage{externalID: state.externalID, generation: state.generation,
						batch: proj.ProjectionBatch{Delivery: delivery.metadata}, invalidation: invalidation})
				}
			}
			if delivery.replay != nil {
				appendFrame(delivery.replay)
			} else {
				for _, state := range publisher.connection.subscriptions {
					if !seenSubscriptions[state.externalID] {
						parts = append(parts, subscriptionBatchMessage{externalID: state.externalID, generation: state.internalID, delivery: delivery.metadata, invalidation: invalidation})
					}
				}
				for _, state := range publisher.connection.projections {
					appendFrame(state)
				}
			}
		}
		filteredParts := make([]deliveryPayload, 0, len(parts))
		for _, rawPart := range parts {
			var externalID string
			var projectionPart bool
			switch part := rawPart.(type) {
			case projectionBatchMessage:
				externalID, projectionPart = part.externalID, true
			case subscriptionBatchMessage:
				externalID = part.externalID
			}
			if isFailed(externalID, projectionPart) {
				continue
			}
			filtered, failure, filterErr := filterVisibilityPart(publisher.connection.ctx, publisher.connection.prepared, authorizationView, rawPart, publisher.connection.subscriptions, publisher.connection.projections)
			if filterErr != nil {
				_ = authorizationView.Close()
				return filterErr
			}
			if failure != nil {
				addFailure(*failure)
				continue
			}
			if filtered != nil {
				if publisher.policy != nil && delivery.metadata.Kind != querysubscription.DeliveryWindow && publisher.policy.withhold(filtered) {
					continue
				}
				filteredParts = append(filteredParts, filtered)
			}
		}
		if err := authorizationView.Close(); err != nil {
			return fmt.Errorf("close access-control view: %w", err)
		}
		if delivery.metadata.Kind == querysubscription.DeliveryWindow {
			for _, rawPart := range filteredParts {
				switch part := rawPart.(type) {
				case projectionBatchMessage:
					if isFailed(part.externalID, true) {
						continue
					}
					if err := publisher.sendProjectionImmediate(part, false); err != nil {
						return err
					}
				case subscriptionBatchMessage:
					if isFailed(part.externalID, false) {
						continue
					}
					if err := publisher.sendSubscriptionImmediate(part, false); err != nil {
						return err
					}
				}
			}
			continue
		}
		batch := &reactivev1.ModelBatch{Checkpoint: checkpointString(delivery.metadata.Checkpoint)}
		if delivery.replay == nil && delivery.metadata.Kind == querysubscription.DeliveryCommit {
			batch.Messages = delivery.messages
		}
		if err := publisher.model.assembleBatch(batch, func() error {
			for _, rawPart := range filteredParts {
				switch part := rawPart.(type) {
				case projectionBatchMessage:
					if isFailed(part.externalID, true) {
						continue
					}
					projection := publisher.connection.projections[part.externalID]
					if projection == nil || (part.generation != "" && part.generation != projection.generation) {
						continue
					}
					modelBatch, err := publisher.model.applyProjection(projection.externalID, part.batch, projection.selected)
					if err != nil {
						return err
					}
					appendModelBatch(batch, modelBatch)
				case subscriptionBatchMessage:
					if isFailed(part.externalID, false) {
						continue
					}
					state := publisher.connection.subscriptions[part.externalID]
					if state == nil || (part.generation != "" && part.generation != state.internalID) {
						continue
					}
					modelBatch, err := publisher.model.applySubscriptionBatch(part.versions, state.queryID, part.delivery, part.batch)
					if err != nil {
						return err
					}
					appendModelBatch(batch, modelBatch)
				default:
					return errors.New("unknown feed delivery payload")
				}
			}
			return nil
		}); err != nil {
			return err
		}
		if delivery.metadata.Kind == querysubscription.DeliveryHydration {
			batch.Kind = reactivev1.ModelBatch_KIND_HYDRATION
		} else {
			batch.Kind = reactivev1.ModelBatch_KIND_COMMIT
		}
		batch.Atomic = delivery.metadata.Atomic
		coalesceEntityEvents(batch)
		if len(batch.Entities) > 0 || len(batch.Queries) > 0 || len(batch.Projections) > 0 || len(batch.Computed) > 0 || len(batch.Messages) > 0 || len(batch.Branches) > 0 {
			publisher.sequence++
			if err := publisher.connection.stream.Send(&reactivev1.ServerMessage{ConnectionSequence: publisher.sequence, Event: &reactivev1.ServerMessage_ModelBatch{ModelBatch: batch}}); err != nil {
				return err
			}
		}
	}
	for _, failure := range failed {
		if failure.projection {
			if state := publisher.connection.projections[failure.externalID]; state != nil {
				if err := publisher.connection.closeProjection(state, failure.cause); err != nil {
					return err
				}
			}
			continue
		}
		if state := publisher.connection.subscriptions[failure.externalID]; state != nil {
			if err := publisher.connection.closeDirect(state, failure.cause); err != nil {
				return err
			}
		}
	}
	for _, delivery := range deliveries {
		if delivery.replay != nil {
			continue
		}
		if err := publisher.outputs.Advance(delivery.metadata.Revision, publisher.consumeProjectionOutput); err != nil {
			return err
		}
	}
	return nil
}

func (publisher *connectionPublisher) sendProjectionImmediate(item projectionBatchMessage, evaluate bool) error {
	projection := publisher.connection.projections[item.externalID]
	if projection == nil {
		return nil
	}
	if item.generation != "" && item.generation != projection.generation {
		return nil
	}
	if failure := item.batch.WindowFailure; failure != nil {
		projection.sequence++
		return publisher.connection.stream.Send(&reactivev1.ServerMessage{
			SubscriptionId: projection.externalID, RequestId: failure.Token, Sequence: projection.sequence,
			Event: &reactivev1.ServerMessage_Error{Error: &reactivev1.SubscriptionError{Code: "WINDOW_FAILED", Message: failure.Cause.Error()}},
		})
	}
	var err error
	if evaluate && item.batch.Window != nil {
		authorizationView, accessErr := acquireVisibilityAt(publisher.connection.ctx, publisher.connection.service.access, publisher.connection.prepared, item.batch.Window.Checkpoint, item.batch.Window.Fence)
		if accessErr != nil {
			return publisher.connection.closeProjection(projection, accessErr)
		}
		item.batch, err = projection.visibility.filterBatch(publisher.connection.ctx, publisher.connection.prepared, authorizationView, item.batch)
		closeErr := authorizationView.Close()
		if err == nil {
			err = closeErr
		}
	} else if evaluate && len(item.batch.Events) > 0 {
		item.batch.Events, err = projection.visibility.filterInitial(publisher.connection.ctx, publisher.connection.service.access, publisher.connection.prepared, item.batch.Events)
	}
	if err != nil {
		return publisher.connection.closeProjection(projection, err)
	}
	if publisher.policy != nil && publisher.policy.withhold(item) {
		if window := item.batch.Window; window != nil {
			projection.publication.acks = append(projection.publication.acks, func() error {
				return publisher.policy.acknowledgeWindow(projection.externalID, window.Token, window.InstanceID, &projection.sequence)
			})
		}
		return nil
	}
	batch, err := publisher.model.applyProjection(projection.externalID, item.batch, projection.selected)
	if err != nil {
		return publisher.connection.closeProjection(projection, err)
	}
	requestID := ""
	if window := item.batch.Window; window != nil {
		windowBatch, windowErr := publisher.model.applyProjectionWindow(window.InstanceID, window.Bounds)
		if windowErr != nil {
			return publisher.connection.closeProjection(projection, windowErr)
		}
		appendModelBatch(batch, windowBatch)
		requestID = window.Token
	}
	batch.Kind = reactivev1.ModelBatch_KIND_HYDRATION
	if item.batch.Window != nil {
		batch.Kind = reactivev1.ModelBatch_KIND_WINDOW
	}
	coalesceEntityEvents(batch)
	if len(batch.Entities) == 0 && len(batch.Queries) == 0 && len(batch.Projections) == 0 && len(batch.Computed) == 0 && len(batch.Branches) == 0 && requestID == "" {
		return nil
	}
	projection.sequence++
	return publisher.connection.stream.Send(&reactivev1.ServerMessage{SubscriptionId: projection.externalID, RequestId: requestID, Sequence: projection.sequence, Event: &reactivev1.ServerMessage_ModelBatch{ModelBatch: batch}})
}

func (publisher *connectionPublisher) sendSubscriptionImmediate(item subscriptionBatchMessage, evaluate bool) error {
	state := publisher.connection.subscriptions[item.externalID]
	if state == nil || (item.generation != "" && item.generation != state.internalID) {
		return nil
	}
	if err := item.delivery.Validate(); err != nil {
		return publisher.connection.closeDirect(state, err)
	}
	if item.delivery.Kind == 0 {
		return publisher.connection.closeDirect(state, errors.New("direct subscription output requires delivery metadata"))
	}
	var batch *reactivev1.ModelBatch
	var err error
	requestID := ""
	if window := item.batch.Window; window != nil {
		if evaluate {
			authorizationView, accessErr := acquireVisibilityAt(publisher.connection.ctx, publisher.connection.service.access, publisher.connection.prepared, item.delivery.Checkpoint, item.delivery.Fence)
			if accessErr != nil {
				return publisher.connection.closeDirect(state, accessErr)
			}
			item, err = state.visibility.filterTransaction(publisher.connection.ctx, publisher.connection.prepared, authorizationView, item)
			closeErr := authorizationView.Close()
			if err == nil {
				err = closeErr
			}
			if err != nil {
				return publisher.connection.closeDirect(state, err)
			}
		}
		requestID = window.Token
	}
	if publisher.policy != nil && publisher.policy.withhold(item) {
		if window := item.batch.Window; window != nil {
			state.publication.acks = append(state.publication.acks, func() error {
				return publisher.policy.acknowledgeWindow(state.externalID, window.Token, state.queryID, &state.sequence)
			})
		}
		return nil
	}
	batch, err = publisher.model.applySubscriptionBatch(item.versions, state.queryID, item.delivery, item.batch)
	if err != nil {
		return publisher.connection.closeDirect(state, err)
	}
	batch.Kind = reactivev1.ModelBatch_KIND_WINDOW
	state.sequence++
	return publisher.connection.stream.Send(&reactivev1.ServerMessage{SubscriptionId: state.externalID, RequestId: requestID, Sequence: state.sequence, Event: &reactivev1.ServerMessage_ModelBatch{ModelBatch: batch}})
}

func (publisher *connectionPublisher) releaseProjection(state *sharedProjection) error {
	err := publisher.outputs.Release(state.projection, state, func(output proj.Output[*sharedProjection]) error {
		if output.Err != nil {
			return publisher.consumeProjectionOutput(output)
		}
		batch := output.Batch
		item := projectionBatchMessage{externalID: state.externalID, generation: state.generation, batch: batch}
		if batch.Delivery.Kind != 0 {
			return publisher.emitDeliveries([]feedDelivery{{
				replay: state, metadata: batch.Delivery, parts: []deliveryPayload{item},
			}})
		}
		return publisher.consumeProjectionOutput(output)
	})
	if err != nil && publisher.connection.projections[state.externalID] == state {
		return publisher.connection.closeProjection(state, err)
	}
	if errors.Is(err, querysubscription.ErrClosed) && publisher.connection.projections[state.externalID] != state {
		return nil
	}
	return err
}
