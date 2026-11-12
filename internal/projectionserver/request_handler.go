package projectionserver

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"google.golang.org/protobuf/proto"

	"github.com/adiom-data/weave/internal/accesscontrol"
	proj "github.com/adiom-data/weave/internal/projection"
	"github.com/adiom-data/weave/internal/querysubscription/model"
	reactivev1 "github.com/adiom-data/weave/pkg/gen/reactive/v1"
)

var errRequestHistoryFull = errors.New("connection request history is full")

type requestLedger struct {
	limit int
	seen  map[string][sha256.Size]byte
}

func newRequestLedger(limit int) *requestLedger {
	return &requestLedger{limit: limit, seen: make(map[string][sha256.Size]byte)}
}

// Accept records one immutable request identity for the lifetime of a
// connection. An identical retry is a no-op: on one ordered gRPC stream the
// original response either remains in flight or the connection fails, in
// which case the next connection intentionally starts a new ledger.
func (l *requestLedger) Accept(message *reactivev1.ClientMessage) (bool, error) {
	if message == nil || message.RequestId == "" {
		return true, nil
	}
	encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(message)
	if err != nil {
		return false, fmt.Errorf("encode request identity: %w", err)
	}
	digest := sha256.Sum256(encoded)
	if previous, exists := l.seen[message.RequestId]; exists {
		if previous != digest {
			return false, errors.New("request_id was reused for a different request")
		}
		return false, nil
	}
	if len(l.seen) >= l.limit {
		return false, fmt.Errorf("%w: limit of %d reached", errRequestHistoryFull, l.limit)
	}
	l.seen[message.RequestId] = digest
	return true, nil
}

func (session *connectionSession) handleRequest(message *reactivev1.ClientMessage) error {
	accepted, requestErr := session.requests.Accept(message)
	if requestErr != nil {
		code := "INVALID_ARGUMENT"
		if errors.Is(requestErr, errRequestHistoryFull) {
			code = "RESOURCE_EXHAUSTED"
		}
		if err := sendSharedError(session.stream, message.GetSubscriptionId(), message.GetRequestId(), code, requestErr.Error()); err != nil {
			return err
		}
		return nil
	}
	if !accepted {
		return nil
	}

	if message.SubscriptionId == "" || message.RequestId == "" {
		return sendSharedError(session.stream, message.SubscriptionId, message.RequestId, "INVALID_ARGUMENT", "subscription_id and request_id are required")
	}

	switch operation := message.Operation.(type) {
	case *reactivev1.ClientMessage_Subscribe:
		if session.subscriptions[message.SubscriptionId] != nil || session.projections[message.SubscriptionId] != nil || hasPending(session.pending, message.SubscriptionId) {
			return sendSharedError(session.stream, message.SubscriptionId, message.RequestId, "ALREADY_EXISTS", "subscription already exists")
		}
		if len(session.subscriptions)+len(session.projections)+len(session.pending) >= session.service.options.MaxSubscriptionsPerConnection {
			return sendSharedError(session.stream, message.SubscriptionId, message.RequestId, "RESOURCE_EXHAUSTED", "the stream subscription limit has been reached")
		}
		spec, selected, err := storageQuery(operation.Subscribe.Query, session.service.options.MaxSelectedFields, session.service.options.MaxPredicateDepth, session.service.options.MaxPredicateNodes)
		if err != nil {
			return sendSharedError(session.stream, message.SubscriptionId, message.RequestId, "INVALID_QUERY", err.Error())
		}
		window := operation.Subscribe.Window
		if window == nil || (window.From != reactivev1.InitialWindow_FROM_START && window.From != reactivev1.InitialWindow_FROM_END) {
			return sendSharedError(session.stream, message.SubscriptionId, message.RequestId, "INVALID_WINDOW", "window must start from START or END")
		}
		internalID := session.prefix + message.SubscriptionId + ":" + strconv.FormatUint(streamSequence.Add(1), 10)
		queryID := "query:" + internalID
		externalID, requestID := message.SubscriptionId, message.RequestId
		initialWindow := querysubscription.InitialWindow{FromEnd: window.From == reactivev1.InitialWindow_FROM_END, Count: int(window.Count)}
		if session.service.relationAccess != nil {
			return session.admissions.query(externalID, requestID, internalID, queryID, spec, selected, initialWindow)
		}
		loadCtx, loadCancel := context.WithCancel(session.ctx)
		pendingState := &pendingSharedSubscription{cancel: loadCancel, owns: func(id string) bool { return id == internalID }}
		session.pending[externalID] = pendingState
		session.loaders.Add(1)
		go func() {
			defer session.loaders.Done()
			visibility := newSubscriptionVisibility()
			subscribeErr := session.queries.SubscribeQuery(loadCtx, querysubscription.QuerySubscribeRequest{
				ID: internalID, Query: spec, Window: initialWindow,
			}, func(subscription querysubscription.QuerySubscription, snapshot querysubscription.QuerySnapshot) error {
				var view accesscontrol.View
				view, installErr := acquireVisibilityAt(loadCtx, session.service.access, session.prepared, snapshot.Checkpoint, snapshot.Fence)
				if installErr != nil {
					return installErr
				}
				filtered, installErr := visibility.filterSnapshot(loadCtx, session.prepared, view, snapshot)
				if closeErr := view.Close(); installErr == nil && closeErr != nil {
					installErr = closeErr
				}
				if installErr != nil {
					return installErr
				}
				finished := make(chan error, 1)
				installed := false
				cleanup := func() { _ = subscription.Close(context.WithoutCancel(session.ctx)) }
				completion := completionMessage{abandon: cleanup, finished: finished, run: func() error {
					if session.pending[externalID] != pendingState {
						cleanup()
						return nil
					}
					state := &sharedSubscription{internalID: internalID, queryID: queryID, externalID: externalID, subscription: subscription, visibility: visibility}
					session.subscriptions[externalID] = state
					delete(session.pending, externalID)
					batch, modelErr := session.publisher.model.openSubscription(externalID, queryID, "", filtered, selected)
					if modelErr != nil {
						delete(session.subscriptions, externalID)
						cleanupErr := session.publisher.cleanupFailedScope(externalID)
						cleanup()
						if cleanupErr != nil {
							return cleanupErr
						}
						return sendSharedError(session.stream, externalID, requestID, "SUBSCRIBE_FAILED", modelErr.Error())
					}
					batch.Kind = reactivev1.ModelBatch_KIND_SNAPSHOT
					state.sequence++
					if sendErr := session.stream.Send(&reactivev1.ServerMessage{SubscriptionId: externalID, RequestId: requestID, Sequence: state.sequence, Event: &reactivev1.ServerMessage_ModelBatch{ModelBatch: batch}}); sendErr != nil {
						return sendErr
					}
					state.sequence++
					if sendErr := session.stream.Send(&reactivev1.ServerMessage{SubscriptionId: externalID, RequestId: requestID, Sequence: state.sequence, Event: &reactivev1.ServerMessage_Ready{Ready: &reactivev1.Ready{}}}); sendErr != nil {
						return sendErr
					}
					installed = true
					return nil
				}}
				select {
				case session.outbound <- completion:
				case <-loadCtx.Done():
					cleanup()
					return loadCtx.Err()
				}
				select {
				case completionErr := <-finished:
					if completionErr != nil {
						return completionErr
					}
					if !installed {
						return querysubscription.ErrClosed
					}
					return nil
				case <-loadCtx.Done():
					cleanup()
					return loadCtx.Err()
				}
			})
			if subscribeErr == nil {
				loadCancel()
				return
			}
			completion := completionMessage{run: func() error {
				if session.pending[externalID] != pendingState {
					return nil
				}
				delete(session.pending, externalID)
				loadCancel()
				return sendSharedError(session.stream, externalID, requestID, "SUBSCRIBE_FAILED", subscribeErr.Error())
			}}
			select {
			case session.outbound <- completion:
			case <-session.ctx.Done():
				loadCancel()
			}
		}()
		return nil
	case *reactivev1.ClientMessage_SubscribeProjection:
		if session.subscriptions[message.SubscriptionId] != nil || session.projections[message.SubscriptionId] != nil || hasPending(session.pending, message.SubscriptionId) {
			return sendSharedError(session.stream, message.SubscriptionId, message.RequestId, "ALREADY_EXISTS", "subscription already exists")
		}
		if len(session.subscriptions)+len(session.projections)+len(session.pending) >= session.service.options.MaxSubscriptionsPerConnection {
			return sendSharedError(session.stream, message.SubscriptionId, message.RequestId, "RESOURCE_EXHAUSTED", "the stream subscription limit has been reached")
		}
		root, selected, err := projectionNode(operation.SubscribeProjection.Root, operation.SubscribeProjection.Parameters, session.service.options.MaxSelectedFields, session.service.options.MaxPredicateDepth, session.service.options.MaxPredicateNodes)
		if err != nil {
			return sendSharedError(session.stream, message.SubscriptionId, message.RequestId, "INVALID_PROJECTION", err.Error())
		}
		if session.service.relationAccess == nil {
			for _, fields := range selected {
				if len(computedSelection(fields)) > 0 {
					return sendSharedError(session.stream, message.SubscriptionId, message.RequestId, "INVALID_PROJECTION", "computed fields require relation definitions")
				}
			}
		}
		externalID, requestID := message.SubscriptionId, message.RequestId
		if session.service.relationAccess != nil {
			return session.admissions.projection(externalID, requestID, session.prefix, root, selected, operation.SubscribeProjection.WaitForSync, operation.SubscribeProjection.AllowPartialErrors, operation.SubscribeProjection.AtLeastFence)
		}
		generation := session.prefix + "projection:" + strconv.FormatUint(streamSequence.Add(1), 10)
		loadCtx, loadCancel := context.WithCancel(session.ctx)
		pendingState := &pendingSharedSubscription{cancel: loadCancel, generation: generation, owns: func(id string) bool { return strings.HasPrefix(id, generation+":") }}
		session.pending[externalID] = pendingState
		projection, beginErr := proj.BeginProjection(loadCtx, session.queries, root, proj.StartOptions{
			Prefix: generation, QueueLimit: session.service.options.SubscriptionQueueLimit, Limits: proj.Limits(session.service.options.ProjectionLimits),
			ExpansionLimiter:   session.service.expansionLimiter,
			WaitForSync:        operation.SubscribeProjection.WaitForSync,
			AtLeastFence:       operation.SubscribeProjection.AtLeastFence,
			AllowPartialErrors: operation.SubscribeProjection.AllowPartialErrors,
		})
		if beginErr != nil {
			delete(session.pending, externalID)
			loadCancel()
			return sendSharedError(session.stream, externalID, requestID, "SUBSCRIBE_FAILED", beginErr.Error())
		}
		pendingState.projection = projection
		go forwardProjection(session.ctx, externalID, generation, projection, session.outbound)
		session.loaders.Add(1)
		go func() {
			defer session.loaders.Done()
			visibility := newProjectionVisibility(selected)
			visibility.partial = operation.SubscribeProjection.AllowPartialErrors
			initial, _, subscribeErr := projection.TakeInitial(loadCtx)
			if subscribeErr == nil {
				initial, subscribeErr = visibility.filterInitial(loadCtx, session.service.access, session.prepared, initial)
			}
			cleanup := func() {
				if projection != nil {
					projection.Close()
				}
				loadCancel()
			}
			completion := completionMessage{abandon: cleanup, run: func() error {
				if session.pending[externalID] != pendingState {
					cleanup()
					return nil
				}
				if terminalErr := projection.Err(); terminalErr != nil {
					subscribeErr = terminalErr
				}
				if subscribeErr != nil {
					delete(session.pending, externalID)
					cleanup()
					return sendSharedError(session.stream, externalID, requestID, "SUBSCRIBE_FAILED", subscribeErr.Error())
				}
				state := &sharedProjection{externalID: externalID, generation: generation, projection: projection, cancel: loadCancel, visibility: visibility, selected: selected}
				session.projections[externalID] = state
				delete(session.pending, externalID)
				// Keep the load context with the installed projection so closing the
				// client-visible scope also releases its parent context resources.
				initialBatch, modelErr := session.publisher.model.applyProjection(externalID, proj.ProjectionBatch{Events: initial}, selected)
				if modelErr != nil {
					delete(session.projections, externalID)
					cleanupErr := session.publisher.cleanupFailedScope(externalID)
					cleanup()
					if cleanupErr != nil {
						return cleanupErr
					}
					return sendSharedError(session.stream, externalID, requestID, "SUBSCRIBE_FAILED", modelErr.Error())
				}
				initialBatch.Kind = reactivev1.ModelBatch_KIND_SNAPSHOT
				if operation.SubscribeProjection.WaitForSync && len(initial) > 0 {
					checkpoint := initial[0].Checkpoint
					for _, event := range initial {
						if !querysubscription.SamePosition(checkpoint, event.Checkpoint) {
							checkpoint = nil
							break
						}
					}
					initialBatch.Checkpoint = checkpointString(checkpoint)
				}
				state.sequence++
				if sendErr := session.stream.Send(&reactivev1.ServerMessage{SubscriptionId: externalID, RequestId: requestID, Sequence: state.sequence, Event: &reactivev1.ServerMessage_ModelBatch{ModelBatch: initialBatch}}); sendErr != nil {
					return sendErr
				}
				state.sequence++
				if sendErr := session.stream.Send(&reactivev1.ServerMessage{SubscriptionId: externalID, RequestId: requestID, Sequence: state.sequence, Event: &reactivev1.ServerMessage_Ready{Ready: &reactivev1.Ready{}}}); sendErr != nil {
					return sendErr
				}
				return session.publisher.releaseProjection(state)
			}}
			select {
			case session.outbound <- completion:
			case <-session.ctx.Done():
				cleanup()
			}
		}()
		return nil
	case *reactivev1.ClientMessage_UpdateWindow:
		state := session.subscriptions[message.SubscriptionId]
		if state == nil {
			return sendSharedError(session.stream, message.SubscriptionId, message.RequestId, "NOT_FOUND", "subscription does not exist")
		}
		changes, err := queryBoundaryChanges(operation.UpdateWindow)
		if err != nil {
			return sendSharedError(session.stream, state.externalID, message.RequestId, "INVALID_WINDOW", err.Error())
		}
		externalID, internalID, requestID := state.externalID, state.internalID, message.RequestId
		go func() {
			if updateErr := state.subscription.UpdateWindow(session.ctx, changes, requestID); updateErr != nil {
				select {
				case session.outbound <- windowFailedMessage{externalID: externalID, generation: internalID, requestID: requestID, cause: updateErr}:
				case <-session.ctx.Done():
				}
			}
		}()
		return nil
	case *reactivev1.ClientMessage_UpdateProjectionWindow:
		state := session.projections[message.SubscriptionId]
		if state == nil {
			return sendSharedError(session.stream, message.SubscriptionId, message.RequestId, "NOT_FOUND", "projection does not exist")
		}
		if operation.UpdateProjectionWindow.InstanceId == "" {
			return sendSharedError(session.stream, message.SubscriptionId, message.RequestId, "INVALID_WINDOW", "instance_id is required")
		}
		changes, err := queryBoundaryChanges(&reactivev1.UpdateWindow{Updates: operation.UpdateProjectionWindow.Updates})
		if err != nil {
			return sendSharedError(session.stream, message.SubscriptionId, message.RequestId, "INVALID_WINDOW", err.Error())
		}
		externalID, generation, instanceID, requestID := state.externalID, state.generation, operation.UpdateProjectionWindow.InstanceId, message.RequestId
		projection := state.projection
		go func() {
			if updateErr := projection.QueueWindowUpdate(session.ctx, instanceID, changes, requestID); updateErr != nil {
				select {
				case session.outbound <- windowFailedMessage{externalID: externalID, generation: generation, requestID: requestID, cause: updateErr}:
				case <-session.ctx.Done():
				}
			}
		}()
		return nil
	case *reactivev1.ClientMessage_Unsubscribe:
		state := session.subscriptions[message.SubscriptionId]
		if state != nil {
			delete(session.subscriptions, message.SubscriptionId)
			_ = state.subscription.Close(session.ctx)
			state.publication.close()
			if state.cancel != nil {
				state.cancel()
			}
			_ = session.prepared.Release(session.ctx, state.visibility.id, nil)
			batch, err := session.publisher.model.closeScope(message.SubscriptionId)
			if err != nil {
				return err
			}
			state.sequence++
			return session.stream.Send(&reactivev1.ServerMessage{SubscriptionId: state.externalID, RequestId: message.RequestId, Sequence: state.sequence, Event: &reactivev1.ServerMessage_ModelBatch{ModelBatch: batch}})
		}
		projection := session.projections[message.SubscriptionId]
		if projection != nil {
			delete(session.projections, message.SubscriptionId)
			session.publisher.outputs.Drop(projection.projection)
			projection.close()
			_ = projection.releaseVisibility(session.ctx, session.prepared)
			batch, err := session.publisher.model.closeScope(message.SubscriptionId)
			if err != nil {
				return err
			}
			projection.sequence++
			return session.stream.Send(&reactivev1.ServerMessage{SubscriptionId: projection.externalID, RequestId: message.RequestId, Sequence: projection.sequence, Event: &reactivev1.ServerMessage_ModelBatch{ModelBatch: batch}})
		}
		if loading := session.pending[message.SubscriptionId]; loading != nil {
			delete(session.pending, message.SubscriptionId)
			loading.cancel()
			return session.stream.Send(&reactivev1.ServerMessage{SubscriptionId: message.SubscriptionId, RequestId: message.RequestId, Sequence: 1, Event: &reactivev1.ServerMessage_ModelBatch{ModelBatch: &reactivev1.ModelBatch{}}})
		}
		return sendSharedError(session.stream, message.SubscriptionId, message.RequestId, "NOT_FOUND", "subscription does not exist")
	default:
		return sendSharedError(session.stream, message.SubscriptionId, message.RequestId, "INVALID_ARGUMENT", "operation is required")
	}
}

func hasPending(pending map[string]*pendingSharedSubscription, subscriptionID string) bool {
	_, exists := pending[subscriptionID]
	return exists
}

func forwardProjection(ctx context.Context, externalID, generation string, projection *proj.Projection, outbound chan<- outboundMessage) {
	for {
		batch, err := projection.Next(ctx)
		var message outboundMessage
		if err != nil {
			message = projectionClosedMessage{externalID: externalID, generation: generation, cause: err}
		} else {
			message = projectionBatchMessage{externalID: externalID, generation: generation, batch: batch}
		}
		select {
		case outbound <- message:
		case <-ctx.Done():
			return
		}
		if err != nil {
			return
		}
	}
}
