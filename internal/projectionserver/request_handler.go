package projectionserver

import (
	"context"
	"strconv"
	"strings"
	"sync"

	"github.com/adiom-data/weave/internal/accesscontrol"
	proj "github.com/adiom-data/weave/internal/projection"
	"github.com/adiom-data/weave/internal/querysubscription/model"
	reactivev1 "github.com/adiom-data/weave/pkg/gen/reactive/v1"
)

func (s *server) handleSharedMessage(ctx context.Context, stream Stream, prefix string, queries querysubscription.Feed, prepared accesscontrol.Prepared, subscriptions map[string]*sharedSubscription, projections map[string]*sharedProjection, pending map[string]*pendingSharedSubscription, loaders *sync.WaitGroup, model *connectionModel, outbound chan<- outboundMessage, projectionOutputs *proj.OutputScheduler[*sharedProjection], releaseProjection func(*sharedProjection) error, cleanupFailedScope func(string) error, message *reactivev1.ClientMessage) error {
	if message.SubscriptionId == "" || message.RequestId == "" {
		return sendSharedError(stream, message.SubscriptionId, message.RequestId, "INVALID_ARGUMENT", "subscription_id and request_id are required")
	}
	admissions := &policyAdmissions{ctx: ctx, service: s, stream: stream, queries: queries, prepared: prepared, subscriptions: subscriptions, projections: projections, pending: pending, loaders: loaders, model: model, outbound: outbound, releaseProjection: releaseProjection, cleanupFailedScope: cleanupFailedScope}
	switch operation := message.Operation.(type) {
	case *reactivev1.ClientMessage_Subscribe:
		if subscriptions[message.SubscriptionId] != nil || projections[message.SubscriptionId] != nil || hasPending(pending, message.SubscriptionId) {
			return sendSharedError(stream, message.SubscriptionId, message.RequestId, "ALREADY_EXISTS", "subscription already exists")
		}
		if len(subscriptions)+len(projections)+len(pending) >= s.options.MaxSubscriptionsPerConnection {
			return sendSharedError(stream, message.SubscriptionId, message.RequestId, "RESOURCE_EXHAUSTED", "the stream subscription limit has been reached")
		}
		spec, selected, err := storageQuery(operation.Subscribe.Query, s.options.MaxSelectedFields, s.options.MaxPredicateDepth, s.options.MaxPredicateNodes)
		if err != nil {
			return sendSharedError(stream, message.SubscriptionId, message.RequestId, "INVALID_QUERY", err.Error())
		}
		window := operation.Subscribe.Window
		if window == nil || (window.From != reactivev1.InitialWindow_FROM_START && window.From != reactivev1.InitialWindow_FROM_END) {
			return sendSharedError(stream, message.SubscriptionId, message.RequestId, "INVALID_WINDOW", "window must start from START or END")
		}
		internalID := prefix + message.SubscriptionId + ":" + strconv.FormatUint(streamSequence.Add(1), 10)
		queryID := "query:" + internalID
		externalID, requestID := message.SubscriptionId, message.RequestId
		initialWindow := querysubscription.InitialWindow{FromEnd: window.From == reactivev1.InitialWindow_FROM_END, Count: int(window.Count)}
		if s.relationAccess != nil {
			return admissions.query(externalID, requestID, internalID, queryID, spec, selected, initialWindow)
		}
		loadCtx, loadCancel := context.WithCancel(ctx)
		pendingState := &pendingSharedSubscription{cancel: loadCancel, owns: func(id string) bool { return id == internalID }}
		pending[externalID] = pendingState
		loaders.Add(1)
		go func() {
			defer loaders.Done()
			visibility := newSubscriptionVisibility()
			subscribeErr := queries.SubscribeQuery(loadCtx, querysubscription.QuerySubscribeRequest{
				ID: internalID, Query: spec, Window: initialWindow,
			}, func(subscription querysubscription.QuerySubscription, snapshot querysubscription.QuerySnapshot) error {
				var view accesscontrol.View
				view, installErr := acquireVisibilityAt(loadCtx, s.access, prepared, snapshot.Checkpoint, snapshot.Fence)
				if installErr != nil {
					return installErr
				}
				filtered, installErr := visibility.filterSnapshot(loadCtx, prepared, view, snapshot)
				if closeErr := view.Close(); installErr == nil && closeErr != nil {
					installErr = closeErr
				}
				if installErr != nil {
					return installErr
				}
				finished := make(chan error, 1)
				installed := false
				cleanup := func() { _ = subscription.Close(context.WithoutCancel(ctx)) }
				completion := completionMessage{abandon: cleanup, finished: finished, run: func() error {
					if pending[externalID] != pendingState {
						cleanup()
						return nil
					}
					state := &sharedSubscription{internalID: internalID, queryID: queryID, externalID: externalID, subscription: subscription, visibility: visibility}
					subscriptions[externalID] = state
					delete(pending, externalID)
					batch, modelErr := model.openSubscription(externalID, queryID, "", filtered, selected)
					if modelErr != nil {
						delete(subscriptions, externalID)
						cleanupErr := cleanupFailedScope(externalID)
						cleanup()
						if cleanupErr != nil {
							return cleanupErr
						}
						return sendSharedError(stream, externalID, requestID, "SUBSCRIBE_FAILED", modelErr.Error())
					}
					batch.Kind = reactivev1.ModelBatch_KIND_SNAPSHOT
					state.sequence++
					if sendErr := stream.Send(&reactivev1.ServerMessage{SubscriptionId: externalID, RequestId: requestID, Sequence: state.sequence, Event: &reactivev1.ServerMessage_ModelBatch{ModelBatch: batch}}); sendErr != nil {
						return sendErr
					}
					state.sequence++
					if sendErr := stream.Send(&reactivev1.ServerMessage{SubscriptionId: externalID, RequestId: requestID, Sequence: state.sequence, Event: &reactivev1.ServerMessage_Ready{Ready: &reactivev1.Ready{}}}); sendErr != nil {
						return sendErr
					}
					installed = true
					return nil
				}}
				select {
				case outbound <- completion:
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
				if pending[externalID] != pendingState {
					return nil
				}
				delete(pending, externalID)
				loadCancel()
				return sendSharedError(stream, externalID, requestID, "SUBSCRIBE_FAILED", subscribeErr.Error())
			}}
			select {
			case outbound <- completion:
			case <-ctx.Done():
				loadCancel()
			}
		}()
		return nil
	case *reactivev1.ClientMessage_SubscribeProjection:
		if subscriptions[message.SubscriptionId] != nil || projections[message.SubscriptionId] != nil || hasPending(pending, message.SubscriptionId) {
			return sendSharedError(stream, message.SubscriptionId, message.RequestId, "ALREADY_EXISTS", "subscription already exists")
		}
		if len(subscriptions)+len(projections)+len(pending) >= s.options.MaxSubscriptionsPerConnection {
			return sendSharedError(stream, message.SubscriptionId, message.RequestId, "RESOURCE_EXHAUSTED", "the stream subscription limit has been reached")
		}
		root, selected, err := projectionNode(operation.SubscribeProjection.Root, operation.SubscribeProjection.Parameters, s.options.MaxSelectedFields, s.options.MaxPredicateDepth, s.options.MaxPredicateNodes)
		if err != nil {
			return sendSharedError(stream, message.SubscriptionId, message.RequestId, "INVALID_PROJECTION", err.Error())
		}
		if s.relationAccess == nil {
			for _, fields := range selected {
				if len(computedSelection(fields)) > 0 {
					return sendSharedError(stream, message.SubscriptionId, message.RequestId, "INVALID_PROJECTION", "computed fields require relation definitions")
				}
			}
		}
		externalID, requestID := message.SubscriptionId, message.RequestId
		if s.relationAccess != nil {
			return admissions.projection(externalID, requestID, prefix, root, selected, operation.SubscribeProjection.WaitForSync, operation.SubscribeProjection.AllowPartialErrors, operation.SubscribeProjection.AtLeastFence)
		}
		generation := prefix + "projection:" + strconv.FormatUint(streamSequence.Add(1), 10)
		loadCtx, loadCancel := context.WithCancel(ctx)
		pendingState := &pendingSharedSubscription{cancel: loadCancel, generation: generation, owns: func(id string) bool { return strings.HasPrefix(id, generation+":") }}
		pending[externalID] = pendingState
		projection, beginErr := proj.BeginProjection(loadCtx, queries, root, proj.StartOptions{
			Prefix: generation, QueueLimit: s.options.SubscriptionQueueLimit, Limits: proj.Limits(s.options.ProjectionLimits),
			ExpansionLimiter:   s.expansionLimiter,
			WaitForSync:        operation.SubscribeProjection.WaitForSync,
			AtLeastFence:       operation.SubscribeProjection.AtLeastFence,
			AllowPartialErrors: operation.SubscribeProjection.AllowPartialErrors,
		})
		if beginErr != nil {
			delete(pending, externalID)
			loadCancel()
			return sendSharedError(stream, externalID, requestID, "SUBSCRIBE_FAILED", beginErr.Error())
		}
		pendingState.projection = projection
		go forwardProjection(ctx, externalID, generation, projection, outbound)
		loaders.Add(1)
		go func() {
			defer loaders.Done()
			visibility := newProjectionVisibility(selected)
			visibility.partial = operation.SubscribeProjection.AllowPartialErrors
			initial, _, subscribeErr := projection.TakeInitial(loadCtx)
			if subscribeErr == nil {
				initial, subscribeErr = visibility.filterInitial(loadCtx, s.access, prepared, initial)
			}
			cleanup := func() {
				if projection != nil {
					projection.Close()
				}
				loadCancel()
			}
			completion := completionMessage{abandon: cleanup, run: func() error {
				if pending[externalID] != pendingState {
					cleanup()
					return nil
				}
				if terminalErr := projection.Err(); terminalErr != nil {
					subscribeErr = terminalErr
				}
				if subscribeErr != nil {
					delete(pending, externalID)
					cleanup()
					return sendSharedError(stream, externalID, requestID, "SUBSCRIBE_FAILED", subscribeErr.Error())
				}
				state := &sharedProjection{externalID: externalID, generation: generation, projection: projection, cancel: loadCancel, visibility: visibility, selected: selected}
				projections[externalID] = state
				delete(pending, externalID)
				// Keep the load context with the installed projection so closing the
				// client-visible scope also releases its parent context resources.
				initialBatch, modelErr := model.applyProjection(externalID, proj.ProjectionBatch{Events: initial}, selected)
				if modelErr != nil {
					delete(projections, externalID)
					cleanupErr := cleanupFailedScope(externalID)
					cleanup()
					if cleanupErr != nil {
						return cleanupErr
					}
					return sendSharedError(stream, externalID, requestID, "SUBSCRIBE_FAILED", modelErr.Error())
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
				if sendErr := stream.Send(&reactivev1.ServerMessage{SubscriptionId: externalID, RequestId: requestID, Sequence: state.sequence, Event: &reactivev1.ServerMessage_ModelBatch{ModelBatch: initialBatch}}); sendErr != nil {
					return sendErr
				}
				state.sequence++
				if sendErr := stream.Send(&reactivev1.ServerMessage{SubscriptionId: externalID, RequestId: requestID, Sequence: state.sequence, Event: &reactivev1.ServerMessage_Ready{Ready: &reactivev1.Ready{}}}); sendErr != nil {
					return sendErr
				}
				return releaseProjection(state)
			}}
			select {
			case outbound <- completion:
			case <-ctx.Done():
				cleanup()
			}
		}()
		return nil
	case *reactivev1.ClientMessage_UpdateWindow:
		state := subscriptions[message.SubscriptionId]
		if state == nil {
			return sendSharedError(stream, message.SubscriptionId, message.RequestId, "NOT_FOUND", "subscription does not exist")
		}
		changes, err := queryBoundaryChanges(operation.UpdateWindow)
		if err != nil {
			return sendSharedError(stream, state.externalID, message.RequestId, "INVALID_WINDOW", err.Error())
		}
		externalID, internalID, requestID := state.externalID, state.internalID, message.RequestId
		go func() {
			if updateErr := state.subscription.UpdateWindow(ctx, changes, requestID); updateErr != nil {
				select {
				case outbound <- windowFailedMessage{externalID: externalID, generation: internalID, requestID: requestID, cause: updateErr}:
				case <-ctx.Done():
				}
			}
		}()
		return nil
	case *reactivev1.ClientMessage_UpdateProjectionWindow:
		state := projections[message.SubscriptionId]
		if state == nil {
			return sendSharedError(stream, message.SubscriptionId, message.RequestId, "NOT_FOUND", "projection does not exist")
		}
		if operation.UpdateProjectionWindow.InstanceId == "" {
			return sendSharedError(stream, message.SubscriptionId, message.RequestId, "INVALID_WINDOW", "instance_id is required")
		}
		changes, err := queryBoundaryChanges(&reactivev1.UpdateWindow{Updates: operation.UpdateProjectionWindow.Updates})
		if err != nil {
			return sendSharedError(stream, message.SubscriptionId, message.RequestId, "INVALID_WINDOW", err.Error())
		}
		externalID, generation, instanceID, requestID := state.externalID, state.generation, operation.UpdateProjectionWindow.InstanceId, message.RequestId
		projection := state.projection
		go func() {
			if updateErr := projection.QueueWindowUpdate(ctx, instanceID, changes, requestID); updateErr != nil {
				select {
				case outbound <- windowFailedMessage{externalID: externalID, generation: generation, requestID: requestID, cause: updateErr}:
				case <-ctx.Done():
				}
			}
		}()
		return nil
	case *reactivev1.ClientMessage_Unsubscribe:
		state := subscriptions[message.SubscriptionId]
		if state != nil {
			delete(subscriptions, message.SubscriptionId)
			_ = state.subscription.Close(ctx)
			state.publication.close()
			if state.cancel != nil {
				state.cancel()
			}
			_ = prepared.Release(ctx, state.visibility.id, nil)
			batch, err := model.closeScope(message.SubscriptionId)
			if err != nil {
				return err
			}
			state.sequence++
			return stream.Send(&reactivev1.ServerMessage{SubscriptionId: state.externalID, RequestId: message.RequestId, Sequence: state.sequence, Event: &reactivev1.ServerMessage_ModelBatch{ModelBatch: batch}})
		}
		projection := projections[message.SubscriptionId]
		if projection != nil {
			delete(projections, message.SubscriptionId)
			projectionOutputs.Drop(projection.projection)
			projection.close()
			_ = projection.releaseVisibility(ctx, prepared)
			batch, err := model.closeScope(message.SubscriptionId)
			if err != nil {
				return err
			}
			projection.sequence++
			return stream.Send(&reactivev1.ServerMessage{SubscriptionId: projection.externalID, RequestId: message.RequestId, Sequence: projection.sequence, Event: &reactivev1.ServerMessage_ModelBatch{ModelBatch: batch}})
		}
		if loading := pending[message.SubscriptionId]; loading != nil {
			delete(pending, message.SubscriptionId)
			loading.cancel()
			return stream.Send(&reactivev1.ServerMessage{SubscriptionId: message.SubscriptionId, RequestId: message.RequestId, Sequence: 1, Event: &reactivev1.ServerMessage_ModelBatch{ModelBatch: &reactivev1.ModelBatch{}}})
		}
		return sendSharedError(stream, message.SubscriptionId, message.RequestId, "NOT_FOUND", "subscription does not exist")
	default:
		return sendSharedError(stream, message.SubscriptionId, message.RequestId, "INVALID_ARGUMENT", "operation is required")
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
