package projectionserver

import (
	"context"
	"strconv"
	"strings"
	"sync"

	"github.com/adiom-data/weave/internal/accesscontrol"
	proj "github.com/adiom-data/weave/internal/projection"
	q "github.com/adiom-data/weave/internal/querysubscription/model"
	"github.com/adiom-data/weave/internal/storage"
	reactivev1 "github.com/adiom-data/weave/pkg/gen/reactive/v1"
)

// policyAdmissions owns the delayed publication of initial images. All map,
// model, and admission callbacks run on the connection consumer. Workers only
// load subscriptions or wait for activity; they never authorize stale images.
type policyAdmissions struct {
	ctx                context.Context
	service            *server
	stream             Stream
	queries            q.Feed
	prepared           accesscontrol.Prepared
	subscriptions      map[string]*sharedSubscription
	projections        map[string]*sharedProjection
	pending            map[string]*pendingSharedSubscription
	loaders            *sync.WaitGroup
	model              *connectionModel
	outbound           chan<- outboundMessage
	releaseProjection  func(*sharedProjection) error
	cleanupFailedScope func(string) error
}

func (a *policyAdmissions) post(ctx context.Context, run func() error, abandon func()) {
	select {
	case a.outbound <- completionMessage{run: run, abandon: abandon}:
	case <-ctx.Done():
		if abandon != nil {
			abandon()
		}
	}
}

func (a *policyAdmissions) retry() error {
	for id, loading := range a.pending {
		if loading.admit == nil || !loading.loaded || !loading.retry {
			continue
		}
		loading.retry = false
		wait, err := loading.admit()
		if err != nil {
			if a.pending[id] != loading {
				return err
			}
			delete(a.pending, id)
			loading.cancel()
			if a.cleanupFailedScope != nil {
				if err := a.cleanupFailedScope(id); err != nil {
					return err
				}
			}
			if err := sendSharedError(a.stream, id, loading.requestID, "SUBSCRIBE_FAILED", err.Error()); err != nil {
				return err
			}
			continue
		}
		if wait.Accepted {
			continue
		}
		a.service.relationAccess.requestProgress(wait.Progress...)
		if wait.Wait != nil && loading.wait != wait.Wait {
			loading.wait = wait.Wait
			signal := wait.Wait
			a.loaders.Add(1)
			go func() {
				defer a.loaders.Done()
				select {
				case <-signal:
					a.post(loading.context, func() error {
						if a.pending[id] == loading {
							loading.retry = true
							loading.wait = nil
						}
						return nil
					}, nil)
				case <-loading.context.Done():
				}
			}()
		}
	}
	return nil
}

func (a *policyAdmissions) query(external, request, internal, queryID string, spec storage.QuerySpec, selected []storage.FieldPath, window q.InitialWindow) error {
	ctx, cancel := context.WithCancel(a.ctx)
	var visibilityID string
	loading := &pendingSharedSubscription{context: ctx, cancel: cancel, owns: func(id string) bool { return id == internal }, policyOwns: func(id string) bool { return id == visibilityID }, requestID: request}
	a.pending[external] = loading
	a.loaders.Add(1)
	go func() {
		defer a.loaders.Done()
		err := a.queries.SubscribeQuery(ctx, q.QuerySubscribeRequest{ID: internal, Query: spec, Window: window}, func(handle q.QuerySubscription, snapshot q.QuerySnapshot) error {
			target, err := q.SnapshotPosition(snapshot.Checkpoint, snapshot.Fence)
			if err != nil {
				return err
			}
			image, err := newInitialQueryImage(internal, snapshot)
			if err != nil {
				return err
			}
			a.post(ctx, func() error {
				if a.pending[external] != loading {
					_ = handle.Close(context.WithoutCancel(ctx))
					return nil
				}
				loading.direct = image
				position, revision := a.service.relationAccess.progress.position(target.SourceID)
				image.observe(position, revision)
				visibilityID = image.visibility.id
				loading.cancel = func() {
					cancel()
					_ = handle.Close(context.WithoutCancel(ctx))
					_ = a.prepared.Release(context.WithoutCancel(ctx), visibilityID, nil)
				}
				loading.admit = func() (proj.AdmissionWait, error) {
					if err := ctx.Err(); err != nil {
						return proj.AdmissionWait{}, err
					}
					covered, err := image.covered()
					if err != nil {
						return proj.AdmissionWait{}, err
					}
					if !covered {
						target, err := q.SnapshotPosition(image.snapshot.Checkpoint, image.snapshot.Fence)
						if image.snapshot.Checkpoint != nil {
							target = image.snapshot.Checkpoint
						}
						return proj.AdmissionWait{Progress: []*q.Position{target}}, err
					}
					resolved, err := image.authorize(ctx, a.service.relationAccess.session, a.prepared)
					if err != nil {
						return proj.AdmissionWait{}, err
					}
					if !resolved {
						return proj.AdmissionWait{}, nil
					}
					state := &sharedSubscription{internalID: internal, externalID: external, queryID: queryID, subscription: handle, visibility: image.visibility, cancel: cancel}
					batch, err := a.model.openSubscriptionVersions(external, queryID, "", image.visibleSnapshot(), selected, image.versions)
					if err != nil {
						return proj.AdmissionWait{}, err
					}
					delete(a.pending, external)
					a.subscriptions[external] = state
					batch.Kind = reactivev1.ModelBatch_KIND_SNAPSHOT
					state.sequence++
					if err := a.stream.Send(&reactivev1.ServerMessage{SubscriptionId: external, RequestId: request, Sequence: state.sequence, Event: &reactivev1.ServerMessage_ModelBatch{ModelBatch: batch}}); err != nil {
						return proj.AdmissionWait{}, err
					}
					state.sequence++
					return proj.AdmissionWait{Accepted: true}, a.stream.Send(&reactivev1.ServerMessage{SubscriptionId: external, RequestId: request, Sequence: state.sequence, Event: &reactivev1.ServerMessage_Ready{Ready: &reactivev1.Ready{}}})
				}
				return nil
			}, func() { _ = handle.Close(context.WithoutCancel(ctx)); cancel() })
			return ctx.Err()
		})
		if err == nil {
			// The installer may run before the feed's progress guarantee applies.
			// Request coverage only after SubscribeQuery itself has completed.
			a.post(ctx, func() error {
				if a.pending[external] == loading {
					loading.loaded = true
					loading.retry = true
				}
				return nil
			}, nil)
		}
		if err != nil {
			a.post(a.ctx, func() error {
				if a.pending[external] != loading {
					return nil
				}
				delete(a.pending, external)
				loading.cancel()
				return sendSharedError(a.stream, external, request, "SUBSCRIBE_FAILED", err.Error())
			}, nil)
		}
	}()
	return nil
}

func (a *policyAdmissions) projection(external, request, prefix string, root *proj.ProjectionNode, selected map[string]*fieldSelection, waitForSync, partial bool) error {
	ctx, cancel := context.WithCancel(a.ctx)
	generation := prefix + "projection:" + strconv.FormatUint(streamSequence.Add(1), 10)
	owns := func(id string) bool { return strings.HasPrefix(id, generation+":") }
	loading := &pendingSharedSubscription{context: ctx, cancel: cancel, generation: generation, owns: owns, policyOwns: owns, requestID: request}
	a.pending[external] = loading
	position, revision := a.service.relationAccess.progress.current()
	p, err := proj.BeginProjection(ctx, a.queries, root, proj.StartOptions{Prefix: generation, QueueLimit: a.service.options.SubscriptionQueueLimit, Limits: proj.Limits(a.service.options.ProjectionLimits), ExpansionLimiter: a.service.expansionLimiter, WaitForSync: waitForSync, AllowPartialErrors: partial, CurrentImageAdmission: true, ConsumedCheckpoint: position, ConsumedRevision: revision})
	if err != nil {
		delete(a.pending, external)
		cancel()
		return sendSharedError(a.stream, external, request, "SUBSCRIBE_FAILED", err.Error())
	}
	loading.projection = p
	go forwardProjection(ctx, external, generation, p, a.outbound)
	image := newInitialPolicyImage(selected)
	image.visibility.partial = partial
	loading.cancel = func() {
		cancel()
		p.Close()
		_ = image.release(context.WithoutCancel(ctx), a.prepared)
	}
	a.loaders.Add(1)
	go func() {
		defer a.loaders.Done()
		err := p.WaitReady(ctx)
		a.post(a.ctx, func() error {
			if a.pending[external] != loading {
				return nil
			}
			if err != nil {
				delete(a.pending, external)
				loading.cancel()
				return sendSharedError(a.stream, external, request, "SUBSCRIBE_FAILED", err.Error())
			}
			loading.loaded = true
			loading.admit = func() (proj.AdmissionWait, error) {
				if err := ctx.Err(); err != nil {
					return proj.AdmissionWait{}, err
				}
				wait, err := p.TryAdmitInitial(func(batch proj.ProjectionBatch) (bool, error) {
					return image.apply(ctx, a.service, a.prepared, batch)
				})
				if err != nil || !wait.Accepted {
					return wait, err
				}
				filtered := image.borrowEvents()
				state := &sharedProjection{externalID: external, generation: generation, projection: p, cancel: cancel, visibility: image.visibility, selected: selected}
				batch, err := a.model.applyProjection(external, proj.ProjectionBatch{Events: filtered}, selected)
				if err != nil {
					return proj.AdmissionWait{}, err
				}
				delete(a.pending, external)
				a.projections[external] = state
				batch.Kind = reactivev1.ModelBatch_KIND_SNAPSHOT
				if waitForSync && len(filtered) > 0 {
					batch.Checkpoint = checkpointString(filtered[0].Checkpoint)
				}
				state.sequence++
				if err = a.stream.Send(&reactivev1.ServerMessage{SubscriptionId: external, RequestId: request, Sequence: state.sequence, Event: &reactivev1.ServerMessage_ModelBatch{ModelBatch: batch}}); err != nil {
					return wait, err
				}
				state.sequence++
				if err = a.stream.Send(&reactivev1.ServerMessage{SubscriptionId: external, RequestId: request, Sequence: state.sequence, Event: &reactivev1.ServerMessage_Ready{Ready: &reactivev1.Ready{}}}); err != nil {
					return wait, err
				}
				return wait, a.releaseProjection(state)
			}
			loading.retry = true
			return nil
		}, nil)
	}()
	return nil
}

func retainInitialOccurrences(ctx context.Context, prepared accesscontrol.Prepared, previous map[string]map[storage.EntityKey]struct{}, events []proj.ProjectionEvent) (map[string]map[storage.EntityKey]struct{}, error) {
	current := make(map[string]map[storage.EntityKey]struct{})
	for _, event := range events {
		if event.Type == proj.ProjectionInstanceOpened {
			current[event.InstanceID] = make(map[storage.EntityKey]struct{})
		}
		if event.Type == proj.ProjectionRowChanged {
			if current[event.InstanceID] == nil {
				current[event.InstanceID] = make(map[storage.EntityKey]struct{})
			}
			current[event.InstanceID][event.Key] = struct{}{}
		}
	}
	for id, keys := range previous {
		if _, exists := current[id]; !exists {
			if err := prepared.Release(ctx, id, nil); err != nil {
				return previous, err
			}
			continue
		}
		for key := range keys {
			if _, exists := current[id][key]; !exists {
				if err := prepared.Release(ctx, id, []storage.EntityKey{key}); err != nil {
					return previous, err
				}
			}
		}
	}
	return current, nil
}
