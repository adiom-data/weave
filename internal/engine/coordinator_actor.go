package engine

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/adiom-data/weave/internal/datastore"
	"github.com/adiom-data/weave/internal/engine/delivery"
	"github.com/adiom-data/weave/internal/engine/queryregistry"
	"github.com/adiom-data/weave/internal/engine/subscriptions"
	"github.com/adiom-data/weave/internal/storage"
)

func (c *Coordinator) run(ctx context.Context, cancel context.CancelFunc) {
	defer close(c.done)
	defer func() {
		cause := c.terminalError()
		if cause == nil {
			cause = context.Canceled
		}
		c.cancelPendingActivations(cause)
		c.cancelReferenceReads(cause)
	}()
	defer c.feed.Close()
	defer c.recovery.CancelAll()
	defer cancel()
	received, readerDone := c.readFeed(ctx)
	defer func() { cancel(); <-readerDone }()
	var eviction <-chan time.Time
	evictionNext := make(chan struct{}, 1)
	var evictionBefore time.Time
	evictBatch := func() {
		if _, more := c.entities.EvictIdle(evictionBefore, c.options.EvictionBatchSize); more {
			select {
			case evictionNext <- struct{}{}:
			default:
			}
		}
	}
	var ticker *time.Ticker
	if c.options.EvictionPeriod > 0 && c.options.EntityIdleTTL > 0 {
		ticker = time.NewTicker(c.options.EvictionPeriod)
		eviction = ticker.C
		defer ticker.Stop()
	}
	var progressTimer *time.Timer
	queryMaintenance := time.NewTicker(c.options.QueryMaintenancePeriod)
	defer queryMaintenance.Stop()
	queryNext := make(chan struct{}, 1)
	maintainQueries := func() {
		failures, more := c.queries.Maintain(time.Now(), c.options.EvictionBatchSize)
		if c.prefetch != nil {
			pending, err := c.prefetch.Maintain(time.Now(), c.options.EvictionBatchSize)
			if err != nil {
				c.stop(err)
				return
			}
			more = more || pending
		}
		for _, failure := range failures {
			c.failQuery(failure.Fingerprint, failure.Cause)
		}
		if more {
			select {
			case queryNext <- struct{}{}:
			default:
			}
		}
	}
	var progressTimerC <-chan time.Time
	stopProgressTimer := func() {
		if progressTimer == nil {
			return
		}
		if !progressTimer.Stop() {
			select {
			case <-progressTimer.C:
			default:
			}
		}
		progressTimer = nil
		progressTimerC = nil
	}
	defer stopProgressTimer()
	for {
		c.tryCompleteReferenceReads()
		c.tryCompleteActivations(ctx)
		pendingFence := c.pendingProgressFence()
		if c.progressFence != nil && (c.appliedThrough(c.progressFence) || pendingFence == nil) {
			c.progressFence = nil
		}
		if c.progressFence == nil && !c.progressStarting && pendingFence != nil {
			if _, ok := c.source.(datastore.ProgressRequester); ok {
				if progressTimer == nil {
					progressTimer = time.NewTimer(c.options.ProgressBarrierDelay)
					progressTimerC = progressTimer.C
				}
			}
		} else {
			stopProgressTimer()
		}
		var afterReceive func()
		select {
		case <-ctx.Done():
			c.stop(ctx.Err())
			return
		case command := <-c.commands:
			afterReceive = command.run
		case now := <-eviction:
			evictionBefore = now.Add(-c.options.EntityIdleTTL)
			afterReceive = evictBatch
		case <-evictionNext:
			afterReceive = evictBatch
		case <-queryMaintenance.C:
			afterReceive = maintainQueries
		case <-queryNext:
			afterReceive = maintainQueries
		case result := <-c.resyncResults:
			afterReceive = func() { c.acceptResyncResult(ctx, result) }
		case result := <-c.hydrationResults:
			afterReceive = func() {
				c.acceptHydrationResult(result)
				c.tryCompleteHydrations(ctx)
			}
		case <-progressTimerC:
			progressTimer = nil
			progressTimerC = nil
			afterReceive = func() { c.startProgressRequest(ctx) }
		case result := <-c.progressResults:
			afterReceive = func() {
				c.completeProgressRequest(result)
			}
		case result := <-received:
			if err := c.applyFeedResult(ctx, result); err != nil {
				c.stop(err)
				return
			}
			stopProgressTimer()
			continue
		}
		// Preserve source-before-auxiliary ordering without canceling the reader.
		applied, err := c.applyReadyFeed(ctx, received)
		if err != nil {
			c.stop(err)
			return
		}
		if applied {
			stopProgressTimer()
		}
		afterReceive()
		if c.hasTerminalError() {
			return
		}
	}
}

func (c *Coordinator) beginSubscribeActor(operation initialWindowOperation) error {
	id := operation.subscriptionID
	plan := operation.plan
	if c.subscriptions.Exists(id) {
		return delivery.ErrSubscriptionExists
	}
	if _, recovering := c.recovery.Relation(plan.Relation()); recovering {
		return storage.ErrResumeRequired
	}
	query, created := c.queries.Acquire(plan)
	if !created && query.Plan.Relation() != plan.Relation() {
		if _, err := c.queries.Release(plan.Fingerprint()); err != nil {
			return fmt.Errorf("release colliding query: %w", err)
		}
		return errors.New("query fingerprint collides across relation incarnations")
	}
	c.subscriptions.BeginInitialization(id, plan)
	return nil
}

func (c *Coordinator) installInitializingWindow(ctx context.Context, operation initialWindowOperation, resolved resolvedWindow) (*materializationRequest, error) {
	if err := resolved.validate(); err != nil {
		return nil, err
	}
	type response struct {
		request *materializationRequest
		err     error
	}
	result := make(chan response, 1)
	command := coordinatorCommand{run: func() {
		request, err := c.installInitializingWindowActor(operation, resolved)
		result <- response{request: request, err: err}
	}}
	select {
	case c.commands <- command:
	case <-c.done:
		return nil, c.terminalError()
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	select {
	case response := <-result:
		return response.request, response.err
	case <-c.done:
		return nil, c.terminalError()
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (c *Coordinator) installInitializingWindowActor(operation initialWindowOperation, resolved resolvedWindow) (*materializationRequest, error) {
	if err := resolved.validate(); err != nil {
		return nil, err
	}
	id := operation.subscriptionID
	fingerprint := operation.plan.Fingerprint()
	bounds := resolved.bounds
	if !c.subscriptions.InitializationInPhase(id, operation.plan, subscriptions.OperationPlanning) {
		return nil, errors.New("subscription initialization was cancelled")
	}
	plan, ok := c.queries.Plan(fingerprint)
	if !ok {
		return nil, storage.ErrResumeRequired
	}
	if _, recovering := c.recovery.Relation(plan.Relation()); recovering || !c.subscriptions.InitializationCurrent(id, plan) {
		return nil, storage.ErrResumeRequired
	}
	gaps, err := c.queries.ReadyGaps(fingerprint, bounds)
	if err != nil {
		return nil, err
	}
	if c.shouldForceFullWindowRead(fingerprint, gaps, resolved.source) {
		// Coverage without a consumed checkpoint, unresolved hydration, or an
		// unfenced discovery still needs an authoritative materialization read.
		gaps = []datastore.Bounds{bounds}
	}
	covered, err := c.queries.InstallWindow(fingerprint, id, bounds)
	if err != nil {
		return nil, err
	}
	if len(gaps) == 0 && !covered {
		return nil, errors.New("initialization has uncovered bounds without a materialization read")
	}
	if !c.subscriptions.StageInitialization(id, operation.plan, bounds, gaps) {
		return nil, errors.New("subscription initialization changed phase during staging")
	}
	return newMaterializationRequest(operation.plan, gaps), nil
}

// shouldForceFullWindowRead reports when ready cache coverage cannot be used
// without an authoritative read. Non-empty gaps already cause a fenced read;
// discovery therefore forces the full range only when it would otherwise have
// no fence of its own.
func (c *Coordinator) shouldForceFullWindowRead(fingerprint datastore.QueryFingerprint, gaps []datastore.Bounds, source windowResolutionSource) bool {
	return c.lastCheckpoint == nil || c.recovery.QueryHasPendingHydration(fingerprint) || (source == windowResolvedFromDiscovery && len(gaps) == 0)
}

func (c *Coordinator) planCachedInitialWindowActor(operation initialWindowOperation, count int, fromEnd bool) (queryregistry.InitialWindowPlan, error) {
	id := operation.subscriptionID
	fingerprint := operation.plan.Fingerprint()
	if !c.subscriptions.InitializationInPhase(id, operation.plan, subscriptions.OperationPlanning) {
		return queryregistry.InitialWindowPlan{}, errors.New("subscription initialization was cancelled")
	}
	plan, ok := c.queries.Plan(fingerprint)
	if !ok {
		return queryregistry.InitialWindowPlan{}, storage.ErrResumeRequired
	}
	if _, recovering := c.recovery.Relation(plan.Relation()); recovering || !c.subscriptions.InitializationCurrent(id, plan) {
		return queryregistry.InitialWindowPlan{}, storage.ErrResumeRequired
	}
	if c.lastCheckpoint == nil || c.recovery.QueryHasPendingHydration(fingerprint) {
		return queryregistry.InitialWindowPlan{}, nil
	}
	if identity, ok := plan.(datastore.IdentityQuery); ok && c.prefetch != nil && c.prefetch.Absent(identity.LookupKey()) {
		full := datastore.Bounds{Left: datastore.Boundary{Kind: datastore.BoundaryStart}, Right: datastore.Boundary{Kind: datastore.BoundaryEnd}}
		if err := c.queries.SetWindow(fingerprint, id, full); err != nil {
			return queryregistry.InitialWindowPlan{}, err
		}
	}
	cached, err := c.queries.PlanInitialWindow(fingerprint, count, fromEnd)
	if err == nil && cached.Available {
		// Pin partial coverage while discovery or materialization runs outside
		// the actor. Resolved coverage is staged in this same actor turn.
		var covered bool
		covered, err = c.queries.InstallWindow(fingerprint, id, cached.Bounds)
		if err == nil && !covered {
			err = errors.New("planned initial cache coverage was no longer ready")
		}
	}
	return cached, err
}

func (c *Coordinator) activateInitializingWindow(ctx context.Context, operation initialWindowOperation, materialized *materializedWindow) error {
	result := make(chan initialActivationResult, 1)
	command := coordinatorCommand{run: func() {
		id := operation.subscriptionID
		plan := operation.plan
		initializing, exists := c.subscriptions.Initialization(id)
		if !exists || !c.subscriptions.InitializationInPhase(id, plan, subscriptions.OperationMaterializing) {
			materialized.cancel(context.Canceled)
			result <- initialActivationResult{err: errors.New("subscription initialization was cancelled")}
			return
		}
		if err := materialized.validate(plan, initializing.Reads); err != nil {
			materialized.cancel(err)
			result <- initialActivationResult{err: err}
			return
		}
		pending := materialized.pending
		if _, recovering := c.recovery.Relation(plan.Relation()); recovering {
			pending.Cancel(storage.ErrResumeRequired)
			result <- initialActivationResult{err: storage.ErrResumeRequired}
			return
		}
		if c.pendingInitial[id] != nil {
			pending.Cancel(errors.New("subscription activation is already pending"))
			result <- initialActivationResult{err: errors.New("subscription activation is already pending")}
			return
		}
		c.pendingInitial[id] = &pendingInitialActivation{operation: operation, materialized: materialized, result: result}
	}}
	select {
	case c.commands <- command:
	case <-c.done:
		materialized.cancel(c.terminalError())
		return c.terminalError()
	case <-ctx.Done():
		materialized.cancel(ctx.Err())
		return ctx.Err()
	}
	select {
	case response := <-result:
		return response.err
	case <-c.done:
		return c.terminalError()
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *Coordinator) finishSubscribeActor(operation initialWindowOperation) (_ *OpenSubscription, err error) {
	id := operation.subscriptionID
	group := operation.group
	plan := operation.plan
	if !c.subscriptions.InitializationInPhase(id, plan, subscriptions.OperationReady) {
		return nil, errors.New("subscription initialization was cancelled")
	}
	if _, recovering := c.recovery.Relation(plan.Relation()); recovering {
		return nil, storage.ErrResumeRequired
	}
	initializing, _ := c.subscriptions.Initialization(id)
	bounds := initializing.Bounds
	cleanup := true
	defer func() {
		if cleanup {
			c.subscriptions.CancelInitialization(id, plan.Fingerprint())
			c.releaseQuery(id, plan.Fingerprint())
		}
	}()
	if err := c.pruneCoverage(plan.Fingerprint()); err != nil {
		return nil, err
	}
	_, identity := plan.(datastore.IdentityQuery)
	current, err := c.queryEntitiesInBounds(plan.Fingerprint(), []datastore.Bounds{bounds}, !identity)
	if err != nil {
		return nil, err
	}
	filtered, err := delivery.FilterOwnedInitial(plan, bounds, current)
	if err != nil {
		return nil, err
	}
	// A single-range registry read is already in query order; filtering keeps it.
	if _, recovering := c.recovery.Relation(plan.Relation()); recovering || !c.subscriptions.InitializationInPhase(id, plan, subscriptions.OperationReady) {
		return nil, storage.ErrResumeRequired
	}
	public, err := c.hub.SubscribePreparedGrouped(id, group, plan, bounds)
	if err != nil {
		return nil, err
	}
	c.subscriptions.Activate(id, activeSubscription{Fingerprint: plan.Fingerprint(), Plan: plan, Bounds: bounds, RelationEpoch: c.subscriptions.Epoch(plan.Relation()), Reference: operation.reference})
	cleanup = false
	return &OpenSubscription{Subscription: public, Plan: plan, ReferenceAccepted: operation.reference != nil, Bounds: bounds, Initial: filtered, Checkpoint: c.lastCheckpoint, Fence: initializing.Fence, Revision: c.deliveryRevision}, nil
}

func (c *Coordinator) cancelInitializing(operation initialWindowOperation) {
	done := make(chan struct{}, 1)
	command := coordinatorCommand{run: func() {
		id := operation.subscriptionID
		fingerprint := operation.plan.Fingerprint()
		c.cancelInitialActivation(id, context.Canceled)
		if c.subscriptions.CancelInitialization(id, fingerprint) {
			c.releaseQuery(id, fingerprint)
		}
		done <- struct{}{}
	}}
	select {
	case c.commands <- command:
		select {
		case <-done:
		case <-c.done:
		}
	case <-c.done:
	}
}

func (c *Coordinator) cancelSubscription(operation initialWindowOperation) {
	done := make(chan struct{}, 1)
	command := coordinatorCommand{run: func() {
		id := operation.subscriptionID
		fingerprint := operation.plan.Fingerprint()
		c.cancelInitialActivation(id, context.Canceled)
		if c.subscriptions.CancelInitialization(id, fingerprint) {
			c.releaseQuery(id, fingerprint)
		} else if active, ok := c.subscriptions.Active(id); ok && active.Fingerprint == fingerprint {
			_ = c.unsubscribeActor(id)
		}
		done <- struct{}{}
	}}
	select {
	case c.commands <- command:
		select {
		case <-done:
		case <-c.done:
		}
	case <-c.done:
	}
}
