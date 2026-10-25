package engine

import (
	"context"
	"errors"
	"fmt"

	"github.com/adiom-data/weave/internal/datastore"
	"github.com/adiom-data/weave/internal/engine/delivery"
	"github.com/adiom-data/weave/internal/engine/entitycache"
	"github.com/adiom-data/weave/internal/engine/queryregistry"
	"github.com/adiom-data/weave/internal/engine/querywindow"
	"github.com/adiom-data/weave/internal/engine/subscriptions"
	"github.com/adiom-data/weave/internal/engine/windowloader"
	"github.com/adiom-data/weave/internal/storage"
)

type cachedBoundaryResolution struct {
	keys          []datastore.SortKey
	extension     queryregistry.ExtensionPlan
	authoritative bool
}

// windowUpdateOperation is the immutable identity and base state of one
// asynchronous window transition. Passing it between phases prevents token,
// stage, subscription, and epoch checks from being reconstructed separately.
type windowUpdateOperation struct {
	subscriptionID string
	token          string
	stageID        string
	base           activeSubscription
}

func windowStageID(subscriptionID, token string) string {
	return "\x00window-stage:" + subscriptionID + ":" + token
}

// planCachedBoundaries resolves every cache-backed part of one window request
// in a single actor turn. No CDC batch can split the left and right plans. When
// a partial tuple makes membership or ordering undecidable, the corresponding
// count operation is marked authoritative and must use datastore discovery.
func (c *Coordinator) planCachedBoundaries(ctx context.Context, operation windowUpdateOperation, changes []BoundaryChange) ([]cachedBoundaryResolution, error) {
	type response struct {
		plans []cachedBoundaryResolution
		err   error
	}
	result := make(chan response, 1)
	command := coordinatorCommand{run: func() {
		active, ok := c.subscriptions.Active(operation.subscriptionID)
		if !ok || !c.subscriptions.WindowInPhase(operation.subscriptionID, operation.token, subscriptions.OperationPlanning) {
			result <- response{err: errors.New("window update was cancelled")}
			return
		}
		if active.Fingerprint != operation.base.Fingerprint {
			result <- response{err: storage.ErrResumeRequired}
			return
		}
		if _, recovering := c.recovery.Relation(active.Plan.Relation()); recovering || active.RelationEpoch != c.subscriptions.Epoch(active.Plan.Relation()) {
			result <- response{err: storage.ErrResumeRequired}
			return
		}
		plans := make([]cachedBoundaryResolution, len(changes))
		pendingHydration := c.recovery.QueryHasPendingHydration(active.Fingerprint)
		for index, change := range changes {
			if change.Operation != BoundaryContract && change.Operation != BoundaryExtend {
				continue
			}
			if pendingHydration {
				plans[index].authoritative = true
				continue
			}
			var err error
			switch change.Operation {
			case BoundaryContract:
				plans[index].keys, err = c.queries.SortKeysInBounds(active.Fingerprint, operation.base.Bounds, change.Count+1, change.Side == BoundaryRight)
			case BoundaryExtend:
				boundary := operation.base.Bounds.Right
				if change.Side == BoundaryLeft {
					boundary = operation.base.Bounds.Left
				}
				plans[index].extension, err = c.queries.PlanExtension(active.Fingerprint, boundary, change.Count, change.Side == BoundaryLeft)
			}
			if err != nil {
				result <- response{err: err}
				return
			}
		}
		result <- response{plans: plans}
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
		return response.plans, response.err
	case <-c.done:
		return nil, c.terminalError()
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (c *Coordinator) beginWindowUpdate(ctx context.Context, id string, changes []BoundaryChange) (windowUpdateOperation, error) {
	type response struct {
		operation windowUpdateOperation
		err       error
	}
	result := make(chan response, 1)
	command := coordinatorCommand{run: func() {
		active, ok := c.subscriptions.Active(id)
		if !ok {
			result <- response{err: errors.New("subscription not found")}
			return
		}
		if len(changes) == 0 {
			result <- response{err: errors.New("at least one boundary change is required")}
			return
		}
		if c.subscriptions.WindowToken(id) != "" {
			result <- response{err: errors.New("subscription already has a window update in progress")}
			return
		}
		if _, recovering := c.recovery.Relation(active.Plan.Relation()); recovering {
			result <- response{err: storage.ErrResumeRequired}
			return
		}
		c.nextOperation++
		token := fmt.Sprintf("%d", c.nextOperation)
		stageID := windowStageID(id, token)
		active, ok = c.subscriptions.BeginWindow(id, token)
		if !ok {
			result <- response{err: errors.New("subscription already has a window update in progress")}
			return
		}
		result <- response{operation: windowUpdateOperation{subscriptionID: id, token: token, stageID: stageID, base: active}}
	}}
	select {
	case c.commands <- command:
	case <-c.done:
		return windowUpdateOperation{}, c.terminalError()
	case <-ctx.Done():
		return windowUpdateOperation{}, ctx.Err()
	}
	select {
	case response := <-result:
		return response.operation, response.err
	case <-c.done:
		return windowUpdateOperation{}, c.terminalError()
	case <-ctx.Done():
		go func() {
			select {
			case response := <-result:
				if response.operation.token != "" {
					c.abortWindowUpdate(response.operation)
				}
			case <-c.done:
			}
		}()
		return windowUpdateOperation{}, ctx.Err()
	}
}

func (c *Coordinator) stageWindowUpdate(ctx context.Context, operation windowUpdateOperation, resolved resolvedWindow) (*materializationRequest, error) {
	if err := resolved.validate(); err != nil {
		return nil, err
	}
	type response struct {
		gaps []datastore.Bounds
		err  error
	}
	result := make(chan response, 1)
	command := coordinatorCommand{run: func() {
		bounds := resolved.bounds
		if !c.subscriptions.WindowInPhase(operation.subscriptionID, operation.token, subscriptions.OperationPlanning) {
			result <- response{err: errors.New("window update was cancelled")}
			return
		}
		active, ok := c.subscriptions.Active(operation.subscriptionID)
		if !ok || active.Fingerprint != operation.base.Fingerprint {
			result <- response{err: errors.New("subscription not found")}
			return
		}
		_, recovering := c.recovery.Relation(active.Plan.Relation())
		if recovering || active.RelationEpoch != c.subscriptions.Epoch(active.Plan.Relation()) {
			result <- response{err: storage.ErrResumeRequired}
			return
		}
		gaps, err := c.queries.ReadyGaps(operation.base.Fingerprint, bounds)
		// A covered transition still needs a source position at which access
		// control can evaluate it. Until the feed supplies a checkpoint, force a
		// fenced read rather than emitting a transition with no provenance.
		if err == nil && c.shouldForceFullWindowRead(operation.base.Fingerprint, gaps, resolved.source) {
			gaps = []datastore.Bounds{bounds}
		}
		var covered bool
		if err == nil {
			covered, err = c.queries.InstallWindow(operation.base.Fingerprint, operation.stageID, bounds)
		}
		if err == nil && len(gaps) == 0 && !covered {
			err = errors.New("window update has uncovered bounds without a materialization read")
		}
		if err == nil && !c.subscriptions.StageWindow(operation.subscriptionID, operation.token, bounds, gaps) {
			err = errors.New("window update changed phase during staging")
		}
		result <- response{gaps: gaps, err: err}
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
		if response.err != nil {
			return nil, response.err
		}
		return newMaterializationRequest(operation.base.Plan, response.gaps), nil
	case <-c.done:
		return nil, c.terminalError()
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (c *Coordinator) activateWindowRead(ctx context.Context, operation windowUpdateOperation, materialized *materializedWindow) ([]entitycache.EntitySnapshot, error) {
	result := make(chan windowActivationResult, 1)
	command := coordinatorCommand{run: func() {
		staged, exists := c.subscriptions.WindowOperation(operation.subscriptionID, operation.token)
		if !exists || staged.Phase != subscriptions.OperationMaterializing {
			materialized.cancel(context.Canceled)
			result <- windowActivationResult{err: errors.New("window update was cancelled")}
			return
		}
		if err := materialized.validate(operation.base.Plan, staged.Reads); err != nil {
			materialized.cancel(err)
			result <- windowActivationResult{err: err}
			return
		}
		pending := materialized.pending
		active, ok := c.subscriptions.Active(operation.subscriptionID)
		relation := operation.base.Plan.Relation()
		_, recovering := c.recovery.Relation(relation)
		if !ok || active.Fingerprint != operation.base.Fingerprint || active.Plan.Relation() != relation || recovering || active.RelationEpoch != c.subscriptions.Epoch(relation) {
			pending.Cancel(storage.ErrResumeRequired)
			result <- windowActivationResult{err: storage.ErrResumeRequired}
			return
		}
		if c.pendingWindows[operation.subscriptionID] != nil {
			err := errors.New("window activation is already pending")
			pending.Cancel(err)
			result <- windowActivationResult{err: err}
			return
		}
		c.pendingWindows[operation.subscriptionID] = &pendingWindowActivation{operation: operation, materialized: materialized, result: result}
	}}
	select {
	case c.commands <- command:
	case <-c.done:
		materialized.cancel(c.terminalError())
		return nil, c.terminalError()
	case <-ctx.Done():
		materialized.cancel(ctx.Err())
		return nil, ctx.Err()
	}
	select {
	case response := <-result:
		return response.rows, response.err
	case <-c.done:
		return nil, c.terminalError()
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (c *Coordinator) finishWindowUpdate(ctx context.Context, operation windowUpdateOperation, deliveryToken string) (datastore.Bounds, error) {
	type response struct {
		bounds datastore.Bounds
		err    error
	}
	result := make(chan response, 1)
	command := coordinatorCommand{run: func() {
		active := operation.base
		staged, stagedOK := c.subscriptions.WindowOperation(operation.subscriptionID, operation.token)
		if !stagedOK || staged.Phase != subscriptions.OperationReady {
			result <- response{err: errors.New("window update was cancelled")}
			return
		}
		bounds := staged.Bounds
		current, ok := c.subscriptions.Active(operation.subscriptionID)
		if !ok || current.Fingerprint != active.Fingerprint {
			result <- response{err: errors.New("subscription not found")}
			return
		}
		if _, recovering := c.recovery.Relation(current.Plan.Relation()); recovering || active.RelationEpoch != c.subscriptions.Epoch(current.Plan.Relation()) {
			result <- response{err: storage.ErrResumeRequired}
			return
		}
		removedBounds, err := querywindow.Difference(active.Plan, active.Bounds, bounds)
		if err != nil {
			result <- response{err: err}
			return
		}
		addedBounds, err := querywindow.Difference(active.Plan, bounds, active.Bounds)
		if err != nil {
			result <- response{err: err}
			return
		}
		// Both the old window and staged replacement are still covered here, so
		// bounded index walks can capture only their symmetric difference before
		// the abandoned membership is pruned.
		removedEntities, err := c.queryEntitiesInBounds(active.Fingerprint, removedBounds, false)
		if err != nil {
			result <- response{err: err}
			return
		}
		addedEntities, err := c.queryEntitiesInBounds(active.Fingerprint, addedBounds, false)
		if err != nil {
			result <- response{err: err}
			return
		}
		transition := delivery.WindowMembershipDelta{Added: addedEntities, Removed: make([]storage.EntityKey, 0, len(removedEntities))}
		for _, entity := range removedEntities {
			transition.Removed = append(transition.Removed, entity.Key)
		}
		if err := c.queries.SetWindow(active.Fingerprint, operation.subscriptionID, bounds); err != nil {
			result <- response{err: err}
			return
		}
		if _, err := c.queries.RemoveWindow(active.Fingerprint, operation.stageID); err != nil {
			c.failQuery(active.Fingerprint, err)
			result <- response{err: err}
			return
		}
		if err := c.pruneCoverage(active.Fingerprint); err != nil {
			c.failQuery(active.Fingerprint, err)
			result <- response{err: err}
			return
		}
		c.deliveryRevision++
		_, err = c.hub.QueueWindowTransition(operation.subscriptionID, bounds, transition, deliveryToken, c.deliveryRevision, staged.Fence, c.lastCheckpoint)
		if err != nil {
			if errors.Is(err, delivery.ErrClientQueueFull) {
				c.dropActive(operation.subscriptionID)
			} else {
				c.failQuery(active.Fingerprint, err)
			}
			result <- response{err: err}
			return
		}
		current.Bounds = bounds
		if !c.subscriptions.FinishWindow(operation.subscriptionID, operation.token, current) {
			c.failQuery(active.Fingerprint, errors.New("window lifecycle changed during commit"))
			result <- response{err: errors.New("window update was cancelled")}
			return
		}
		result <- response{bounds: bounds}
	}}
	select {
	case c.commands <- command:
	case <-c.done:
		return datastore.Bounds{}, c.terminalError()
	case <-ctx.Done():
		return datastore.Bounds{}, ctx.Err()
	}
	select {
	case response := <-result:
		return response.bounds, response.err
	case <-c.done:
		return datastore.Bounds{}, c.terminalError()
	case <-ctx.Done():
		return datastore.Bounds{}, ctx.Err()
	}
}

func (c *Coordinator) abortWindowUpdate(operation windowUpdateOperation) {
	done := make(chan struct{}, 1)
	command := coordinatorCommand{run: func() {
		c.cancelWindowActivation(operation.subscriptionID, context.Canceled)
		if c.subscriptions.AbortWindow(operation.subscriptionID, operation.token) {
			if _, err := c.queries.RemoveWindow(operation.base.Fingerprint, operation.stageID); err != nil {
				c.failQuery(operation.base.Fingerprint, fmt.Errorf("abort staged window: %w", err))
			} else if err := c.pruneCoverage(operation.base.Fingerprint); err != nil {
				c.failQuery(operation.base.Fingerprint, fmt.Errorf("prune aborted window: %w", err))
			}
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

func (c *Coordinator) waitCommand(ctx context.Context, command coordinatorCommand, result <-chan error) error {
	select {
	case c.commands <- command:
	case <-c.done:
		return c.terminalError()
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case err := <-result:
		return err
	case <-c.done:
		return c.terminalError()
	case <-ctx.Done():
		return ctx.Err()
	}
}

func resolvedInitialBounds(plan datastore.PreparedQuery, discovered []windowloader.WindowDiscovery, initial InitialWindow) (datastore.Bounds, error) {
	bounds := datastore.Bounds{Left: datastore.Boundary{Kind: datastore.BoundaryStart}, Right: datastore.Boundary{Kind: datastore.BoundaryEnd}}
	if len(discovered) <= initial.Count {
		return bounds, nil
	}
	if err := sortWindowDiscoveries(plan, discovered); err != nil {
		return datastore.Bounds{}, err
	}
	if initial.FromEnd {
		bounds.Left = datastore.Boundary{Kind: datastore.BoundaryCursor, Key: discovered[len(discovered)-initial.Count-1].Key}
	} else {
		bounds.Right = datastore.Boundary{Kind: datastore.BoundaryCursor, Key: discovered[initial.Count-1].Key}
	}
	return bounds, nil
}
