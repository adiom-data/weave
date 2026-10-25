package engine

import (
	"context"
	"errors"

	"github.com/adiom-data/weave/internal/engine/entitycache"
	"github.com/adiom-data/weave/internal/engine/subscriptions"
	"github.com/adiom-data/weave/internal/engine/windowloader"
	"github.com/adiom-data/weave/internal/storage"
)

var errSnapshotNotApplied = errors.New("snapshot fence has not been applied by the coordinator")

// Snapshot admission lives at the actor boundary, not in the IO loader. Every
// reconciliation path (including relation recovery) must use these helpers.
func (c *Coordinator) admitSnapshot(window *windowloader.PendingWindow) error {
	if window == nil || window.Fence == nil {
		return errors.New("snapshot has no fence")
	}
	if !c.appliedThrough(window.Fence) {
		return errSnapshotNotApplied
	}
	return nil
}

func (c *Coordinator) activateSnapshot(ctx context.Context, window *windowloader.PendingWindow) ([]entitycache.EntitySnapshot, error) {
	if err := c.admitSnapshot(window); err != nil {
		return nil, err
	}
	return window.Activate(ctx)
}

func (c *Coordinator) reconcileSnapshot(ctx context.Context, window *windowloader.PendingWindow) error {
	if err := c.admitSnapshot(window); err != nil {
		return err
	}
	return window.Reconcile(ctx)
}

// tryCompleteActivations advances fenced snapshot work from the normal actor
// loop. Waiting for source progress therefore never creates a nested feed loop
// or prevents unrelated coordinator commands from running.
func (c *Coordinator) tryCompleteActivations(ctx context.Context) {
	for id, activation := range c.pendingInitial {
		if !c.appliedThrough(activation.materialized.pending.Fence) {
			continue
		}
		err := c.completeInitialActivation(ctx, activation)
		delete(c.pendingInitial, id)
		if err != nil {
			activation.materialized.cancel(err)
		}
		activation.result <- initialActivationResult{err: err}
	}
	for id, activation := range c.pendingWindows {
		if !c.appliedThrough(activation.materialized.pending.Fence) {
			continue
		}
		rows, err := c.completeWindowActivation(ctx, activation)
		delete(c.pendingWindows, id)
		if err != nil {
			activation.materialized.cancel(err)
		}
		activation.result <- windowActivationResult{rows: rows, err: err}
	}
}

func (c *Coordinator) completeInitialActivation(ctx context.Context, activation *pendingInitialActivation) error {
	operation := activation.operation
	plan := operation.plan
	id := operation.subscriptionID
	if _, recovering := c.recovery.Relation(plan.Relation()); recovering ||
		!c.subscriptions.InitializationInPhase(id, plan, subscriptions.OperationMaterializing) {
		return storage.ErrResumeRequired
	}
	initializing, _ := c.subscriptions.Initialization(id)
	if err := activation.materialized.validate(plan, initializing.Reads); err != nil {
		return err
	}
	if _, err := c.activateSnapshot(ctx, activation.materialized.pending); err != nil {
		return err
	}
	if err := c.queries.MarkWindowReady(plan.Fingerprint(), id); err != nil {
		return err
	}
	if !c.subscriptions.ReadyInitialization(id, plan, activation.materialized.pending.Fence) {
		return errors.New("subscription initialization changed phase during activation")
	}
	return nil
}

func (c *Coordinator) completeWindowActivation(ctx context.Context, activation *pendingWindowActivation) ([]entitycache.EntitySnapshot, error) {
	operation := activation.operation
	staged, exists := c.subscriptions.WindowOperation(operation.subscriptionID, operation.token)
	if !exists || staged.Phase != subscriptions.OperationMaterializing {
		return nil, errors.New("window update was cancelled")
	}
	active, ok := c.subscriptions.Active(operation.subscriptionID)
	relation := operation.base.Plan.Relation()
	_, recovering := c.recovery.Relation(relation)
	if !ok || active.Fingerprint != operation.base.Fingerprint || active.Plan.Relation() != relation || recovering ||
		active.RelationEpoch != c.subscriptions.Epoch(relation) {
		return nil, storage.ErrResumeRequired
	}
	if err := activation.materialized.validate(operation.base.Plan, staged.Reads); err != nil {
		return nil, err
	}
	rows, err := c.activateSnapshot(ctx, activation.materialized.pending)
	if err == nil {
		err = c.queries.MarkWindowReady(operation.base.Fingerprint, operation.stageID)
	}
	if err == nil && !c.subscriptions.ReadyWindow(operation.subscriptionID, operation.token, activation.materialized.pending.Fence) {
		err = errors.New("window update changed phase during activation")
	}
	return rows, err
}

func (c *Coordinator) cancelInitialActivation(id string, cause error) {
	activation := c.pendingInitial[id]
	if activation == nil {
		return
	}
	delete(c.pendingInitial, id)
	activation.materialized.cancel(cause)
	activation.result <- initialActivationResult{err: cause}
}

func (c *Coordinator) cancelWindowActivation(id string, cause error) {
	activation := c.pendingWindows[id]
	if activation == nil {
		return
	}
	delete(c.pendingWindows, id)
	activation.materialized.cancel(cause)
	activation.result <- windowActivationResult{err: cause}
}

func (c *Coordinator) cancelPendingActivations(cause error) {
	for id := range c.pendingInitial {
		c.cancelInitialActivation(id, cause)
	}
	for id := range c.pendingWindows {
		c.cancelWindowActivation(id, cause)
	}
}
