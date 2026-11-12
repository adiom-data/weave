package engine

import (
	"context"
	"errors"
	"fmt"

	"github.com/adiom-data/weave/internal/datastore"
	"github.com/adiom-data/weave/internal/engine/changeprocessor"
	"github.com/adiom-data/weave/internal/engine/delivery"
	"github.com/adiom-data/weave/internal/engine/recovery"
	"github.com/adiom-data/weave/internal/engine/referencefetch"
	"github.com/adiom-data/weave/internal/engine/windowloader"
	"github.com/adiom-data/weave/internal/storage"
)

func (c *Coordinator) applyFeedEvent(ctx context.Context, event datastore.ChangeEvent) error {
	switch {
	case event.Transaction != nil && event.Progress == nil:
		if err := c.validateNextSourcePosition(event.Transaction.Checkpoint); err != nil {
			return err
		}
		return c.applyAndPublish(ctx, *event.Transaction)
	case event.Transaction == nil && event.Progress != nil:
		if err := c.validateNextSourcePosition(event.Progress); err != nil {
			return err
		}
		c.publishDelivery(event.Progress, true, delivery.DeliveryProgress, nil)
		if err := c.feed.Ack(ctx, event.Progress); err != nil {
			return err
		}
		c.lastCheckpoint = event.Progress
		c.tryCompleteResyncs(ctx)
		c.tryCompleteHydrations(ctx)
		return nil
	default:
		return fmt.Errorf("%w: change feed event must contain exactly one payload", changeprocessor.ErrFatalSource)
	}
}

func (c *Coordinator) validateNextSourcePosition(position storage.Checkpoint) error {
	if position == nil || position.Source() != c.source.ID() {
		return fmt.Errorf("%w: event has a foreign or nil source position", changeprocessor.ErrFatalSource)
	}
	if c.lastCheckpoint != nil && position.Order() <= c.lastCheckpoint.Order() {
		return fmt.Errorf("%w: source position %d does not advance past %d", changeprocessor.ErrFatalSource, position.Order(), c.lastCheckpoint.Order())
	}
	return nil
}

// pendingProgressFence returns any ready snapshot fence currently waiting on
// normal CDC. One barrier emitted after this point will generally cover all
// such fences; later fences are reconsidered after that barrier is consumed.
func (c *Coordinator) pendingProgressFence() storage.Fence {
	fence := c.recovery.PendingFence(c.appliedThrough)
	consider := func(candidate storage.Fence) {
		if candidate == nil || c.appliedThrough(candidate) {
			return
		}
		if fence == nil || candidate.Order() > fence.Order() {
			fence = candidate
		}
	}
	for _, activation := range c.pendingInitial {
		consider(activation.materialized.pending.Fence)
	}
	for _, activation := range c.pendingWindows {
		consider(activation.materialized.pending.Fence)
	}
	for wait := range c.readFences {
		consider(wait.fence)
	}
	for _, reads := range c.referenceReads {
		for read := range reads {
			if read.ready {
				consider(read.fence)
			}
		}
	}
	return fence
}

func (c *Coordinator) startProgressRequest(ctx context.Context) {
	fence := c.pendingProgressFence()
	requester, ok := c.source.(datastore.ProgressRequester)
	if fence == nil || !ok || c.progressStarting || c.progressFence != nil {
		return
	}
	c.progressFence = fence
	c.progressStarting = true
	go func() {
		err := requester.RequestProgress(ctx, fence)
		select {
		case c.progressResults <- progressResult{fence: fence, err: err}:
		case <-ctx.Done():
		case <-c.done:
		}
	}()
}

func (c *Coordinator) failPendingProgress(requested storage.Fence, cause error) {
	for wait := range c.readFences {
		if !c.appliedThrough(wait.fence) && fenceAtOrBefore(wait.fence, requested) {
			delete(c.readFences, wait)
			wait.result <- cause
		}
	}
	for _, reads := range c.referenceReads {
		for read := range reads {
			if read.ready && !c.appliedThrough(read.fence) && fenceAtOrBefore(read.fence, requested) {
				c.removeReferenceRead(read)
				read.result <- cause
			}
		}
	}
	for id, activation := range c.pendingInitial {
		fence := activation.materialized.pending.Fence
		if !c.appliedThrough(fence) && fenceAtOrBefore(fence, requested) {
			c.cancelInitialActivation(id, cause)
		}
	}
	for id, activation := range c.pendingWindows {
		fence := activation.materialized.pending.Fence
		if !c.appliedThrough(fence) && fenceAtOrBefore(fence, requested) {
			c.cancelWindowActivation(id, cause)
		}
	}
	c.recovery.RangeHydrations(func(key storage.EntityKey, state *recovery.HydrationState) bool {
		if !state.Ready() || state.Covered() || state.Fence() == nil || c.appliedThrough(state.Fence()) || !fenceAtOrBefore(state.Fence(), requested) {
			return true
		}
		c.acceptHydrationResult(hydrationReadResult{key: key, load: state.Load(), err: cause})
		return true
	})
	c.recovery.RangeRelations(func(relation storage.RelationKey, state *recovery.RelationState) bool {
		if !state.Ready() || c.windowsApplied(state.Windows()) || !c.windowsAwaitFenceAtOrBefore(state.Windows(), requested) {
			return true
		}
		c.recovery.RemoveRelation(relation)
		cancelWindows(state.Windows(), cause)
		c.failRelation(relation, cause)
		return true
	})
}

func fenceAtOrBefore(candidate, requested storage.Fence) bool {
	return candidate != nil && requested != nil && candidate.Source() == requested.Source() && candidate.Order() <= requested.Order()
}

func (c *Coordinator) windowsAwaitFenceAtOrBefore(windows []*windowloader.PendingWindow, requested storage.Fence) bool {
	for _, window := range windows {
		if window != nil && !c.appliedThrough(window.Fence) && fenceAtOrBefore(window.Fence, requested) {
			return true
		}
	}
	return false
}

func (c *Coordinator) applyAndPublish(ctx context.Context, batch datastore.CommittedChanges) error {
	partitions, err := splitBatchByRelation(batch)
	if err != nil {
		return err
	}
	return c.applyPartitionsAndPublish(ctx, batch, partitions)
}

func (c *Coordinator) applyPartitionsAndPublish(ctx context.Context, batch datastore.CommittedChanges, partitions []relationBatch) error {
	if batch.Checkpoint != nil && batch.Checkpoint.Source() != c.source.ID() {
		return fmt.Errorf("%w: checkpoint belongs to source %q", changeprocessor.ErrFatalSource, batch.Checkpoint.Source())
	}
	var published []changeprocessor.QueryChange
	atomic := batch.Atomic
	for _, partition := range partitions {
		relation := partition.relation
		relationBatch := partition.batch
		if err := c.captureSnapshotChanges(relation, relationBatch.Changes); err != nil {
			return err
		}
		// The source feed may cover relations that have never been prepared or
		// cannot be subscribed to (for example a FOR ALL TABLES publication that
		// also contains a table without a primary key). A later subscription starts
		// from an authoritative snapshot, so changes for a relation with no live
		// query state are both unnecessary and unsafe to interpret as entities.
		if !c.queries.HasRelation(relation) {
			continue
		}
		if loading, recovering := c.recovery.Relation(relation); recovering {
			if err := c.recovery.Buffer(relation, relationBatch); err != nil {
				c.recovery.RemoveRelation(relation)
				cancelWindows(loading.Windows(), storage.ErrResumeRequired)
				c.failRelation(relation, errors.New("relation refresh buffer limit exceeded"))
				continue
			}
			atomic = false
			continue
		}
		result, err := c.processor.ApplyRelationBatch(relationBatch)
		if err != nil {
			if errors.Is(err, changeprocessor.ErrFatalSource) {
				return err
			}
			c.failRelation(relation, err)
			atomic = false
			continue
		}
		if err := c.applyProcessEffects(ctx, relation, result); err != nil {
			if terminal := c.currentTerminalError(); terminal != nil {
				return terminal
			}
			c.failRelation(relation, err)
			atomic = false
			continue
		}
		if len(result.Resync) > 0 {
			c.startRelationResync(ctx, relation, relationBatch)
			atomic = false
			continue
		}
		published = append(published, result.Changes...)
	}
	// A committed checkpoint is meaningful even when none of the currently
	// cached data queries changed. The delivery hub uses this progress-only
	// publication to advance each connection's transaction clock so auxiliary
	// streams such as authorization can attach invalidations atomically.
	if len(published) > 0 || batch.Checkpoint != nil {
		c.publish(batch.Checkpoint, atomic, published)
	}
	if batch.Checkpoint != nil {
		if err := c.feed.Ack(ctx, batch.Checkpoint); err != nil {
			return err
		}
		// Actor-owned state now reflects every change through this position. Reads
		// installed after this point must use it rather than their older snapshot
		// fence when synchronizing auxiliary state such as authorization.
		c.lastCheckpoint = batch.Checkpoint
	}
	c.tryCompleteResyncs(ctx)
	c.tryCompleteHydrations(ctx)
	return nil
}

type relationBatch struct {
	relation storage.RelationKey
	batch    datastore.CommittedChanges
}

// splitBatchByRelation preserves both relation first-appearance order and each
// change's ordinal in the original source transaction.
func splitBatchByRelation(batch datastore.CommittedChanges) ([]relationBatch, error) {
	if len(batch.SourceOrdinals) != 0 && len(batch.SourceOrdinals) != len(batch.Changes) {
		return nil, fmt.Errorf("%w: source ordinals do not match changes", changeprocessor.ErrFatalSource)
	}
	var result []relationBatch
	positions := make(map[storage.RelationKey]int)
	var previousOrdinal uint32
	for index, change := range batch.Changes {
		if change == nil {
			return nil, fmt.Errorf("%w: committed batch contains a nil change", changeprocessor.ErrFatalSource)
		}
		ordinal := uint32(index + 1)
		if len(batch.SourceOrdinals) != 0 {
			ordinal = batch.SourceOrdinals[index]
		}
		if ordinal == 0 || ordinal <= previousOrdinal {
			return nil, fmt.Errorf("%w: source ordinals must be positive and strictly increasing", changeprocessor.ErrFatalSource)
		}
		previousOrdinal = ordinal
		relation := change.Relation()
		if relation.Source == "" || relation.Name == "" {
			return nil, fmt.Errorf("%w: change has an incomplete relation identity", changeprocessor.ErrFatalSource)
		}
		position, exists := positions[relation]
		if !exists {
			position = len(result)
			positions[relation] = position
			result = append(result, relationBatch{relation: relation, batch: datastore.CommittedChanges{Checkpoint: batch.Checkpoint, Atomic: batch.Atomic}})
		}
		result[position].batch.Changes = append(result[position].batch.Changes, change)
		result[position].batch.SourceOrdinals = append(result[position].batch.SourceOrdinals, ordinal)
	}
	return result, nil
}

// captureSnapshotChanges resolves key metadata once per change, then routes it
// to interested point reads and reference prefetches. Range reads capture the
// relation prefix until their snapshot keys become known to their worker.
func (c *Coordinator) captureSnapshotChanges(relation storage.RelationKey, changes []datastore.Change) error {
	reads := c.windowReads[relation]
	references := c.referenceReads[relation]
	prefetch := c.prefetch != nil && c.prefetch.HasRelation(relation)
	if len(reads) == 0 && len(references) == 0 && !prefetch {
		return nil
	}
	for _, change := range changes {
		identity := referencefetch.Resolve(change)
		for read := range reads {
			read.observe(change, identity, c.options.MaxResyncChanges)
		}
		if prefetch {
			if err := c.prefetch.Observe(identity); err != nil {
				return err
			}
		}
		for read := range references {
			read.capture.ObserveResolved(identity)
		}
	}
	return nil
}
