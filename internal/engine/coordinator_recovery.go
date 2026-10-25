package engine

import (
	"context"
	"errors"
	"fmt"
	"reflect"

	"github.com/adiom-data/weave/internal/datastore"
	"github.com/adiom-data/weave/internal/engine/changeprocessor"
	"github.com/adiom-data/weave/internal/engine/entitycache"
	"github.com/adiom-data/weave/internal/engine/recovery"
	"github.com/adiom-data/weave/internal/engine/windowloader"
	"github.com/adiom-data/weave/internal/storage"
)

func (c *Coordinator) startRelationResync(ctx context.Context, relation storage.RelationKey, trigger datastore.CommittedChanges) {
	// The triggering transaction committed before this refresh starts and is
	// therefore represented by the snapshot itself. Only subsequently observed
	// changes need replay.
	c.subscriptions.BumpEpoch(relation)
	// A refresh completes asynchronously and may coalesce later commits. It is
	// no longer the complete, directly decoded source transaction.
	trigger.Atomic = false
	before := c.captureRelation(relation)
	// Explicit invalidation supersedes every older load for the relation, not
	// only entity hydrations tracked by the recovery manager. Older snapshot
	// owners wake with ErrInvalidated; the relation refresh owns new generations.
	c.recovery.RemoveRelationHydrations(relation)
	c.entities.PrepareRelationRefresh(relation)
	resyncCtx, cancel := context.WithCancel(ctx)
	state, started := c.recovery.StartRelation(relation, before, trigger, cancel)
	if !started {
		c.failRelation(relation, errors.New("relation refresh is already active"))
		return
	}
	generation := state.Generation()
	type refreshRequest struct {
		plan   datastore.PreparedQuery
		bounds datastore.Bounds
	}
	var requests []refreshRequest
	for _, query := range c.queries.RelationCandidates(relation) {
		for _, bounds := range c.queries.Coverage(query.Plan.Fingerprint()) {
			requests = append(requests, refreshRequest{plan: query.Plan, bounds: bounds})
		}
	}
	go func() {
		var windows []*windowloader.PendingWindow
		failed := make(map[datastore.QueryFingerprint]struct{})
		var failures []changeprocessor.QueryFailure
		for _, request := range requests {
			fingerprint := request.plan.Fingerprint()
			if _, skip := failed[fingerprint]; skip {
				continue
			}
			bounds := request.bounds
			for {
				pending, err := c.loader.ReadRefresh(resyncCtx, request.plan, datastore.WindowRequest{Bounds: bounds, Limit: c.options.SnapshotPageRows})
				if err != nil {
					failed[fingerprint] = struct{}{}
					failures = append(failures, changeprocessor.QueryFailure{Query: fingerprint, Err: fmt.Errorf("refresh query %q: %w", fingerprint, err)})
					break
				}
				windows = append(windows, pending)
				if pending.Len() < c.options.SnapshotPageRows {
					break
				}
				last, present, err := pending.LastKey()
				if err != nil {
					failed[fingerprint] = struct{}{}
					failures = append(failures, changeprocessor.QueryFailure{Query: fingerprint, Err: fmt.Errorf("page refreshed query %q: %w", fingerprint, err)})
					break
				}
				if !present {
					break
				}
				bounds.Left = datastore.Boundary{Kind: datastore.BoundaryCursor, Key: last}
			}
		}
		if err := windowloader.WaitForExternalLoads(resyncCtx, windows); err != nil {
			cancelWindows(windows, err)
			c.sendResyncResult(resyncCtx, resyncReadResult{relation: relation, generation: generation, err: err})
			return
		}
		c.sendResyncResult(resyncCtx, resyncReadResult{relation: relation, generation: generation, windows: windows, failures: failures})
	}()
}

func (c *Coordinator) startEntityHydration(ctx context.Context, key storage.EntityKey, queries []datastore.QueryFingerprint) error {
	if state, exists := c.recovery.Hydration(key); exists {
		entity, cached := c.entities.Get(key)
		if cached && entity.Generation == state.Load().Generation() {
			c.recovery.ReplaceHydrationQueries(key, queries)
			return nil
		}
		// A complete CDC tuple may have superseded the cache load earlier in
		// this transaction before a later partial tuple became the final state.
		// Replace that stale recovery registration with a fresh load.
		c.recovery.RemoveHydration(key)
	}
	if err := c.recovery.CheckHydrationCapacity(); err != nil {
		return err
	}
	load := c.entities.BeginRefresh(key)
	hydrationCtx, cancel := context.WithCancel(ctx)
	_, _, err := c.recovery.StartHydration(key, load, queries, cancel)
	if err != nil {
		cancel()
		_ = c.entities.Fail(load, err)
		return err
	}
	if !load.Owned() {
		go func() {
			result := hydrationReadResult{key: key, load: load, covered: true}
			select {
			case <-load.Done():
				if err := c.entities.LoadError(load); err != nil {
					result.err = err
				} else if entity, ok := c.entities.Get(key); ok && (entity.State == entitycache.EntityCurrent || entity.State == entitycache.EntityUnreferenced) {
					result.exists = true
					result.record = entity.Record
				} else {
					result.exists = false
				}
			case <-hydrationCtx.Done():
				return
			}
			c.sendHydrationResult(hydrationCtx, result)
		}()
		return nil
	}
	var query datastore.PreparedQuery
	for _, fingerprint := range queries {
		candidate, ok := c.queries.Plan(fingerprint)
		if ok && candidate.Relation() == key.Relation {
			query = candidate
			break
		}
	}
	if query == nil {
		err := errors.New("entity hydration has no live prepared query for its relation")
		c.recovery.RemoveHydration(key)
		cancel()
		_ = c.entities.Fail(load, err)
		return err
	}
	go func() {
		result := hydrationReadResult{key: key, load: load}
		snapshot, err := c.source.BeginSnapshot(hydrationCtx, query)
		if err != nil {
			result.err = err
			c.sendHydrationResult(hydrationCtx, result)
			return
		}
		if snapshot == nil {
			result.err = errors.New("source returned a nil hydration snapshot")
			c.sendHydrationResult(hydrationCtx, result)
			return
		}
		if snapshot.Relation() != key.Relation {
			_ = snapshot.Abort(context.WithoutCancel(hydrationCtx))
			result.err = errors.New("source returned a hydration snapshot for another relation")
			c.sendHydrationResult(hydrationCtx, result)
			return
		}
		finished := false
		defer func() {
			if !finished {
				_ = snapshot.Abort(context.WithoutCancel(hydrationCtx))
			}
		}()
		record, err := snapshot.FetchEntity(hydrationCtx, key)
		if errors.Is(err, datastore.ErrNotFound) {
			result.exists = false
		} else if err != nil {
			result.err = err
			c.sendHydrationResult(hydrationCtx, result)
			return
		} else {
			if err := record.Validate(); err != nil {
				result.err = err
				c.sendHydrationResult(hydrationCtx, result)
				return
			}
			result.exists = true
			result.record = record.Record
		}
		result.fence, err = snapshot.Finish(hydrationCtx)
		if err != nil {
			result.err = err
			c.sendHydrationResult(hydrationCtx, result)
			return
		}
		finished = true
		if result.fence == nil || result.fence.Source() != c.source.ID() {
			result.err = errors.New("hydration snapshot returned a foreign or nil fence")
			c.sendHydrationResult(hydrationCtx, result)
			return
		}
		c.sendHydrationResult(hydrationCtx, result)
	}()
	return nil
}

// applyProcessEffects is the single coordinator boundary for processor-owned
// cache/query transitions that require asynchronous recovery lifecycle work.
// Query failures are isolated first so an irrelevant partial can be discarded
// after all failed-query references have been released.
func (c *Coordinator) applyProcessEffects(ctx context.Context, relation storage.RelationKey, result changeprocessor.ProcessResult) error {
	if result.RelationCleared {
		c.recovery.RemoveRelationHydrations(relation)
	}
	c.isolateQueryFailures(result.QueryFailures)
	if err := c.currentTerminalError(); err != nil {
		return err
	}
	for _, action := range result.EntityActions {
		switch action.Kind {
		case changeprocessor.EntityHydrate:
			if err := c.startEntityHydration(ctx, action.Key, action.Queries); err != nil {
				return err
			}
		case changeprocessor.EntityCancelHydration:
			c.recovery.RemoveHydration(action.Key)
		case changeprocessor.EntityDiscardPartial:
			c.recovery.RemoveHydration(action.Key)
			entity, exists := c.entities.Get(action.Key)
			if !exists || entity.State == entitycache.EntityCurrent || entity.State == entitycache.EntityUnreferenced {
				continue
			}
			if entity.References != 0 {
				return fmt.Errorf("discard unneeded partial entity %q with %d references", action.Key.Value, entity.References)
			}
			if _, err := c.entities.DiscardIfGeneration(action.Key, entity.Generation, datastore.ErrInvalidated); err != nil {
				return fmt.Errorf("discard unneeded partial entity %q: %w", action.Key.Value, err)
			}
		default:
			return fmt.Errorf("unknown entity recovery action %d", action.Kind)
		}
	}
	return nil
}

func (c *Coordinator) sendHydrationResult(ctx context.Context, result hydrationReadResult) {
	select {
	case c.hydrationResults <- result:
	case <-ctx.Done():
	case <-c.done:
	}
}

func (c *Coordinator) acceptHydrationResult(result hydrationReadResult) {
	state, ok := c.recovery.Hydration(result.key)
	if !ok || state.Load().Generation() != result.load.Generation() {
		return
	}
	if result.err != nil {
		if err := c.entities.Fail(state.Load(), result.err); err != nil && !errors.Is(err, entitycache.ErrStaleLoad) {
			c.failRelation(result.key.Relation, fmt.Errorf("fail hydration load: %w", err))
			return
		}
		c.recovery.RemoveHydration(result.key)
		memberships, err := c.detachAndDiscardEntity(result.key, state.Load().Generation(), result.err)
		if err != nil {
			c.failRelation(result.key.Relation, fmt.Errorf("discard failed hydration: %w", err))
			return
		}
		state.AddQueries(memberships)
		c.failEntityHydration(result.key, state.QueryFingerprints(), result.err)
		return
	}
	state.AcceptRead(result.fence, result.record, result.exists, result.covered)
}

// detachAndDiscardEntity is the single coordinator protocol for removing a
// failed entity load. Query memberships own cache references, so they must be
// detached before the generation-checked cache discard can succeed.
func (c *Coordinator) detachAndDiscardEntity(key storage.EntityKey, generation uint64, cause error) ([]datastore.QueryFingerprint, error) {
	memberships, err := c.queries.RemoveEntity(key)
	if err != nil {
		return nil, fmt.Errorf("detach entity from queries: %w", err)
	}
	// A failed refresh cannot leave either the partial CDC tuple or its older
	// pre-refresh value reusable as source-current by a later snapshot.
	if _, err := c.entities.DiscardIfGeneration(key, generation, cause); err != nil {
		return nil, fmt.Errorf("discard entity generation: %w", err)
	}
	return memberships, nil
}

func (c *Coordinator) tryCompleteHydrations(ctx context.Context) {
	c.recovery.RangeHydrations(func(key storage.EntityKey, state *recovery.HydrationState) bool {
		if !state.Ready() || (!state.Covered() && (state.Fence() == nil || !c.appliedThrough(state.Fence()))) {
			return true
		}
		var reconciled changeprocessor.ProcessResult
		var err error
		if state.Exists() && state.Covered() {
			reconciled, err = c.processor.Reconcile(key, state.Record(), true)
		} else if state.Exists() {
			var record storage.Record
			record, err = c.entities.CompleteMerged(state.Load(), state.Record())
			if err == nil {
				reconciled, err = c.processor.Reconcile(key, record, true)
			}
		} else {
			if failErr := c.entities.Fail(state.Load(), datastore.ErrNotFound); failErr != nil && !errors.Is(failErr, entitycache.ErrStaleLoad) {
				err = failErr
			} else {
				reconciled, err = c.processor.Reconcile(key, nil, false)
			}
		}
		c.recovery.RemoveHydration(key)
		if errors.Is(err, entitycache.ErrStaleLoad) {
			return true
		}
		if err != nil {
			c.failEntityHydration(key, state.QueryFingerprints(), err)
			return true
		}
		c.isolateQueryFailures(reconciled.QueryFailures)
		if len(reconciled.Changes) > 0 {
			c.deliveryRevision++
			published := c.hub.PublishHydration(state.Fence(), c.lastCheckpoint, c.deliveryRevision, reconciled.Changes)
			for _, id := range published.Dropped {
				c.dropActive(id)
			}
		}
		return true
	})
	_ = ctx
}

func (c *Coordinator) failEntityHydration(key storage.EntityKey, queries []datastore.QueryFingerprint, cause error) {
	for _, fingerprint := range queries {
		c.failQuery(fingerprint, fmt.Errorf("hydrate entity %s: %w", key.Value, cause))
	}
}

func (c *Coordinator) sendResyncResult(ctx context.Context, result resyncReadResult) {
	select {
	case c.resyncResults <- result:
	case <-ctx.Done():
		cancelWindows(result.windows, ctx.Err())
	case <-c.done:
		cancelWindows(result.windows, c.terminalError())
	}
}

func (c *Coordinator) acceptResyncResult(ctx context.Context, result resyncReadResult) {
	state, ok := c.recovery.Relation(result.relation)
	if !ok || state.Generation() != result.generation {
		cancelWindows(result.windows, context.Canceled)
		return
	}
	if result.err != nil {
		c.recovery.RemoveRelation(result.relation)
		c.failRelation(result.relation, result.err)
		return
	}
	c.isolateQueryFailures(result.failures)
	state.InstallWindows(result.windows)
	c.tryCompleteResyncs(ctx)
}

func (c *Coordinator) tryCompleteResyncs(ctx context.Context) {
	c.recovery.RangeRelations(func(relation storage.RelationKey, state *recovery.RelationState) bool {
		if !state.Ready() || !c.windowsApplied(state.Windows()) {
			return true
		}
		if err := c.completeRelationResync(ctx, relation, state); err != nil {
			c.recovery.RemoveRelation(relation)
			cancelWindows(state.Windows(), err)
			c.failRelation(relation, err)
		}
		return true
	})
}

func (c *Coordinator) windowsApplied(windows []*windowloader.PendingWindow) bool {
	for _, window := range windows {
		if !c.appliedThrough(window.Fence) {
			return false
		}
	}
	return true
}

func (c *Coordinator) completeRelationResync(ctx context.Context, relation storage.RelationKey, state *recovery.RelationState) error {
	type clearedQuery struct {
		fingerprint datastore.QueryFingerprint
		changeIndex uint32
	}
	var cleared []clearedQuery
	clearedAt := make(map[datastore.QueryFingerprint]int)
	for _, batch := range state.Buffered() {
		result, err := c.processor.ApplyRelationBatch(batch)
		if err != nil {
			return err
		}
		for _, change := range result.Changes {
			if change.Kind != changeprocessor.QueryCleared {
				continue
			}
			if index, exists := clearedAt[change.Query]; exists {
				cleared[index].changeIndex = change.ChangeIndex
			} else {
				clearedAt[change.Query] = len(cleared)
				cleared = append(cleared, clearedQuery{fingerprint: change.Query, changeIndex: change.ChangeIndex})
			}
		}
		if err := c.applyProcessEffects(ctx, relation, result); err != nil {
			return err
		}
		if len(result.Resync) > 0 {
			return errors.New("relation change remained incomplete after refresh")
		}
	}
	seen := make(map[datastore.QueryFingerprint]map[storage.EntityKey]struct{})
	for _, window := range state.Windows() {
		fingerprint := window.Query().Fingerprint()
		if _, exists := c.queries.Plan(fingerprint); !exists {
			if err := c.reconcileSnapshot(ctx, window); err != nil {
				return err
			}
			continue
		}
		rows, err := c.activateSnapshot(ctx, window)
		if err != nil {
			var queryErr *windowloader.QueryEvaluationError
			if errors.As(err, &queryErr) {
				c.failQuery(fingerprint, fmt.Errorf("activate refreshed query %q: %w", fingerprint, err))
				continue
			}
			return err
		}
		set := seen[fingerprint]
		if set == nil {
			set = make(map[storage.EntityKey]struct{})
			seen[fingerprint] = set
		}
		for _, row := range rows {
			set[row.Key] = struct{}{}
		}
	}
	var removalErr error
	state.Before().RangeMemberships(func(fingerprint datastore.QueryFingerprint, key storage.EntityKey, _ storage.Record) bool {
		if _, present := seen[fingerprint][key]; !present && c.queries.IsMember(fingerprint, key) {
			_, removalErr = c.queries.RemoveMember(fingerprint, key)
			if removalErr == nil {
				if entity, exists := c.entities.Get(key); exists && entity.References == 0 && entity.State != entitycache.EntityCurrent && entity.State != entitycache.EntityUnreferenced {
					_, removalErr = c.entities.DiscardIfGeneration(key, entity.Generation, datastore.ErrInvalidated)
				}
			}
		}
		return removalErr == nil
	})
	if removalErr != nil {
		return removalErr
	}
	changes := c.diffRelation(state.Before(), relation)
	if len(cleared) > 0 {
		// A buffered truncate remains a first-class clear. Remove ordinary diffs
		// for affected queries, then repopulate only their final post-clear state.
		filtered := changes[:0]
		for _, change := range changes {
			if _, wasCleared := clearedAt[change.Query]; !wasCleared {
				filtered = append(filtered, change)
			}
		}
		changes = filtered
		for _, clear := range cleared {
			plan, exists := c.queries.Plan(clear.fingerprint)
			if !exists {
				continue
			}
			changes = append(changes, changeprocessor.QueryChange{Query: clear.fingerprint, Kind: changeprocessor.QueryCleared, ChangeIndex: clear.changeIndex})
			var members []entitycache.EntitySnapshot
			for _, key := range c.queries.Members(clear.fingerprint) {
				if entity, ok := c.entities.Get(key); ok {
					members = append(members, entity)
				}
			}
			if err := sortEntities(plan, members); err != nil {
				return err
			}
			for _, entity := range members {
				pending := entity.State != entitycache.EntityCurrent && entity.State != entitycache.EntityUnreferenced
				changes = append(changes, changeprocessor.QueryChange{Query: clear.fingerprint, Kind: changeprocessor.QueryAdded, Key: entity.Key, Record: entity.Record, Pending: pending})
			}
		}
	}
	c.publish(state.Checkpoint(), state.Atomic(), changes)
	c.recovery.RemoveRelation(relation)
	return nil
}

func cancelWindows(windows []*windowloader.PendingWindow, err error) {
	for _, window := range windows {
		window.Cancel(err)
	}
}

func (c *Coordinator) captureRelation(relation storage.RelationKey) *recovery.RelationSnapshot {
	result := recovery.NewRelationSnapshot()
	for _, query := range c.queries.RelationCandidates(relation) {
		fingerprint := query.Plan.Fingerprint()
		for _, key := range c.queries.Members(query.Plan.Fingerprint()) {
			result.AddMembership(fingerprint, key)
			if !result.HasRecord(key) {
				if entity, ok := c.entities.Get(key); ok {
					result.StoreRecord(key, entity.Record)
				}
			}
		}
	}
	return result
}

func (c *Coordinator) diffRelation(before *recovery.RelationSnapshot, relation storage.RelationKey) []changeprocessor.QueryChange {
	var changes []changeprocessor.QueryChange
	before.RangeMemberships(func(fingerprint datastore.QueryFingerprint, key storage.EntityKey, record storage.Record) bool {
		if !c.queries.IsMember(fingerprint, key) {
			changes = append(changes, changeprocessor.QueryChange{Query: fingerprint, Kind: changeprocessor.QueryRemoved, Key: key, PreviousRecord: record})
		}
		return true
	})
	for _, query := range c.queries.RelationCandidates(relation) {
		fingerprint := query.Plan.Fingerprint()
		after := make(map[storage.EntityKey]entitycache.EntitySnapshot)
		for _, key := range c.queries.Members(fingerprint) {
			if entity, ok := c.entities.Get(key); ok {
				after[key] = entity
			}
		}
		for key, entity := range after {
			record := entity.Record
			old, existed := before.Lookup(fingerprint, key)
			kind := changeprocessor.QueryAdded
			var previous storage.Record
			if existed {
				if reflect.DeepEqual(old, record) {
					continue
				}
				kind = changeprocessor.QueryUpdated
				previous = old
			}
			pending := entity.State != entitycache.EntityCurrent && entity.State != entitycache.EntityUnreferenced
			changes = append(changes, changeprocessor.QueryChange{Query: fingerprint, Kind: kind, Key: key, PreviousRecord: previous, Record: record, Pending: pending})
		}
	}
	return changes
}
