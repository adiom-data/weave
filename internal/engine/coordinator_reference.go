package engine

import (
	"context"
	"errors"
	"reflect"

	"github.com/adiom-data/weave/internal/datastore"
	"github.com/adiom-data/weave/internal/engine/entitycache"
	"github.com/adiom-data/weave/internal/engine/querywindow"
	"github.com/adiom-data/weave/internal/engine/referencefetch"
	"github.com/adiom-data/weave/internal/engine/subscriptions"
	"github.com/adiom-data/weave/internal/storage"
)

// Metadata reuse is explicitly scoped to an active hinted parent on this
// feed. An unrelated new selector still goes through normal datastore
// preparation; there is no global query-text metadata cache to go stale.
func (c *Coordinator) prepareFromReference(ctx context.Context, spec storage.QuerySpec, from *ReferenceSource) (datastore.PreparedQuery, error) {
	if from == nil {
		return nil, nil
	}
	var prepared datastore.PreparedQuery
	done := make(chan error, 1)
	err := c.waitCommand(ctx, coordinatorCommand{run: func() {
		prepared = c.prepareFromReferenceActor(spec, from)
		done <- nil
	}}, done)
	if err != nil {
		return nil, err
	}
	return prepared, nil
}

func (c *Coordinator) prepareFromReferenceActor(spec storage.QuerySpec, from *ReferenceSource) datastore.PreparedQuery {
	if from == nil {
		return nil
	}
	parent, exists := c.subscriptions.Active(from.SubscriptionID)
	if !exists || parent.Reference == nil || from.ParentKey.Relation != parent.Plan.Relation() || !c.queries.IsMember(parent.Fingerprint, from.ParentKey) {
		return nil
	}
	var prepared datastore.PreparedQuery
	_, _ = c.entities.Inspect(from.ParentKey, func(record storage.Record, state entitycache.EntityState) error {
		if state != entitycache.EntityCurrent {
			return nil
		}
		key, err := parent.Plan.Key(record)
		if err != nil {
			return nil
		}
		after, err := querywindow.AfterLeft(parent.Plan, key, parent.Bounds.Left)
		if err != nil || !after {
			return nil
		}
		before, err := querywindow.AtOrBeforeRight(parent.Plan, key, parent.Bounds.Right)
		if err != nil || !before {
			return nil
		}
		plan, expected, _, matchable, err := parent.Reference.Bind(record)
		if err == nil && matchable && reflect.DeepEqual(spec, expected) {
			prepared = plan
		}
		return nil
	})
	return prepared
}

// referenceRead owns only speculative loading, not a joined result cache or
// live subscriptions. Clean targets are installed as ordinary exact selectors;
// dirty ones take the ordinary subscription initialization path instead.
type referenceRead struct {
	relation  storage.RelationKey
	reference datastore.PreparedReference
	capture   *referencefetch.Capture
	rows      []datastore.ReferenceWindowRow
	fence     storage.Fence
	ready     bool
	result    chan error
}

func (c *Coordinator) beginReferenceRead(ctx context.Context, reference datastore.PreparedReference) (*referenceRead, error) {
	relation := reference.Fetch().Target.Relation()
	read := &referenceRead{relation: relation, reference: reference, capture: referencefetch.New(relation, c.options.MaxReferenceInvalidations), result: make(chan error, 1)}
	done := make(chan error, 1)
	err := c.waitCommand(ctx, coordinatorCommand{run: func() {
		if c.referenceReads == nil {
			c.referenceReads = make(map[*referenceRead]struct{})
		}
		c.referenceReads[read] = struct{}{}
		done <- nil
	}}, done)
	if err != nil {
		c.abortReferenceRead(read)
		return nil, err
	}
	return read, nil
}

func (c *Coordinator) abortReferenceRead(read *referenceRead) {
	if read == nil {
		return
	}
	done := make(chan error, 1)
	_ = c.waitCommand(context.Background(), coordinatorCommand{run: func() {
		delete(c.referenceReads, read)
		done <- nil
	}}, done)
}

func (c *Coordinator) finishReferenceRead(ctx context.Context, read *referenceRead, rows []datastore.ReferenceWindowRow, fence storage.Fence) error {
	if fence == nil || fence.Source() != c.source.ID() {
		return errors.New("reference snapshot has a foreign or nil fence")
	}
	done := make(chan error, 1)
	if err := c.waitCommand(ctx, coordinatorCommand{run: func() {
		if _, exists := c.referenceReads[read]; !exists {
			done <- storage.ErrResumeRequired
			return
		}
		read.rows, read.fence, read.ready = rows, fence, true
		done <- nil
	}}, done); err != nil {
		return err
	}
	select {
	case err := <-read.result:
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-c.done:
		return c.terminalError()
	}
}

func (c *Coordinator) cancelReferenceReads(cause error) {
	for read := range c.referenceReads {
		delete(c.referenceReads, read)
		read.result <- cause
	}
}

func (c *Coordinator) tryCompleteReferenceReads() {
	for read := range c.referenceReads {
		if !read.ready || !c.appliedThrough(read.fence) {
			continue
		}
		delete(c.referenceReads, read)
		read.result <- c.installReferenceRows(read)
	}
}

func (c *Coordinator) installReferenceRows(read *referenceRead) error {
	target := read.reference.Fetch().Target
	seen := map[storage.EntityKey]bool{}
	for _, row := range read.rows {
		lookup, matchable, err := read.reference.Resolve(row.Row.Record)
		if err != nil {
			return err
		}
		if !matchable {
			continue
		}
		key := lookup.Key()
		if seen[key] || read.capture.Dirty(key) {
			continue
		}
		// A failed guard is not an entity-absence proof. Do not deduplicate that
		// key until a full target image has actually been admitted.
		if _, recovering := c.recovery.Relation(key.Relation); recovering {
			continue
		}
		if row.Reference == nil {
			if lookup.UnfilteredIdentity() {
				if err := c.prefetch.PutAbsent(key); err != nil {
					return err
				}
			}
			continue
		}
		if err := row.Reference.Validate(); err != nil {
			return err
		}
		actual, err := target.EntityKey(row.Reference.Record)
		if err != nil || actual != key {
			return errors.New("reference snapshot returned a foreign entity")
		}
		matches, err := lookup.Matches(row.Reference.Record)
		if err != nil {
			return err
		}
		if !matches {
			return errors.New("reference snapshot returned an entity outside its selector")
		}
		if err := c.prefetch.Put(key, row.Reference.Record); err != nil {
			return err
		}
		seen[key] = true
	}
	return nil
}

// prefetchCurrentWindow covers warm association windows and the already cached
// part of a partial hit. Only missing target selectors go to the adapter.
func (c *Coordinator) prefetchCurrentWindow(ctx context.Context, id string, reference datastore.PreparedReference) error {
	if reference == nil {
		return nil
	}
	var base datastore.PreparedQuery
	var parents []storage.Record
	done := make(chan error, 1)
	if err := c.waitCommand(ctx, coordinatorCommand{run: func() {
		var bounds datastore.Bounds
		var ranges []datastore.Bounds
		if initial, ok := c.subscriptions.Initialization(id); ok && initial.Phase == subscriptions.OperationReady {
			base, _ = c.queries.Plan(initial.Fingerprint)
			bounds = initial.Bounds
		} else if active, ok := c.subscriptions.Active(id); ok {
			base, bounds = active.Plan, active.Bounds
			if staged, ok := c.subscriptions.WindowOperation(id, c.subscriptions.WindowToken(id)); ok && staged.Phase == subscriptions.OperationReady {
				bounds = staged.Bounds
				var err error
				ranges, err = querywindow.Difference(base, bounds, active.Bounds)
				if err != nil {
					done <- err
					return
				}
				if len(ranges) == 0 {
					done <- nil
					return
				}
			}
		}
		if base == nil {
			done <- errors.New("reference parent window is no longer available")
			return
		}
		if ranges == nil {
			ranges = []datastore.Bounds{bounds}
		}
		keys, err := c.queries.EntityKeysInBounds(base.Fingerprint(), ranges)
		if err != nil {
			done <- err
			return
		}
		seen := map[storage.EntityKey]bool{}
		for _, parentKey := range keys {
			var lookup datastore.ReferenceLookup
			var matchable bool
			exists, err := c.entities.Inspect(parentKey, func(record storage.Record, state entitycache.EntityState) (err error) {
				if state == entitycache.EntityCurrent {
					lookup, matchable, err = reference.Resolve(record)
				}
				return err
			})
			if err != nil {
				done <- err
				return
			}
			if !exists {
				done <- errors.New("reference parent missing from entity cache")
				return
			}
			if !matchable {
				continue
			}
			key := lookup.Key()
			if c.prefetch.Absent(key) {
				continue
			}
			if seen[key] {
				continue
			}
			if _, recovering := c.recovery.Relation(key.Relation); recovering {
				continue
			}
			if c.entities.IsCurrent(key) && c.lastCheckpoint != nil {
				seen[key] = true
				continue
			}
			// Only a miss needs a complete parent image for the adapter read.
			// Async datastore reads still receive owned complete records, never
			// a borrowed cache image.
			full, exists := c.entities.Get(parentKey)
			if !exists {
				done <- errors.New("reference parent missing from entity cache")
				return
			}
			parents = append(parents, full.Record)
		}
		done <- nil
	}}, done); err != nil {
		return err
	}
	for len(parents) > 0 {
		count := min(len(parents), c.options.SnapshotPageRows)
		if err := c.readMissingReferences(ctx, base, reference, parents[:count]); err != nil {
			return err
		}
		parents = parents[count:]
	}
	return nil
}

func (c *Coordinator) readMissingReferences(ctx context.Context, base datastore.PreparedQuery, reference datastore.PreparedReference, parents []storage.Record) error {
	read, err := c.beginReferenceRead(ctx, reference)
	if err != nil {
		return err
	}
	defer c.abortReferenceRead(read)
	snapshot, err := c.source.BeginSnapshot(ctx, base)
	if err != nil {
		return err
	}
	defer snapshot.Abort(context.WithoutCancel(ctx))
	reader, ok := snapshot.(datastore.ReferenceWindowReader)
	if !ok {
		return nil // Hint unsupported: ordinary entity subscriptions still work.
	}
	records, err := reader.LoadReferences(ctx, reference.Fetch(), parents)
	if err != nil {
		return err
	}
	rows := make([]datastore.ReferenceWindowRow, 0, len(parents))
	var selectors []datastore.ReferenceLookup
	positions := map[storage.EntityKey][]int{}
	for _, parent := range parents {
		lookup, matchable, err := reference.Resolve(parent)
		if err != nil {
			return err
		}
		if matchable {
			key := lookup.Key()
			positions[key] = append(positions[key], len(rows))
			rows = append(rows, datastore.ReferenceWindowRow{Row: datastore.AuthoritativeRecord{Record: parent}})
			selectors = append(selectors, lookup)
		}
	}
	seen := map[storage.EntityKey]bool{}
	for index := range records {
		if err := records[index].Validate(); err != nil {
			return err
		}
		key, err := reference.Fetch().Target.EntityKey(records[index].Record)
		if err != nil {
			return err
		}
		if len(positions[key]) == 0 || seen[key] {
			return errors.New("reference read returned an unrequested or duplicate entity")
		}
		seen[key] = true
		matched := false
		for _, position := range positions[key] {
			matches, err := selectors[position].Matches(records[index].Record)
			if err != nil {
				return err
			}
			if matches {
				rows[position].Reference = &records[index]
				matched = true
			}
		}
		if !matched {
			return errors.New("reference read returned an entity outside its requested selectors")
		}
	}
	fence, err := snapshot.Finish(ctx)
	if err != nil {
		return err
	}
	return c.finishReferenceRead(ctx, read, rows, fence)
}
