package projectionserver

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/adiom-data/weave/internal/accesscontrol"
	"github.com/adiom-data/weave/internal/querysubscription/model"
	"github.com/adiom-data/weave/internal/storage"
	"github.com/adiom-data/weave/internal/visibilityindex"
)

// subscriptionVisibility is the connection-local authorization gate for one
// opaque query subscription. It mirrors the raw ordered membership so policy
// invalidations can hide and later re-expose rows without reaching through the
// query-subscription boundary.
type subscriptionVisibility struct {
	id         string
	rows       map[storage.EntityKey]subscriptionVisibilityRow
	order      *visibilityindex.Index
	checkpoint *querysubscription.Position
	bounds     querysubscription.Bounds
}

type subscriptionVisibilityRow struct {
	querysubscription.Row
	version querysubscription.RecordVersion
}

func newSubscriptionVisibility() *subscriptionVisibility {
	return &subscriptionVisibility{id: fmt.Sprintf("visibility-%d", streamSequence.Add(1)), rows: make(map[storage.EntityKey]subscriptionVisibilityRow), order: visibilityindex.New()}
}

func (v *subscriptionVisibility) filterSnapshot(ctx context.Context, prepared accesscontrol.Prepared, view accesscontrol.View, snapshot querysubscription.QuerySnapshot) (querysubscription.QuerySnapshot, error) {
	candidates := make([]accesscontrol.Candidate, len(snapshot.Rows))
	for i, row := range snapshot.Rows {
		candidates[i] = accesscontrol.Candidate{Key: row.Key, Record: storage.CloneRecord(row.Record), Complete: !row.Pending, Context: accesscontrol.Scope{InstanceID: v.id}}
	}
	visible := make([]bool, 0, len(candidates))
	var err error
	if len(candidates) > 0 {
		visible, err = prepared.Evaluate(ctx, view, candidates)
		if err != nil {
			return querysubscription.QuerySnapshot{}, err
		}
	}
	if len(visible) != len(snapshot.Rows) {
		return querysubscription.QuerySnapshot{}, errors.New("access controller returned the wrong number of decisions")
	}
	result := snapshot
	v.checkpoint = snapshot.Checkpoint
	if floor, err := querysubscription.SnapshotPosition(snapshot.Checkpoint, snapshot.Fence); err == nil {
		v.checkpoint = floor
	}
	v.bounds = snapshot.Bounds
	result.Rows = make([]querysubscription.QueryRow, 0, len(snapshot.Rows))
	for i, row := range snapshot.Rows {
		row.Record = storage.CloneRecord(row.Record)
		row.Cursor = append([]byte(nil), row.Cursor...)
		v.rows[row.Key] = subscriptionVisibilityRow{Row: row.Row, version: querysubscription.RecordVersion{Revision: snapshot.Revision, ChangeIndex: row.ChangeIndex}}
		if err := v.order.Append(row.Key, visible[i]); err != nil {
			return querysubscription.QuerySnapshot{}, err
		}
		if visible[i] {
			result.Rows = append(result.Rows, row)
		}
	}
	return result, nil
}

func (v *subscriptionVisibility) filterBatch(ctx context.Context, prepared accesscontrol.Prepared, view accesscontrol.View, batch querysubscription.Batch) (querysubscription.Batch, error) {
	part, err := v.filterTransaction(ctx, prepared, view, subscriptionBatchMessage{batch: batch})
	return part.batch, err
}

// Policy facts determine visibility, never the raw record or its freshness.
// Raw effects and invalidations are reduced together before emitting deltas.
func (v *subscriptionVisibility) filterTransaction(ctx context.Context, prepared accesscontrol.Prepared, view accesscontrol.View, part subscriptionBatchMessage) (subscriptionBatchMessage, error) {
	batch := part.batch
	if part.invalidation.All {
		return subscriptionBatchMessage{}, errVisibilityResyncRequired
	}
	if (len(part.invalidation.Candidates) > 0 || len(part.invalidation.Targets) > 0) && (part.delivery.Kind != querysubscription.DeliveryCommit && part.delivery.Kind != querysubscription.DeliveryHydration || part.delivery.Checkpoint == nil) {
		return subscriptionBatchMessage{}, errors.New("visibility invalidation requires a committed checkpoint")
	}
	candidates := make([]accesscontrol.Candidate, 0, len(batch.Changes))
	candidateIndexes := make([]int, 0, len(batch.Changes))
	for i, change := range batch.Changes {
		if change.Kind == querysubscription.Removed {
			continue
		}
		candidateIndexes = append(candidateIndexes, i)
		candidates = append(candidates, accesscontrol.Candidate{Key: change.Key, Record: storage.CloneRecord(change.Record), Complete: !change.Pending, Context: accesscontrol.Scope{InstanceID: v.id}})
	}
	decisions := make([]bool, 0, len(candidates))
	var err error
	if len(candidates) > 0 {
		decisions, err = prepared.Evaluate(ctx, view, candidates)
		if err != nil {
			return subscriptionBatchMessage{}, err
		}
	}
	if len(decisions) != len(candidates) {
		return subscriptionBatchMessage{}, errors.New("access controller returned the wrong number of decisions")
	}
	decisionByChange := make(map[int]bool, len(candidateIndexes))
	for i, changeIndex := range candidateIndexes {
		decisionByChange[changeIndex] = decisions[i]
	}

	if batch.Clear {
		keys := make([]storage.EntityKey, 0, len(v.rows))
		for key := range v.rows {
			keys = append(keys, key)
		}
		if err := prepared.Release(ctx, v.id, keys); err != nil {
			return subscriptionBatchMessage{}, err
		}
		clear(v.rows)
		v.order = visibilityindex.New()
	}
	touched := make(map[storage.EntityKey]bool)
	rawTouched := make(map[storage.EntityKey]bool)
	removedByMembership := make(map[storage.EntityKey]struct{}, len(batch.Membership.Removed))
	touchOrder := make([]storage.EntityKey, 0, len(batch.Changes)+len(batch.Membership.Removed)+len(batch.Membership.Insertions))
	touch := func(key storage.EntityKey) {
		if _, exists := touched[key]; exists {
			return
		}
		visible, _ := v.order.Visible(key)
		touched[key] = visible
		touchOrder = append(touchOrder, key)
	}
	for _, key := range batch.Membership.Removed {
		touch(key)
		rawTouched[key] = true
		removedByMembership[key] = struct{}{}
	}
	for _, insertion := range batch.Membership.Insertions {
		touch(insertion.Key)
		rawTouched[insertion.Key] = true
	}
	if err := v.order.ApplyMembership(batch.Membership); err != nil {
		return subscriptionBatchMessage{}, fmt.Errorf("apply raw subscription membership: %w", err)
	}

	result := batch
	result.Changes = nil
	result.Membership = querysubscription.MembershipDelta{}
	latest := make(map[storage.EntityKey]querysubscription.Change, len(batch.Changes))
	for i, change := range batch.Changes {
		touch(change.Key)
		latest[change.Key] = change
		switch change.Kind {
		case querysubscription.Removed:
			if !v.order.Contains(change.Key) {
				// Membership deltas remove first. If the change is not controlled
				// by one, tolerate the idempotent row removal below.
			} else if _, removed := removedByMembership[change.Key]; !removed {
				if _, err := v.order.Remove(change.Key); err != nil {
					return subscriptionBatchMessage{}, err
				}
			}
			delete(v.rows, change.Key)
			if err := prepared.Release(ctx, v.id, []storage.EntityKey{change.Key}); err != nil {
				return subscriptionBatchMessage{}, err
			}
		case querysubscription.Added, querysubscription.Updated:
			allowed, decided := decisionByChange[i]
			if !decided {
				return subscriptionBatchMessage{}, errors.New("subscription row has no authorization decision")
			}
			row := change.Row
			row.Record = storage.CloneRecord(row.Record)
			row.Cursor = append([]byte(nil), row.Cursor...)
			v.rows[change.Key] = subscriptionVisibilityRow{Row: row, version: querysubscription.RecordVersion{Revision: part.delivery.Revision, ChangeIndex: row.ChangeIndex}}
			if !v.order.Contains(change.Key) {
				if err := v.order.Append(change.Key, false); err != nil {
					return subscriptionBatchMessage{}, err
				}
			}
			if _, err := v.order.SetVisible(change.Key, allowed); err != nil {
				return subscriptionBatchMessage{}, err
			}
		default:
			return subscriptionBatchMessage{}, errors.New("unknown query change kind")
		}
	}

	byKey := make(map[storage.EntityKey]accesscontrol.Candidate, len(part.invalidation.Candidates))
	for _, candidate := range part.invalidation.Candidates {
		if _, exists := v.rows[candidate.Key]; exists {
			candidate.Context.InstanceID = v.id
			byKey[candidate.Key] = candidate
		}
	}
	for _, target := range part.invalidation.Targets {
		if target.InstanceID != v.id {
			continue
		}
		if row, exists := v.rows[target.Key]; exists {
			byKey[target.Key] = accesscontrol.Candidate{Key: target.Key, Record: storage.CloneRecord(row.Record), Complete: !row.Pending, Context: accesscontrol.Scope{InstanceID: v.id}}
		}
	}
	// Independently installed snapshots can already include this transaction.
	if baseline, position := v.checkpoint, part.delivery.Checkpoint; baseline != nil && position != nil && baseline.SourceID == position.SourceID && baseline.Value > position.Value {
		clear(byKey)
	}
	inputs := make([]accesscontrol.Candidate, 0, len(byKey))
	for _, candidate := range byKey {
		inputs = append(inputs, candidate)
	}
	if len(inputs) > 0 {
		allowed, err := prepared.Evaluate(ctx, view, inputs)
		if err != nil {
			return subscriptionBatchMessage{}, err
		}
		if len(allowed) != len(inputs) {
			return subscriptionBatchMessage{}, errors.New("access controller returned the wrong number of decisions")
		}
		for i, input := range inputs {
			touch(input.Key)
			if _, err := v.order.SetVisible(input.Key, allowed[i]); err != nil {
				return subscriptionBatchMessage{}, err
			}
		}
	}
	part.versions = make(map[storage.EntityKey]querysubscription.RecordVersion)
	for _, key := range touchOrder {
		before := touched[key]
		finalRank, after := v.order.VisibleRank(key)
		if rawTouched[key] {
			_, hasRow := v.rows[key]
			if v.order.Contains(key) != hasRow {
				return subscriptionBatchMessage{}, errors.New("subscription membership and changed rows disagree")
			}
		}
		change, changed := latest[key]
		if changed || before != after {
			if !changed {
				change.Row = v.rows[key].Row
			}
			switch {
			case !before && after:
				change.Row = v.rows[key].Row
				result.Changes = append(result.Changes, cloneQueryChange(change, querysubscription.Added))
			case before && after:
				change.Row = v.rows[key].Row
				result.Changes = append(result.Changes, cloneQueryChange(change, querysubscription.Updated))
			case before && !after:
				result.Changes = append(result.Changes, cloneQueryChange(change, querysubscription.Removed))
			}
			if after {
				part.versions[key] = v.rows[key].version
			}
		}
		if before == after && !rawTouched[key] {
			continue
		}
		if before {
			result.Membership.Removed = append(result.Membership.Removed, key)
		}
		if after {
			result.Membership.Insertions = append(result.Membership.Insertions, querysubscription.Insertion{Key: key, FinalIndex: finalRank})
		}
	}
	sort.Slice(result.Membership.Insertions, func(i, j int) bool {
		return result.Membership.Insertions[i].FinalIndex < result.Membership.Insertions[j].FinalIndex
	})
	sort.SliceStable(result.Changes, func(i, j int) bool {
		return result.Changes[i].ChangeIndex < result.Changes[j].ChangeIndex
	})
	part.batch = result
	if batch.Window != nil {
		v.bounds = batch.Window.Bounds
	}
	if batch.Window != nil {
		position, err := querysubscription.SnapshotPosition(part.delivery.Checkpoint, part.delivery.Fence)
		if err != nil {
			return subscriptionBatchMessage{}, err
		}
		if v.checkpoint == nil || v.checkpoint.SourceID == position.SourceID && v.checkpoint.Value < position.Value {
			copy := *position
			v.checkpoint = &copy
		}
	}
	return part, nil
}

func cloneQueryChange(change querysubscription.Change, kind querysubscription.ChangeKind) querysubscription.Change {
	change.Kind = kind
	change.Record = storage.CloneRecord(change.Record)
	change.Cursor = append([]byte(nil), change.Cursor...)
	if kind == querysubscription.Removed {
		change.Record = nil
		change.Cursor = nil
		change.Pending = false
	}
	return change
}
