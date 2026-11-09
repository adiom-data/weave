package projectionserver

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/adiom-data/weave/internal/accesscontrol"
	"github.com/adiom-data/weave/internal/projection"
	"github.com/adiom-data/weave/internal/querysubscription/model"
	"github.com/adiom-data/weave/internal/storage"
	"github.com/adiom-data/weave/internal/visibilityindex"
)

// projectionVisibility retains only connection-local authorization decisions
// and raw projection topology. Entities remain owned by the shared cache and
// projection. Keeping decisions per occurrence (rather than per entity) makes
// diamonds correct: one path may be pruned while another remains visible.
type projectionVisibility struct {
	selected  map[string]*fieldSelection
	instances map[string]*projectionVisibilityInstance
	// Indexed against consumer state, which can intentionally lag the raw
	// projection actor while an initial image is being authorized.
	occurrences map[storage.EntityKey]map[string]*projectionVisibilityRow
}

type projectionVisibilityInstance struct {
	open      projection.ProjectionEvent
	effective bool
	rows      map[storage.EntityKey]*projectionVisibilityRow
	order     *visibilityindex.Index
	children  map[storage.EntityKey]map[string]struct{}
}

type projectionVisibilityRow struct {
	event     projection.ProjectionEvent
	allowed   bool
	effective bool
}

// visibilityChanges records only occurrences whose raw position or effective
// visibility can change in one batch. Every other visible occurrence keeps its
// relative order, so removing and reinserting this set at final visible ranks
// produces an exact delta without diffing the whole window.
type visibilityChanges struct {
	byInstance map[string]*instanceVisibilityChanges
}

type instanceVisibilityChanges struct {
	before map[storage.EntityKey]bool
	raw    map[storage.EntityKey]bool
	order  []storage.EntityKey
}

func newVisibilityChanges() *visibilityChanges {
	return &visibilityChanges{byInstance: make(map[string]*instanceVisibilityChanges)}
}

func (c *visibilityChanges) mark(instanceID string, instance *projectionVisibilityInstance, key storage.EntityKey) {
	changes := c.byInstance[instanceID]
	if changes == nil {
		changes = &instanceVisibilityChanges{before: make(map[storage.EntityKey]bool), raw: make(map[storage.EntityKey]bool)}
		c.byInstance[instanceID] = changes
	}
	if _, marked := changes.before[key]; marked {
		return
	}
	visible, _ := instance.order.Visible(key)
	changes.before[key] = visible
	changes.order = append(changes.order, key)
}

func (c *visibilityChanges) markRaw(instanceID string, instance *projectionVisibilityInstance, key storage.EntityKey) {
	c.mark(instanceID, instance, key)
	c.byInstance[instanceID].raw[key] = true
}

func newProjectionVisibility(selected ...map[string]*fieldSelection) *projectionVisibility {
	return &projectionVisibility{selected: optionalProjectionSelections(selected), instances: make(map[string]*projectionVisibilityInstance), occurrences: make(map[storage.EntityKey]map[string]*projectionVisibilityRow)}
}

func requiresProjectionDecision(event projection.ProjectionEvent) bool {
	// A pending reference can have no payload at all. The policy still decides
	// using its identity/path and Complete=false, rather than failing the scope.
	return event.Type == projection.ProjectionRowChanged && event.Kind != querysubscription.Removed
}

// filterBatch handles publications without authorization invalidations.
func (v *projectionVisibility) filterBatch(ctx context.Context, prepared accesscontrol.Prepared, view accesscontrol.View, batch projection.ProjectionBatch) (projection.ProjectionBatch, error) {
	return v.filterTransaction(ctx, prepared, view, batch, accesscontrol.Invalidation{})
}

// filterTransaction is the single publication boundary for raw effects and
// authorization effects, including scoped initialization catch-up. Membership
// deltas are computed only after both have reached their final state.
// A policy or structural error is terminal for v: no output has been published,
// but some raw state may have advanced, so the caller must discard this scope.
func (v *projectionVisibility) filterTransaction(ctx context.Context, prepared accesscontrol.Prepared, view accesscontrol.View, batch projection.ProjectionBatch, invalidation accesscontrol.Invalidation) (projection.ProjectionBatch, error) {
	if invalidation.All {
		return projection.ProjectionBatch{}, errVisibilityResyncRequired
	}
	if (len(invalidation.Candidates) > 0 || len(invalidation.Targets) > 0) && (batch.Delivery.Kind != querysubscription.DeliveryCommit && batch.Delivery.Kind != querysubscription.DeliveryHydration || batch.Delivery.Checkpoint == nil) {
		return projection.ProjectionBatch{}, errors.New("visibility invalidation requires a committed projection frame")
	}
	capture := newPolicyCapture(v, overlay(batch.Events))
	work := v.authorizationWork(batch, invalidation, capture)
	if err := work.evaluate(ctx, prepared, view); err != nil {
		return projection.ProjectionBatch{}, err
	}
	transition, err := v.stageVisibility(batch.Events, work.decisions, batch.Membership, work.targets)
	if err != nil {
		return projection.ProjectionBatch{}, err
	}
	events, err := transition.publish()
	if err != nil {
		return projection.ProjectionBatch{}, err
	}
	for _, event := range batch.Events {
		if event.Type == projection.ProjectionInstanceClosed {
			if err := prepared.Release(ctx, event.InstanceID, nil); err != nil {
				return projection.ProjectionBatch{}, err
			}
		}
		if event.Type == projection.ProjectionRowChanged && event.Kind == querysubscription.Removed {
			if final := capture.updates.rows[occurrenceRow{event.InstanceID, event.Key}]; final.Kind != querysubscription.Removed {
				continue
			}
			if err := prepared.Release(ctx, event.InstanceID, []storage.EntityKey{event.Key}); err != nil {
				return projection.ProjectionBatch{}, err
			}
		}
	}
	result := batch
	result.Events = events
	result.Membership = nil
	for instanceID, dirty := range transition.changes.byInstance {
		instance := v.instances[instanceID]
		if instance == nil {
			continue
		}
		visibleDelta := instance.visibleDelta(dirty)
		if len(visibleDelta.Removed) > 0 || len(visibleDelta.Insertions) > 0 {
			if result.Membership == nil {
				result.Membership = make(map[string]querysubscription.MembershipDelta)
			}
			result.Membership[instanceID] = visibleDelta
		}
	}
	return result, nil
}

// filterInitialProjection evaluates each table-level snapshot at the source
// checkpoint actually incorporated by that projection instance. The resulting
// gate is retained so a later authorization invalidation can reversibly expose
// a previously pruned subtree without rebuilding the projection.
func (v *projectionVisibility) filterInitial(ctx context.Context, controller accesscontrol.Controller, prepared accesscontrol.Prepared, events []projection.ProjectionEvent) (_ []projection.ProjectionEvent, err error) {
	views := make(map[string]accesscontrol.View)
	sharedViews := make(map[visibilityPoint]accesscontrol.View)
	defer func() {
		for _, view := range sharedViews {
			if closeErr := view.Close(); err == nil && closeErr != nil {
				err = fmt.Errorf("close access-control view: %w", closeErr)
			}
		}
	}()
	rowsByInstance := make(map[string][]int)
	for _, event := range events {
		if event.Type != projection.ProjectionInstanceOpened {
			continue
		}
		point, err := visibilityPointAt(event.Checkpoint, event.Fence)
		if err != nil {
			return nil, err
		}
		view := sharedViews[point]
		if view == nil {
			view, err = acquireVisibilityAt(ctx, controller, prepared, event.Checkpoint, event.Fence)
			if err != nil {
				return nil, err
			}
			sharedViews[point] = view
		}
		views[event.InstanceID] = view
	}
	for index, event := range events {
		if requiresProjectionDecision(event) {
			rowsByInstance[event.InstanceID] = append(rowsByInstance[event.InstanceID], index)
		}
	}
	decisions := make(map[int]projectionRowAction)
	capture := newPolicyCapture(v, overlay(events))
	for instanceID, indexes := range rowsByInstance {
		view := views[instanceID]
		if view == nil {
			return nil, errors.New("projection row has no stable authorization view")
		}
		candidates := make([]accesscontrol.Candidate, len(indexes))
		for candidateIndex, eventIndex := range indexes {
			event := events[eventIndex]
			candidates[candidateIndex] = capture.candidate(event)
		}
		visible, err := prepared.Evaluate(ctx, view, candidates)
		if err != nil {
			return nil, err
		}
		if len(visible) != len(indexes) {
			return nil, errors.New("access controller returned the wrong number of decisions")
		}
		for decision, eventIndex := range indexes {
			decisions[eventIndex] = projectionRowAuthorization(visible[decision])
			events[eventIndex].Computed = computedValues(prepared, events[eventIndex].InstanceID, events[eventIndex].Key)
		}
	}
	transition, err := v.stageVisibility(events, decisions, nil, nil)
	if err != nil {
		return nil, err
	}
	return transition.publish()
}

func (v *projectionVisibility) setRowEffective(instanceID string, instance *projectionVisibilityInstance, key storage.EntityKey, row *projectionVisibilityRow, effective bool, changes *visibilityChanges) error {
	if row.effective == effective {
		return nil
	}
	changes.mark(instanceID, instance, key)
	row.effective = effective
	if instance.order.Contains(key) {
		_, err := instance.order.SetVisible(key, effective)
		return err
	}
	return nil
}

func (v *projectionVisibility) rowEffective(instanceID string, key storage.EntityKey) bool {
	instance := v.instances[instanceID]
	return instance != nil && instance.rows[key] != nil && instance.rows[key].effective
}

func (v *projectionVisibility) orderedRows(instance *projectionVisibilityInstance) []*projectionVisibilityRow {
	rows := make([]*projectionVisibilityRow, 0, instance.order.Len())
	for _, key := range instance.order.Keys() {
		if row := instance.rows[key]; row != nil {
			rows = append(rows, row)
		}
	}
	return rows
}

func (instance *projectionVisibilityInstance) visibleDelta(changes *instanceVisibilityChanges) querysubscription.MembershipDelta {
	delta := querysubscription.MembershipDelta{}
	for _, key := range changes.order {
		before := changes.before[key]
		finalRank, after := instance.order.VisibleRank(key)
		if before == after && !changes.raw[key] {
			continue
		}
		if before {
			delta.Removed = append(delta.Removed, key)
		}
		if after {
			delta.Insertions = append(delta.Insertions, querysubscription.Insertion{Key: key, FinalIndex: finalRank})
		}
	}
	sort.Slice(delta.Insertions, func(left, right int) bool {
		return delta.Insertions[left].FinalIndex < delta.Insertions[right].FinalIndex
	})
	return delta
}

func (v *projectionVisibility) detachInstance(id string) {
	instance := v.instances[id]
	if instance == nil {
		return
	}
	for key := range instance.rows {
		v.removeOccurrence(key, id)
	}
	if parent := v.instances[instance.open.ParentInstanceID]; parent != nil {
		children := parent.children[instance.open.ParentEntityKey]
		delete(children, id)
		if len(children) == 0 {
			delete(parent.children, instance.open.ParentEntityKey)
		}
	}
	delete(v.instances, id)
}

func (v *projectionVisibility) removeOccurrence(key storage.EntityKey, id string) {
	delete(v.occurrences[key], id)
	if len(v.occurrences[key]) == 0 {
		delete(v.occurrences, key)
	}
}

func sortedChildIDs(children map[string]struct{}) []string {
	ids := make([]string, 0, len(children))
	for id := range children {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

func cloneProjectionEvent(event projection.ProjectionEvent) projection.ProjectionEvent {
	event.Path = append([]string(nil), event.Path...)
	event.Lineage = append([]projection.PathStep(nil), event.Lineage...)
	event.Record = storage.CloneRecord(event.Record)
	event.Computed = storage.CloneRecord(event.Computed)
	event.Cursor = append([]byte(nil), event.Cursor...)
	return event
}

func removedProjectionRow(event projection.ProjectionEvent) projection.ProjectionEvent {
	// Removal owns only metadata; do not deep-copy stored/computed payloads or
	// a cursor that is immediately discarded. Clear them before cloning slices.
	event.Record = nil
	event.Computed = nil
	event.Cursor = nil
	event = cloneProjectionEvent(event)
	event.Kind = querysubscription.Removed
	event.Pending = false
	return event
}

func closedProjectionInstance(open projection.ProjectionEvent) projection.ProjectionEvent {
	return projection.ProjectionEvent{
		Type: projection.ProjectionInstanceClosed, Path: append([]string(nil), open.Path...), Lineage: append([]projection.PathStep(nil), open.Lineage...), Node: open.Node,
		EntityNode: open.EntityNode, InstanceID: open.InstanceID, ParentInstanceID: open.ParentInstanceID,
		ParentEntityKey: open.ParentEntityKey,
	}
}
