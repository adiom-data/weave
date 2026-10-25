package projectionserver

import (
	"container/heap"
	"errors"
	"fmt"

	"github.com/adiom-data/weave/internal/projection"
	q "github.com/adiom-data/weave/internal/querysubscription/model"
	"github.com/adiom-data/weave/internal/storage"
	"github.com/adiom-data/weave/internal/visibilityindex"
)

// visibilityTransaction installs final raw state before deriving any effective
// visibility. Old effective bits remain the publication baseline until publish.
// Removed rows/instances survive privately until their final deltas are emitted.
type visibilityTransaction struct {
	v       *projectionVisibility
	changes *visibilityChanges
	updates map[string]*visibilityInstanceUpdate
	queue   visibilityQueue
	depths  map[string]int
	removes []projection.ProjectionEvent
	adds    []projection.ProjectionEvent
}

type visibilityInstanceUpdate struct {
	id       string
	instance *projectionVisibilityInstance
	rows     map[storage.EntityKey]*visibilityRowUpdate
	closed   bool
	queued   bool
	depth    int
}

type visibilityRowUpdate struct {
	changed bool
	removed bool
}

func (v *projectionVisibility) stageVisibility(events []projection.ProjectionEvent, decisions map[int]projectionRowAction, membership map[string]q.MembershipDelta, targets []*projectionAuthorizationTarget) (*visibilityTransaction, error) {
	t := &visibilityTransaction{v: v, changes: newVisibilityChanges(), updates: make(map[string]*visibilityInstanceUpdate), depths: make(map[string]int)}
	if err := t.stageInstances(events); err != nil {
		return nil, err
	}
	if err := t.stageMembership(membership); err != nil {
		return nil, err
	}
	if err := t.stageRows(events, decisions, membership); err != nil {
		return nil, err
	}
	t.stageAuthorization(targets)
	if err := t.validateMembership(); err != nil {
		return nil, err
	}
	return t, nil
}

func (t *visibilityTransaction) stageInstances(events []projection.ProjectionEvent) error {
	// Create every instance first, then attach them. No visibility or public
	// events are derived while topology or membership is incomplete.
	for _, event := range events {
		if event.Type != projection.ProjectionInstanceOpened {
			continue
		}
		if t.v.instances[event.InstanceID] != nil {
			return fmt.Errorf("projection instance %q opened twice", event.InstanceID)
		}
		instance := &projectionVisibilityInstance{open: cloneProjectionEvent(event), rows: make(map[storage.EntityKey]*projectionVisibilityRow), order: visibilityindex.New(), children: make(map[storage.EntityKey]map[string]struct{})}
		t.v.instances[event.InstanceID] = instance
		t.touch(event.InstanceID)
	}
	for _, event := range events {
		if event.Type == projection.ProjectionInstanceClosed {
			if update := t.touch(event.InstanceID); update != nil {
				update.closed = true
			}
		}
		if event.Type != projection.ProjectionInstanceOpened || event.ParentInstanceID == "" {
			continue
		}
		parent := t.v.instances[event.ParentInstanceID]
		if parent == nil {
			return fmt.Errorf("projection instance %q has unknown parent %q", event.InstanceID, event.ParentInstanceID)
		}
		if parent.children[event.ParentEntityKey] == nil {
			parent.children[event.ParentEntityKey] = make(map[string]struct{})
		}
		parent.children[event.ParentEntityKey][event.InstanceID] = struct{}{}
	}
	return nil
}

func (t *visibilityTransaction) stageMembership(membership map[string]q.MembershipDelta) error {
	for id, delta := range membership {
		update := t.touch(id)
		if update == nil {
			return fmt.Errorf("projection membership belongs to unknown instance %q", id)
		}
		if update.closed {
			continue
		}
		for _, key := range delta.Removed {
			t.changes.markRaw(id, update.instance, key)
		}
		for _, insertion := range delta.Insertions {
			t.changes.markRaw(id, update.instance, insertion.Key)
		}
		if err := update.instance.order.ApplyMembership(delta); err != nil {
			return fmt.Errorf("apply raw projection membership: %w", err)
		}
	}
	return nil
}

func (t *visibilityTransaction) stageRows(events []projection.ProjectionEvent, decisions map[int]projectionRowAction, membership map[string]q.MembershipDelta) error {
	for index, event := range events {
		if event.Type == projection.ProjectionInstanceOpened || event.Type == projection.ProjectionInstanceClosed {
			continue
		}
		if event.Type != projection.ProjectionRowChanged {
			return errors.New("unknown projection event type")
		}
		update := t.touch(event.InstanceID)
		if update == nil {
			return fmt.Errorf("projection row belongs to unknown instance %q", event.InstanceID)
		}
		if update.closed {
			continue
		}
		instance := update.instance
		_, controlled := membership[event.InstanceID]
		if event.Kind == q.Removed {
			if !controlled {
				t.changes.markRaw(update.id, instance, event.Key)
				if _, err := instance.order.Remove(event.Key); err != nil {
					return err
				}
			}
			if instance.rows[event.Key] != nil {
				update.row(event.Key).removed = true
			}
			continue
		}
		action, exists := decisions[index]
		if !exists {
			return errors.New("projection row has no authorization decision")
		}
		if !instance.order.Contains(event.Key) {
			if controlled {
				if action != projectionRowSuperseded {
					return errors.New("projection membership does not contain changed row")
				}
			} else {
				t.changes.mark(update.id, instance, event.Key)
				visible := false
				if row := instance.rows[event.Key]; row != nil {
					visible = row.effective
				}
				if err := instance.order.Append(event.Key, visible); err != nil {
					return err
				}
			}
		}
		if action == projectionRowSuperseded {
			continue
		}
		row := instance.rows[event.Key]
		if row == nil {
			row = &projectionVisibilityRow{}
			instance.rows[event.Key] = row
			if t.v.occurrences[event.Key] == nil {
				t.v.occurrences[event.Key] = make(map[string]*projectionVisibilityRow)
			}
			t.v.occurrences[event.Key][event.InstanceID] = row
		}
		row.event, row.allowed = cloneProjectionEvent(event), action == projectionRowAllowed
		change := update.row(event.Key)
		change.changed, change.removed = true, false
	}
	return nil
}

func (t *visibilityTransaction) stageAuthorization(targets []*projectionAuthorizationTarget) {
	// All decisions are installed before a parent can reveal any descendants.
	for _, target := range targets {
		update := t.touch(target.key.instance)
		if update == nil || update.closed {
			continue
		}
		if row := update.instance.rows[target.key.key]; row != nil {
			row.allowed = target.visible
			update.row(target.key.key)
		}
	}
}

func (t *visibilityTransaction) validateMembership() error {
	for id, update := range t.updates {
		if update.closed {
			continue
		}
		consistent := func(key storage.EntityKey) bool {
			change := update.rows[key]
			present := update.instance.rows[key] != nil && (change == nil || !change.removed)
			return update.instance.order.Contains(key) == present
		}
		for key := range update.rows {
			if !consistent(key) {
				return errors.New("projection membership and changed rows disagree")
			}
		}
		if dirty := t.changes.byInstance[id]; dirty != nil {
			for key := range dirty.raw {
				if !consistent(key) {
					return errors.New("projection membership and changed rows disagree")
				}
			}
		}
	}
	return nil
}

func (t *visibilityTransaction) touch(id string) *visibilityInstanceUpdate {
	if update := t.updates[id]; update != nil {
		return update
	}
	instance := t.v.instances[id]
	if instance == nil {
		return nil
	}
	update := &visibilityInstanceUpdate{id: id, instance: instance, rows: make(map[storage.EntityKey]*visibilityRowUpdate)}
	t.updates[id] = update
	return update
}

func (u *visibilityInstanceUpdate) row(key storage.EntityKey) *visibilityRowUpdate {
	if change := u.rows[key]; change != nil {
		return change
	}
	change := &visibilityRowUpdate{}
	u.rows[key] = change
	return change
}

func (t *visibilityTransaction) depth(id string, visiting map[string]bool) (int, error) {
	if depth, exists := t.depths[id]; exists {
		return depth, nil
	}
	instance := t.v.instances[id]
	if instance == nil || visiting[id] {
		return 0, errors.New("projection topology has a missing or cyclic parent")
	}
	visiting[id] = true
	depth := 0
	if parent := instance.open.ParentInstanceID; parent != "" {
		value, err := t.depth(parent, visiting)
		if err != nil {
			return 0, err
		}
		depth = value + 1
	}
	delete(visiting, id)
	t.depths[id] = depth
	return depth, nil
}

func (t *visibilityTransaction) schedule(id string) error {
	update := t.touch(id)
	if update == nil || update.queued {
		return nil
	}
	depth, err := t.depth(id, make(map[string]bool))
	if err != nil {
		return err
	}
	update.depth, update.queued = depth, true
	heap.Push(&t.queue, update)
	return nil
}

func (t *visibilityTransaction) publish() ([]projection.ProjectionEvent, error) {
	for id := range t.updates {
		if err := t.schedule(id); err != nil {
			return nil, err
		}
	}
	for t.queue.Len() > 0 {
		update := heap.Pop(&t.queue).(*visibilityInstanceUpdate)
		instance := update.instance
		before := instance.effective
		effective := !update.closed && (instance.open.ParentInstanceID == "" || t.v.rowEffective(instance.open.ParentInstanceID, instance.open.ParentEntityKey))
		instance.effective = effective
		if before && !effective {
			// Reversing removals below closes descendants before parents.
			t.removes = append(t.removes, closedProjectionInstance(instance.open))
		}
		if !before && effective {
			t.adds = append(t.adds, cloneProjectionEvent(instance.open))
		}
		if before != effective {
			for _, key := range instance.order.Keys() {
				if err := t.publishRow(update, key); err != nil {
					return nil, err
				}
			}
		}
		for key := range update.rows {
			if before != effective && instance.order.Contains(key) {
				continue
			}
			if err := t.publishRow(update, key); err != nil {
				return nil, err
			}
		}
	}
	for id, update := range t.updates {
		if update.closed {
			t.v.detachInstance(id)
			continue
		}
		for key, change := range update.rows {
			if change.removed {
				delete(update.instance.rows, key)
				t.v.removeOccurrence(key, id)
			}
		}
	}
	for left, right := 0, len(t.removes)-1; left < right; left, right = left+1, right-1 {
		t.removes[left], t.removes[right] = t.removes[right], t.removes[left]
	}
	return append(t.removes, t.adds...), nil
}

func (t *visibilityTransaction) publishRow(update *visibilityInstanceUpdate, key storage.EntityKey) error {
	instance := update.instance
	row := instance.rows[key]
	if row == nil {
		return nil
	}
	change := update.rows[key]
	before := row.effective
	effective := instance.effective && row.allowed && (change == nil || !change.removed)
	if err := t.v.setRowEffective(update.id, instance, key, row, effective, t.changes); err != nil {
		return err
	}
	if before && !effective && instance.effective {
		t.removes = append(t.removes, removedProjectionRow(row.event))
	}
	if effective && (!before || change != nil && change.changed) {
		event := cloneProjectionEvent(row.event)
		event.Kind = q.Added
		if before {
			event.Kind = q.Updated
		}
		t.adds = append(t.adds, event)
	}
	if before != effective {
		for id := range instance.children[key] {
			if err := t.schedule(id); err != nil {
				return err
			}
		}
	}
	return nil
}

type visibilityQueue []*visibilityInstanceUpdate

func (q visibilityQueue) Len() int { return len(q) }
func (q visibilityQueue) Less(i, j int) bool {
	if q[i].depth != q[j].depth {
		return q[i].depth < q[j].depth
	}
	return q[i].id < q[j].id
}
func (q visibilityQueue) Swap(i, j int)   { q[i], q[j] = q[j], q[i] }
func (q *visibilityQueue) Push(value any) { *q = append(*q, value.(*visibilityInstanceUpdate)) }
func (q *visibilityQueue) Pop() any {
	values := *q
	last := len(values) - 1
	value := values[last]
	values[last] = nil
	*q = values[:last]
	return value
}
