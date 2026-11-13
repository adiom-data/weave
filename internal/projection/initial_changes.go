package projection

import (
	"errors"
	"sort"

	q "github.com/adiom-data/weave/internal/querysubscription/model"
	"github.com/adiom-data/weave/internal/storage"
)

// initialChanges coalesces identities, not records or transaction batches.
// Current records remain owned by Projection. All mutations hold p.mu; reads
// also hold processMu so the consumer accepts one coherent materialization cut.
type initialChanges struct {
	started    bool
	opened     map[string]struct{}
	closed     map[string]ProjectionEvent
	rows       map[string]map[storage.EntityKey]bool // membership before the first mutation
	membership map[string]map[storage.EntityKey]bool
	branches   map[string]ProjectionEvent
}

func newInitialChanges() *initialChanges {
	return &initialChanges{opened: make(map[string]struct{}), closed: make(map[string]ProjectionEvent), rows: make(map[string]map[storage.EntityKey]bool), membership: make(map[string]map[storage.EntityKey]bool), branches: map[string]ProjectionEvent{}}
}

func (c *initialChanges) row(instance *projectionInstance, key storage.EntityKey) {
	if !c.started {
		return
	}
	if _, fresh := c.opened[instance.id]; fresh {
		return
	}
	if c.rows[instance.id] == nil {
		c.rows[instance.id] = make(map[storage.EntityKey]bool)
	}
	if _, exists := c.rows[instance.id][key]; !exists {
		_, present := instance.members[key]
		c.rows[instance.id][key] = present
	}
}

func (c *initialChanges) move(instance *projectionInstance, key storage.EntityKey) {
	if !c.started {
		return
	}
	if _, fresh := c.opened[instance.id]; fresh {
		return
	}
	if c.membership[instance.id] == nil {
		c.membership[instance.id] = make(map[storage.EntityKey]bool)
	}
	if _, exists := c.membership[instance.id][key]; !exists {
		_, present := instance.members[key]
		c.membership[instance.id][key] = present
	}
}

func (c *initialChanges) open(id string) {
	if c.started {
		c.opened[id] = struct{}{}
	}
}
func (c *initialChanges) close(event ProjectionEvent) {
	if !c.started {
		return
	}
	if _, fresh := c.opened[event.InstanceID]; fresh {
		delete(c.opened, event.InstanceID)
	} else {
		c.closed[event.InstanceID] = event
	}
	delete(c.rows, event.InstanceID)
	delete(c.membership, event.InstanceID)
	delete(c.branches, event.InstanceID)
}

// initialDelta is called only after all expansions and their source fences
// are covered. It consumes the journal; the admission owner must retain/apply
// each returned delta even when authorization is not yet resolved.
func (p *Projection) initialDelta(root string) (ProjectionBatch, error) {
	p.mu.Lock()
	c := p.publication.initialChanges
	first := !c.started
	if len(p.latestCheckpoints) > 1 {
		p.mu.Unlock()
		return ProjectionBatch{}, errors.New("initial admission requires one ordered source")
	}
	var position *q.Position
	for _, at := range p.latestCheckpoints {
		position = clonePosition(at)
		break
	}
	batch := ProjectionBatch{Delivery: q.DeliveryMetadata{Kind: q.DeliveryHydration, Checkpoint: position, Revision: p.latestRevision}}
	p.mu.Unlock()
	if first {
		events, err := p.currentEvents(root)
		if err != nil {
			return ProjectionBatch{}, err
		}
		batch.Events = events
	} else {
		p.mu.Lock()
		for _, event := range c.closed {
			batch.Events = append(batch.Events, event)
		}
		for _, event := range c.branches {
			batch.Events = append(batch.Events, event)
		}
		sort.Slice(batch.Events, func(i, j int) bool { return len(batch.Events[i].Path) > len(batch.Events[j].Path) })
		for id, dirty := range c.rows {
			instance := p.graph.instances[id]
			if instance == nil {
				continue
			}
			for key, before := range dirty {
				record, present := instance.members[key]
				if !present && !before {
					continue
				}
				event := ProjectionEvent{Type: ProjectionRowChanged, InstanceID: id, Node: instance.node.Name, EntityNode: instance.node.Entity, Path: append([]string(nil), instance.path...), Lineage: cloneLineage(instance.lineage), ParentInstanceID: instance.parentID, ParentEntityKey: instance.parentKey, Key: key, Kind: q.Removed, Checkpoint: position}
				if present {
					event.Kind = q.Added
					if before {
						event.Kind = q.Updated
					}
					event.Record, event.Version = storage.CloneRecord(record.record), record.version
					event.Pending, event.Cursor = record.pending, append([]byte(nil), record.cursor...)
				}
				batch.Events = append(batch.Events, event)
			}
		}
		for id, dirty := range c.membership {
			instance := p.graph.instances[id]
			if instance == nil || instance.node.Entity {
				continue
			}
			delta := q.MembershipDelta{}
			for key, before := range dirty {
				if before {
					delta.Removed = append(delta.Removed, key)
				}
			}
			for index, key := range instance.order {
				if _, changed := dirty[key]; changed {
					delta.Insertions = append(delta.Insertions, q.Insertion{Key: key, FinalIndex: index})
				}
			}
			if batch.Membership == nil {
				batch.Membership = make(map[string]q.MembershipDelta)
			}
			batch.Membership[id] = delta
		}
		var roots []string
		for id := range c.opened {
			instance := p.graph.instances[id]
			if instance == nil {
				continue
			}
			if _, parentNew := c.opened[instance.parentID]; !parentNew {
				roots = append(roots, id)
			}
		}
		p.mu.Unlock()
		sort.Strings(roots)
		for _, id := range roots {
			events, err := p.currentEvents(id)
			if err != nil {
				return ProjectionBatch{}, err
			}
			batch.Events = append(batch.Events, events...)
		}
	}
	p.mu.Lock()
	p.publication.advanceJournalLocked()
	p.mu.Unlock()
	return batch, nil
}
