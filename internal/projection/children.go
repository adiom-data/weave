package projection

import (
	"reflect"

	"github.com/adiom-data/weave/internal/storage"
)

// reconcileChildren separates immutable row images from expansion lifetimes.
// Unchanged selectors retain subscriptions (including their current windows).
// Published predecessors stay live until their replacements are published, so
// deletions and policy changes still apply to the displayed subtree. Private
// predecessors can be discarded immediately; no consumer has seen them.
// The caller holds processMu; workers may still finish opening old generations.
func (p *Projection) reconcileChildren(parent *projectionInstance, previous, next *projectionMember) ([]ProjectionEvent, bool, error) {
	if previous == nil {
		return nil, false, nil
	}
	if next == nil {
		var closed []ProjectionEvent
		for _, roots := range previous.replacementRoots {
			for _, id := range roots {
				closed = append(closed, p.closeInstanceEvents(id)...)
			}
		}
		for _, id := range previous.children {
			closed = append(closed, p.closeInstanceEvents(id)...)
		}
		return closed, false, nil
	}
	unchanged := make(map[*ProjectionNode]bool)
	all := !previous.pending && !next.pending
	if all {
		for _, child := range parent.node.Children {
			before, err := child.BuildQuery(storage.CloneRecord(previous.record))
			if err != nil {
				return nil, false, err
			}
			after, err := child.BuildQuery(storage.CloneRecord(next.record))
			if err != nil {
				return nil, false, err
			}
			unchanged[child] = reflect.DeepEqual(before, after)
			all = all && unchanged[child]
		}
	}
	p.mu.Lock()
	next.replacementRoots = previous.replacementRoots
	public := p.outputReleased && !p.awaitingAdmission
	var obsolete []*projectionInstance
	for _, id := range previous.children {
		if instance := p.instances[id]; instance != nil && unchanged[instance.node] {
			next.children = append(next.children, id)
		} else {
			if instance != nil {
				obsolete = append(obsolete, instance)
			}
		}
	}
	if all {
		// A row update is not a new expansion generation when its selectors
		// are identical. In-flight loads can safely attach to the new image.
		next.expansionToken, next.expanded = previous.expansionToken, previous.expanded
	}
	p.mu.Unlock()
	var closed []ProjectionEvent
	for _, instance := range obsolete {
		if public && instance.published {
			p.mu.Lock()
			if next.replacementRoots == nil {
				next.replacementRoots = make(map[*ProjectionNode][]string)
			}
			next.replacementRoots[instance.node] = append(next.replacementRoots[instance.node], instance.id)
			p.mu.Unlock()
		} else {
			events := p.closeInstanceEvents(instance.id)
			if !public {
				closed = append(closed, events...)
			}
		}
	}
	return closed, all, nil
}

// retireReplacedChildren requires processMu. A publication can have
// several sibling roots. Each takes only its own slot's detach set, so a stale
// sibling publication cannot clear another slot whose replacement is loading.
func (p *Projection) retireReplacedChildren(instance *projectionInstance) []ProjectionEvent {
	p.mu.Lock()
	parent := p.instances[instance.parentID]
	if parent == nil {
		p.mu.Unlock()
		return nil
	}
	member := parent.members[instance.parentKey]
	if member == nil {
		p.mu.Unlock()
		return nil
	}
	for _, id := range member.children {
		if id == instance.id {
			roots := member.replacementRoots[instance.node]
			delete(member.replacementRoots, instance.node)
			p.mu.Unlock()
			var closed []ProjectionEvent
			for _, id := range roots {
				closed = append(closed, p.closeInstanceEvents(id)...)
			}
			return closed
		}
	}
	p.mu.Unlock()
	return nil
}
