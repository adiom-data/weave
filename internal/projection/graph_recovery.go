package projection

import (
	"reflect"
	"time"

	"github.com/adiom-data/weave/internal/projection/retry"
	q "github.com/adiom-data/weave/internal/querysubscription/model"
	"github.com/adiom-data/weave/internal/storage"
)

// scheduleRecoveryLocked indexes an installed failed instance and preserves its
// existing retry budget. The runtime owns waking the recovery worker.
func (g *projectionGraph) scheduleRecoveryLocked(instance *projectionInstance, now time.Time, policy retry.Policy) {
	g.failed[instance.id] = instance
	instance.recovery.Failed(now, policy)
}

// failInstanceLocked leaves an addressable error placeholder, but transfers its
// subscription to the runtime and removes its live rows. The caller holds both
// runtime locks and captures publication bookkeeping before this transition.
func (g *projectionGraph) failInstanceLocked(instance *projectionInstance, cause error, now time.Time, policy retry.Policy) q.Subscription {
	instance.members, instance.order, instance.failure = map[storage.EntityKey]*projectionMember{}, nil, cause
	g.scheduleRecoveryLocked(instance, now, policy)
	return instance.takeSubscriptionLocked()
}

func (g *projectionGraph) isRootLocked(instance *projectionInstance) bool {
	return g.instances[instance.id] == instance && g.rootID == instance.id
}

// replaceRootLocked switches only a still-current predecessor to an installed
// replacement. The predecessor stays resident until replacement publication.
// Requires Projection.mu and processMu.
func (g *projectionGraph) replaceRootLocked(previous, next *projectionInstance) bool {
	if !g.isRootLocked(previous) || g.instances[next.id] != next {
		return false
	}
	g.rootID, next.replaces = next.id, previous.id
	delete(g.failed, previous.id)
	return true
}

// Only unchanged selectors may reuse a failed predecessor's opaque cursors.
// A parent rebind creates a different query and resolves its own initial window.
// Requires Projection.mu and processMu, so bounds see reconciled ownership.
func (g *projectionGraph) replacementBoundsLocked(node *ProjectionNode, spec storage.QuerySpec, owner *projectionExpansion) *q.Bounds {
	if node.Entity {
		return nil
	}
	previous := g.failedReplacementLocked(node, owner)
	if previous != nil && reflect.DeepEqual(previous.spec, spec) && previous.initial.Bounds.Validate() == nil {
		return previous.initial.Bounds.Clone()
	}
	return nil
}

// recoveryWorkLocked moves due failed children into replacement slots and
// returns work to schedule. It never loads, publishes, or waits. Both runtime
// locks must be held, as for other selector-generation transitions.
func (g *projectionGraph) recoveryWorkLocked(now time.Time) (time.Time, *projectionInstance, []projectionExpansion) {
	var next time.Time
	var root *projectionInstance
	var expansions []projectionExpansion
	seen := map[expansionKey]bool{}
	for id, instance := range g.failed {
		if !instance.published || !g.instanceCurrentLocked(instance) {
			continue
		}
		if instance.parentID == "" {
			if instance.recovery.Take(now) {
				root = instance
			} else {
				next = retry.Earlier(next, instance.recovery.Next())
			}
			continue
		}
		parent := g.instances[instance.parentID]
		if parent == nil {
			continue
		}
		member := parent.members[instance.parentKey]
		if member == nil || member.pending {
			continue
		}
		for i, child := range member.children {
			if child != id {
				continue
			}
			if !instance.recovery.Take(now) {
				next = retry.Earlier(next, instance.recovery.Next())
				break
			}
			member.children = append(member.children[:i], member.children[i+1:]...)
			if member.replacementRoots == nil {
				member.replacementRoots = map[*ProjectionNode][]string{}
			}
			member.replacementRoots[instance.node] = append(member.replacementRoots[instance.node], id)
			member.expanded = false
			delete(g.failed, id)
			key := expansionKey{parent.id, instance.parentKey}
			if !seen[key] {
				expansions = append(expansions, projectionExpansion{parent: parent, key: instance.parentKey, member: member, revision: parent.revision})
				seen[key] = true
			}
			break
		}
	}
	return next, root, expansions
}
