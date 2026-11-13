package projection

import "fmt"

// A failed placeholder and its in-flight replacement occupy one logical slot.
// The placeholder owns no query or rows, but stays addressable until the atomic
// replacement publication. Reference counting keeps either close order safe.
type instanceSlot struct{ references int }

// failedReplacementLocked identifies only this node's failed predecessor, never
// an unrelated failure whose capacity another expansion could borrow.
// Requires Projection.mu (and processMu when used during installation).
func (g *projectionGraph) failedReplacementLocked(node *ProjectionNode, owner *projectionExpansion) *projectionInstance {
	if owner == nil {
		previous := g.instances[g.rootID]
		if previous != nil && previous.node == node && previous.failure != nil {
			return previous
		}
		return nil
	}
	if owner.member == nil {
		return nil
	}
	member := owner.parent.members[owner.key]
	if member == nil || member.expansionToken != owner.member.expansionToken {
		return nil
	}
	for _, id := range member.replacementRoots[node] {
		if previous := g.instances[id]; previous != nil && previous.failure != nil {
			return previous
		}
	}
	return nil
}

func (g *projectionGraph) replacementSlotLocked(node *ProjectionNode, owner *projectionExpansion) *instanceSlot {
	if previous := g.failedReplacementLocked(node, owner); previous != nil && previous.slot != nil && previous.slot.references == 1 {
		return previous.slot
	}
	return nil
}

func (g *projectionGraph) canInstallInstanceLocked(node *ProjectionNode, owner *projectionExpansion, maxInstances int) bool {
	return g.activeSlots < maxInstances || g.replacementSlotLocked(node, owner) != nil
}

// installInstanceLocked is the sole registration/capacity boundary. The early
// preflight before IO is advisory; this check decides concurrent admissions.
// Requires Projection.mu and processMu.
func (g *projectionGraph) installInstanceLocked(instance *projectionInstance, maxInstances int) error {
	if g.instances[instance.id] != nil {
		return fmt.Errorf("projection instance %q is already installed", instance.id)
	}
	slot := g.replacementSlotLocked(instance.node, instance.opening)
	if slot == nil {
		if g.activeSlots >= maxInstances {
			return fmt.Errorf("projection exceeds maximum of %d active instances", maxInstances)
		}
		slot = &instanceSlot{}
		g.activeSlots++
	}
	slot.references++
	instance.slot = slot
	g.instances[instance.id] = instance
	return nil
}

func (g *projectionGraph) releaseInstanceSlotLocked(instance *projectionInstance) {
	if slot := instance.slot; slot != nil {
		instance.slot = nil
		slot.references--
		if slot.references == 0 {
			g.activeSlots--
		}
	}
}
