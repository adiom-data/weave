package projection

import "fmt"

// A failed placeholder and its in-flight replacement occupy one logical slot.
// The placeholder owns no query or rows, but stays addressable until the atomic
// replacement publication. Reference counting keeps either close order safe.
type instanceSlot struct{ references int }

// failedReplacementLocked identifies only this node's failed predecessor, never
// an unrelated failure whose capacity another expansion could borrow.
// Requires p.mu (and processMu when used during installation).
func (p *Projection) failedReplacementLocked(node *ProjectionNode, owner *projectionExpansion) *projectionInstance {
	if owner == nil {
		previous := p.instances[p.rootID]
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
		if previous := p.instances[id]; previous != nil && previous.failure != nil {
			return previous
		}
	}
	return nil
}

func (p *Projection) replacementSlotLocked(node *ProjectionNode, owner *projectionExpansion) *instanceSlot {
	if previous := p.failedReplacementLocked(node, owner); previous != nil && previous.slot != nil && previous.slot.references == 1 {
		return previous.slot
	}
	return nil
}

func (p *Projection) canInstallInstanceLocked(node *ProjectionNode, owner *projectionExpansion) bool {
	return p.activeSlots < p.limits.MaxActiveInstances || p.replacementSlotLocked(node, owner) != nil
}

// installInstanceLocked is the sole registration/capacity boundary. The early
// preflight before IO is advisory; this check decides concurrent admissions.
// Requires p.mu and processMu.
func (p *Projection) installInstanceLocked(instance *projectionInstance) error {
	if p.instances[instance.id] != nil {
		return fmt.Errorf("projection instance %q is already installed", instance.id)
	}
	slot := p.replacementSlotLocked(instance.node, instance.opening)
	if slot == nil {
		if p.activeSlots >= p.limits.MaxActiveInstances {
			return fmt.Errorf("projection exceeds maximum of %d active instances", p.limits.MaxActiveInstances)
		}
		slot = &instanceSlot{}
		p.activeSlots++
	}
	slot.references++
	instance.slot = slot
	p.instances[instance.id] = instance
	if p.initialChanges != nil {
		p.initialChanges.open(instance.id)
	}
	return nil
}

func (p *Projection) releaseInstanceSlotLocked(instance *projectionInstance) {
	if slot := instance.slot; slot != nil {
		instance.slot = nil
		slot.references--
		if slot.references == 0 {
			p.activeSlots--
		}
	}
}
