package projection

// currentInstance is the common delivery/failure routing boundary. The caller
// holds processMu so parent replacement and ownership reconciliation are one
// transition. Obsolete registered loads are retired before payload evaluation;
// published predecessors remain live until their normal replacement boundary.
func (p *Projection) currentInstance(id string) *projectionInstance {
	p.mu.Lock()
	instance := p.instances[id]
	current := p.instanceCurrentLocked(instance)
	p.mu.Unlock()
	if !current {
		if instance != nil {
			p.closeInstance(id)
		}
		return nil
	}
	return instance
}

// instanceCurrentLocked distinguishes a registered load from a current
// interest. A child can be installed before its siblings finish and before its
// parent attaches it. Ownership must therefore be checked through every still
// opening ancestor, not inferred from registration or a subscription ID prefix.
// Published predecessors remain current until their atomic replacement retires
// them, even if their original parent's selector has changed.
// Projection.mu must be held. Checks that classify work against a concurrent
// parent update also hold processMu, covering row replacement AND reconciliation.
func (p *Projection) instanceCurrentLocked(instance *projectionInstance) bool {
	for instance != nil {
		if p.instances[instance.id] != instance {
			return false
		}
		if instance.published || instance.parentID == "" {
			return true
		}
		parent := p.instances[instance.parentID]
		if parent == nil {
			return false
		}
		if owner := instance.opening; owner != nil {
			member := parent.members[instance.parentKey]
			if parent != owner.parent || member == nil || member.expansionToken != owner.member.expansionToken {
				return false
			}
		}
		instance = parent
	}
	return false
}

// attachExpansionLocked transfers installed interests from the load generation
// to the current parent's ordinary topology. Unchanged parent images share the
// same generation, so they can adopt the same in-flight work safely.
func (p *Projection) attachExpansionLocked(expansion projectionExpansion, ids []string) {
	expansion.parent.members[expansion.key].completeExpansion(ids)
	for _, id := range ids {
		if instance := p.instances[id]; instance != nil {
			instance.opening = nil
		}
	}
}
