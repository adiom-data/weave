package projection

import q "github.com/adiom-data/weave/internal/querysubscription/model"

// takeSubscriptionLocked transfers the sole Close responsibility and disables
// future window requests atomically. The caller holds Projection.mu and must
// close the returned handle outside that lock.
func (instance *projectionInstance) takeSubscriptionLocked() q.Subscription {
	handle := instance.subscription
	instance.subscription, instance.query = nil, nil
	return handle
}

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

// attachExpansionLocked transfers a complete set of interests into the current
// row image. A sibling retry can remove coverage while a pass is loading; that
// pass must fill the new gap before it can declare the generation expanded.
// Requires processMu and p.mu.
func (p *Projection) attachExpansionLocked(expansion projectionExpansion, ids []string) bool {
	member := expansion.parent.members[expansion.key]
	covered := p.coveredChildrenLocked(member, ids)
	for _, child := range expansion.parent.node.Children {
		if !covered[child] {
			return false
		}
	}
	for _, id := range ids {
		if instance := p.instances[id]; instance != nil {
			member.children = append(member.children, id)
			instance.opening = nil
		}
	}
	member.expanded = true
	return true
}

// coveredChildrenLocked includes private interests still owned by a load as
// well as attached children. Neither list contains entity records. Requires p.mu.
func (p *Projection) coveredChildrenLocked(member *projectionMember, opened []string) map[*ProjectionNode]bool {
	covered := make(map[*ProjectionNode]bool, len(member.children)+len(opened))
	add := func(ids []string) {
		for _, id := range ids {
			if instance := p.instances[id]; instance != nil {
				covered[instance.node] = true
			}
		}
	}
	add(member.children)
	add(opened)
	return covered
}
