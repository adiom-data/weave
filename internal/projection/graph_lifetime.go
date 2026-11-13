package projection

// instanceCurrentLocked distinguishes a registered load from a current
// interest. A child can be installed before its siblings finish and before its
// parent attaches it. Ownership must therefore be checked through every still
// opening ancestor, not inferred from registration or a subscription ID prefix.
// Published predecessors remain current until their atomic replacement retires
// them, even if their original parent's selector has changed.
// Projection.mu must be held. Checks that classify work against a concurrent
// parent update also hold processMu, covering row replacement AND reconciliation.
func (g *projectionGraph) instanceCurrentLocked(instance *projectionInstance) bool {
	for instance != nil {
		if g.instances[instance.id] != instance {
			return false
		}
		if instance.published || instance.parentID == "" {
			return true
		}
		parent := g.instances[instance.parentID]
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
func (g *projectionGraph) attachExpansionLocked(expansion projectionExpansion, ids []string) bool {
	member := expansion.parent.members[expansion.key]
	covered := g.coveredChildrenLocked(member, ids)
	for _, child := range expansion.parent.node.Children {
		if !covered[child] {
			return false
		}
	}
	for _, id := range ids {
		if instance := g.instances[id]; instance != nil {
			member.children = append(member.children, id)
			instance.opening = nil
		}
	}
	member.expanded = true
	return true
}

// coveredChildrenLocked includes private interests still owned by a load as
// well as attached children. Neither list contains entity records. Requires p.mu.
func (g *projectionGraph) coveredChildrenLocked(member *projectionMember, opened []string) map[*ProjectionNode]bool {
	covered := make(map[*ProjectionNode]bool, len(member.children)+len(opened))
	add := func(ids []string) {
		for _, id := range ids {
			if instance := g.instances[id]; instance != nil {
				covered[instance.node] = true
			}
		}
	}
	add(member.children)
	add(opened)
	return covered
}

// reconcileChildrenLocked transfers compatible selectors to the new row image.
// Published predecessors stay live until their own replacement is published.
// The result contains only interests that the runtime can retire immediately.
func (g *projectionGraph) reconcileChildrenLocked(previous, next *projectionMember, unchanged map[*ProjectionNode]bool, all, public bool) []string {
	next.replacementRoots = previous.replacementRoots
	var obsolete []string
	for _, id := range previous.children {
		instance := g.instances[id]
		if instance == nil {
			continue
		}
		if unchanged[instance.node] {
			next.children = append(next.children, id)
		} else if public && instance.published {
			if next.replacementRoots == nil {
				next.replacementRoots = make(map[*ProjectionNode][]string)
			}
			next.replacementRoots[instance.node] = append(next.replacementRoots[instance.node], id)
		} else {
			obsolete = append(obsolete, id)
		}
	}
	if all {
		// Equivalent selectors retain both attached children and in-flight loads.
		next.expansionToken, next.expanded = previous.expansionToken, previous.expanded
	}
	return obsolete
}

// takeReplacedChildrenLocked consumes only the attached replacement's slot.
// A stale sibling publication cannot retire another slot's predecessor.
func (g *projectionGraph) takeReplacedChildrenLocked(instance *projectionInstance) []string {
	parent := g.instances[instance.parentID]
	if parent == nil {
		return nil
	}
	member := parent.members[instance.parentKey]
	if member == nil {
		return nil
	}
	for _, id := range member.children {
		if id == instance.id {
			roots := member.replacementRoots[instance.node]
			delete(member.replacementRoots, instance.node)
			return roots
		}
	}
	return nil
}
