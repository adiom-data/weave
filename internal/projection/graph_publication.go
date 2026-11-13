package projection

import "sort"

// publicationStateLocked returns the current reachable unpublished subtree and rows
// whose descendants were invalidated while that subtree was catching up.
// Projection.mu and processMu must be held by the caller.
func (g *projectionGraph) publicationStateLocked(rootIDs []string) ([]*projectionInstance, []projectionExpansion) {
	var reachable []*projectionInstance
	var missing []projectionExpansion
	seen := make(map[string]struct{})
	var visit func(string)
	visit = func(id string) {
		if _, ok := seen[id]; ok {
			return
		}
		seen[id] = struct{}{}
		instance := g.instances[id]
		if instance == nil || instance.published {
			return
		}
		reachable = append(reachable, instance)
		for key, member := range instance.members {
			if len(instance.node.Children) > 0 && !member.pending && !member.expanded {
				missing = append(missing, projectionExpansion{parent: instance, key: key, member: member})
			}
			for _, childID := range member.children {
				visit(childID)
			}
		}
	}
	for _, id := range rootIDs {
		visit(id)
	}
	sort.Slice(reachable, func(i, j int) bool { return reachable[i].id < reachable[j].id })
	return reachable, missing
}

// missingPublishedExpansionsLocked finds descendants invalidated after the
// current-image cut but before the serialized consumer installed the publication.
// Publishing the current parent row first is safe: projection references may be
// temporarily unresolved, and the follow-up expansion publishes its own current
// image. p.mu and processMu must be held by the caller.
func (g *projectionGraph) missingPublishedExpansionsLocked(rootIDs []string) []projectionExpansion {
	var missing []projectionExpansion
	seen := make(map[string]struct{})
	var visit func(string)
	visit = func(id string) {
		if _, ok := seen[id]; ok {
			return
		}
		seen[id] = struct{}{}
		instance := g.instances[id]
		if instance == nil || !instance.published {
			return
		}
		for key, member := range instance.members {
			if len(instance.node.Children) > 0 && !member.pending && !member.expanded {
				missing = append(missing, projectionExpansion{parent: instance, key: key, member: member, revision: instance.revision})
			}
			for _, childID := range member.children {
				visit(childID)
			}
		}
	}
	for _, id := range rootIDs {
		visit(id)
	}
	return missing
}
