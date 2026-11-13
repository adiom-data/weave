package projection

import (
	"context"
	"fmt"
	"reflect"
	"time"

	"github.com/adiom-data/weave/internal/projection/retry"
	q "github.com/adiom-data/weave/internal/querysubscription/model"
	"github.com/adiom-data/weave/internal/storage"
)

// failedNode reserves a real topology slot, not a successful empty query. It
// owns no upstream handle or entities and stays addressable until replaced.
func (p *Projection) failedNode(node *ProjectionNode, spec storage.QuerySpec, path []string, owner *projectionExpansion, cause error) *projectionInstance {
	p.processMu.Lock()
	defer p.processMu.Unlock()
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.ctx.Err() != nil || owner != nil && !p.graph.expansionCurrentLocked(*owner) {
		return nil
	}
	instance := &projectionInstance{id: fmt.Sprintf("%s:%d", p.prefix, projectionSequence.Add(1)), node: node, spec: spec, depth: 1, path: append([]string(nil), path...), members: map[storage.EntityKey]*projectionMember{}, failure: cause, opening: owner}
	if previous := p.graph.failedReplacementLocked(node, owner); previous != nil && reflect.DeepEqual(previous.spec, spec) {
		instance.recovery = previous.recovery
	}
	if bounds := p.graph.replacementBoundsLocked(node, spec, owner); bounds != nil {
		instance.initial.Bounds = *bounds
	}
	if owner != nil {
		instance.parentID, instance.parentKey, instance.depth = owner.parent.id, owner.key, owner.parent.depth+1
		instance.lineage = append(cloneLineage(owner.parent.lineage), PathStep{Node: owner.parent.node.Name, Key: owner.key})
	}
	if err := p.installInstanceLocked(instance); err != nil {
		return nil
	}
	p.graph.scheduleRecoveryLocked(instance, time.Now(), p.limits.recoveryPolicy())
	p.signalRecovery()
	return instance
}

// failInstance runs under processMu. Retire raw contents immediately; consumers
// may preserve stale presentation separately, never as live cache references.
// Teardown can remove the instance after delivery routing; in that case there
// is no state transition or publication, and the returned bool is false.
func (p *Projection) failInstance(instance *projectionInstance, cause error) (ProjectionBatch, bool) {
	p.mu.Lock()
	if p.graph.instances[instance.id] != instance || p.terminalErr != nil {
		p.mu.Unlock()
		return ProjectionBatch{}, false
	}
	var events []ProjectionEvent
	var children []string
	removed := append([]storage.EntityKey(nil), instance.order...)
	for key, member := range instance.members {
		if p.publication.initialChanges != nil {
			p.publication.initialChanges.row(instance, key)
			p.publication.initialChanges.move(instance, key)
		}
		children = append(children, member.children...)
		for _, roots := range member.replacementRoots {
			children = append(children, roots...)
		}
		events = append(events, ProjectionEvent{Type: ProjectionRowChanged, InstanceID: instance.id, Node: instance.node.Name, EntityNode: instance.node.Entity, Key: key, Kind: q.Removed})
	}
	handle := p.graph.failInstanceLocked(instance, cause, time.Now(), p.limits.recoveryPolicy())
	p.signalRecovery()
	status := branchEvent(instance, cause)
	if p.publication.initialChanges != nil {
		p.publication.initialChanges.branches[instance.id] = status
	}
	p.mu.Unlock()
	for _, id := range children {
		events = append(events, p.closeInstanceEvents(id)...)
	}
	if handle != nil {
		_ = handle.Close(context.WithoutCancel(p.ctx))
	}
	events = append(events, status)
	p.mu.Lock()
	metadata := q.DeliveryMetadata{Kind: q.DeliveryHydration, Checkpoint: p.publicationCheckpointLocked(instance), Revision: p.latestRevision}
	p.mu.Unlock()
	batch := ProjectionBatch{Events: events, Delivery: metadata}
	if !instance.node.Entity && len(removed) > 0 {
		batch.Membership = map[string]q.MembershipDelta{instance.id: {Removed: removed}}
	}
	return batch, true
}

func branchEvent(instance *projectionInstance, cause error) ProjectionEvent {
	return ProjectionEvent{Type: ProjectionBranchChanged, InstanceID: instance.id, Node: instance.node.Name, EntityNode: instance.node.Entity, Path: append([]string(nil), instance.path...), Lineage: cloneLineage(instance.lineage), ParentInstanceID: instance.parentID, ParentEntityKey: instance.parentKey, Failure: cause}
}

// Retry only failed slots, on bounded backoff schedules. Child IO uses the normal
// bounded scheduler; unique replacement IDs fence late failed-load events.
func (p *Projection) recoverBranches() {
	select {
	case <-p.ready:
	case <-p.ctx.Done():
		return
	}
	for {
		if p.ctx.Err() != nil {
			return
		}
		p.processMu.Lock()
		p.mu.Lock()
		next, root, expansions := p.graph.recoveryWorkLocked(time.Now())
		p.mu.Unlock()
		err := p.startExpansions(expansions)
		p.processMu.Unlock()
		if err != nil {
			p.shutdown(err)
			return
		}
		if root != nil {
			p.recoverRoot(root)
		}
		if !retry.Wait(p.ctx, p.recoveryWake, next) {
			return
		}
	}
}

func (p *Projection) signalRecovery() {
	select {
	case p.recoveryWake <- struct{}{}:
	default:
	}
}

func (p *Projection) recoverRoot(previous *projectionInstance) {
	// An unsuccessful root replacement leaves the same error placeholder.
	// Schedule its next attempt without changing the public branch identity.
	defer func() {
		p.mu.Lock()
		if p.graph.isRootLocked(previous) {
			p.graph.scheduleRecoveryLocked(previous, time.Now(), p.limits.recoveryPolicy())
			p.signalRecovery()
		}
		p.mu.Unlock()
	}()
	spec, matchable, err := previous.node.BuildQuery(nil)
	if err != nil {
		return
	}
	p.mu.Lock()
	if !p.graph.isRootLocked(previous) || p.ctx.Err() != nil {
		p.mu.Unlock()
		return
	}
	p.mu.Unlock()
	next, err := p.loadNode(previous.node, boundQuery{spec: spec, matchable: matchable}, previous.path, nil)
	if err != nil {
		return
	}
	p.processMu.Lock()
	p.mu.Lock()
	current := p.ctx.Err() == nil && p.graph.replaceRootLocked(previous, next)
	p.mu.Unlock()
	p.processMu.Unlock()
	if !current {
		p.closeInstance(next.id)
		return
	}
	if err := p.publishSubtrees([]string{next.id}, 0); err != nil && p.ctx.Err() == nil {
		p.shutdown(err)
	}
}
