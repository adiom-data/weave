package projectionserver

import (
	"context"
	"errors"
	"sort"

	"github.com/adiom-data/weave/internal/accesscontrol"
	proj "github.com/adiom-data/weave/internal/projection"
	q "github.com/adiom-data/weave/internal/querysubscription/model"
)

// initialPolicyImage retains private, incrementally authorized materialization.
// Each delta is applied even when admission is rejected. Only successful
// admission serializes the complete visible image for the client.
type initialPolicyImage struct {
	visibility *projectionVisibility
	epoch      uint64
	position   *q.Position
	owned      map[string]struct{}
}

func newInitialPolicyImage(selected ...map[string]*fieldSelection) *initialPolicyImage {
	return &initialPolicyImage{visibility: newProjectionVisibility(selected...), owned: make(map[string]struct{})}
}

func (i *initialPolicyImage) apply(ctx context.Context, s *server, prepared accesscontrol.Prepared, batch proj.ProjectionBatch) (bool, error) {
	for _, event := range batch.Events {
		i.owned[event.InstanceID] = struct{}{}
	}
	// This policy-owned path needs evidence, not the generic controller's
	// complete invalidation list (which also contains every unresolved row).
	view, err := s.relationAccess.session.OpenView(batch.Delivery.Checkpoint)
	if err != nil {
		return false, err
	}
	epoch, changed := s.relationAccess.session.FactChangesAfter(i.epoch)
	var targets []accesscontrol.InvalidationTarget
	for _, target := range changed {
		targets = append(targets, accesscontrol.InvalidationTarget{InstanceID: target.Scope, Key: target.Key})
	}
	_, err = i.visibility.filterTransaction(ctx, prepared, view, batch, accesscontrol.Invalidation{Targets: targets})
	err = errors.Join(err, view.Close())
	if err != nil {
		return false, err
	}
	for _, event := range batch.Events {
		if event.Type == proj.ProjectionInstanceClosed {
			delete(i.owned, event.InstanceID)
		}
	}
	i.epoch, i.position = epoch, batch.Delivery.Checkpoint
	return !s.relationAccess.session.PendingIn(func(id string) bool { return i.visibility.instances[id] != nil }), nil
}

func (i *initialPolicyImage) release(ctx context.Context, prepared accesscontrol.Prepared) error {
	var err error
	for id := range i.owned {
		err = errors.Join(err, prepared.Release(ctx, id, nil))
	}
	clear(i.owned)
	return err
}

// borrowEvents returns a private event slice borrowing immutable retained
// record/metadata images. Consume it synchronously without mutating its payloads.
// Visibility replaces owned events rather than mutating their captured values;
// connectionModel takes its own copies at the publication ownership boundary.
func (i *initialPolicyImage) borrowEvents() []proj.ProjectionEvent {
	var result []proj.ProjectionEvent
	var visit func(string)
	visit = func(id string) {
		instance := i.visibility.instances[id]
		if instance == nil || !instance.effective {
			return
		}
		open := instance.open
		open.Checkpoint, open.Initial = i.position, true
		result = append(result, open)
		for _, row := range i.visibility.orderedRows(instance) {
			if !row.effective {
				continue
			}
			event := row.event
			event.Kind, event.Checkpoint, event.Initial = q.Added, i.position, true
			result = append(result, event)
			for _, child := range sortedChildIDs(instance.children[event.Key]) {
				visit(child)
			}
		}
	}
	var roots []string
	for id, instance := range i.visibility.instances {
		if instance.open.ParentInstanceID == "" {
			roots = append(roots, id)
		}
	}
	sort.Strings(roots)
	for _, id := range roots {
		visit(id)
	}
	return result
}
