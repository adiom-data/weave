package projectionserver

import (
	"context"
	"errors"
	"sort"

	"github.com/adiom-data/weave/internal/accesscontrol"
	"github.com/adiom-data/weave/internal/projection"
	q "github.com/adiom-data/weave/internal/querysubscription/model"
	"github.com/adiom-data/weave/internal/storage"
)

// projectionAuthorizationWork combines raw changes and policy invalidations
// before evaluating anything. Each final occurrence has one parent-first check;
// unchanged invalidated rows retain their authoritative data separately from
// optional policy-only candidate images.
type projectionAuthorizationWork struct {
	targets   []*projectionAuthorizationTarget
	decisions map[int]projectionRowAction
}

// A superseded image is not an authorization denial. It contributes only its
// raw position, never a visibility transition or a publishable record image.
type projectionRowAction uint8

const (
	projectionRowSuperseded projectionRowAction = iota
	projectionRowDenied
	projectionRowAllowed
	projectionRowFailed
	projectionRowPending
)

func projectionRowAuthorization(visible bool) projectionRowAction {
	if visible {
		return projectionRowAllowed
	}
	return projectionRowDenied
}

type projectionAuthorizationTarget struct {
	key        occurrenceRow
	event      projection.ProjectionEvent
	candidate  accesscontrol.Candidate
	changed    bool
	eventIndex int
	visible    bool
	failure    error
	pending    bool
}

func (v *projectionVisibility) authorizationWork(batch projection.ProjectionBatch, invalidation accesscontrol.Invalidation, capture *policyCapture) *projectionAuthorizationWork {
	work := &projectionAuthorizationWork{decisions: make(map[int]projectionRowAction)}
	byOccurrence := make(map[occurrenceRow]*projectionAuthorizationTarget)
	closed := make(map[string]bool)
	for _, event := range batch.Events {
		if event.Type == projection.ProjectionInstanceClosed {
			closed[event.InstanceID] = true
		}
	}
	add := func(event projection.ProjectionEvent, changed bool) *projectionAuthorizationTarget {
		key := occurrenceRow{event.InstanceID, event.Key}
		if closed[key.instance] || event.Kind == q.Removed {
			return nil
		}
		target := byOccurrence[key]
		if target == nil {
			target = &projectionAuthorizationTarget{key: key, event: event, changed: changed}
			byOccurrence[key] = target
			work.targets = append(work.targets, target)
		}
		return target
	}
	for index, event := range batch.Events {
		if !requiresProjectionDecision(event) {
			continue
		}
		// Multiple changes to one occurrence use its final transaction image.
		// A final removal needs no check. Earlier upserts reserve raw ordering
		// positions but must not affect visibility or publish intermediate data.
		work.decisions[index] = projectionRowSuperseded
		if target := add(capture.updates.rows[occurrenceRow{event.InstanceID, event.Key}], true); target != nil {
			target.eventIndex = index
		}
	}
	current := func(key occurrenceRow) bool {
		open, exists := capture.updates.opens[key.instance]
		if !exists {
			if instance := v.instances[key.instance]; instance != nil {
				open = instance.open
			}
		}
		if baseline, position := open.Checkpoint, batch.Delivery.Checkpoint; baseline != nil && position != nil && baseline.SourceID == position.SourceID && baseline.Value > position.Value {
			return false // An older invalidation cannot rewind a newer snapshot.
		}
		return true
	}
	retained := func(key occurrenceRow) *projectionAuthorizationTarget {
		if !current(key) {
			return nil
		}
		event, exists := capture.updates.rows[key]
		if !exists {
			instance := v.instances[key.instance]
			if instance == nil || instance.rows[key.key] == nil {
				return nil
			}
			event = instance.rows[key.key].event
		}
		return add(event, false)
	}
	policyImages := make(map[storage.EntityKey]accesscontrol.Candidate, len(invalidation.Candidates))
	for _, candidate := range invalidation.Candidates {
		policyImages[candidate.Key] = candidate
		for id := range v.occurrences[candidate.Key] {
			retained(occurrenceRow{id, candidate.Key})
		}
	}
	for _, target := range invalidation.Targets {
		retained(occurrenceRow{target.InstanceID, target.Key})
	}
	// A parent's policy/computed-value invalidation can alter propagation even
	// without a raw row change. Reevaluate its descendants in the same pass.
	roots := make([]projection.ProjectionEvent, len(work.targets))
	for i, target := range work.targets {
		roots[i] = target.event
	}
	for _, target := range v.descendantTargets(roots) {
		retained(occurrenceRow{target.InstanceID, target.Key})
	}
	for _, target := range work.targets {
		candidate := capture.candidate(target.event)
		if override, exists := policyImages[target.event.Key]; exists && current(target.key) {
			// Invalidation images are policy inputs, never stored row images.
			candidate.Record, candidate.Complete = override.Record, override.Complete
		}
		target.candidate = candidate
	}
	sort.SliceStable(work.targets, func(i, j int) bool {
		return work.targets[i].candidate.Context.Projection.Len() < work.targets[j].candidate.Context.Projection.Len()
	})
	return work
}

func (w *projectionAuthorizationWork) evaluate(ctx context.Context, prepared accesscontrol.Prepared, view accesscontrol.View, partial bool) error {
	if len(w.targets) == 0 {
		return nil
	}
	candidates := make([]accesscontrol.Candidate, len(w.targets))
	for index, target := range w.targets {
		candidates[index] = target.candidate
	}
	visible, err := prepared.Evaluate(ctx, view, candidates)
	if err != nil {
		if !partial || ctx.Err() != nil {
			return err
		}
		visible = make([]accesscontrol.Decision, len(candidates))
		for i := range visible {
			visible[i].Err = err
		}
	}
	if len(visible) != len(w.targets) {
		return errors.New("access controller returned the wrong number of decisions")
	}
	for index, target := range w.targets {
		if visible[index].Err != nil && !partial {
			return visible[index].Err
		}
		target.visible, target.failure = visible[index].Visible, visible[index].Err
		target.pending = visible[index].Pending && target.failure == nil
		target.event.Computed = computedValues(prepared, target.key.instance, target.key.key)
		if target.changed {
			// Only the final raw image can be published under this decision.
			// Earlier same-occurrence images may not satisfy its policy.
			w.decisions[target.eventIndex] = projectionRowAuthorization(visible[index].Visible)
			if target.failure != nil {
				w.decisions[target.eventIndex] = projectionRowFailed
			}
			if target.pending {
				w.decisions[target.eventIndex] = projectionRowPending
			}
		}
	}
	return nil
}
