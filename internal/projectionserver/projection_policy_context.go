package projectionserver

import (
	"context"
	"errors"
	"reflect"

	"github.com/adiom-data/weave/internal/accesscontrol"
	"github.com/adiom-data/weave/internal/projection"
	q "github.com/adiom-data/weave/internal/querysubscription/model"
	"github.com/adiom-data/weave/internal/storage"
)

type occurrenceRow struct {
	instance string
	key      storage.EntityKey
}
type policyOverlay struct {
	rows  map[occurrenceRow]projection.ProjectionEvent
	opens map[string]projection.ProjectionEvent
}

func overlay(events []projection.ProjectionEvent) policyOverlay {
	result := policyOverlay{rows: make(map[occurrenceRow]projection.ProjectionEvent), opens: make(map[string]projection.ProjectionEvent)}
	for _, event := range events {
		if event.Type == projection.ProjectionInstanceOpened {
			result.opens[event.InstanceID] = event
		}
		if event.Type == projection.ProjectionRowChanged {
			result.rows[occurrenceRow{event.InstanceID, event.Key}] = event
		}
	}
	return result
}

// policyCapture belongs to one publication/evaluation pass, never the mutable
// lifetime of a projection. A final-transaction overlay supplies parent images;
// each concrete occurrence is captured once and siblings share its immutable
// path node. A later pass captures new nodes, leaving in-flight inputs unchanged.
type policyCapture struct {
	visibility *projectionVisibility
	updates    policyOverlay
	paths      map[occurrenceRow]capturedPolicyPath
}
type capturedPolicyPath struct {
	path    *accesscontrol.ProjectionPath
	source  storage.Record
	version q.RecordVersion
}

func newPolicyCapture(v *projectionVisibility, updates policyOverlay) *policyCapture {
	return &policyCapture{visibility: v, updates: updates, paths: make(map[occurrenceRow]capturedPolicyPath)}
}
func (c *policyCapture) candidate(event projection.ProjectionEvent) accesscontrol.Candidate {
	path := c.capture(event)
	step := path.Step()
	return accesscontrol.Candidate{Key: event.Key, Record: step.Record, Complete: step.Complete, Context: accesscontrol.Scope{InstanceID: event.InstanceID, Projection: path, Computed: computedSelection(c.visibility.selected[event.Node])}}
}
func (c *policyCapture) capture(event projection.ProjectionEvent) *accesscontrol.ProjectionPath {
	var parent *accesscontrol.ProjectionPath
	if length := len(event.Lineage); length > 0 {
		expected := event.Lineage[length-1]
		ancestor := projection.ProjectionEvent{Node: expected.Node, Key: expected.Key, InstanceID: event.ParentInstanceID, Lineage: event.Lineage[:length-1], Pending: true}
		// Lineage defines the required path. Missing/removed ancestors remain
		// unresolved placeholders, not a shorter path that could bypass checks.
		if event.ParentInstanceID != "" && event.ParentEntityKey == expected.Key {
			row, found := c.updates.rows[occurrenceRow{event.ParentInstanceID, event.ParentEntityKey}]
			if !found {
				if instance := c.visibility.instances[event.ParentInstanceID]; instance != nil {
					if retained := instance.rows[event.ParentEntityKey]; retained != nil {
						row, found = retained.event, true
					}
				}
			}
			if found && row.Kind != q.Removed && row.Key == expected.Key {
				ancestor.Record, ancestor.Pending, ancestor.Version = row.Record, row.Pending, row.Version
			}
			open, found := c.updates.opens[event.ParentInstanceID]
			if !found {
				if instance := c.visibility.instances[event.ParentInstanceID]; instance != nil {
					open, found = instance.open, true
				}
			}
			if found {
				ancestor.ParentInstanceID, ancestor.ParentEntityKey = open.ParentInstanceID, open.ParentEntityKey
			}
		}
		parent = c.capture(ancestor)
	}
	key := occurrenceRow{event.InstanceID, event.Key}
	if cached, found := c.paths[key]; found && event.InstanceID != "" {
		step := cached.path.Step()
		if cached.path.Parent() == parent && step.Node == event.Node && step.Complete == !event.Pending && cached.version == event.Version && reflect.DeepEqual(cached.source, event.Record) {
			return cached.path
		}
	}
	path := accesscontrol.NewProjectionPath(parent, accesscontrol.ProjectionStep{Node: event.Node, Key: event.Key, Record: event.Record, Complete: !event.Pending})
	if event.InstanceID != "" {
		c.paths[key] = capturedPolicyPath{path: path, source: event.Record, version: event.Version}
	}
	return path
}

func (v *projectionVisibility) release(ctx context.Context, prepared accesscontrol.Prepared) error {
	var err error
	for id := range v.instances {
		err = errors.Join(err, prepared.Release(ctx, id, nil))
	}
	return err
}

// Ancestor field changes can alter a descendant's propagation even if no fact
// selector changed. Visit only the affected retained subtree, not other lists.
func (v *projectionVisibility) descendantTargets(events []projection.ProjectionEvent) []accesscontrol.InvalidationTarget {
	visited := make(map[string]struct{})
	var targets []accesscontrol.InvalidationTarget
	var visit func(string)
	visit = func(id string) {
		if _, exists := visited[id]; exists {
			return
		}
		visited[id] = struct{}{}
		instance := v.instances[id]
		if instance == nil {
			return
		}
		for key := range instance.rows {
			targets = append(targets, accesscontrol.InvalidationTarget{InstanceID: id, Key: key})
			for child := range instance.children[key] {
				visit(child)
			}
		}
	}
	for _, event := range events {
		if event.Type != projection.ProjectionRowChanged {
			continue
		}
		if instance := v.instances[event.InstanceID]; instance != nil {
			for child := range instance.children[event.Key] {
				visit(child)
			}
		}
	}
	return targets
}
