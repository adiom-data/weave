package projectionserver

import (
	"testing"

	proj "github.com/adiom-data/weave/internal/projection"
	q "github.com/adiom-data/weave/internal/querysubscription/model"
	"github.com/adiom-data/weave/internal/storage"
)

func policyContextFixture() []proj.ProjectionEvent {
	project := policyRow("projects", "p1", "Project")
	events := []proj.ProjectionEvent{{Type: proj.ProjectionInstanceOpened, Node: "projects", InstanceID: "projects"}, {Type: proj.ProjectionRowChanged, Node: "projects", InstanceID: "projects", Key: project.Key, Record: project.Record, Kind: q.Added, Version: q.RecordVersion{Revision: 1}}}
	for _, side := range []string{"left", "right"} {
		link := policyRow("project_tasks", side, "Association")
		events = append(events,
			proj.ProjectionEvent{Type: proj.ProjectionInstanceOpened, Node: "links", InstanceID: side + ":links", ParentInstanceID: "projects", ParentEntityKey: project.Key},
			proj.ProjectionEvent{Type: proj.ProjectionRowChanged, Node: "links", InstanceID: side + ":links", ParentInstanceID: "projects", ParentEntityKey: project.Key, Key: link.Key, Record: link.Record, Kind: q.Added, Lineage: []proj.PathStep{{Node: "projects", Key: project.Key}}, Version: q.RecordVersion{Revision: 1}},
			proj.ProjectionEvent{Type: proj.ProjectionInstanceOpened, Node: "task", InstanceID: side + ":task", ParentInstanceID: side + ":links", ParentEntityKey: link.Key},
			proj.ProjectionEvent{Type: proj.ProjectionRowChanged, Node: "task", InstanceID: side + ":task", ParentInstanceID: side + ":links", ParentEntityKey: link.Key, Key: policyRow("tasks", "shared", "Task").Key, Record: modelRecord("shared", "1", "Task"), Kind: q.Added, Lineage: []proj.PathStep{{Node: "projects", Key: project.Key}, {Node: "links", Key: link.Key}}, Version: q.RecordVersion{Revision: 1}},
		)
	}
	return events
}

func TestPolicyCaptureSharesAncestorsWithoutMergingDiamondOccurrences(t *testing.T) {
	events := policyContextFixture()
	capture := newPolicyCapture(newProjectionVisibility(), overlay(events))
	left := capture.candidate(events[5])
	right := capture.candidate(events[9])
	root := capture.candidate(events[1])
	if left.Key != right.Key || left.Context.Projection == right.Context.Projection || left.Context.Projection.Parent() == right.Context.Projection.Parent() {
		t.Fatal("diamond occurrences were merged")
	}
	if left.Context.Projection.Parent().Parent() != root.Context.Projection || right.Context.Projection.Parent().Parent() != root.Context.Projection {
		t.Fatal("siblings copied their shared project context")
	}
	if capture.candidate(events[5]).Context.Projection != left.Context.Projection {
		t.Fatal("one pass recaptured the same occurrence")
	}
	leaf := left.Context.Projection.Step()
	if &leaf.Record["title"].Data[0] != &left.Record["title"].Data[0] {
		t.Fatal("candidate and leaf contain separate record copies")
	}
	events[1].Record["title"].Data[0] = 'X'
	events[5].Record["title"].Data[0] = 'X'
	if string(root.Record["title"].Data) != "Project" || string(left.Record["title"].Data) != "Task" {
		t.Fatal("captured images alias mutable inputs")
	}
}

func TestPolicyCaptureUsesFinalParentOverlayAndKeepsOldContextsStable(t *testing.T) {
	events := policyContextFixture()
	updated := events[1]
	updated.Record = modelRecord("p1", "1", "New project")
	updated.Version.Revision = 2
	final := append(append([]proj.ProjectionEvent(nil), events...), updated)
	capture := newPolicyCapture(newProjectionVisibility(), overlay(final))
	old := capture.candidate(events[1])
	task := capture.candidate(events[5])
	current := capture.candidate(updated)
	if task.Context.Projection.Parent().Parent() != current.Context.Projection || string(current.Record["title"].Data) != "New project" {
		t.Fatal("task did not capture final transaction parent")
	}
	if old.Context.Projection == current.Context.Projection || string(old.Record["title"].Data) != "Project" {
		t.Fatal("newer capture mutated an existing context")
	}
	changedTask := events[5]
	changedTask.Record = modelRecord("shared", "1", "New task")
	changedTask.Version.ChangeIndex = 1
	newTask := capture.candidate(changedTask)
	if newTask.Context.Projection == task.Context.Projection || newTask.Context.Projection.Parent() != task.Context.Projection.Parent() || string(task.Record["title"].Data) != "Task" {
		t.Fatal("leaf replacement was stale or rebuilt unchanged ancestors")
	}
	fresh := newPolicyCapture(newProjectionVisibility(), overlay(final)).candidate(events[5])
	if fresh.Context.Projection.Parent().Parent() == current.Context.Projection {
		t.Fatal("independent evaluation passes share a mutable capture cache")
	}
}

func TestPolicyCapturePreservesMissingAndRemovedAncestors(t *testing.T) {
	events := policyContextFixture()
	task := events[5]
	for _, removed := range []bool{false, true} {
		inputs := []proj.ProjectionEvent{task}
		if removed {
			root := events[1]
			root.Kind = q.Removed
			link := events[3]
			link.Kind = q.Removed
			inputs = append(inputs, root, link, events[2])
		}
		candidate := newPolicyCapture(newProjectionVisibility(), overlay(inputs)).candidate(task)
		if candidate.Context.Projection.Len() != 3 {
			t.Fatal("missing ancestors shortened the required authorization path")
		}
		for node := candidate.Context.Projection.Parent(); node != nil; node = node.Parent() {
			if node.Step().Complete || node.Step().Record != nil {
				t.Fatal("missing ancestor acquired visibility evidence")
			}
		}
	}
}

func TestPolicyCaptureDoesNotBorrowAnotherParentEntity(t *testing.T) {
	events := policyContextFixture()
	task := events[5]
	wrong := policyRow("project_tasks", "unrelated", "Association")
	task.ParentEntityKey = wrong.Key
	events = append(events, proj.ProjectionEvent{Type: proj.ProjectionRowChanged, InstanceID: task.ParentInstanceID, Key: wrong.Key, Kind: q.Added, Record: storage.Record{"title": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("unrelated")}}})
	candidate := newPolicyCapture(newProjectionVisibility(), overlay(events)).candidate(task)
	if candidate.Context.Projection.Parent().Step().Complete {
		t.Fatal("parent identity mismatch borrowed an unrelated record")
	}
}
