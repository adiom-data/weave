package projectionserver

import (
	"testing"

	proj "github.com/adiom-data/weave/internal/projection"
	"github.com/adiom-data/weave/internal/querysubscription/model"
	"github.com/adiom-data/weave/internal/storage"
)

func TestProjectionReferencesCarryExplicitScopeAndOccurrenceParents(t *testing.T) {
	model := newConnectionModel()
	relation := storage.RelationKey{Source: "test", Namespace: "tenant", Name: "entities"}
	parentKey, memberKey, taskKey := modelKey(relation, "project"), modelKey(relation, "membership"), modelKey(relation, "task")
	for _, scope := range []string{"first", "second"} {
		root, list, task := scope+":root", scope+":list", scope+":task"
		batch, err := model.applyProjection(scope, proj.ProjectionBatch{Events: []proj.ProjectionEvent{
			{Type: proj.ProjectionInstanceOpened, InstanceID: root, Node: "project", EntityNode: true},
			{Type: proj.ProjectionRowChanged, InstanceID: root, Node: "project", EntityNode: true, Kind: querysubscription.Added, Key: parentKey, Record: modelRecord("project", "1", "Project")},
			{Type: proj.ProjectionInstanceOpened, InstanceID: list, Node: "memberships", ParentInstanceID: root, ParentEntityKey: parentKey, Bounds: modelQueryBounds()},
			{Type: proj.ProjectionRowChanged, InstanceID: list, Node: "memberships", ParentInstanceID: root, ParentEntityKey: parentKey, Kind: querysubscription.Added, Key: memberKey, Record: modelRecord("membership", "1", "Membership")},
			{Type: proj.ProjectionInstanceOpened, InstanceID: task, Node: "task", EntityNode: true, ParentInstanceID: list, ParentEntityKey: memberKey},
			{Type: proj.ProjectionRowChanged, InstanceID: task, Node: "task", EntityNode: true, ParentInstanceID: list, ParentEntityKey: memberKey, Kind: querysubscription.Added, Key: taskKey, Record: modelRecord("task", "1", "Task")},
		}})
		if err != nil {
			t.Fatal(err)
		}
		if len(batch.Projections) != 3 {
			t.Fatalf("projection references = %+v", batch.Projections)
		}
		parents := map[string]string{root: "", list: root, task: list}
		entityParents := map[string]string{root: "", list: publicEntityID(parentKey), task: publicEntityID(memberKey)}
		for _, reference := range batch.Projections {
			if reference.SubscriptionId != scope || reference.ParentOccurrenceId != parents[reference.OccurrenceId] || reference.ParentEntityId != entityParents[reference.OccurrenceId] {
				t.Fatalf("ambiguous attachment: %+v", reference)
			}
		}
		// Closing the scope has no original source event to recover routing
		// from: the model must retain it for every detached occurrence.
		closed, err := model.closeScope(scope)
		if err != nil {
			t.Fatal(err)
		}
		for _, reference := range closed.Projections {
			if reference.SubscriptionId != scope || reference.ParentOccurrenceId != parents[reference.OccurrenceId] || reference.ParentEntityId != entityParents[reference.OccurrenceId] {
				t.Fatalf("ambiguous detachment: %+v", reference)
			}
		}
	}
}
