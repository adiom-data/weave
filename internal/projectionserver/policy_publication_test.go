package projectionserver

import (
	"context"
	"testing"

	"github.com/adiom-data/weave/internal/accesscontrol"
	proj "github.com/adiom-data/weave/internal/projection"
	q "github.com/adiom-data/weave/internal/querysubscription/model"
)

func TestHeldQueryReconciliationPreservesVersionsAndPublishesFinalBounds(t *testing.T) {
	m := newConnectionModel()
	a, b, c := policyRow("tasks", "a", "a"), policyRow("tasks", "b", "b"), policyRow("tasks", "c", "c")
	if _, err := m.openSubscription("scope", "query", "", policySnapshot(1, a, b), nil); err != nil {
		t.Fatal(err)
	}
	current := policySnapshot(10, b, c)
	current.Revision = 4
	current.Bounds.Left = q.Boundary{Kind: q.BoundaryCursor, Cursor: []byte("a")}
	v := newSubscriptionVisibility()
	_, err := v.filterSnapshot(context.Background(), visibilityTestPrepared{decide: func(accesscontrol.Candidate) bool { return true }}, visibilityTestView{}, current)
	if err != nil {
		t.Fatal(err)
	}
	batch, err := m.reconcileSubscription("query", v, q.DeliveryMetadata{Kind: q.DeliveryHydration, Revision: 10})
	if err != nil {
		t.Fatal(err)
	}
	if !sameBounds(m.queries["query"].publicBounds, current.Bounds) {
		t.Fatal("final bounds not reconciled")
	}
	if len(batch.Queries) != 2 || batch.Queries[0].GetDelta() == nil || batch.Queries[1].GetWindowChanged() == nil {
		t.Fatalf("expected list diff and final bounds: %v", batch)
	}
	if got := m.entities[publicEntityID(c.Key)].version.Revision; got != 4 {
		t.Fatalf("record was promoted to publication revision: %d", got)
	}
	if len(m.queries["query"].ordered) != 2 || m.queries["query"].ordered[0] != b.Key || m.queries["query"].ordered[1] != c.Key {
		t.Fatal("incorrect resumed membership")
	}
}

func TestHeldProjectionReconciliationKeepsSurvivingDiamondReference(t *testing.T) {
	m := newConnectionModel()
	row := policyRow("tasks", "shared", "task")
	open := func(id string) proj.ProjectionEvent {
		return proj.ProjectionEvent{Type: proj.ProjectionInstanceOpened, InstanceID: id, EntityNode: true}
	}
	entity := func(id string, revision uint64) proj.ProjectionEvent {
		return proj.ProjectionEvent{Type: proj.ProjectionRowChanged, InstanceID: id, EntityNode: true, Kind: q.Added, Key: row.Key, Record: row.Record, Version: q.RecordVersion{Revision: revision}}
	}
	if _, err := m.applyProjection("scope", proj.ProjectionBatch{Events: []proj.ProjectionEvent{open("left"), entity("left", 1), open("right"), entity("right", 1)}}); err != nil {
		t.Fatal(err)
	}
	batch, err := m.reconcileProjection("scope", []proj.ProjectionEvent{open("right"), entity("right", 1)}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(batch.Entities) != 0 || m.entityRefs[publicEntityID(row.Key)] != 1 {
		t.Fatalf("surviving diamond was released or redundantly updated: %v", batch)
	}
	if _, exists := m.occurrences["left"]; exists {
		t.Fatal("retired occurrence survived")
	}
	if _, exists := m.occurrences["right"]; !exists {
		t.Fatal("surviving occurrence lost")
	}
	// Losing the last path removes the entity, without retaining hidden
	// records in the public model or needing a replacement table snapshot.
	batch, err = m.reconcileProjection("scope", []proj.ProjectionEvent{open("right")}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(batch.Entities) != 1 || batch.Entities[0].GetReleased() == nil {
		t.Fatalf("last reference not removed: %v", batch)
	}
	if len(m.entityKeys) != 0 {
		t.Fatal("unreferenced identity retained")
	}
}
