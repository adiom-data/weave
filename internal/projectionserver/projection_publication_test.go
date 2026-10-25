package projectionserver

import (
	"context"
	"testing"

	"github.com/adiom-data/weave/internal/accesscontrol"
	"github.com/adiom-data/weave/internal/projection"
	"github.com/adiom-data/weave/internal/querysubscription/model"
)

func TestProjectionVisibilityInvalidationCannotReplaceOrReversionRecords(t *testing.T) {
	ctx := context.Background()
	key := modelKey(snapshotTestRelation(), "shared")
	allow := visibilityTestPrepared{decide: func(accesscontrol.Candidate) bool { return true }}
	deny := visibilityTestPrepared{decide: func(accesscontrol.Candidate) bool { return false }}
	gate := newProjectionVisibility()
	model := newConnectionModel()
	initial, err := gate.filterBatch(ctx, allow, visibilityTestView{}, projection.ProjectionBatch{Events: []projection.ProjectionEvent{
		{Type: projection.ProjectionInstanceOpened, InstanceID: "slow", EntityNode: true, Checkpoint: &querysubscription.Position{SourceID: "test", Value: 1}},
		{Type: projection.ProjectionRowChanged, InstanceID: "slow", EntityNode: true, Kind: querysubscription.Added, Key: key, Record: modelRecord("shared", "1", "T1"), Version: querysubscription.RecordVersion{Revision: 1}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := model.applyProjection("slow", initial); err != nil {
		t.Fatal(err)
	}
	// A second, already-live scope has seen a newer copy of the same entity.
	if _, err := model.openSubscription("healthy", "healthy-query", "healthy", querysubscription.QuerySnapshot{Bounds: modelQueryBounds(), Revision: 4, Rows: []querysubscription.QueryRow{{Row: querysubscription.Row{Key: key, Record: modelRecord("shared", "1", "T4")}}}}, nil); err != nil {
		t.Fatal(err)
	}
	policyOnly := accesscontrol.Invalidation{Candidates: []accesscontrol.Candidate{{Key: key, Record: modelRecord("shared", "1", "policy evaluation input, not data"), Complete: true}}}
	batch, err := gate.filterTransaction(ctx, allow, visibilityTestView{}, projection.ProjectionBatch{
		Delivery: testDelivery(2), Events: []projection.ProjectionEvent{{Type: projection.ProjectionRowChanged, InstanceID: "slow", EntityNode: true, Kind: querysubscription.Updated, Key: key, Record: modelRecord("shared", "1", "T2"), Version: querysubscription.RecordVersion{Revision: 2}}},
	}, policyOnly)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := model.applyProjection("slow", batch); err != nil {
		t.Fatal(err)
	}
	if got := string(model.entities[publicEntityID(key)].record["title"].Data); got != "T4" {
		t.Fatalf("historical catch-up replaced newer shared entity: %s", got)
	}
	for index, prepared := range []accesscontrol.Prepared{deny, allow} {
		batch, err := gate.filterTransaction(ctx, prepared, visibilityTestView{}, projection.ProjectionBatch{Delivery: testDelivery(uint64(3 + index))}, policyOnly)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := model.applyProjection("slow", batch); err != nil {
			t.Fatal(err)
		}
		row := gate.instances["slow"].rows[key].event
		if string(row.Record["title"].Data) != "T2" || row.Version.Revision != 2 {
			t.Fatalf("authorization replaced raw state: %+v", row)
		}
		if got := string(model.entities[publicEntityID(key)].record["title"].Data); got != "T4" {
			t.Fatalf("visibility transition replaced newer shared entity: %s", got)
		}
	}
}

func TestProjectionBootstrapVersionDoesNotBypassFreshness(t *testing.T) {
	key := modelKey(snapshotTestRelation(), "shared")
	model := newConnectionModel()
	for index, title := range []string{"bootstrap", "newer", "stale bootstrap"} {
		version := uint64(0)
		if index == 1 {
			version = 2
		}
		batch := projection.ProjectionBatch{Events: []projection.ProjectionEvent{{Type: projection.ProjectionRowChanged, InstanceID: "entity", EntityNode: true, Kind: querysubscription.Updated, Key: key, Record: modelRecord("shared", "1", title), Version: querysubscription.RecordVersion{Revision: version}}}}
		if index == 0 {
			batch.Events = append([]projection.ProjectionEvent{{Type: projection.ProjectionInstanceOpened, InstanceID: "entity", EntityNode: true}}, batch.Events...)
		}
		if _, err := model.applyProjection("scope", batch); err != nil {
			t.Fatal(err)
		}
		expected := title
		if index == 2 {
			expected = "newer"
		}
		if got := string(model.entities[publicEntityID(key)].record["title"].Data); got != expected {
			t.Fatalf("bootstrap version handling: got %q want %q", got, expected)
		}
	}
}

func TestProjectionVisibilityGrantDoesNotHydratePendingRecord(t *testing.T) {
	ctx := context.Background()
	key := modelKey(snapshotTestRelation(), "pending")
	gate := newProjectionVisibility()
	deny := visibilityTestPrepared{decide: func(accesscontrol.Candidate) bool { return false }}
	allow := visibilityTestPrepared{decide: func(accesscontrol.Candidate) bool { return true }}
	if _, err := gate.filterBatch(ctx, deny, visibilityTestView{}, projection.ProjectionBatch{Events: []projection.ProjectionEvent{
		{Type: projection.ProjectionInstanceOpened, InstanceID: "entity", EntityNode: true},
		{Type: projection.ProjectionRowChanged, InstanceID: "entity", EntityNode: true, Kind: querysubscription.Added, Key: key, Record: modelRecord("pending", "1", "unresolved"), Pending: true, Version: querysubscription.RecordVersion{Revision: 1}},
	}}); err != nil {
		t.Fatal(err)
	}
	batch, err := gate.filterTransaction(ctx, allow, visibilityTestView{}, projection.ProjectionBatch{Delivery: testDelivery(2)}, accesscontrol.Invalidation{Candidates: []accesscontrol.Candidate{{Key: key, Record: modelRecord("pending", "1", "complete policy input"), Complete: true}}})
	if err != nil {
		t.Fatal(err)
	}
	if len(batch.Events) != 1 || !batch.Events[0].Pending || batch.Events[0].Version.Revision != 1 || string(batch.Events[0].Record["title"].Data) != "unresolved" {
		t.Fatalf("policy facts hydrated a pending record: %+v", batch)
	}
}

func TestProjectionPayloadFreePendingReferenceGetsPolicyDecision(t *testing.T) {
	for _, initial := range []bool{false, true} {
		for _, allowed := range []bool{false, true} {
			name := "live"
			if initial {
				name = "initial"
			}
			if allowed {
				name += "/allow"
			} else {
				name += "/deny"
			}
			t.Run(name, func(t *testing.T) {
				gate := newProjectionVisibility()
				key := modelKey(snapshotTestRelation(), "pending")
				calls := 0
				prepared := visibilityTestPrepared{decide: func(candidate accesscontrol.Candidate) bool {
					calls++
					if candidate.Record != nil || candidate.Complete || candidate.Key != key || candidate.Context.Projection.Len() != 1 {
						t.Fatalf("wrong pending policy input: %+v", candidate)
					}
					return allowed
				}}
				events := []projection.ProjectionEvent{
					{Type: projection.ProjectionInstanceOpened, InstanceID: "pending", EntityNode: true, Checkpoint: &querysubscription.Position{SourceID: "test", Value: 1}},
					{Type: projection.ProjectionRowChanged, InstanceID: "pending", EntityNode: true, Kind: querysubscription.Added, Key: key, Pending: true, Version: querysubscription.RecordVersion{Revision: 1}},
				}
				var filtered []projection.ProjectionEvent
				if initial {
					var err error
					filtered, err = gate.filterInitial(context.Background(), visibilityTestController{}, prepared, events)
					if err != nil {
						t.Fatal(err)
					}
				} else {
					batch, err := gate.filterBatch(context.Background(), prepared, visibilityTestView{}, projection.ProjectionBatch{Events: events})
					if err != nil {
						t.Fatal(err)
					}
					filtered = batch.Events
				}
				if calls != 1 {
					t.Fatalf("policy calls = %d", calls)
				}
				rows := 0
				for _, event := range filtered {
					if event.Type == projection.ProjectionRowChanged {
						rows++
						if !event.Pending || event.Record != nil {
							t.Fatalf("pending row was changed: %+v", event)
						}
					}
				}
				expected := 0
				if allowed {
					expected = 1
				}
				if rows != expected {
					t.Fatalf("visible pending rows = %d, want %d", rows, expected)
				}
				model := newConnectionModel()
				batch, err := model.applyProjection("scope", projection.ProjectionBatch{Events: filtered})
				if err != nil {
					t.Fatal(err)
				}
				if len(batch.Entities) != expected || (allowed && batch.Entities[0].GetPending() == nil) {
					t.Fatalf("pending reference was not preserved in the public model: %v", batch)
				}
			})
		}
	}
}

func TestProjectionVisibilityOccurrenceIndexTracksHiddenDiamondsAndRemoval(t *testing.T) {
	ctx := context.Background()
	key := modelKey(snapshotTestRelation(), "shared")
	gate := newProjectionVisibility()
	allow := visibilityTestPrepared{decide: func(accesscontrol.Candidate) bool { return true }}
	deny := visibilityTestPrepared{decide: func(accesscontrol.Candidate) bool { return false }}
	for _, id := range []string{"left", "right"} {
		if _, err := gate.filterBatch(ctx, deny, visibilityTestView{}, projection.ProjectionBatch{Events: []projection.ProjectionEvent{
			{Type: projection.ProjectionInstanceOpened, InstanceID: id, EntityNode: true, Checkpoint: &querysubscription.Position{SourceID: "test", Value: 5}},
			{Type: projection.ProjectionRowChanged, InstanceID: id, EntityNode: true, Kind: querysubscription.Added, Key: key, Record: modelRecord("shared", "1", "raw"), Version: querysubscription.RecordVersion{Revision: 5}},
		}}); err != nil {
			t.Fatal(err)
		}
	}
	inv := accesscontrol.Invalidation{Candidates: []accesscontrol.Candidate{{Key: key, Record: modelRecord("shared", "1", "facts"), Complete: true}}}
	// An older transaction must not rewind a newly installed snapshot's policy.
	batch, err := gate.filterTransaction(ctx, allow, visibilityTestView{}, projection.ProjectionBatch{Delivery: testDelivery(4)}, inv)
	if err != nil || len(batch.Events) != 0 {
		t.Fatalf("older policy frame: %+v %v", batch, err)
	}
	batch, err = gate.filterTransaction(ctx, allow, visibilityTestView{}, projection.ProjectionBatch{Delivery: testDelivery(6)}, inv)
	if err != nil || len(batch.Events) != 2 {
		t.Fatalf("hidden diamond did not regain both occurrences: %+v %v", batch, err)
	}
	if _, err := gate.filterBatch(ctx, allow, visibilityTestView{}, projection.ProjectionBatch{Events: []projection.ProjectionEvent{{Type: projection.ProjectionInstanceClosed, InstanceID: "left"}}}); err != nil {
		t.Fatal(err)
	}
	if len(gate.occurrences[key]) != 1 || gate.occurrences[key]["right"] == nil {
		t.Fatalf("closing one diamond arm lost the other: %+v", gate.occurrences)
	}
	if _, err := gate.filterBatch(ctx, allow, visibilityTestView{}, projection.ProjectionBatch{Events: []projection.ProjectionEvent{{Type: projection.ProjectionRowChanged, InstanceID: "right", Kind: querysubscription.Removed, Key: key}}}); err != nil {
		t.Fatal(err)
	}
	batch, err = gate.filterTransaction(ctx, allow, visibilityTestView{}, projection.ProjectionBatch{Delivery: testDelivery(7)}, inv)
	if err != nil || len(batch.Events) != 0 || len(gate.occurrences) != 0 {
		t.Fatalf("invalidation resurrected removed member: %+v %v", batch, err)
	}
}
