package projectionserver

import (
	"context"
	"reflect"
	"testing"

	"github.com/adiom-data/weave/internal/accesscontrol"
	proj "github.com/adiom-data/weave/internal/projection"
	q "github.com/adiom-data/weave/internal/querysubscription/model"
	"github.com/adiom-data/weave/internal/storage"
)

func TestVisibilityStagesMembershipBeforeParentReveal(t *testing.T) {
	for _, existing := range []bool{false, true} {
		for _, visible := range []bool{false, true} {
			name := "initial"
			if existing {
				name = "existing hidden parent"
			}
			if visible {
				name += "/allow"
			} else {
				name += "/deny"
			}
			t.Run(name, func(t *testing.T) {
				ctx := context.Background()
				gate, model := newProjectionVisibility(), newConnectionModel()
				prepared := &recordedAuthorization{visibilityTestPrepared: visibilityTestPrepared{decide: func(c accesscontrol.Candidate) bool {
					return c.Key.Relation.Name != "projects" || string(c.Record["title"].Data) == "allowed"
				}}}
				events := policyContextFixture()[:6]
				for i := range events {
					events[i].Bounds = modelQueryBounds()
					if events[i].InstanceID == "projects" || events[i].InstanceID == "left:task" {
						events[i].EntityNode = true
					}
				}
				apply := func(events []proj.ProjectionEvent, membership map[string]q.MembershipDelta) {
					t.Helper()
					batch, err := gate.filterBatch(ctx, prepared, visibilityTestView{}, proj.ProjectionBatch{Events: events, Membership: membership})
					if err != nil {
						t.Fatal(err)
					}
					if _, err := model.applyProjection("projection", batch); err != nil {
						t.Fatal(err)
					}
				}
				if existing {
					apply(events[:2], nil)
					prepared.calls, prepared.candidates = 0, nil
				}
				final := events[1]
				final.Kind, final.Version.Revision = q.Updated, 2
				final.Record = modelRecord("p1", "2", "denied")
				if visible {
					final.Record = modelRecord("p1", "2", "allowed")
				}
				next := append(append([]proj.ProjectionEvent(nil), events...), final)
				if existing {
					next = next[2:]
				}
				apply(next, map[string]q.MembershipDelta{"left:links": {Insertions: []q.Insertion{{Key: events[3].Key, FinalIndex: 0}}}})
				for _, item := range []struct {
					id  string
					key storage.EntityKey
				}{{"projects", events[1].Key}, {"left:links", events[3].Key}, {"left:task", events[5].Key}} {
					row := gate.instances[item.id].rows[item.key]
					if row.effective != visible {
						t.Fatalf("%s effective=%v want=%v", item.id, row.effective, visible)
					}
				}
				if prepared.calls != 1 || len(prepared.candidates) != 3 {
					t.Fatalf("repeated authorization: calls=%d candidates=%d", prepared.calls, len(prepared.candidates))
				}
				if visible {
					if got := model.queries["left:links"].ordered; !reflect.DeepEqual(got, []storage.EntityKey{events[3].Key}) {
						t.Fatalf("downstream list=%v", got)
					}
					if len(model.entities) != 3 || model.occurrences["left:task"].entityID != publicEntityID(events[5].Key) {
						t.Fatal("authorized nested entity was not published")
					}
				} else if len(model.entities) != 0 || model.queries["left:links"] != nil {
					t.Fatal("denied subtree reached the public model")
				}
			})
		}
	}
}

func TestVisibilityPublishesEntityWinnerRemovalBeforeReplacement(t *testing.T) {
	ctx := context.Background()
	gate, model := newProjectionVisibility(), newConnectionModel()
	allow := visibilityTestPrepared{decide: func(accesscontrol.Candidate) bool { return true }}
	a, b := modelKey(snapshotTestRelation(), "a"), modelKey(snapshotTestRelation(), "b")
	apply := func(events []proj.ProjectionEvent) {
		t.Helper()
		batch, err := gate.filterBatch(ctx, allow, visibilityTestView{}, proj.ProjectionBatch{Events: events})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := model.applyProjection("projection", batch); err != nil {
			t.Fatal(err)
		}
	}
	apply([]proj.ProjectionEvent{{Type: proj.ProjectionInstanceOpened, InstanceID: "entity", EntityNode: true}, {Type: proj.ProjectionRowChanged, InstanceID: "entity", EntityNode: true, Kind: q.Added, Key: a, Record: modelRecord("a", "1", "first")}})
	// Input order must not cause the later old-winner removal to clear b.
	apply([]proj.ProjectionEvent{{Type: proj.ProjectionRowChanged, InstanceID: "entity", EntityNode: true, Kind: q.Added, Key: b, Record: modelRecord("b", "2", "replacement")}, {Type: proj.ProjectionRowChanged, InstanceID: "entity", EntityNode: true, Kind: q.Removed, Key: a}})
	if model.occurrences["entity"].entityID != publicEntityID(b) || len(model.entities) != 1 {
		t.Fatal("old winner removal cleared its replacement")
	}
}

func TestVisibilityInstallsChildDenialBeforeParentReveal(t *testing.T) {
	ctx := context.Background()
	gate, model := newProjectionVisibility(), newConnectionModel()
	prepared := visibilityTestPrepared{decide: func(c accesscontrol.Candidate) bool {
		if c.Key.Relation.Name == "projects" {
			return string(c.Record["title"].Data) == "allowed"
		}
		return string(c.Record["title"].Data) != "denied"
	}}
	events := policyContextFixture()[:6]
	for index := range events {
		events[index].Bounds = modelQueryBounds()
		if events[index].InstanceID == "projects" || events[index].InstanceID == "left:task" {
			events[index].EntityNode = true
		}
	}
	apply := func(batch proj.ProjectionBatch, invalidation accesscontrol.Invalidation) {
		t.Helper()
		filtered, err := gate.filterTransaction(ctx, prepared, visibilityTestView{}, batch, invalidation)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := model.applyProjection("projection", filtered); err != nil {
			t.Fatal(err)
		}
	}
	apply(proj.ProjectionBatch{Events: events}, accesscontrol.Invalidation{})
	parent := events[1]
	parent.Record, parent.Kind, parent.Version.Revision = modelRecord("p1", "2", "allowed"), q.Updated, 2
	link := events[3]
	apply(proj.ProjectionBatch{Delivery: testDelivery(2), Events: []proj.ProjectionEvent{parent}}, accesscontrol.Invalidation{Candidates: []accesscontrol.Candidate{{Key: link.Key, Record: modelRecord("left", "1", "denied"), Complete: true}}})
	if len(model.entities) != 1 || model.queries["left:links"] == nil || len(model.queries["left:links"].ordered) != 0 || model.occurrences["left:task"].entityID != "" {
		t.Fatal("parent reveal published a child denied in the same transaction")
	}
	apply(proj.ProjectionBatch{Delivery: testDelivery(3)}, accesscontrol.Invalidation{Targets: []accesscontrol.InvalidationTarget{{InstanceID: "left:links", Key: link.Key}}})
	if len(model.entities) != 3 || len(model.queries["left:links"].ordered) != 1 || model.occurrences["left:task"].entityID == "" {
		t.Fatal("later authorization did not restore the retained nested subtree")
	}
}
