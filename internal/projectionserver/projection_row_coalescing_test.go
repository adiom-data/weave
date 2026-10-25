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

func TestProjectionCoalescesRowImagesWithoutChangingMembership(t *testing.T) {
	for _, test := range []struct {
		name           string
		initial, final bool
		remove         bool
		explicit       bool
		deleted        bool
	}{
		{name: "visible repeated update", initial: true, final: true},
		{name: "hidden becomes visible", final: true},
		{name: "visible becomes hidden", initial: true},
		{name: "hidden stays hidden"},
		{name: "remove reinsert moves row", initial: true, final: true, remove: true},
		{name: "explicit repeated update", initial: true, final: true, explicit: true},
		{name: "explicit remove reinsert", initial: true, final: true, remove: true, explicit: true},
		{name: "update then delete", initial: true, deleted: true},
		{name: "explicit update then delete", initial: true, explicit: true, deleted: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			gate, model := newProjectionVisibility(), newConnectionModel()
			prepared := &recordedAuthorization{visibilityTestPrepared: visibilityTestPrepared{decide: func(c accesscontrol.Candidate) bool {
				return string(c.Record["visible"].Data) == "yes"
			}}}
			a, b := modelKey(snapshotTestRelation(), "a"), modelKey(snapshotTestRelation(), "b")
			row := func(key storage.EntityKey, title string, visible bool, index uint32) proj.ProjectionEvent {
				record := modelRecord(key.Value, "1", title)
				text := "no"
				if visible {
					text = "yes"
				}
				record["visible"] = storage.Value{Type: storage.TypeText, State: storage.ValuePresent, Data: []byte(text)}
				return proj.ProjectionEvent{Type: proj.ProjectionRowChanged, InstanceID: "list", Kind: q.Updated, Key: key, Record: record, Version: q.RecordVersion{Revision: 2, ChangeIndex: index}}
			}
			initial := []proj.ProjectionEvent{{Type: proj.ProjectionInstanceOpened, InstanceID: "list", Bounds: modelQueryBounds()}, row(a, "a", test.initial, 0), row(b, "b", true, 0)}
			var queryEvents int
			apply := func(events []proj.ProjectionEvent, membership map[string]q.MembershipDelta) {
				t.Helper()
				batch, err := gate.filterBatch(ctx, prepared, visibilityTestView{}, proj.ProjectionBatch{Delivery: testDelivery(2), Events: events, Membership: membership})
				if err != nil {
					t.Fatal(err)
				}
				published, err := model.applyProjection("scope", batch)
				if err != nil {
					t.Fatal(err)
				}
				queryEvents = len(published.Queries)
				for _, event := range batch.Events {
					if event.Type == proj.ProjectionRowChanged && event.Kind != q.Removed && string(event.Record["title"].Data) == "intermediate" {
						t.Fatal("superseded row payload escaped the transaction")
					}
				}
			}
			apply(initial, nil)
			prepared.calls, prepared.candidates = 0, nil
			events := []proj.ProjectionEvent{row(a, "intermediate", !test.final, 1), row(a, "final", test.final, 2)}
			if test.remove {
				events = append(events[:1], append([]proj.ProjectionEvent{{Type: proj.ProjectionRowChanged, InstanceID: "list", Kind: q.Removed, Key: a}}, events[1:]...)...)
			}
			if test.deleted {
				events[1] = proj.ProjectionEvent{Type: proj.ProjectionRowChanged, InstanceID: "list", Kind: q.Removed, Key: a}
			}
			var membership map[string]q.MembershipDelta
			if test.explicit {
				delta := q.MembershipDelta{}
				if test.remove || test.deleted {
					delta.Removed = []storage.EntityKey{a}
				}
				if test.remove {
					delta.Insertions = []q.Insertion{{Key: a, FinalIndex: 1}}
				}
				membership = map[string]q.MembershipDelta{"list": delta}
			}
			apply(events, membership)
			want := []storage.EntityKey{b}
			if test.final {
				if test.remove {
					want = append(want, a)
				} else {
					want = []storage.EntityKey{a, b}
				}
			}
			if got := model.queries["list"].ordered; !reflect.DeepEqual(got, want) {
				t.Fatalf("downstream order=%v want=%v", got, want)
			}
			checks := 1
			if test.deleted {
				checks = 0
			}
			if prepared.calls != checks || len(prepared.candidates) != checks {
				t.Fatalf("authorization repeated: calls=%d candidates=%d", prepared.calls, len(prepared.candidates))
			}
			if test.final && string(model.entities[publicEntityID(a)].record["title"].Data) != "final" {
				t.Fatal("downstream entity did not retain the final row image")
			}
			if test.initial == test.final && !test.remove && !test.deleted && queryEvents != 0 {
				t.Fatal("unchanged final membership emitted client-visible list operations")
			}
		})
	}
}

func TestProjectionSupersededInsertionReservesItsOriginalPosition(t *testing.T) {
	gate, model := newProjectionVisibility(), newConnectionModel()
	prepared := &recordedAuthorization{visibilityTestPrepared: visibilityTestPrepared{decide: func(accesscontrol.Candidate) bool { return true }}}
	a, b := modelKey(snapshotTestRelation(), "a"), modelKey(snapshotTestRelation(), "b")
	row := func(key storage.EntityKey, title string) proj.ProjectionEvent {
		return proj.ProjectionEvent{Type: proj.ProjectionRowChanged, InstanceID: "list", Kind: q.Added, Key: key, Record: modelRecord(key.Value, "1", title), Version: q.RecordVersion{Revision: 1}}
	}
	batch, err := gate.filterBatch(context.Background(), prepared, visibilityTestView{}, proj.ProjectionBatch{Events: []proj.ProjectionEvent{
		{Type: proj.ProjectionInstanceOpened, InstanceID: "list", Bounds: modelQueryBounds()},
		row(a, "intermediate"), row(b, "b"), row(a, "final"),
	}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := model.applyProjection("scope", batch); err != nil {
		t.Fatal(err)
	}
	if got := model.queries["list"].ordered; !reflect.DeepEqual(got, []storage.EntityKey{a, b}) {
		t.Fatalf("first raw insertion position was lost: %v", got)
	}
	if string(model.entities[publicEntityID(a)].record["title"].Data) != "final" || len(prepared.candidates) != 2 {
		t.Fatal("intermediate insertion was published or evaluated")
	}
}
