package referenceview

import (
	"context"
	"testing"
	"time"

	q "github.com/adiom-data/weave/internal/querysubscription/model"
	"github.com/adiom-data/weave/internal/storage"
)

func TestPendingBindingRetainsExistingHandlesAndRefreshesAfterHydration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	key := testKey("link")
	shape := storage.ReferenceFetch{Query: storage.QuerySpec{Relation: storage.RelationName{Name: "items"}}, Bindings: []storage.ReferenceBinding{{Source: "target", Target: "id"}}}
	record := storage.Record{"target": testValue("1")}
	query, token := selector(shape, record)
	target := func(title string) q.Row {
		return q.Row{Key: testKey("1"), Record: storage.Record{"id": testValue("1"), "title": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte(title)}}}
	}
	initial := target("old")
	base := &testFeed{events: make(chan q.Event, 8), snapshot: q.QuerySnapshot{OwnedReferences: true, Rows: []q.QueryRow{{Row: q.Row{Key: key, Record: record}, Expanded: &q.EntitySnapshot{Revision: 1, Row: &initial}}}}}
	base.entitySubscribe = func(_ context.Context, request q.EntitySubscribeRequest, _ q.EntitySnapshotInstaller) error {
		if request.FetchFrom != nil {
			t.Error("reused a pending binding")
		}
		return nil
	}
	f := WrapFeed(ctx, base, q.FeedOptions{BufferLimit: 8})
	defer f.Close(context.Background())
	if err := f.SubscribeQuery(ctx, q.QuerySubscribeRequest{ID: "parent", Query: shape.Query, Reference: &shape, Window: q.InitialWindow{Count: 1}}, func(q.QuerySubscription, q.QuerySnapshot) error { return nil }); err != nil {
		t.Fatal(err)
	}
	request := q.EntitySubscribeRequest{ID: "existing", Query: query, FetchFrom: &q.ReferenceSource{SubscriptionID: "parent", ParentKey: key}}
	if err := f.SubscribeEntity(ctx, request, func(q.EntitySubscription, q.EntitySnapshot) error { return nil }); err != nil {
		t.Fatal(err)
	}
	send := func(revision uint64, batch q.Batch, entities ...q.EntityEffect) q.Event {
		base.events <- q.Event{Delivery: &q.DeliveryEnvelope{Delivery: q.DeliveryMetadata{Kind: q.DeliveryCommit, Revision: revision, PreviousRevision: revision - 1, Checkpoint: &q.Position{SourceID: "test", Value: revision}}, Effects: []q.SubscriptionEffect{{SubscriptionID: "parent", Batch: batch}}, Entities: entities}}
		event, err := f.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		return event
	}
	// A pending row can move in ordering without its last known binding being lost.
	pending := q.Batch{Changes: []q.Change{{Kind: q.Updated, Row: q.Row{Key: key, Pending: true, Record: storage.Record{"target": {Type: storage.TypeInteger, State: storage.ValueUnavailable}}}}}, Membership: q.MembershipDelta{Removed: []storage.EntityKey{key}, Insertions: []q.Insertion{{Key: key, FinalIndex: 0}}}}
	send(2, pending)
	request.ID = "while-pending"
	if err := f.SubscribeEntity(ctx, request, func(q.EntitySubscription, q.EntitySnapshot) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if base.entityCalls != 1 {
		t.Fatal("pending binding did not fall back")
	}
	latest := target("latest")
	event := send(3, q.Batch{}, q.EntityEffect{Target: referenceTarget(token, key), Delta: q.EntityDelta{Changes: []q.Change{{Kind: q.Updated, Row: latest}}}})
	seen := false
	for effect := range event.Delivery.SubscriptionEffects() {
		if effect.SubscriptionID == "existing" {
			seen = true
		}
	}
	if !seen {
		t.Fatal("existing handle lost a transaction while parent was pending")
	}
	send(4, q.Batch{BindingResets: []storage.EntityKey{key}, Changes: []q.Change{{Kind: q.Updated, Row: q.Row{Key: key, Record: record}}}})
	request.ID = "after-hydration"
	wait := &observedWaitContext{Context: ctx, waiting: make(chan struct{})}
	done := make(chan error, 1)
	go func() {
		done <- f.SubscribeEntity(wait, request, func(_ q.EntitySubscription, snapshot q.EntitySnapshot) error {
			if snapshot.Row == nil || string(snapshot.Row.Record["title"].Data) != "latest" {
				t.Errorf("reused stale child: %+v", snapshot)
			}
			return nil
		})
	}()
	select {
	case <-wait.waiting:
	case <-ctx.Done():
		t.Fatal("hydrated handle did not wait for its refreshed image")
	}
	base.events <- q.Event{Entity: &q.EntityInstallation{Target: referenceTarget(token, key), Snapshot: q.EntitySnapshot{Revision: 3, Row: &latest}}}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if base.entityCalls != 1 {
		t.Fatal("hydrated binding did not reuse the current interest")
	}
}
