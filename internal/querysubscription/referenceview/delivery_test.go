package referenceview

import (
	"context"
	"fmt"
	"testing"
	"time"

	q "github.com/adiom-data/weave/internal/querysubscription/model"
	"github.com/adiom-data/weave/internal/storage"
)

func TestSnapshotCannotConsumeExistingAliasTransaction(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	key := testKey("link")
	shape := storage.ReferenceFetch{Query: storage.QuerySpec{Relation: storage.RelationName{Name: "items"}}, Bindings: []storage.ReferenceBinding{{Source: "target", Target: "id"}}}
	record := storage.Record{"target": testValue("1")}
	query, token := selector(shape, record)
	image := q.EntitySnapshot{Revision: 1, Row: &q.Row{Key: testKey("1"), Record: storage.Record{"id": testValue("1")}}}
	base := &testFeed{events: make(chan q.Event, 8), snapshot: q.QuerySnapshot{OwnedReferences: true, Rows: []q.QueryRow{{Row: q.Row{Key: key, Record: record}, Expanded: &image}}}}
	f := WrapFeed(ctx, base, q.FeedOptions{BufferLimit: 8})
	defer f.Close(context.Background())
	if err := f.SubscribeQuery(ctx, q.QuerySubscribeRequest{ID: "parent", Query: shape.Query, Reference: &shape, Window: q.InitialWindow{Count: 1}}, func(q.QuerySubscription, q.QuerySnapshot) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if err := f.SubscribeEntity(ctx, q.EntitySubscribeRequest{ID: "child", Query: query, FetchFrom: &q.ReferenceSource{SubscriptionID: "parent", ParentKey: key}}, func(q.EntitySubscription, q.EntitySnapshot) error { return nil }); err != nil {
		t.Fatal(err)
	}
	image.Revision = 3
	image.Row = &q.Row{Key: testKey("1"), Record: storage.Record{"id": testValue("1"), "v": testValue("3")}}
	// An asynchronous image can overtake the matching transaction's enqueue.
	base.events <- q.Event{Entity: &q.EntityInstallation{Target: referenceTarget(token, key), Snapshot: image}}
	if _, err := f.Next(ctx); err != nil {
		t.Fatal(err)
	}
	if err := f.SubscribeEntity(ctx, q.EntitySubscribeRequest{ID: "later", Query: query, FetchFrom: &q.ReferenceSource{SubscriptionID: "parent", ParentKey: key}}, func(_ q.EntitySubscription, s q.EntitySnapshot) error {
		if s.Revision != 3 {
			t.Fatalf("new handle's floor=%d", s.Revision)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	for revision := uint64(2); revision <= 4; revision++ {
		row := q.Row{Key: testKey("1"), Record: storage.Record{"id": testValue("1"), "v": testValue(fmt.Sprint(revision))}}
		change := q.EntityEffect{Target: referenceTarget(token, key), Delta: q.EntityDelta{Changes: []q.Change{{Kind: q.Updated, Row: row}}}}
		base.events <- q.Event{Delivery: &q.DeliveryEnvelope{Delivery: q.DeliveryMetadata{Kind: q.DeliveryCommit, Revision: revision, PreviousRevision: revision - 1, Checkpoint: &q.Position{SourceID: "test", Value: revision}}, Entities: []q.EntityEffect{change}}}
		event, err := f.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		seen := map[string]bool{}
		for effect := range event.Delivery.SubscriptionEffects() {
			seen[effect.SubscriptionID] = true
		}
		if !seen["child"] || seen["later"] != (revision > 3) {
			t.Fatalf("revision %d routed to %v", revision, seen)
		}
		wrapped := f.(*feed)
		wrapped.mu.Lock()
		cached := wrapped.parents["parent"].cells[key].snapshot.Revision
		wrapped.mu.Unlock()
		if cached != max(image.Revision, revision) {
			t.Fatalf("shared image regressed to %d", cached)
		}
	}
}
