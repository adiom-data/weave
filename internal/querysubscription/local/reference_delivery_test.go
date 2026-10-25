package local

import (
	"context"
	"testing"
	"time"

	q "github.com/adiom-data/weave/internal/querysubscription/model"
	"github.com/adiom-data/weave/internal/querysubscription/referenceview"
	"github.com/adiom-data/weave/internal/storage"
)

type referenceDeliveryFeed struct {
	q.Feed
	snapshot q.QuerySnapshot
	events   chan q.Event
}
type referenceDeliveryQuery struct{ q.QuerySubscription }

func (referenceDeliveryQuery) ID() string                  { return "links" }
func (referenceDeliveryQuery) Close(context.Context) error { return nil }
func (f *referenceDeliveryFeed) SubscribeQuery(_ context.Context, _ q.QuerySubscribeRequest, install q.QuerySnapshotInstaller) error {
	return install(referenceDeliveryQuery{}, f.snapshot)
}
func (f *referenceDeliveryFeed) Next(ctx context.Context) (q.Event, error) {
	select {
	case e := <-f.events:
		return e, nil
	case <-ctx.Done():
		return q.Event{}, ctx.Err()
	}
}
func (f *referenceDeliveryFeed) Close(context.Context) error { return nil }

func TestReferenceClearPreservesChangeIndex(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	key := storage.EntityKey{Relation: storage.RelationKey{Source: "test", Name: "links"}, Value: "1"}
	record := storage.Record{"task_id": {Type: storage.TypeInteger, State: storage.ValuePresent, Data: []byte("1")}}
	shape := storage.ReferenceFetch{Query: storage.QuerySpec{Relation: storage.RelationName{Name: "tasks"}}, Bindings: []storage.ReferenceBinding{{Source: "task_id", Target: "id"}}}
	r, err := newOwnedReferences(&localSubscription{id: "links"}, shape, []q.Row{{Key: key, Record: record}})
	if err != nil {
		t.Fatal(err)
	}
	child := r.parents[key]
	target := q.Row{Key: storage.EntityKey{Relation: storage.RelationKey{Source: "test", Name: "tasks"}, Value: "1"}, Record: storage.Record{"id": record["task_id"]}}
	base := &referenceDeliveryFeed{events: make(chan q.Event, 8), snapshot: q.QuerySnapshot{OwnedReferences: true, Rows: []q.QueryRow{{Row: q.Row{Key: key, Record: record}, Expanded: &q.EntitySnapshot{Revision: 1, Row: &target}}}}}
	feed := referenceview.WrapFeed(ctx, base, q.FeedOptions{BufferLimit: 8})
	defer feed.Close(context.Background())
	if err := feed.SubscribeQuery(ctx, q.QuerySubscribeRequest{ID: "links", Query: storage.QuerySpec{Relation: storage.RelationName{Name: "links"}}, Window: q.InitialWindow{Count: 1}, Reference: &shape}, func(q.QuerySubscription, q.QuerySnapshot) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if err := feed.SubscribeEntity(ctx, q.EntitySubscribeRequest{ID: "task", Query: child.query, FetchFrom: &q.ReferenceSource{SubscriptionID: "links", ParentKey: key}}, func(q.EntitySubscription, q.EntitySnapshot) error { return nil }); err != nil {
		t.Fatal(err)
	}
	metadata := q.DeliveryMetadata{Kind: q.DeliveryCommit, Revision: 2, PreviousRevision: 1, Checkpoint: &q.Position{SourceID: "test", Value: 2}}
	reference := child.advance(metadata, q.Batch{Clear: true, ClearChangeIndex: 7})
	base.events <- q.Event{Delivery: &q.DeliveryEnvelope{Delivery: metadata, Entities: []q.EntityEffect{*reference}}}
	event, err := feed.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for effect := range event.Delivery.SubscriptionEffects() {
		if effect.SubscriptionID == "task" {
			if !effect.Batch.Clear || effect.Batch.ClearChangeIndex != 7 {
				t.Fatalf("clear marker became %+v", effect.Batch)
			}
			return
		}
	}
	t.Fatal("no entity clear")
}
