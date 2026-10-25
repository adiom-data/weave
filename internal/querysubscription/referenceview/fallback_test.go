package referenceview

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	q "github.com/adiom-data/weave/internal/querysubscription/model"
	"github.com/adiom-data/weave/internal/storage"
)

func TestStaleReferenceOriginFallsBack(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	key := testKey("link")
	shape := storage.ReferenceFetch{Query: storage.QuerySpec{Relation: storage.RelationName{Name: "items"}}, Bindings: []storage.ReferenceBinding{{Source: "target", Target: "id"}}}
	record := storage.Record{"target": testValue("1")}
	query, _ := selector(shape, record)
	base := &testFeed{events: make(chan q.Event, 8), snapshot: q.QuerySnapshot{OwnedReferences: true, Rows: []q.QueryRow{{Row: q.Row{Key: key, Record: record}, Expanded: &q.EntitySnapshot{Revision: 1}}}}}
	base.entitySubscribe = func(_ context.Context, request q.EntitySubscribeRequest, _ q.EntitySnapshotInstaller) error {
		if request.FetchFrom != nil {
			t.Error("forwarded stale origin")
		}
		return nil
	}
	f := WrapFeed(ctx, base, q.FeedOptions{BufferLimit: 8})
	defer f.Close(context.Background())
	if err := f.SubscribeQuery(ctx, q.QuerySubscribeRequest{ID: "parent", Query: shape.Query, Reference: &shape, Window: q.InitialWindow{Count: 1}}, func(q.QuerySubscription, q.QuerySnapshot) error { return nil }); err != nil {
		t.Fatal(err)
	}
	base.events <- q.Event{Delivery: &q.DeliveryEnvelope{Delivery: q.DeliveryMetadata{Kind: q.DeliveryCommit, Revision: 2, PreviousRevision: 1, Checkpoint: &q.Position{SourceID: "test", Value: 2}}, Effects: []q.SubscriptionEffect{{SubscriptionID: "parent", Batch: q.Batch{Membership: q.MembershipDelta{Removed: []storage.EntityKey{key}}, BindingResets: []storage.EntityKey{key}}}}}}
	if _, err := f.Next(ctx); err != nil {
		t.Fatal(err)
	}
	if err := f.SubscribeEntity(ctx, q.EntitySubscribeRequest{ID: "child", Query: query, FetchFrom: &q.ReferenceSource{SubscriptionID: "parent", ParentKey: key}}, func(q.EntitySubscription, q.EntitySnapshot) error { return nil }); err != nil {
		t.Fatalf("stale origin rejected a valid selector: %v", err)
	}
	if base.entityCalls != 1 {
		t.Fatal("stale origin did not use ordinary selection")
	}
}

type observedWaitContext struct {
	context.Context
	once    sync.Once
	waiting chan struct{}
}

func (c *observedWaitContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.waiting) })
	return c.Context.Done()
}

func TestReferenceInvalidatedWhileWaiting(t *testing.T) {
	for _, closeFeed := range []bool{false, true} {
		name := "stale-origin"
		if closeFeed {
			name = "closed-feed"
		}
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			key := testKey("link")
			shape := storage.ReferenceFetch{Query: storage.QuerySpec{Relation: storage.RelationName{Name: "items"}}, Bindings: []storage.ReferenceBinding{{Source: "target", Target: "id"}}}
			record := storage.Record{"target": testValue("1")}
			query, _ := selector(shape, record)
			base := &testFeed{events: make(chan q.Event, 8), snapshot: q.QuerySnapshot{OwnedReferences: true, Rows: []q.QueryRow{{Row: q.Row{Key: key, Record: record}}}}}
			base.entitySubscribe = func(_ context.Context, request q.EntitySubscribeRequest, _ q.EntitySnapshotInstaller) error {
				if request.FetchFrom != nil {
					t.Error("forwarded stale origin")
				}
				return nil
			}
			f := WrapFeed(ctx, base, q.FeedOptions{BufferLimit: 8})
			defer f.Close(context.Background())
			if err := f.SubscribeQuery(ctx, q.QuerySubscribeRequest{ID: "parent", Query: shape.Query, Reference: &shape, Window: q.InitialWindow{Count: 1}}, func(q.QuerySubscription, q.QuerySnapshot) error { return nil }); err != nil {
				t.Fatal(err)
			}
			waiting := &observedWaitContext{Context: ctx, waiting: make(chan struct{})}
			done := make(chan error, 1)
			go func() {
				done <- f.SubscribeEntity(waiting, q.EntitySubscribeRequest{ID: "child", Query: query, FetchFrom: &q.ReferenceSource{SubscriptionID: "parent", ParentKey: key}}, func(q.EntitySubscription, q.EntitySnapshot) error { return nil })
			}()
			select {
			case <-waiting.waiting:
			case <-ctx.Done():
				t.Fatal("entity did not wait for the unresolved reference")
			}
			if closeFeed {
				if err := f.Close(ctx); err != nil {
					t.Fatal(err)
				}
			} else {
				base.events <- q.Event{Delivery: &q.DeliveryEnvelope{Delivery: q.DeliveryMetadata{Kind: q.DeliveryCommit, Revision: 2, PreviousRevision: 1, Checkpoint: &q.Position{SourceID: "test", Value: 2}}, Effects: []q.SubscriptionEffect{{SubscriptionID: "parent", Batch: q.Batch{Membership: q.MembershipDelta{Removed: []storage.EntityKey{key}}, BindingResets: []storage.EntityKey{key}}}}}}
			}
			select {
			case err := <-done:
				if closeFeed {
					if !errors.Is(err, q.ErrClosed) || base.entityCalls != 0 {
						t.Fatalf("closed feed: %v, calls=%d", err, base.entityCalls)
					}
				} else if err != nil || base.entityCalls != 1 {
					t.Fatalf("stale origin: %v, calls=%d", err, base.entityCalls)
				}
			case <-ctx.Done():
				t.Fatal("pending subscription did not finish")
			}
		})
	}
}
