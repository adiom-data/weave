package referenceview

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	q "github.com/adiom-data/weave/internal/querysubscription/model"
	"github.com/adiom-data/weave/internal/storage"
)

type testFeed struct {
	q.Feed
	events          chan q.Event
	snapshot        q.QuerySnapshot
	entityCalls     int
	entitySubscribe func(context.Context, q.EntitySubscribeRequest, q.EntitySnapshotInstaller) error
}

func (s *testFeed) SubscribeQuery(_ context.Context, r q.QuerySubscribeRequest, install q.QuerySnapshotInstaller) error {
	return install(testQuery{id: r.ID}, s.snapshot)
}
func (s *testFeed) SubscribeEntity(ctx context.Context, request q.EntitySubscribeRequest, install q.EntitySnapshotInstaller) error {
	s.entityCalls++
	if s.entitySubscribe != nil {
		return s.entitySubscribe(ctx, request, install)
	}
	return nil
}
func (s *testFeed) Next(ctx context.Context) (q.Event, error) {
	select {
	case event := <-s.events:
		return event, nil
	case <-ctx.Done():
		return q.Event{}, ctx.Err()
	}
}
func (*testFeed) Close(context.Context) error { return nil }

type testQuery struct {
	q.QuerySubscription
	id string
}

func (s testQuery) ID() string                { return s.id }
func (testQuery) Close(context.Context) error { return nil }

func testValue(v string) storage.Value {
	return storage.Value{Type: storage.TypeInteger, State: storage.ValuePresent, Data: []byte(v)}
}
func testKey(v string) storage.EntityKey {
	return storage.EntityKey{Relation: storage.RelationKey{Source: "test", Name: "items"}, Value: v}
}
func selector(shape storage.ReferenceFetch, row storage.Record) (storage.QuerySpec, string) {
	query, _, _, err := storage.BindReference(shape, row)
	if err != nil {
		panic(err)
	}
	encoded, _ := json.Marshal(query)
	return query, fmt.Sprintf("%x", sha256.Sum256(encoded))
}

func referenceTarget(selector string, parents ...storage.EntityKey) q.EntityTarget {
	return q.EntityTarget{SubscriptionID: "parent", Reference: &q.ReferenceRoute{Selector: selector, Parents: parents}}
}

func TestOwnedReferencesUseLocalHandlesAndKeepTransaction(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	key := testKey("link")
	shape := storage.ReferenceFetch{Query: storage.QuerySpec{Relation: storage.RelationName{Name: "items"}}, Bindings: []storage.ReferenceBinding{{Source: "target", Target: "id"}}}
	row := storage.Record{"target": testValue("1")}
	query, token := selector(shape, row)
	base := &testFeed{events: make(chan q.Event, 8), snapshot: q.QuerySnapshot{OwnedReferences: true, Rows: []q.QueryRow{{Row: q.Row{Key: key, Record: row}, Expanded: &q.EntitySnapshot{Revision: 1, Row: &q.Row{Key: testKey("1"), Record: storage.Record{"id": testValue("1")}}}}}}}
	f := WrapFeed(ctx, base, q.FeedOptions{BufferLimit: 8})
	defer f.Close(context.Background())
	var parent q.QuerySubscription
	if err := f.SubscribeQuery(ctx, q.QuerySubscribeRequest{ID: "parent", Query: shape.Query, Reference: &shape, Window: q.InitialWindow{Count: 1}}, func(h q.QuerySubscription, _ q.QuerySnapshot) error { parent = h; return nil }); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"one", "two"} {
		if err := f.SubscribeEntity(ctx, q.EntitySubscribeRequest{ID: id, Query: query, FetchFrom: &q.ReferenceSource{SubscriptionID: "parent", ParentKey: key}}, func(_ q.EntitySubscription, s q.EntitySnapshot) error {
			if s.Row == nil || s.Row.Key != testKey("1") {
				t.Fatalf("bad child: %+v", s)
			}
			// Installer ownership must not mutate the other local handle.
			s.Row.Record["id"] = testValue("999")
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	if base.entityCalls != 0 {
		t.Fatalf("issued %d child RPCs", base.entityCalls)
	}
	metadata := q.DeliveryMetadata{Kind: q.DeliveryCommit, Atomic: true, Revision: 2, PreviousRevision: 1, Checkpoint: &q.Position{SourceID: "test", Value: 2}}
	change := q.EntityEffect{Target: referenceTarget(token, key), Delta: q.EntityDelta{Changes: []q.Change{{Kind: q.Updated, Row: q.Row{Key: testKey("1"), Record: storage.Record{"id": testValue("1")}}}}}}
	base.events <- q.Event{Delivery: &q.DeliveryEnvelope{Delivery: metadata, Entities: []q.EntityEffect{change}}}
	event, err := f.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if event.Delivery == nil || event.Delivery.Delivery != metadata || len(event.Delivery.Entities) != 3 {
		t.Fatalf("lost atomic child effects: %+v", event)
	}
	if err := parent.Close(ctx); err != nil {
		t.Fatal(err)
	}
	wrapped := f.(*feed)
	wrapped.mu.Lock()
	defer wrapped.mu.Unlock()
	if len(wrapped.aliases) != 0 || len(wrapped.parents) != 0 {
		t.Fatal("parent retained child handles after close")
	}
}

func TestLateReferenceSnapshotCannotReplaceReboundParent(t *testing.T) {
	shape := &storage.ReferenceFetch{Query: storage.QuerySpec{Relation: storage.RelationName{Name: "items"}}, Bindings: []storage.ReferenceBinding{{Source: "target", Target: "id"}}}
	key := testKey("link")
	_, old := selector(*shape, storage.Record{"target": testValue("1")})
	_, current := selector(*shape, storage.Record{"target": testValue("2")})
	p := &parent{shape: shape, rows: map[storage.EntityKey]storage.Record{key: {"target": testValue("2")}}, cells: map[storage.EntityKey]*cell{}}
	f := &feed{}
	f.installEntity(p, q.EntityInstallation{Target: referenceTarget(old, key), Snapshot: q.EntitySnapshot{Revision: 100}})
	if len(p.cells) != 0 {
		t.Fatal("late old-selector snapshot was accepted")
	}
	f.installEntity(p, q.EntityInstallation{Target: referenceTarget(current, key), Snapshot: q.EntitySnapshot{Revision: 2, Row: &q.Row{Key: testKey("2")}}})
	f.installEntity(p, q.EntityInstallation{Target: referenceTarget(current, key), Snapshot: q.EntitySnapshot{Revision: 1}})
	if c := p.cells[key]; c == nil || c.snapshot.Row == nil || c.snapshot.Row.Key != testKey("2") {
		t.Fatal("older snapshot replaced newer child")
	}
}
