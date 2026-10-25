package local

import (
	"context"
	"testing"
	"time"

	q "github.com/adiom-data/weave/internal/querysubscription/model"
	"github.com/adiom-data/weave/internal/storage"
)

func TestPendingParentResumesExistingInterestWithLatestImage(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	feed, r, key, record := referenceFixture(t, ctx, coordinatorStub{unsubscribe: func(context.Context, string) error { return nil }})
	child := r.parents[key]
	child.finished = true
	close(child.ready)
	child.current = q.EntitySnapshot{Revision: 1, Row: &q.Row{Key: key, Record: storage.Record{"title": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("old")}}}}
	unknown := storage.Record{"task_id": {Type: storage.TypeInteger, State: storage.ValueUnavailable}}
	jobs, err := r.advance(&q.Batch{Changes: []q.Change{{Kind: q.Updated, Row: q.Row{Key: key, Pending: true, Record: unknown}}}})
	if err != nil || len(jobs) != 0 || r.parents[key] != child || child.retired {
		t.Fatalf("pending transition: %v, jobs=%d", err, len(jobs))
	}
	// The original selector remains tracked while the parent is unresolved.
	current := q.Row{Key: key, Record: storage.Record{"title": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("new")}}}
	if change := child.advance(q.DeliveryMetadata{Revision: 2}, q.Batch{Changes: []q.Change{{Kind: q.Updated, Row: current}}}); change == nil {
		t.Fatal("pending parent lost its existing interest")
	}
	batch := q.Batch{Changes: []q.Change{{Kind: q.Updated, Row: q.Row{Key: key, Record: record}}}}
	jobs, err = r.advance(&batch)
	if err != nil || len(jobs) != 1 || len(batch.BindingResets) != 1 || r.pending[key] {
		t.Fatalf("hydration transition: %v, jobs=%d", err, len(jobs))
	}
	for _, job := range jobs {
		job()
	}
	event, err := feed.Next(ctx)
	if err != nil || event.Entity == nil || event.Entity.Snapshot.Revision != 2 || string(event.Entity.Snapshot.Row.Record["title"].Data) != "new" {
		t.Fatalf("resumed with a stale image: %+v, %v", event, err)
	}
	if len(feed.referenceWork.loads) != 0 {
		t.Fatal("unchanged interest was reloaded")
	}
}

func TestPendingInitialParentAndRemoval(t *testing.T) {
	shape := storage.ReferenceFetch{Query: storage.QuerySpec{Relation: storage.RelationName{Name: "tasks"}}, Bindings: []storage.ReferenceBinding{{Source: "task_ref", Target: "id"}}}
	key := storage.EntityKey{Relation: storage.RelationKey{Source: "test", Name: "links"}, Value: "1"}
	row := q.Row{Key: key, Pending: true, Record: storage.Record{"task_ref": {Type: storage.TypeText, State: storage.ValueUnavailable}}}
	for _, clearAll := range []bool{false, true} {
		r, err := newOwnedReferences(&localSubscription{}, shape, []q.Row{row})
		if err != nil || !r.pending[key] || len(r.children) != 0 || len(r.initial) != 0 {
			t.Fatalf("initial pending row: %v", err)
		}
		batch := q.Batch{Clear: clearAll}
		if !clearAll {
			batch.Membership.Removed = []storage.EntityKey{key}
		}
		jobs, err := r.advance(&batch)
		if err != nil || len(jobs) != 0 || len(r.pending) != 0 {
			t.Fatal("removed pending row was retained")
		}
	}
}

func TestPendingParentWaitsForBindingHydration(t *testing.T) {
	shape := storage.ReferenceFetch{Query: storage.QuerySpec{Relation: storage.RelationName{Name: "tasks"}}, Bindings: []storage.ReferenceBinding{{Source: "task_ref", Target: "id"}}}
	r, err := newOwnedReferences(&localSubscription{}, shape, nil)
	if err != nil {
		t.Fatal(err)
	}
	key := storage.EntityKey{Relation: storage.RelationKey{Source: "test", Name: "links"}, Value: "1"}
	pending := q.Change{Kind: q.Added, Row: q.Row{Key: key, Pending: true, Record: storage.Record{"task_ref": {Type: storage.TypeText, State: storage.ValueUnavailable}}}}
	jobs, err := r.advance(&q.Batch{Changes: []q.Change{pending}})
	if err != nil {
		t.Fatalf("pending reference failed instead of waiting for hydration: %v", err)
	}
	if len(jobs) != 0 {
		t.Fatal("unresolved binding scheduled a load")
	}
	complete := q.Change{Kind: q.Updated, Row: q.Row{Key: key, Record: storage.Record{"task_ref": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("task-1")}}}}
	jobs, err = r.advance(&q.Batch{Changes: []q.Change{complete}})
	if err != nil || len(jobs) != 1 {
		t.Fatalf("hydrated binding did not schedule: %v / %d", err, len(jobs))
	}
}
