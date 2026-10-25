package local

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/adiom-data/weave/internal/engine"
	"github.com/adiom-data/weave/internal/engine/delivery"
	q "github.com/adiom-data/weave/internal/querysubscription/model"
	"github.com/adiom-data/weave/internal/storage"
)

func TestReferenceWorkIsBoundedAndCleanupDoesNotWaitForLoads(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	w := newReferenceWork(ctx, ReferenceOptions{LoadConcurrency: 1, LoadQueueLimit: 1}, 1)
	started, release := make(chan struct{}), make(chan struct{})
	defer close(release)
	if err := w.submit(func() { close(started); <-release }, false); err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if err := w.submit(func() {}, false); err != nil {
		t.Fatal(err)
	}
	if err := w.submit(func() {}, false); !errors.Is(err, q.ErrBackpressure) {
		t.Fatalf("overflow=%v", err)
	}
	cleaned := make(chan struct{})
	if err := w.submit(func() { close(cleaned) }, true); err != nil {
		t.Fatal(err)
	}
	select {
	case <-cleaned:
	case <-ctx.Done():
		t.Fatal("cleanup blocked on load")
	}
	cancel()
	if err := w.submit(func() { t.Error("ran work after closure") }, false); !errors.Is(err, q.ErrClosed) {
		t.Fatal(err)
	}
}

func referenceFixture(t *testing.T, ctx context.Context, coordinator Coordinator) (*localFeed, *ownedReferences, storage.EntityKey, storage.Record) {
	t.Helper()
	opened, err := NewWithOptions(coordinator, ReferenceOptions{LoadConcurrency: 1, LoadQueueLimit: 1}).openFeed(ctx, q.FeedOptions{BufferLimit: 8})
	if err != nil {
		t.Fatal(err)
	}
	feed := opened
	t.Cleanup(func() { _ = feed.Close(context.Background()) })
	key := storage.EntityKey{Relation: storage.RelationKey{Source: "test", Name: "links"}, Value: "old"}
	record := storage.Record{"task_id": {Type: storage.TypeInteger, State: storage.ValuePresent, Data: []byte("1")}}
	parent := &localSubscription{id: "parent", nativeID: feed.nativeID("parent"), owner: feed, coordinator: coordinator}
	shape := storage.ReferenceFetch{Query: storage.QuerySpec{Relation: storage.RelationName{Name: "tasks"}}, Bindings: []storage.ReferenceBinding{{Source: "task_id", Target: "id"}}}
	r, err := newOwnedReferences(parent, shape, []q.Row{{Key: key, Record: record}})
	if err != nil {
		t.Fatal(err)
	}
	parent.references = r
	r.parents[key].pins = 0
	r.initial = nil
	return feed, r, key, record
}

func TestReferenceLoadDeduplicatesAndCancelsOnLastOwnerRetirement(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	started, canceled := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	coordinator := coordinatorStub{subscribe: func(ctx context.Context, _, _ string, _ storage.QuerySpec, _ engine.InitialWindow, _ int) (*engine.OpenSubscription, error) {
		calls.Add(1)
		close(started)
		<-ctx.Done()
		close(canceled)
		return nil, ctx.Err()
	}, unsubscribe: func(context.Context, string) error { return nil }}
	_, r, key, _ := referenceFixture(t, ctx, coordinator)
	child := r.parents[key]
	r.schedule(child)
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	for range 100 {
		r.schedule(child)
	}
	r.mu.Lock()
	delete(child.parents, key)
	delete(r.parents, key)
	r.retireLocked(child)
	r.mu.Unlock()
	select {
	case <-canceled:
	case <-ctx.Done():
		t.Fatal("retired load was not canceled")
	}
	r.schedule(child)
	if calls.Load() != 1 {
		t.Fatalf("started %d loads", calls.Load())
	}
}

func TestReferenceOwnerTransferPreservesChildAndItsCommit(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, r, old, record := referenceFixture(t, ctx, coordinatorStub{unsubscribe: func(context.Context, string) error { return nil }})
	child := r.parents[old]
	child.finished = true
	close(child.ready)
	newKey := old
	newKey.Value = "new"
	batch := q.Batch{Membership: q.MembershipDelta{Removed: []storage.EntityKey{old}, Insertions: []q.Insertion{{Key: newKey, FinalIndex: 0}}}, Changes: []q.Change{{Kind: q.Added, Row: q.Row{Key: newKey, Record: record}}}}
	jobs, err := r.advance(&batch)
	if err != nil {
		t.Fatal(err)
	}
	if r.parents[newKey] != child || child.retired {
		t.Fatal("owner transfer retired the child")
	}
	metadata := q.DeliveryMetadata{Revision: 2}
	change := child.advance(metadata, q.Batch{Changes: []q.Change{{Kind: q.Updated, Row: q.Row{Key: newKey, Record: record}}}})
	if change == nil || len(change.Target.Reference.Parents) != 1 || change.Target.Reference.Parents[0] != newKey {
		t.Fatalf("commit lost new owner: %+v", change)
	}
	for _, job := range jobs {
		job()
	}
	if child.queued || len(r.parent.owner.referenceWork.loads) != 0 {
		t.Fatal("loaded child was sent through the load queue")
	}
}

func TestReferenceLoadOverflowFailsOnlyItsParent(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	started, canceled, closed := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	coordinator := coordinatorStub{subscribe: func(ctx context.Context, _, _ string, _ storage.QuerySpec, _ engine.InitialWindow, _ int) (*engine.OpenSubscription, error) {
		if calls.Add(1) != 1 {
			return nil, errors.New("unexpected extra load")
		}
		close(started)
		<-ctx.Done()
		close(canceled)
		return nil, ctx.Err()
	}, unsubscribe: func(context.Context, string) error { close(closed); return nil }, progress: func(context.Context, string) (*delivery.ProgressReport, error) { return nil, nil }}
	feed, r, key, _ := referenceFixture(t, ctx, coordinator)
	r.schedule(r.parents[key])
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	for _, value := range []string{"2", "3"} {
		other := key
		other.Value = value
		record := storage.Record{"task_id": {Type: storage.TypeInteger, State: storage.ValuePresent, Data: []byte(value)}}
		r.mu.Lock()
		child, _, err := r.bindLocked(other, record)
		r.mu.Unlock()
		if err != nil {
			t.Fatal(err)
		}
		r.schedule(child)
	}
	event, err := feed.Next(ctx)
	if err != nil || event.Failure == nil || event.Failure.SubscriptionID != "parent" || !errors.Is(event.Failure.Cause, q.ErrBackpressure) {
		t.Fatalf("overflow: %+v, %v", event, err)
	}
	select {
	case <-canceled:
	case <-ctx.Done():
		t.Fatal("overflow did not cancel in-flight work")
	}
	select {
	case <-closed:
	case <-ctx.Done():
		t.Fatal("failed parent was not retired")
	}
	if err := feed.RequestProgress(ctx); err != nil {
		t.Fatalf("overflow killed the feed: %v", err)
	}
	if calls.Load() != 1 {
		t.Fatal("failed parent's queued load ran")
	}
}

func TestRetiredQueuedReferenceDoesNotStartIO(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	var calls atomic.Int32
	feed, r, key, _ := referenceFixture(t, ctx, coordinatorStub{subscribe: func(context.Context, string, string, storage.QuerySpec, engine.InitialWindow, int) (*engine.OpenSubscription, error) {
		calls.Add(1)
		return nil, errors.New("retired work started")
	}})
	started, release, processed := make(chan struct{}), make(chan struct{}), make(chan struct{})
	if err := feed.referenceWork.submit(func() {
		close(started)
		select {
		case <-release:
		case <-ctx.Done():
		}
	}, false); err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	child := r.parents[key]
	r.schedule(child)
	r.mu.Lock()
	delete(r.parents, key)
	delete(child.parents, key)
	r.retireLocked(child)
	r.mu.Unlock()
	// Let the worker advance and wait behind the canceled queued interest.
	close(release)
	select {
	case feed.referenceWork.loads <- func() { close(processed) }:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	select {
	case <-processed:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if calls.Load() != 0 {
		t.Fatal("retired interest performed IO")
	}
}
