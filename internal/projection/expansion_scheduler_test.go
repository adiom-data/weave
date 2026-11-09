package projection

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/adiom-data/weave/internal/querysubscription/model"
	"github.com/adiom-data/weave/internal/storage"
)

func TestExpansionSchedulerBoundsConcurrency(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	release := make(chan struct{})
	started := make(chan struct{}, 20)
	var active atomic.Int32
	var maximum atomic.Int32
	scheduler := newExpansionScheduler(ctx, 3, 20, func(projectionExpansion) {
		current := active.Add(1)
		for {
			observed := maximum.Load()
			if current <= observed || maximum.CompareAndSwap(observed, current) {
				break
			}
		}
		started <- struct{}{}
		<-release
		active.Add(-1)
	})
	defer scheduler.stop()

	parent := &projectionInstance{id: "parent"}
	jobs := make([]projectionExpansion, 20)
	for index := range jobs {
		jobs[index] = projectionExpansion{parent: parent, key: storage.EntityKey{Value: fmt.Sprint(index)}}
	}
	if err := scheduler.enqueue(jobs); err != nil {
		t.Fatal(err)
	}
	for range 3 {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("workers did not start")
		}
	}
	if got := maximum.Load(); got != 3 {
		t.Fatalf("maximum concurrency = %d, want 3", got)
	}
	close(release)
	if err := scheduler.waitIdle(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestExpansionLimiterBoundsSnapshotReads(t *testing.T) {
	limiter := NewExpansionLimiter(2)
	if err := limiter.acquire(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := limiter.acquire(context.Background()); err != nil {
		t.Fatal(err)
	}
	timed, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := limiter.acquire(timed); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("third acquire error = %v, want deadline exceeded", err)
	}
	limiter.release()
	if err := limiter.acquire(context.Background()); err != nil {
		t.Fatal(err)
	}
	limiter.release()
	limiter.release()
}

func TestExpansionSchedulerCoalescesQueuedGenerations(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	block := make(chan struct{})
	started := make(chan struct{})
	var once sync.Once
	var generations []*projectionMember
	var generationsMu sync.Mutex
	scheduler := newExpansionScheduler(ctx, 1, 4, func(expansion projectionExpansion) {
		if expansion.parent.id == "blocker" {
			once.Do(func() { close(started) })
			<-block
			return
		}
		generationsMu.Lock()
		generations = append(generations, expansion.member)
		generationsMu.Unlock()
	})
	defer scheduler.stop()

	if err := scheduler.enqueue([]projectionExpansion{{parent: &projectionInstance{id: "blocker"}, key: storage.EntityKey{Value: "row"}}}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("blocking expansion did not start")
	}
	parent := &projectionInstance{id: "parent"}
	first, second := &projectionMember{}, &projectionMember{}
	if err := scheduler.enqueue([]projectionExpansion{{parent: parent, key: storage.EntityKey{Value: "row"}, member: first}}); err != nil {
		t.Fatal(err)
	}
	if err := scheduler.enqueue([]projectionExpansion{{parent: parent, key: storage.EntityKey{Value: "row"}, member: second}}); err != nil {
		t.Fatal(err)
	}
	close(block)
	if err := scheduler.waitIdle(context.Background()); err != nil {
		t.Fatal(err)
	}
	generationsMu.Lock()
	defer generationsMu.Unlock()
	if len(generations) != 1 || generations[0] != second {
		t.Fatalf("processed generations = %v, want only the second member", generations)
	}
}

func TestExpansionSchedulerRejectsExcessPendingWork(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	block := make(chan struct{})
	started := make(chan struct{})
	var once sync.Once
	scheduler := newExpansionScheduler(ctx, 1, 1, func(projectionExpansion) {
		once.Do(func() { close(started) })
		<-block
	})
	defer scheduler.stop()

	if err := scheduler.enqueue([]projectionExpansion{{parent: &projectionInstance{id: "running"}, key: storage.EntityKey{Value: "row"}}}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("blocking expansion did not start")
	}
	if err := scheduler.enqueue([]projectionExpansion{{parent: &projectionInstance{id: "queued"}, key: storage.EntityKey{Value: "one"}}}); err != nil {
		t.Fatal(err)
	}
	err := scheduler.enqueue([]projectionExpansion{{parent: &projectionInstance{id: "overflow"}, key: storage.EntityKey{Value: "two"}}})
	if !errors.Is(err, errExpansionBacklogFull) {
		t.Fatalf("overflow error = %v, want %v", err, errExpansionBacklogFull)
	}
	close(block)
	if err := scheduler.waitIdle(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestExpansionSchedulerSerializesAndCoalescesTheSameGeneration(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	release := make(chan struct{})
	defer close(release)
	started := make(chan uint64, 8)
	scheduler := newExpansionScheduler(ctx, 2, 2, func(expansion projectionExpansion) {
		started <- expansion.revision
		if expansion.revision == 1 {
			select {
			case <-release:
			case <-ctx.Done():
			}
		}
	})
	defer scheduler.stop()
	parent := &projectionInstance{id: "parent"}
	member := newProjectionMember(nil, querysubscription.RecordVersion{}, nil, false)
	first := projectionExpansion{parent: parent, key: storage.EntityKey{Value: "one"}, member: member, revision: 1}
	if err := scheduler.enqueue([]projectionExpansion{first}); err != nil {
		t.Fatal(err)
	}
	receive := func(want uint64) {
		t.Helper()
		select {
		case got := <-started:
			if got != want {
				t.Fatalf("started revision %d, want %d", got, want)
			}
		case <-ctx.Done():
			t.Fatal("runnable work was blocked by another key")
		}
	}
	receive(1)
	second, third, other := first, first, first
	second.revision, third.revision, other.revision = 2, 3, 4
	other.key.Value = "two"
	if err := scheduler.enqueue([]projectionExpansion{second, third, other}); err != nil {
		t.Fatal(err)
	}
	receive(4) // The same generation must wait, but another row must not.
	// Stop must discard the blocked successor without declaring an active
	// generation idle or running the superseded revision on another worker.
	scheduler.stop()
	if scheduler.isIdle() {
		t.Fatal("stopping discarded a still-running expansion")
	}
	cancel()
	if err := scheduler.waitIdle(t.Context()); err != nil {
		t.Fatal(err)
	}
	if len(started) != 0 {
		t.Fatal("stopped scheduler ran a blocked successor")
	}
}

func TestExpansionSchedulerRunsLatestSuccessorAndDoesNotBlockRebinds(t *testing.T) {
	for _, rebind := range []bool{false, true} {
		name := "same-generation"
		if rebind {
			name = "new-selector"
		}
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			release := make(chan struct{})
			var once sync.Once
			defer once.Do(func() { close(release) })
			started := make(chan uint64, 8)
			scheduler := newExpansionScheduler(ctx, 2, 1, func(expansion projectionExpansion) {
				started <- expansion.revision
				if expansion.revision == 1 {
					select {
					case <-release:
					case <-ctx.Done():
					}
				}
			})
			defer scheduler.stop()
			first := projectionExpansion{parent: &projectionInstance{id: "parent"}, key: storage.EntityKey{Value: "row"}, member: newProjectionMember(nil, querysubscription.RecordVersion{}, nil, false), revision: 1}
			if err := scheduler.enqueue([]projectionExpansion{first}); err != nil {
				t.Fatal(err)
			}
			select {
			case <-started:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			second, latest := first, first
			second.revision, latest.revision = 2, 3
			if rebind {
				latest.member = newProjectionMember(nil, querysubscription.RecordVersion{}, nil, false)
			}
			if err := scheduler.enqueue([]projectionExpansion{second, latest}); err != nil {
				t.Fatal(err)
			}
			if !rebind {
				once.Do(func() { close(release) })
			}
			select {
			case revision := <-started:
				if revision != 3 {
					t.Fatalf("superseded successor ran: %d", revision)
				}
			case <-ctx.Done():
				t.Fatal("new selector waited for an obsolete generation")
			}
			once.Do(func() { close(release) })
			if err := scheduler.waitIdle(ctx); err != nil {
				t.Fatal(err)
			}
			if len(started) != 0 {
				t.Fatal("successor ran more than once")
			}
		})
	}
}
