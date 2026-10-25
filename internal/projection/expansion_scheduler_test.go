package projection

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

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
