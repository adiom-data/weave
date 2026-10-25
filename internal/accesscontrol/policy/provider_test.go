package policy

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestProviderPinsOneVersionPerPassAndReusesCompilation(t *testing.T) {
	var mu sync.Mutex
	version, calls := 1, 0
	provider := ProviderFunc(func(context.Context, Key) (Snapshot, error) {
		mu.Lock()
		defer mu.Unlock()
		calls++
		return Snapshot{Version: fmt.Sprint(version), Policy: &Relation{Allow: []Rule{{"allow", fmt.Sprint(version == 1)}}}}, nil
	})
	e, err := NewEvaluator(context.Background(), provider, DefaultLimits(), CacheOptions{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	first := e.Begin()
	original, err := first.load(context.Background(), projects)
	if err != nil {
		t.Fatal(err)
	}
	same, err := e.Begin().load(context.Background(), projects)
	if err != nil || same != original {
		t.Fatalf("compilation not reused: %v", err)
	}
	mu.Lock()
	version = 2
	mu.Unlock()
	pinned, err := first.load(context.Background(), projects)
	if err != nil || pinned != original {
		t.Fatal("pass changed policies")
	}
	latest, err := e.Begin().load(context.Background(), projects)
	if err != nil || latest == original {
		t.Fatal("new pass did not adopt version")
	}
	mu.Lock()
	defer mu.Unlock()
	if calls != 3 {
		t.Fatalf("provider calls=%d", calls)
	}
}

func TestPolicyInvalidationClosesRegistrationRaceAndIsTargeted(t *testing.T) {
	hints := make(chan []Key)
	e, err := NewEvaluator(context.Background(), ProviderFunc(func(context.Context, Key) (Snapshot, error) { return Snapshot{Version: "v1", Policy: &Relation{}}, nil }), DefaultLimits(), CacheOptions{}, hints)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	a, b := e.Watch(), e.Watch()
	defer a.Close()
	defer b.Close()
	pass := e.Begin()
	if _, err := pass.load(context.Background(), projects); err != nil {
		t.Fatal(err)
	}
	other := e.Begin()
	if _, err := other.load(context.Background(), tasks); err != nil {
		t.Fatal(err)
	}
	b.Replace(nil, other.Stamps([]Key{tasks}))
	// An invalidation between fetching and registering must not be lost.
	e.store.mu.Lock()
	e.store.invalidateLocked(projects, e.store.entries[projects])
	e.store.mu.Unlock()
	keys := a.Replace(nil, pass.Stamps([]Key{projects}))
	if len(keys) != 1 || len(a.Drain()) != 1 {
		t.Fatal("lost pre-registration invalidation")
	}
	select {
	case <-a.Changed():
	default:
		t.Fatal("no wakeup")
	}
	hints <- []Key{projects}
	select {
	case <-a.Changed():
	case <-time.After(time.Second):
		t.Fatal("no targeted invalidation")
	}
	if len(a.Drain()) != 1 || len(b.Drain()) != 0 {
		t.Fatal("unrelated watcher invalidated")
	}
	a.Release(keys)
	e.store.expire(time.Now().Add(time.Hour))
	e.store.mu.Lock()
	defer e.store.mu.Unlock()
	if e.store.entries[projects] != nil || e.store.entries[tasks] == nil {
		t.Fatal("idle eviction did not preserve active policies")
	}
}

func TestSlowProviderDoesNotHoldSharedCacheLock(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	e, err := NewEvaluator(context.Background(), ProviderFunc(func(ctx context.Context, key Key) (Snapshot, error) {
		if key == projects {
			close(started)
			select {
			case <-release:
			case <-ctx.Done():
				return Snapshot{}, ctx.Err()
			}
		}
		return Snapshot{Version: "1", Policy: &Relation{}}, nil
	}), DefaultLimits(), CacheOptions{LoadConcurrency: 2}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	done := make(chan error, 1)
	go func() { _, err := e.Begin().load(context.Background(), projects); done <- err }()
	<-started
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := e.Begin().load(ctx, tasks); err != nil {
		t.Fatal(err)
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestPolicyProviderMissingVersionAndCompilationErrors(t *testing.T) {
	for _, snapshot := range []Snapshot{{}, {Version: "1", Policy: &Relation{Allow: []Rule{{"invalid", "42"}}}}} {
		e, err := NewEvaluator(context.Background(), ProviderFunc(func(context.Context, Key) (Snapshot, error) { return snapshot, nil }), DefaultLimits(), CacheOptions{}, nil)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := e.Begin().load(context.Background(), projects); err == nil {
			t.Error("invalid provider response accepted")
		}
		e.Close()
	}
}
