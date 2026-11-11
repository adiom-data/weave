package postgres

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/adiom-data/weave/internal/datastore"
	"github.com/adiom-data/weave/internal/storage"
)

func TestCompilationDoesNotMutateSharedTableMetadata(t *testing.T) {
	metadata := contractPrepared().plan
	original := cloneQuerySpec(metadata.query)
	var wg sync.WaitGroup
	for range 32 {
		wg.Go(func() {
			query := cloneQuerySpec(original)
			compiled, err := compileCompletePlan(metadata, query, DefaultQueryLimits())
			if err != nil {
				t.Error(err)
				return
			}
			if compiled == metadata || compiled.query.Filter == query.Filter || len(compiled.comparisons) == 0 {
				t.Error("query preparation did not own its expression state")
			}
		})
	}
	wg.Wait()
	if !reflect.DeepEqual(metadata.query, original) || len(metadata.comparisons) != 0 {
		t.Fatal("shared table metadata was mutated")
	}
}

type metadataWaitContext struct {
	context.Context
	entered chan struct{}
	once    sync.Once
}

func (c *metadataWaitContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.entered) })
	return c.Context.Done()
}

func TestMetadataSharedDiscoverySurvivesCreatorCancellation(t *testing.T) {
	var cache metadataCache
	name := storage.RelationName{Namespace: "app", Name: "tasks"}
	creator, cancel := context.WithCancel(context.Background())
	defer cancel()
	started, release, canceled := make(chan struct{}), make(chan struct{}), make(chan struct{})
	want := &plan{relationID: 1}
	load := func(ctx context.Context) (*plan, error) {
		close(started)
		select {
		case <-release:
			return want, nil
		case <-ctx.Done():
			close(canceled)
			return nil, ctx.Err()
		}
	}
	first := make(chan error, 1)
	go func() { _, err := cache.get(creator, name, load); first <- err }()
	<-started
	follower := &metadataWaitContext{Context: context.Background(), entered: make(chan struct{})}
	second := make(chan error, 1)
	go func() {
		got, err := cache.get(follower, name, load)
		if err == nil && got != want {
			err = errors.New("wrong metadata")
		}
		second <- err
	}()
	<-follower.entered
	cancel()
	if err := <-first; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	select {
	case <-canceled:
		t.Fatal("one request canceled another's discovery")
	default:
	}
	close(release)
	if err := <-second; err != nil {
		t.Fatal(err)
	}
}

func TestMetadataDiscoveryCancelsWhenLastWaiterLeaves(t *testing.T) {
	var cache metadataCache
	ctx, cancel := context.WithCancel(context.Background())
	started, canceled := make(chan struct{}), make(chan struct{})
	done := make(chan error, 1)
	go func() {
		_, err := cache.get(ctx, storage.RelationName{Namespace: "app", Name: "tasks"}, func(ctx context.Context) (*plan, error) {
			close(started)
			<-ctx.Done()
			close(canceled)
			return nil, ctx.Err()
		})
		done <- err
	}()
	<-started
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	<-canceled
}

func TestMetadataDiscoveryCoalescesAndCachesOnlySuccess(t *testing.T) {
	source := &PostgresDataSource{}
	name := storage.RelationName{Namespace: "app", Name: "tasks"}
	ctx := context.Background()
	want := &plan{relationID: 42}
	started, release := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	load := func(context.Context) (*plan, error) {
		if calls.Add(1) == 1 {
			close(started)
		}
		<-release
		return want, nil
	}
	var wg sync.WaitGroup
	for range 20 {
		wg.Go(func() {
			got, err := source.metadata.get(ctx, name, load)
			if err != nil || got != want {
				t.Errorf("get = %p, %v", got, err)
			}
		})
	}
	<-started
	status, _ := metadataCounts(source)
	if status.CachedTables != 0 || status.LoadingTables != 1 {
		t.Fatal(status)
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := source.metadata.get(cancelled, name, load); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	close(release)
	wg.Wait()
	if calls.Load() != 1 {
		t.Fatalf("loads = %d", calls.Load())
	}
	source.metadata.mu.Lock()
	retainedContext := source.metadata.entries[name].cancel != nil
	source.metadata.mu.Unlock()
	if retainedContext {
		t.Fatal("completed metadata retained its request cancellation context")
	}
	status, _ = metadataCounts(source)
	if status.CachedTables != 1 || status.LoadingTables != 0 {
		t.Fatal(status)
	}
	if err := source.InvalidateMetadata(ctx, &name); err != nil {
		t.Fatal(err)
	}
	failure := errors.New("not published")
	if _, err := source.metadata.get(ctx, name, func(context.Context) (*plan, error) { return nil, failure }); !errors.Is(err, failure) {
		t.Fatal(err)
	}
	got, err := source.metadata.get(ctx, name, func(context.Context) (*plan, error) { return want, nil })
	if got != want || err != nil {
		t.Fatal(got, err)
	}
}

func TestMetadataInvalidationSupersedesInflightLoad(t *testing.T) {
	for _, all := range []bool{false, true} {
		t.Run(map[bool]string{false: "table", true: "all"}[all], func(t *testing.T) {
			source := &PostgresDataSource{}
			name := storage.RelationName{Namespace: "app", Name: "tasks"}
			ctx := context.Background()
			started, release := make(chan struct{}), make(chan struct{})
			done := make(chan error, 1)
			go func() {
				_, err := source.metadata.get(ctx, name, func(context.Context) (*plan, error) { close(started); <-release; return &plan{relationID: 1}, nil })
				done <- err
			}()
			<-started
			target := &name
			if all {
				target = nil
			}
			if err := source.InvalidateMetadata(ctx, target); err != nil {
				t.Fatal(err)
			}
			fresh := &plan{relationID: 2}
			if got, err := source.metadata.get(ctx, name, func(context.Context) (*plan, error) { return fresh, nil }); got != fresh || err != nil {
				t.Fatal(got, err)
			}
			close(release)
			if err := <-done; !errors.Is(err, datastore.ErrInvalidated) {
				t.Fatal(err)
			}
			got, err := source.metadata.get(ctx, name, func(context.Context) (*plan, error) { t.Error("unexpected reload"); return nil, nil })
			if got != fresh || err != nil {
				t.Fatal(got, err)
			}
			status, _ := metadataCounts(source)
			if status.CachedTables != 1 || status.LoadingTables != 0 {
				t.Fatal(status)
			}
		})
	}
}

func TestMetadataCacheKeysUseSeparateNamespaceAndTable(t *testing.T) {
	source := &PostgresDataSource{}
	ctx := context.Background()
	for i, name := range []storage.RelationName{{Namespace: "a.b", Name: "c"}, {Namespace: "a", Name: "b.c"}} {
		got, err := source.metadata.get(ctx, name, func(context.Context) (*plan, error) { return &plan{relationID: uint32(i + 1)}, nil })
		if err != nil || got.relationID != uint32(i+1) {
			t.Fatal(got, err)
		}
	}
	status, _ := metadataCounts(source)
	if status.CachedTables != 2 {
		t.Fatal(status)
	}
}

// Test-only inspection; metrics never acquire cache entries or issue actor commands.
func metadataCounts(s *PostgresDataSource) (struct{ CachedTables, LoadingTables uint64 }, error) {
	s.metadata.mu.Lock()
	defer s.metadata.mu.Unlock()
	return struct{ CachedTables, LoadingTables uint64 }{s.metadata.cached, uint64(len(s.metadata.entries)) - s.metadata.cached}, nil
}
