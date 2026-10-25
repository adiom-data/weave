package dependency

import (
	"context"
	"fmt"
	"testing"
	"time"

	q "github.com/adiom-data/weave/internal/querysubscription/model"
	"github.com/adiom-data/weave/internal/storage"
)

func cleanupGeneration(size int) *generation {
	return &generation{owner: &Cache{options: Options{CleanupBatchSize: size}}, ctx: context.Background(), entities: make(map[storage.EntityKey]*entityCell), shared: make(map[string]*entry), wake: make(chan struct{}, 1)}
}

func TestRetirementTransfersRowsAndReleasesExactCellsOnce(t *testing.T) {
	g := cleanupGeneration(2)
	e := &entry{id: "old", fingerprint: "query", finished: true, cancel: func() {}, state: result{rows: make(map[storage.EntityKey]row)}}
	g.shared[e.fingerprint] = e
	for n := 0; n < 10; n++ {
		value := testRow(fmt.Sprint(n), "old")
		e.state.rows[value.Key] = row{cell: g.retain(value)}
	}
	g.retireLocked(e)
	g.retireLocked(e)
	if e.state.rows != nil || g.shared[e.fingerprint] != nil || len(g.cleanup) != 1 || len(g.retired) != 1 {
		t.Fatal("retirement was not an exactly-once ownership transfer")
	}
	if g.entities[testRow("0", "").Key].refs != 1 || len(g.entities) != 10 {
		t.Fatal("retirement walked and released rows eagerly")
	}
	// Reuse one exact cell and replace another after retirement while old
	// ownership is still pending. Cleanup must not remove either new owner.
	sharedRow := testRow("0", "old")
	shared := g.retain(sharedRow)
	newRow := testRow("1", "new")
	replacement := g.retain(newRow)
	rows := g.cleanup[0]
	g.cleanup = nil
	if !g.cleanupRows(rows) {
		t.Fatal("cleanup canceled")
	}
	if len(rows) != 0 || len(g.entities) != 2 || shared.refs != 1 || g.entities[newRow.Key] != replacement || replacement.refs != 1 {
		t.Fatal("cleanup damaged shared or replacement ownership")
	}
	if !g.cleanupRows(rows) || shared.refs != 1 {
		t.Fatal("cleanup released references twice")
	}
	g.release(sharedRow.Key, shared)
	g.release(newRow.Key, replacement)
	if len(g.entities) != 0 {
		t.Fatal("retired references leaked")
	}
}

type cleanupProbeContext struct {
	context.Context
	probe func() error
}

func (c cleanupProbeContext) Err() error { return c.probe() }

func TestCleanupReleasesLockBetweenBoundedRowBatches(t *testing.T) {
	g := cleanupGeneration(2)
	rows := make(map[storage.EntityKey]row)
	for n := 0; n < 5; n++ {
		value := testRow(fmt.Sprint(n), "value")
		rows[value.Key] = row{cell: g.retain(value)}
	}
	var remaining []int
	g.ctx = cleanupProbeContext{Context: context.Background(), probe: func() error {
		if !g.owner.mu.TryLock() {
			t.Fatal("cleanup retained the cache lock between batches")
		}
		remaining = append(remaining, len(g.entities))
		g.owner.mu.Unlock()
		return nil
	}}
	if !g.cleanupRows(rows) {
		t.Fatal("cleanup canceled")
	}
	if len(remaining) != 3 || remaining[0] != 5 || remaining[1] != 3 || remaining[2] != 1 || len(g.entities) != 0 {
		t.Fatalf("unexpected batch boundaries: %v", remaining)
	}
}

func TestCleanupStopsOnGenerationShutdown(t *testing.T) {
	g := cleanupGeneration(2)
	ctx, cancel := context.WithCancel(context.Background())
	g.ctx = ctx
	rows := make(map[storage.EntityKey]row)
	for n := 0; n < 5; n++ {
		value := testRow(fmt.Sprint(n), "value")
		rows[value.Key] = row{cell: g.retain(value)}
	}
	cancel()
	if g.cleanupRows(rows) {
		t.Fatal("cleanup ignored shutdown")
	}
	if len(g.entities) != 5 {
		t.Fatal("canceled cleanup mutated the discarded generation")
	}
}

func TestMaintenanceCleansLargeDependencyWithoutRemovingSharedEntity(t *testing.T) {
	source := &testClient{}
	c := newCache(t, source)
	c.options.CleanupBatchSize = 3 // Before any generation/worker is started.
	f := open(t, c, 16)
	other, _ := subscribe(t, f, "other", "tasks")
	var handle q.QuerySubscription
	if err := f.SubscribeQuery(context.Background(), q.QuerySubscribeRequest{ID: "list", Query: query("tasks"), Window: q.InitialWindow{Count: 1}}, func(h q.QuerySubscription, _ q.QuerySnapshot) error { handle = h; return nil }); err != nil {
		t.Fatal(err)
	}
	s := handle.(*subscription)
	changes := make([]q.Change, 0, 500)
	insertions := make([]q.Insertion, 0, 500)
	for n := 2; n <= 501; n++ {
		value := testRow(fmt.Sprint(n), "value")
		changes = append(changes, q.Change{Kind: q.Added, Row: value})
		insertions = append(insertions, q.Insertion{Key: value.Key, FinalIndex: n - 1})
	}
	source.latest().emit([]q.SubscriptionEffect{{SubscriptionID: s.entry.id, Batch: q.Batch{Changes: changes, Membership: q.MembershipDelta{Insertions: insertions}}}}, false)
	next(t, f)
	if err := handle.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case <-source.latest().closedSubscriptions:
	case <-time.After(time.Second):
		t.Fatal("retired dependency did not finish cleanup and close")
	}
	c.mu.Lock()
	g := s.feed.generation
	remaining := len(g.entities)
	cell := g.entities[testRow("1", "").Key]
	valid := cell != nil && cell.refs == 1 && !other.(*subscription).entry.retired
	c.mu.Unlock()
	if remaining != 1 || !valid {
		t.Fatalf("cleanup left %d cells or damaged live subscription", remaining)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	cleared := g.cleanup == nil && g.entities == nil && g.entries == nil
	c.mu.Unlock()
	if !cleared {
		t.Fatal("generation shutdown retained cleanup state")
	}
}
