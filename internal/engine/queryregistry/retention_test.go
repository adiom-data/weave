package queryregistry

import (
	"testing"
	"time"

	"github.com/adiom-data/weave/internal/datastore"
	"github.com/adiom-data/weave/internal/engine/entitycache"
	"github.com/adiom-data/weave/internal/storage"
)

func retainedRegistry(t *testing.T) (*QueryRegistry, *entitycache.EntityCache, fakePlan, *time.Time) {
	t.Helper()
	entities := entitycache.NewEntityCache()
	r := NewQueryRegistryWithRetention(entities, time.Minute)
	now := time.Unix(100, 0)
	r.now = func() time.Time { return now }
	plan := fakePlan{fingerprint: "query", relation: storage.RelationKey{Source: "test", Name: "tasks"}}
	r.Acquire(plan)
	if err := r.SetWindow("query", "old", retentionAll()); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"a", "b", "c", "d"} {
		key := storage.EntityKey{Relation: plan.relation, Value: id}
		entities.StoreCurrent(key, storage.Record{"id": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte(id)}})
		if _, _, err := r.AddMember("query", key); err != nil {
			t.Fatal(err)
		}
	}
	return r, entities, plan, &now
}
func retentionAll() datastore.Bounds {
	return datastore.Bounds{Left: datastore.Boundary{Kind: datastore.BoundaryStart}, Right: datastore.Boundary{Kind: datastore.BoundaryEnd}}
}
func retentionCursor(key string) datastore.Boundary {
	return datastore.Boundary{Kind: datastore.BoundaryCursor, Key: datastore.EncodedSortKey(key)}
}
func drainRetention(t *testing.T, r *QueryRegistry, now time.Time) {
	t.Helper()
	for step := 0; step < 100; step++ {
		failures, more := r.Maintain(now, 2)
		if len(failures) > 0 {
			t.Fatal(failures)
		}
		if !more {
			return
		}
	}
	t.Fatal("maintenance did not finish")
}

func closeRetainedWindow(t *testing.T, r *QueryRegistry, id string) {
	t.Helper()
	if _, err := r.RemoveWindow("query", id); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Release("query"); err != nil {
		t.Fatal(err)
	}
}

func TestRetainedQueryRemainsReadyUntilExpiredAndCanBePartiallyRescued(t *testing.T) {
	r, entities, plan, now := retainedRegistry(t)
	if _, err := r.RemoveWindow("query", "old"); err != nil {
		t.Fatal(err)
	}
	if removed, err := r.Release("query"); err != nil || removed {
		t.Fatalf("idle query retired early: %v %v", removed, err)
	}
	if gaps, err := r.ReadyGaps("query", retentionAll()); err != nil || len(gaps) != 0 {
		t.Fatalf("retained coverage not ready: %v %v", gaps, err)
	}
	drainRetention(t, r, now.Add(59*time.Second))
	if _, exists := r.Plan("query"); !exists {
		t.Fatal("query disappeared during grace")
	}
	r.Acquire(plan)
	rescue := datastore.Bounds{Left: retentionCursor("a"), Right: retentionCursor("c")}
	if ready, err := r.InstallWindow("query", "new", rescue); err != nil || !ready {
		t.Fatalf("warm interval not reused: %v %v", ready, err)
	}
	drainRetention(t, r, now.Add(time.Minute))
	for _, id := range []string{"a", "b", "c", "d"} {
		key := storage.EntityKey{Relation: plan.relation, Value: id}
		want := id == "b" || id == "c"
		if r.IsMember("query", key) != want {
			t.Fatalf("membership for %s not rescued/pruned correctly", id)
		}
		value, _ := entities.Get(key)
		if (value.References > 0) != want {
			t.Fatalf("wrong entity ownership for %s", id)
		}
	}
	*now = now.Add(2 * time.Minute)
	if _, err := r.RemoveWindow("query", "new"); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Release("query"); err != nil {
		t.Fatal(err)
	}
	drainRetention(t, r, now.Add(time.Minute))
	if _, exists := r.Plan("query"); exists {
		t.Fatal("fully expired query remained registered")
	}
}

func TestRetentionDoesNotExtendOrDuplicateActiveCoverage(t *testing.T) {
	r, _, plan, _ := retainedRegistry(t)
	// Strict expansion of an already narrow window retains nothing.
	narrow := datastore.Bounds{Left: retentionCursor("a"), Right: retentionCursor("c")}
	if err := r.SetWindow("query", "old", narrow); err != nil {
		t.Fatal(err)
	}
	if len(r.retained) != 2 {
		t.Fatalf("expected two abandoned ranges, got %d", len(r.retained))
	}
	count := len(r.retained)
	if err := r.SetWindow("query", "old", retentionAll()); err != nil {
		t.Fatal(err)
	}
	if len(r.retained) != count {
		t.Fatal("strict expansion created a retention window")
	}
	r.Acquire(plan)
	if err := r.SetWindow("query", "other", retentionAll()); err != nil {
		t.Fatal(err)
	}
	if _, err := r.RemoveWindow("query", "old"); err != nil {
		t.Fatal(err)
	}
	if len(r.retained) != count {
		t.Fatal("covered removal created redundant retention")
	}
}

func TestNewGraceIsNotShortenedByOlderRetention(t *testing.T) {
	r, _, plan, now := retainedRegistry(t)
	closeRetainedWindow(t, r, "old")
	*now = now.Add(30 * time.Second)
	r.Acquire(plan)
	if err := r.SetWindow("query", "new", retentionAll()); err != nil {
		t.Fatal(err)
	}
	closeRetainedWindow(t, r, "new")
	drainRetention(t, r, now.Add(31*time.Second))
	if gaps, err := r.ReadyGaps("query", retentionAll()); err != nil || len(gaps) != 0 {
		t.Fatal("older expiry shortened new grace")
	}
	drainRetention(t, r, now.Add(time.Minute))
	if _, exists := r.Plan("query"); exists {
		t.Fatal("new grace did not expire")
	}
}

func TestPruningRechecksCoverageAndSurvivesTruncateAndGenerationReplacement(t *testing.T) {
	r, entities, plan, now := retainedRegistry(t)
	// Keep a live, empty window so expiry prunes without deleting the query.
	empty := datastore.Bounds{Left: retentionCursor("z"), Right: retentionCursor("z")}
	if err := r.SetWindow("query", "old", empty); err != nil {
		t.Fatal(err)
	}
	if failures, more := r.Maintain(now.Add(time.Minute), 1); len(failures) != 0 || !more {
		t.Fatalf("expected bounded backlog: %v %v", failures, more)
	}
	if err := r.SetWindow("query", "rescue", retentionAll()); err != nil {
		t.Fatal(err)
	}
	drainRetention(t, r, now.Add(time.Minute))
	for _, id := range []string{"b", "c", "d"} {
		if !r.IsMember("query", storage.EntityKey{Relation: plan.relation, Value: id}) {
			t.Fatal("new coverage did not rescue queued member")
		}
	}
	// Queue old roots again, then TRUNCATE must release all actual ownership.
	if _, err := r.RemoveWindow("query", "rescue"); err != nil {
		t.Fatal(err)
	}
	r.queries["query"].prune = append(r.queries["query"].prune, retentionAll())
	if err := r.PruneOutsideCoverage("query"); err != nil {
		t.Fatal(err)
	}
	if _, err := r.ClearRelation(plan.relation); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"a", "b", "c", "d"} {
		if row, _ := entities.Get(storage.EntityKey{Relation: plan.relation, Value: id}); row.References != 0 {
			t.Fatal("truncate leaked ownership")
		}
	}
	// Explicit failure can replace a fingerprint while old prune roots remain.
	if err := r.Discard("query"); err != nil {
		t.Fatal(err)
	}
	r.Acquire(plan)
	if err := r.SetWindow("query", "replacement", retentionAll()); err != nil {
		t.Fatal(err)
	}
	key := storage.EntityKey{Relation: plan.relation, Value: "b"}
	entities.StoreCurrent(key, storage.Record{"id": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("b")}})
	if _, _, err := r.AddMember("query", key); err != nil {
		t.Fatal(err)
	}
	drainRetention(t, r, now.Add(3*time.Minute))
	if !r.IsMember("query", key) {
		t.Fatal("old cleanup damaged new query generation")
	}
}

func TestUnreadyWindowNeverBecomesReusableRetention(t *testing.T) {
	r := NewQueryRegistryWithRetention(entitycache.NewEntityCache(), time.Minute)
	plan := fakePlan{fingerprint: "query", relation: storage.RelationKey{Source: "test", Name: "tasks"}}
	r.Acquire(plan)
	if _, err := r.InstallWindow("query", "loading", retentionAll()); err != nil {
		t.Fatal(err)
	}
	if _, err := r.RemoveWindow("query", "loading"); err != nil {
		t.Fatal(err)
	}
	if len(r.retained) != 0 {
		t.Fatal("unready range was retained as ready")
	}
}

func TestPendingPruneOwnershipRemainsVisibleToEntityDeletion(t *testing.T) {
	r, entities, plan, now := retainedRegistry(t)
	closeRetainedWindow(t, r, "old")
	if failures, more := r.Maintain(now.Add(time.Minute), 1); len(failures) != 0 || !more {
		t.Fatalf("expected cleanup backlog: %v %v", failures, more)
	}
	key := storage.EntityKey{Relation: plan.relation, Value: "b"}
	queries, err := r.RemoveEntity(key)
	if err != nil || len(queries) != 1 {
		t.Fatalf("pending ownership was hidden from delete: %v %v", queries, err)
	}
	if err := entities.DeleteAuthoritative(key); err != nil {
		t.Fatal(err)
	}
	drainRetention(t, r, now.Add(time.Minute))
	if _, exists := r.Plan("query"); exists {
		t.Fatal("expired query remained after deletion and cleanup")
	}
}

func TestLastReleaseDuringPruningRemainsBounded(t *testing.T) {
	r, entities, plan, now := retainedRegistry(t)
	empty := datastore.Bounds{Left: retentionCursor("z"), Right: retentionCursor("z")}
	if err := r.SetWindow("query", "old", empty); err != nil {
		t.Fatal(err)
	}
	expired := now.Add(time.Minute)
	if failures, more := r.Maintain(expired, 1); len(failures) != 0 || !more {
		t.Fatalf("expected pruning backlog: %v %v", failures, more)
	}
	if got := len(r.Members("query")); got != 3 {
		t.Fatalf("first batch left %d members, want 3", got)
	}
	closeRetainedWindow(t, r, "old")
	if _, exists := r.Plan("query"); !exists {
		t.Fatal("last release synchronously tore down a pruning query")
	}
	if got := len(r.Members("query")); got != 3 {
		t.Fatalf("last release removed memberships: remaining=%d", got)
	}
	for _, id := range []string{"b", "c", "d"} {
		key := storage.EntityKey{Relation: plan.relation, Value: id}
		if row, _ := entities.Get(key); row.References != 1 {
			t.Fatalf("last release changed ownership for %s", id)
		}
	}
	if failures, more := r.Maintain(expired, 1); len(failures) != 0 || !more {
		t.Fatalf("expected bounded continuation: %v %v", failures, more)
	}
	if got := len(r.Members("query")); got != 2 {
		t.Fatalf("continuation left %d members, want 2", got)
	}
	drainRetention(t, r, expired)
	if _, exists := r.Plan("query"); exists {
		t.Fatal("drained query remained registered")
	}
	for _, id := range []string{"a", "b", "c", "d"} {
		if row, _ := entities.Get(storage.EntityKey{Relation: plan.relation, Value: id}); row.References != 0 {
			t.Fatalf("cleanup leaked ownership for %s", id)
		}
	}
}

func TestReleasedQueryCanBeReacquiredDuringPruning(t *testing.T) {
	r, _, plan, now := retainedRegistry(t)
	closeRetainedWindow(t, r, "old")
	expired := now.Add(time.Minute)
	if failures, more := r.Maintain(expired, 1); len(failures) != 0 || !more {
		t.Fatalf("expected pruning backlog: %v %v", failures, more)
	}
	// Releasing an initialization that never installed a window must not
	// synchronously consume the old cleanup backlog either.
	r.Acquire(plan)
	if removed, err := r.Release("query"); err != nil || removed {
		t.Fatalf("release during cleanup: %v %v", removed, err)
	}
	if _, created := r.Acquire(plan); created {
		t.Fatal("pending query was discarded instead of reused")
	}
	rescue := datastore.Bounds{Left: retentionCursor("b"), Right: retentionCursor("d")}
	if err := r.SetWindow("query", "rescued", rescue); err != nil {
		t.Fatal(err)
	}
	drainRetention(t, r, expired)
	if got := len(r.Members("query")); got != 2 {
		t.Fatalf("rescued membership count = %d, want 2", got)
	}
	for _, id := range []string{"c", "d"} {
		if !r.IsMember("query", storage.EntityKey{Relation: plan.relation, Value: id}) {
			t.Fatalf("cleanup removed rescued member %s", id)
		}
	}
	closeRetainedWindow(t, r, "rescued")
	drainRetention(t, r, expired)
	if _, exists := r.Plan("query"); exists {
		t.Fatal("rescued query did not eventually retire")
	}
}
