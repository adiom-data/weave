package recovery

import (
	"errors"
	"testing"

	"github.com/adiom-data/weave/internal/datastore"
	"github.com/adiom-data/weave/internal/engine/entitycache"
	"github.com/adiom-data/weave/internal/storage"
)

func TestManagerEnforcesRelationBufferLimits(t *testing.T) {
	manager := NewManager(2, 1, 2)
	relation := storage.RelationKey{Source: "test", Name: "tasks"}
	manager.StartRelation(relation, nil, datastore.CommittedChanges{Atomic: true}, func() {})
	if err := manager.Buffer(relation, datastore.CommittedChanges{Changes: []datastore.Change{nil, nil}}); err != nil {
		t.Fatalf("buffer first batch: %v", err)
	}
	if err := manager.Buffer(relation, datastore.CommittedChanges{}); !errors.Is(err, ErrBufferLimit) {
		t.Fatalf("second batch error = %v, want ErrBufferLimit", err)
	}
}

func TestManagerGeneratesRelationAttemptsAndCancelsRemoval(t *testing.T) {
	manager := NewManager(1, 1, 1)
	relation := storage.RelationKey{Source: "test", Name: "tasks"}
	cancelled := 0
	first, started := manager.StartRelation(relation, NewRelationSnapshot(), datastore.CommittedChanges{}, func() { cancelled++ })
	if !started {
		t.Fatal("first relation recovery did not start")
	}
	if _, duplicate := manager.StartRelation(relation, NewRelationSnapshot(), datastore.CommittedChanges{}, func() { cancelled++ }); duplicate {
		t.Fatal("duplicate relation recovery started")
	}
	if cancelled != 1 {
		t.Fatalf("duplicate attempt cancellation count = %d, want 1", cancelled)
	}
	manager.RemoveRelation(relation)
	if cancelled != 2 {
		t.Fatalf("active attempt cancellation count = %d, want 2", cancelled)
	}
	second, started := manager.StartRelation(relation, NewRelationSnapshot(), datastore.CommittedChanges{}, func() {})
	if !started || second.Generation() <= first.Generation() {
		t.Fatalf("relation generations did not advance: first=%d second=%d started=%v", first.Generation(), second.Generation(), started)
	}
}

func TestRelationSnapshotStoresOverlappingEntityOnce(t *testing.T) {
	snapshot := NewRelationSnapshot()
	key := storage.EntityKey{Relation: storage.RelationKey{Source: "test", Name: "tasks"}, Value: "one"}
	record := storage.Record{"id": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("one")}}
	snapshot.AddMembership("a", key)
	snapshot.AddMembership("b", key)
	snapshot.StoreRecord(key, record)
	record["id"] = storage.Value{Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("mutated")}
	for _, fingerprint := range []datastore.QueryFingerprint{"a", "b"} {
		stored, ok := snapshot.Lookup(fingerprint, key)
		if !ok || string(stored["id"].Data) != "one" {
			t.Fatalf("lookup %q = %+v, %v", fingerprint, stored, ok)
		}
	}
}

func TestManagerSharesHydrationAndReplacesQueriesForNewerState(t *testing.T) {
	manager := NewManager(1, 1, 1)
	key := storage.EntityKey{Relation: storage.RelationKey{Source: "test", Name: "tasks"}, Value: "one"}
	load := entitycache.NewEntityCache().BeginRefresh(key)
	cancelled := 0
	state, started, err := manager.StartHydration(key, load, []datastore.QueryFingerprint{"a"}, func() { cancelled++ })
	if err != nil || !started {
		t.Fatalf("start hydration: started=%v err=%v", started, err)
	}
	if !manager.ReplaceHydrationQueries(key, []datastore.QueryFingerprint{"b"}) {
		t.Fatal("failed to replace queries on existing hydration")
	}
	if manager.QueryHasPendingHydration("a") || !manager.QueryHasPendingHydration("b") {
		t.Fatal("query hydration dependencies were not replaced")
	}
	queries := make(map[datastore.QueryFingerprint]struct{})
	for _, fingerprint := range state.QueryFingerprints() {
		queries[fingerprint] = struct{}{}
	}
	if _, ok := queries["a"]; ok {
		t.Fatal("obsolete query dependency survived replacement")
	}
	if _, ok := queries["b"]; !ok {
		t.Fatal("new query missing")
	}
	duplicateCancelled := 0
	duplicate, started, err := manager.StartHydration(key, load, []datastore.QueryFingerprint{"c"}, func() { duplicateCancelled++ })
	if err != nil || started || duplicate != state || duplicateCancelled != 1 {
		t.Fatalf("duplicate hydration: state=%p started=%v err=%v cancelled=%d", duplicate, started, err, duplicateCancelled)
	}
	queries = make(map[datastore.QueryFingerprint]struct{})
	for _, fingerprint := range state.QueryFingerprints() {
		queries[fingerprint] = struct{}{}
	}
	if _, ok := queries["b"]; ok {
		t.Fatal("duplicate start retained an obsolete query dependency")
	}
	if _, ok := queries["c"]; !ok {
		t.Fatal("duplicate start did not install the newest query dependency")
	}
	if err := manager.CheckHydrationCapacity(); err == nil {
		t.Fatal("hydration capacity did not account for the active load")
	}
	manager.RemoveHydration(key)
	if manager.QueryHasPendingHydration("c") {
		t.Fatal("removed hydration remained visible as a query dependency")
	}
	if cancelled != 1 {
		t.Fatalf("hydration cancellation count = %d, want 1", cancelled)
	}
}

func TestManagerCancelAllCancelsEveryRecoveryOwner(t *testing.T) {
	manager := NewManager(2, 2, 2)
	relation := storage.RelationKey{Source: "test", Name: "tasks"}
	key := storage.EntityKey{Relation: relation, Value: "one"}
	relationCanceled := 0
	hydrationCanceled := 0
	manager.StartRelation(relation, NewRelationSnapshot(), datastore.CommittedChanges{}, func() { relationCanceled++ })
	load := entitycache.NewEntityCache().BeginRefresh(key)
	if _, started, err := manager.StartHydration(key, load, nil, func() { hydrationCanceled++ }); err != nil || !started {
		t.Fatalf("start hydration: started=%v err=%v", started, err)
	}
	manager.CancelAll()
	if relationCanceled != 1 || hydrationCanceled != 1 {
		t.Fatalf("cancellation counts: relation=%d hydration=%d", relationCanceled, hydrationCanceled)
	}
	if _, exists := manager.Relation(relation); exists {
		t.Fatal("relation recovery survived CancelAll")
	}
	if _, exists := manager.Hydration(key); exists {
		t.Fatal("hydration survived CancelAll")
	}
}
