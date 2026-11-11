package entitycache

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/adiom-data/weave/internal/datastore"
	"github.com/adiom-data/weave/internal/storage"
)

func cacheKey(value string) storage.EntityKey {
	return storage.EntityKey{Relation: storage.RelationKey{Source: "test", Namespace: "app", Name: "tasks"}, Value: value}
}

func cacheRecord(title string) storage.Record {
	return storage.Record{"title": {Type: "string", State: storage.ValuePresent, Data: []byte(title)}}
}

func TestInspectBorrowsForEvaluationAndPropagatesErrors(t *testing.T) {
	cache := NewEntityCache()
	key := cacheKey("fields")
	record := storage.Record{
		"binding": {Type: storage.TypeObject, State: storage.ValuePresent, Object: map[string]storage.Value{
			"ids": {Type: storage.TypeList, State: storage.ValuePresent, List: []storage.Value{{Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("one")}}},
		}},
		"payload": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("not requested")},
	}
	cache.StoreCurrent(key, record)
	cause := errors.New("evaluation failed")
	exists, err := cache.Inspect(key, func(borrowed storage.Record, state EntityState) error {
		if state != EntityCurrent || !storage.EqualValues(borrowed["binding"], record["binding"]) {
			t.Fatal("inspection did not see the complete current record")
		}
		return cause
	})
	if !exists || !errors.Is(err, cause) {
		t.Fatalf("inspection result = %v, %v", exists, err)
	}
	// Evaluation errors must not leave the read lock held.
	cache.StoreCurrent(key, record)
	if exists, err := cache.Inspect(cacheKey("absent"), func(storage.Record, EntityState) error {
		t.Fatal("called visitor for a missing entity")
		return nil
	}); exists || err != nil {
		t.Fatal("missing entity was fabricated")
	}
}

func TestRetainHasAcquireReferenceSemanticsWithoutAnImage(t *testing.T) {
	cache := NewEntityCache()
	key := cacheKey("retained")
	if cache.Retain(key) {
		t.Fatal("retained a missing entity")
	}
	cache.StoreCurrent(key, cacheRecord("current"))
	cache.Acquire(key)
	cache.Release(key)
	if !cache.Retain(key) {
		t.Fatal("could not retain an idle entity")
	}
	got, _ := cache.Get(key)
	if got.References != 1 || got.State != EntityCurrent {
		t.Fatalf("retained state = %+v", got)
	}
	cache.Release(key)
	got, _ = cache.Get(key)
	if got.References != 0 || got.State != EntityUnreferenced {
		t.Fatalf("released state = %+v", got)
	}
}

func TestEntityCacheDeduplicatesLoadsAndSharesReferences(t *testing.T) {
	cache := NewEntityCache()
	key := cacheKey("1")
	request := []SnapshotLoadRequest{{Key: key, Record: PrepareSnapshotRecord(cacheRecord("first"))}}
	owner := cache.StageSnapshotBatch(request)[0]
	waiter := cache.StageSnapshotBatch(request)[0]
	if !owner.Owned() || waiter.Owned() || owner.Generation() != waiter.Generation() {
		t.Fatalf("unexpected load ownership: owner=%+v waiter=%+v", owner, waiter)
	}
	if err := cache.Complete(owner, PrepareSnapshotRecord(cacheRecord("current"))); err != nil {
		t.Fatal(err)
	}
	select {
	case <-waiter.Done():
	default:
		t.Fatal("shared waiter was not released")
	}
	first, ok := cache.Acquire(key)
	if !ok || first.References != 1 || string(first.Record["title"].Data) != "current" {
		t.Fatalf("first acquire = %+v, %v", first, ok)
	}
	second, _ := cache.Acquire(key)
	if second.References != 2 {
		t.Fatalf("references = %d", second.References)
	}
	if !cache.Release(key) {
		t.Fatal("failed to release first reference")
	}
	if !cache.Release(key) {
		t.Fatal("failed to release references")
	}
	final, _ := cache.Get(key)
	if final.State != EntityUnreferenced || final.References != 0 {
		t.Fatalf("final entity = %+v", final)
	}
}

func TestEntityCacheStagesSnapshotBatchAtomically(t *testing.T) {
	cache := NewEntityCache()
	relation := storage.RelationKey{Source: "test", Name: "tasks"}
	record := func(id string) storage.Record {
		return storage.Record{"id": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte(id)}}
	}
	firstRequests := []SnapshotLoadRequest{
		{Key: storage.EntityKey{Relation: relation, Value: "1"}, Record: PrepareSnapshotRecord(record("1"))},
		{Key: storage.EntityKey{Relation: relation, Value: "2"}, Record: PrepareSnapshotRecord(record("2"))},
	}
	first := cache.StageSnapshotBatch(firstRequests)
	second := cache.StageSnapshotBatch([]SnapshotLoadRequest{firstRequests[1], firstRequests[0]})
	if len(first) != 2 || !first[0].Owned() || !first[1].Owned() {
		t.Fatalf("first batch ownership = %+v", first)
	}
	if len(second) != 2 || second[0].Owned() || second[1].Owned() {
		t.Fatalf("overlapping batch split ownership = %+v", second)
	}
	if second[0].Generation() != first[1].Generation() || second[1].Generation() != first[0].Generation() {
		t.Fatalf("waiters did not coalesce onto the first batch: first=%+v second=%+v", first, second)
	}
	for index, request := range firstRequests {
		entity, ok := cache.Get(request.Key)
		if !ok || entity.State != EntityReconciling || string(entity.Record["id"].Data) != string(request.Record.record["id"].Data) {
			t.Fatalf("staged entity %d = %+v, exists=%v", index, entity, ok)
		}
	}
}

func TestEntityCacheDoesNotReuseGenerationAfterDeletion(t *testing.T) {
	cache := NewEntityCache()
	key := cacheKey("1")
	request := []SnapshotLoadRequest{{Key: key, Record: PrepareSnapshotRecord(cacheRecord("first"))}}
	first := cache.StageSnapshotBatch(request)[0]
	if err := cache.Fail(first, errors.New("cancel first generation")); err != nil {
		t.Fatal(err)
	}
	second := cache.StageSnapshotBatch(request)[0]
	if second.Generation() <= first.Generation() {
		t.Fatalf("recreated generation = %d, want greater than %d", second.Generation(), first.Generation())
	}
	if removed, err := cache.DiscardIfGeneration(key, first.Generation(), errors.New("stale cleanup")); err != nil || removed {
		t.Fatalf("stale cleanup removed=%v err=%v", removed, err)
	}
	if _, ok := cache.Get(key); !ok {
		t.Fatal("stale cleanup removed the newer generation")
	}
	_ = cache.Fail(second, context.Canceled)
}

func TestEntityCacheCompletesUnreferencedInitialLoadAsUnreferenced(t *testing.T) {
	cache := NewEntityCache()
	key := cacheKey("1")
	record := cacheRecord("snapshot")
	load := cache.StageSnapshotBatch([]SnapshotLoadRequest{{Key: key, Record: PrepareSnapshotRecord(record)}})[0]
	if err := cache.Complete(load, PrepareSnapshotRecord(record)); err != nil {
		t.Fatal(err)
	}
	entity, ok := cache.Get(key)
	if !ok || entity.State != EntityUnreferenced || entity.References != 0 {
		t.Fatalf("completed entity = %+v, exists=%v", entity, ok)
	}
}

func TestEntityCacheRestagesFailedPartialRefreshWithoutLosingNewerValues(t *testing.T) {
	cache := NewEntityCache()
	key := cacheKey("1")
	cache.StoreCurrent(key, storage.Record{
		"title": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("old")},
		"body":  {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("old body")},
	})
	failed := cache.BeginRefresh(key)
	cache.StorePartial(key, storage.Record{
		"title": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("newer CDC")},
		"body":  {Type: storage.TypeText, State: storage.ValueUnavailable},
	})
	if err := cache.Fail(failed, errors.New("hydration failed")); err != nil {
		t.Fatal(err)
	}
	snapshot := storage.Record{
		"title": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("snapshot")},
		"body":  {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("snapshot body")},
	}
	replacement := cache.StageSnapshotBatch([]SnapshotLoadRequest{{Key: key, Record: PrepareSnapshotRecord(snapshot)}})[0]
	if !replacement.Owned() || replacement.Generation() <= failed.Generation() {
		t.Fatalf("replacement load = %+v, failed generation=%d", replacement, failed.Generation())
	}
	if removed, err := cache.DiscardIfGeneration(key, failed.Generation(), errors.New("late cleanup")); err != nil || removed {
		t.Fatalf("late cleanup removed=%v err=%v", removed, err)
	}
	if err := cache.Complete(replacement, PrepareSnapshotRecord(snapshot)); err != nil {
		t.Fatal(err)
	}
	entity, ok := cache.Get(key)
	if !ok || entity.State != EntityUnreferenced {
		t.Fatalf("completed replacement = %+v, exists=%v", entity, ok)
	}
	if got := string(entity.Record["title"].Data); got != "newer CDC" {
		t.Fatalf("title = %q, want newer CDC", got)
	}
	if got := string(entity.Record["body"].Data); got != "snapshot body" {
		t.Fatalf("body = %q, want snapshot body", got)
	}
}

func TestEntityCacheDiscardNotifiesCoalescedWaiters(t *testing.T) {
	cache := NewEntityCache()
	key := cacheKey("1")
	request := []SnapshotLoadRequest{{Key: key, Record: PrepareSnapshotRecord(cacheRecord("snapshot"))}}
	owner := cache.StageSnapshotBatch(request)[0]
	waiter := cache.StageSnapshotBatch(request)[0]
	want := errors.New("discard incomplete generation")
	removed, err := cache.DiscardIfGeneration(key, owner.Generation(), want)
	if err != nil || !removed {
		t.Fatalf("discard removed=%v err=%v", removed, err)
	}
	<-waiter.Done()
	if err := cache.LoadError(waiter); !errors.Is(err, want) {
		t.Fatalf("waiter error = %v, want %v", err, want)
	}
}

func TestEntityCacheEvictsOnlyIdleUnreferencedEntities(t *testing.T) {
	cache := NewEntityCache()
	now := time.Unix(100, 0)
	cache.now = func() time.Time { return now }
	idle, active := cacheKey("idle"), cacheKey("active")
	cache.StoreCurrent(idle, cacheRecord("idle"))
	cache.StoreCurrent(active, cacheRecord("active"))
	cache.Acquire(idle)
	cache.Release(idle)
	cache.Acquire(active)
	now = now.Add(time.Minute)
	evicted, _ := cache.EvictIdle(now.Add(-time.Second), 128)
	if len(evicted) != 1 || evicted[0] != idle {
		t.Fatalf("evicted = %+v", evicted)
	}
	if _, ok := cache.Get(active); !ok {
		t.Fatal("active entity was evicted")
	}
}

func TestEntityCacheRejectsSupersededLoad(t *testing.T) {
	cache := NewEntityCache()
	key := cacheKey("1")
	load := cache.StageSnapshotBatch([]SnapshotLoadRequest{{Key: key, Record: PrepareSnapshotRecord(cacheRecord("older"))}})[0]
	current := cache.StoreCurrent(load.Key(), cacheRecord("newer"))
	if current.Generation <= load.Generation() {
		t.Fatalf("superseding tuple kept stale load generation: load=%d current=%d", load.Generation(), current.Generation)
	}
	if err := cache.Complete(load, PrepareSnapshotRecord(cacheRecord("older"))); !errors.Is(err, ErrStaleLoad) {
		t.Fatalf("complete error = %v", err)
	}
}

func TestEntityCacheReportsOwnerFailureToCoalescedWaiter(t *testing.T) {
	cache := NewEntityCache()
	key := cacheKey("1")
	request := []SnapshotLoadRequest{{Key: key, Record: PrepareSnapshotRecord(cacheRecord("snapshot"))}}
	owner := cache.StageSnapshotBatch(request)[0]
	waiter := cache.StageSnapshotBatch(request)[0]
	want := errors.New("snapshot cancelled")
	if err := cache.Fail(owner, want); err != nil {
		t.Fatal(err)
	}
	<-waiter.Done()
	if err := cache.LoadError(waiter); !errors.Is(err, want) {
		t.Fatalf("waiter error = %v, want %v", err, want)
	}
}

func TestEntityCachePreparesRelationRefreshAcrossAllLoads(t *testing.T) {
	cache := NewEntityCache()
	relation := storage.RelationKey{Source: "test", Name: "tasks"}
	referenced := storage.EntityKey{Relation: relation, Value: "referenced"}
	idle := storage.EntityKey{Relation: relation, Value: "idle"}
	other := storage.EntityKey{Relation: storage.RelationKey{Source: "test", Name: "projects"}, Value: "other"}
	record := func(id string) storage.Record {
		return storage.Record{"id": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte(id)}}
	}

	cache.StoreCurrent(referenced, record("referenced"))
	if _, ok := cache.Acquire(referenced); !ok {
		t.Fatal("failed to acquire cached entity")
	}
	referencedLoad := cache.BeginRefresh(referenced)
	idleLoad := cache.BeginRefresh(idle)
	otherLoad := cache.BeginRefresh(other)

	cache.PrepareRelationRefresh(relation)

	if err := cache.LoadError(referencedLoad); !errors.Is(err, datastore.ErrInvalidated) {
		t.Fatalf("load error = %v, want ErrInvalidated", err)
	}
	if err := cache.LoadError(idleLoad); !errors.Is(err, datastore.ErrInvalidated) {
		t.Fatalf("idle load error = %v, want ErrInvalidated", err)
	}
	entity, exists := cache.Get(referenced)
	if !exists || entity.References != 1 || string(entity.Record["id"].Data) != "referenced" {
		t.Fatalf("referenced baseline = %+v, exists=%v", entity, exists)
	}
	if _, exists := cache.Get(idle); exists {
		t.Fatal("unreferenced invalidated load remained cached")
	}
	if _, exists := cache.Get(other); !exists {
		t.Fatal("unrelated relation was invalidated")
	}
	select {
	case <-otherLoad.Done():
		t.Fatal("unrelated relation load was retired")
	default:
	}
	replacement := cache.BeginRefresh(referenced)
	if !replacement.Owned() || replacement.Generation() <= referencedLoad.Generation() {
		t.Fatalf("replacement load = %+v, old generation=%d", replacement, referencedLoad.Generation())
	}
}

func TestEntityCacheRefreshKeepsCurrentGenerationVisible(t *testing.T) {
	cache := NewEntityCache()
	key := cacheKey("1")
	cache.StoreCurrent(key, cacheRecord("current"))
	cache.Acquire(key)
	refresh := cache.BeginRefresh(key)
	visible, ok := cache.Get(key)
	if !ok || visible.State != EntityCurrent || string(visible.Record["title"].Data) != "current" {
		t.Fatalf("refresh exposed staged generation: %+v, exists=%v", visible, ok)
	}
	if err := cache.Complete(refresh, PrepareSnapshotRecord(cacheRecord("snapshot"))); err != nil {
		t.Fatal(err)
	}
	current, _ := cache.Get(key)
	if current.State != EntityCurrent || string(current.Record["title"].Data) != "snapshot" {
		t.Fatalf("completed refresh = %+v", current)
	}
}

func TestEntityCacheSnapshotReplacementRemovesAbsentFields(t *testing.T) {
	cache := NewEntityCache()
	key := cacheKey("1")
	cache.StoreCurrent(key, storage.Record{
		"title":    {Type: "string", State: storage.ValuePresent, Data: []byte("task")},
		"obsolete": {Type: "string", State: storage.ValuePresent, Data: []byte("old")},
	})
	cache.StoreCurrent(key, storage.Record{
		"title": {Type: "string", State: storage.ValuePresent, Data: []byte("task")},
	})
	entity, ok := cache.Get(key)
	if !ok {
		t.Fatal("entity was not cached")
	}
	if _, exists := entity.Record["obsolete"]; exists {
		t.Fatalf("absent snapshot field survived: %+v", entity.Record)
	}
}

func TestEntityCacheRejectsDeletingReferencedEntity(t *testing.T) {
	cache := NewEntityCache()
	key := cacheKey("referenced")
	cache.StoreCurrent(key, cacheRecord("value"))
	if _, ok := cache.Acquire(key); !ok {
		t.Fatal("failed to acquire cached entity")
	}
	if err := cache.DeleteAuthoritative(key); err == nil {
		t.Fatal("referenced entity deletion succeeded")
	}
	if _, ok := cache.Get(key); !ok {
		t.Fatal("failed deletion removed referenced entity")
	}
}

func TestEntityCacheCompletingSnapshotPreservesNewerPartialCDCValues(t *testing.T) {
	cache := NewEntityCache()
	key := cacheKey("1")
	snapshot := storage.Record{
		"title": {Type: "string", State: storage.ValuePresent, Data: []byte("snapshot")},
		"body":  {Type: "string", State: storage.ValuePresent, Data: []byte("hydrated body")},
	}
	load := cache.StageSnapshotBatch([]SnapshotLoadRequest{{Key: key, Record: PrepareSnapshotRecord(snapshot)}})[0]
	cache.StorePartial(key, storage.Record{
		"title": {Type: "string", State: storage.ValuePresent, Data: []byte("newer CDC")},
		"body":  {Type: "string", State: storage.ValueUnavailable},
	})
	if err := cache.Complete(load, PrepareSnapshotRecord(snapshot)); err != nil {
		t.Fatal(err)
	}
	entity, ok := cache.Get(key)
	if !ok {
		t.Fatal("entity was not cached")
	}
	if got := string(entity.Record["title"].Data); got != "newer CDC" {
		t.Fatalf("title = %q, want newer CDC", got)
	}
	if got := string(entity.Record["body"].Data); got != "hydrated body" {
		t.Fatalf("body = %q, want hydrated body", got)
	}
}

func TestEntityCacheAuthoritativeSnapshotStillRemovesAbsentFieldsAfterRefresh(t *testing.T) {
	cache := NewEntityCache()
	key := cacheKey("1")
	cache.StoreCurrent(key, storage.Record{
		"title":    {Type: "string", State: storage.ValuePresent, Data: []byte("old")},
		"obsolete": {Type: "string", State: storage.ValuePresent, Data: []byte("remove me")},
	})
	refresh := cache.BeginRefresh(key)
	snapshot := storage.Record{
		"title": {Type: "string", State: storage.ValuePresent, Data: []byte("snapshot")},
	}
	if err := cache.Complete(refresh, PrepareSnapshotRecord(snapshot)); err != nil {
		t.Fatal(err)
	}
	entity, _ := cache.Get(key)
	if _, exists := entity.Record["obsolete"]; exists {
		t.Fatalf("absent field survived authoritative refresh: %+v", entity.Record)
	}
}

func TestSnapshotRecordOwnsItsInputAndSurvivesCDCOverlay(t *testing.T) {
	cache := NewEntityCache()
	key := cacheKey("prepared")
	record := cacheRecord("snapshot")
	record["body"] = storage.Value{Type: storage.TypeObject, State: storage.ValuePresent, Object: map[string]storage.Value{
		"items": {Type: storage.TypeList, State: storage.ValuePresent, List: []storage.Value{{Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("original")}}},
	}}
	image := PrepareSnapshotRecord(record)
	record["title"].Data[0] = 'X'
	record["body"].Object["items"].List[0].Data[0] = 'X'
	load := cache.StageSnapshotBatch([]SnapshotLoadRequest{{Key: key, Record: image}})[0]
	cache.StorePartial(key, cacheRecord("updated"))
	if err := cache.Complete(load, image); err != nil {
		t.Fatal(err)
	}
	got, _ := cache.Get(key)
	if string(got.Record["title"].Data) != "updated" || string(got.Record["body"].Object["items"].List[0].Data) != "original" {
		t.Fatalf("snapshot ownership or overlay lost: %+v", got.Record)
	}
	// The same immutable baseline remains usable after another load's overlay.
	other := cacheKey("other")
	load = cache.StageSnapshotBatch([]SnapshotLoadRequest{{Key: other, Record: image}})[0]
	if err := cache.Complete(load, image); err != nil {
		t.Fatal(err)
	}
	got, _ = cache.Get(other)
	if string(got.Record["title"].Data) != "snapshot" {
		t.Fatal("completion mutated the prepared image")
	}
	got.Record["body"].Object["items"].List[0].Data[0] = 'X'
	got, _ = cache.Get(other)
	if string(got.Record["body"].Object["items"].List[0].Data) != "original" {
		t.Fatal("cache read exposed immutable snapshot data")
	}
}

// Preparation is intentionally outside the timed section. Admission and
// completion should allocate independently of the snapshot payload size.
func BenchmarkPreparedSnapshotAdmission(b *testing.B) {
	for _, size := range []int{1024, 1024 * 1024} {
		name := "1KiB"
		if size > 1024 {
			name = "1MiB"
		}
		b.Run(name, func(b *testing.B) {
			cache := NewEntityCache()
			key := cacheKey("prepared")
			record := cacheRecord("snapshot")
			record["body"] = storage.Value{Type: storage.TypeText, State: storage.ValuePresent, Data: bytes.Repeat([]byte("x"), size)}
			image := PrepareSnapshotRecord(record)
			requests := []SnapshotLoadRequest{{Key: key, Record: image}}
			b.ReportAllocs()
			for b.Loop() {
				load := cache.StageSnapshotBatch(requests)[0]
				if err := cache.Complete(load, image); err != nil {
					b.Fatal(err)
				}
				if err := cache.DeleteAuthoritative(key); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
