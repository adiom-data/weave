package prefetchcache

import (
	"testing"
	"time"

	"github.com/adiom-data/weave/internal/engine/entitycache"
	"github.com/adiom-data/weave/internal/storage"
)

func TestPinsAreDeduplicatedPreserveCurrentAndExpireInBatches(t *testing.T) {
	entities := entitycache.NewEntityCache()
	cache := New(entities, time.Second)
	relation := storage.RelationKey{Source: "test", Name: "tasks"}
	record := func(value string) storage.Record {
		return storage.Record{"value": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte(value)}}
	}
	for _, id := range []string{"1", "2"} {
		key := storage.EntityKey{Relation: relation, Value: id}
		if err := cache.Put(key, record("new")); err != nil {
			t.Fatal(err)
		}
		if err := cache.Put(key, record("old")); err != nil {
			t.Fatal(err)
		}
		row, _ := entities.Get(key)
		if row.References != 1 || row.State != entitycache.EntityCurrent || string(row.Record["value"].Data) != "new" {
			t.Fatalf("row=%+v", row)
		}
	}
	more, err := cache.Maintain(time.Now().Add(2*time.Second), 1)
	if err != nil || !more {
		t.Fatalf("first batch: %v %v", more, err)
	}
	more, err = cache.Maintain(time.Now().Add(2*time.Second), 1)
	if err != nil || more || cache.HasRelation(relation) {
		t.Fatalf("last batch: %v %v", more, err)
	}
	for _, id := range []string{"1", "2"} {
		row, _ := entities.Get(storage.EntityKey{Relation: relation, Value: id})
		if row.References != 0 {
			t.Fatal("leaked pin")
		}
	}
}

func TestInvalidatingPrefetchDoesNotReleaseActiveConsumer(t *testing.T) {
	entities := entitycache.NewEntityCache()
	cache := New(entities, time.Minute)
	key := storage.EntityKey{Relation: storage.RelationKey{Source: "test", Name: "tasks"}, Value: "1"}
	if err := cache.Put(key, storage.Record{"id": {Type: storage.TypeInteger, State: storage.ValuePresent, Data: []byte("1")}}); err != nil {
		t.Fatal(err)
	}
	entities.Acquire(key)
	if err := cache.InvalidateRelation(key.Relation); err != nil {
		t.Fatal(err)
	}
	row, _ := entities.Get(key)
	if row.References != 1 || row.State != entitycache.EntityCurrent {
		t.Fatalf("active consumer lost state: %+v", row)
	}
	if err := cache.InvalidateRelation(key.Relation); err != nil {
		t.Fatal(err)
	}
}
