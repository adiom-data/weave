package entitycache

import (
	"testing"

	"github.com/adiom-data/weave/internal/storage"
)

func TestPrefetchNeverReplacesTrackedCurrentOrInFlightState(t *testing.T) {
	for _, mode := range []string{"current", "loading", "refreshing", "partial"} {
		t.Run(mode, func(t *testing.T) {
			cache := NewEntityCache()
			key := cacheKey("one")
			newer := cacheRecord("newer")
			var generation uint64
			switch mode {
			case "current":
				cache.StoreCurrent(key, newer)
				cache.Acquire(key)
			case "loading":
				generation = cache.BeginRefresh(key).Generation()
			case "refreshing":
				cache.StoreCurrent(key, newer)
				cache.Acquire(key)
				generation = cache.BeginRefresh(key).Generation()
			case "partial":
				cache.StorePartial(key, newer)
			}
			older := cacheRecord("older")
			older["extra"] = storage.Value{Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("must not trigger refresh")}
			status := cache.InstallPrefetched(key, older)
			if status.Usable != (mode == "current") {
				t.Fatalf("status=%v for %s", status, mode)
			}
			got, _ := cache.Get(key)
			if mode != "loading" && string(got.Record["title"].Data) != "newer" {
				t.Fatal("prefetch overwrote newer state")
			}
			if _, exists := got.Record["extra"]; exists {
				t.Fatal("prefetch changed the authoritative shape of an existing item")
			}
			if generation != 0 && got.Generation != generation {
				t.Fatal("prefetch superseded an in-flight load")
			}
		})
	}
}

func TestPrefetchFillsMissingOrIdleWithoutLeakingReferences(t *testing.T) {
	cache := NewEntityCache()
	key := cacheKey("one")
	if status := cache.InstallPrefetched(key, nil); !status.Usable || status.Present {
		t.Fatal("missing entity cannot represent an empty selector")
	}
	if _, exists := cache.Get(key); exists {
		t.Fatal("empty selector inserted a fictional entity")
	}
	for _, title := range []string{"first", "replacement"} {
		status := cache.InstallPrefetched(key, cacheRecord(title))
		got, _ := cache.Get(key)
		if !status.Usable || !status.Present || got.State != EntityUnreferenced || got.References != 0 || string(got.Record["title"].Data) != title {
			t.Fatalf("prefetch did not fill idle entry: %+v", got)
		}
	}
	cache.InstallPrefetched(key, nil)
	if _, exists := cache.Get(key); exists {
		t.Fatal("empty reference left an idle stale merge baseline")
	}
}
