package expiryindex

import (
	"math/rand"
	"testing"
	"time"
)

func TestIndexMatchesReferenceThroughUpdatesAndRemovals(t *testing.T) {
	var index Index[int]
	reference := make(map[int]time.Time)
	rng := rand.New(rand.NewSource(81))
	for step := 0; step < 10000; step++ {
		key := rng.Intn(200)
		if rng.Intn(3) == 0 {
			index.Remove(key)
			delete(reference, key)
		} else {
			at := time.Unix(int64(rng.Intn(1000)), 0)
			index.Set(key, at)
			reference[key] = at
		}
		if index.Len() != len(reference) || len(index.queue) != len(reference) {
			t.Fatal("stale or missing heap entries")
		}
		first, at, ok := index.First()
		if ok != (len(reference) > 0) {
			t.Fatal("incorrect empty state")
		}
		if ok {
			if !reference[first].Equal(at) {
				t.Fatal("wrong earliest key")
			}
			for _, value := range reference {
				if value.Before(at) {
					t.Fatal("heap minimum is incorrect")
				}
			}
		}
		for position, item := range index.queue {
			if item.index != position || index.items[item.key] != item {
				t.Fatal("heap reverse index is incorrect")
			}
		}
	}
	for key := range reference {
		index.Remove(key)
	}
	if index.Len() != 0 || len(index.queue) != 0 {
		t.Fatal("heap did not empty")
	}
}
