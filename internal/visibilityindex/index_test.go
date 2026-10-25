package visibilityindex

import (
	"math/rand"
	"reflect"
	"testing"

	"github.com/adiom-data/weave/internal/querysubscription/model"
	"github.com/adiom-data/weave/internal/storage"
)

func testKey(value string) storage.EntityKey { return storage.EntityKey{Value: value} }

func TestIndexMembershipAndVisibleRanks(t *testing.T) {
	index := New()
	for _, value := range []string{"a", "b", "c", "d"} {
		if err := index.Append(testKey(value), value != "b"); err != nil {
			t.Fatal(err)
		}
	}
	if rank, ok := index.VisibleRank(testKey("c")); !ok || rank != 1 {
		t.Fatalf("rank(c) = %d, %v", rank, ok)
	}
	delta := querysubscription.MembershipDelta{
		Removed: []storage.EntityKey{testKey("a"), testKey("c")},
		Insertions: []querysubscription.Insertion{
			{Key: testKey("c"), FinalIndex: 0},
			{Key: testKey("e"), FinalIndex: 2},
		},
	}
	if err := index.ApplyMembership(delta); err != nil {
		t.Fatal(err)
	}
	if changed, err := index.SetVisible(testKey("e"), true); err != nil || !changed {
		t.Fatalf("set visible = %v, %v", changed, err)
	}
	if got, want := index.Keys(), []storage.EntityKey{testKey("c"), testKey("b"), testKey("e"), testKey("d")}; !reflect.DeepEqual(got, want) {
		t.Fatalf("keys = %v, want %v", got, want)
	}
	if got, want := index.VisibleKeys(), []storage.EntityKey{testKey("c"), testKey("e"), testKey("d")}; !reflect.DeepEqual(got, want) {
		t.Fatalf("visible = %v, want %v", got, want)
	}
	for want, key := range index.VisibleKeys() {
		if got, ok := index.VisibleRank(key); !ok || got != want {
			t.Fatalf("rank(%v) = %d, %v; want %d", key, got, ok, want)
		}
	}
}

func TestIndexMatchesSliceModel(t *testing.T) {
	rng := rand.New(rand.NewSource(7))
	index := New()
	var order []storage.EntityKey
	visible := make(map[storage.EntityKey]bool)
	next := 0
	for step := 0; step < 2000; step++ {
		switch rng.Intn(4) {
		case 0:
			key := testKey(string(rune('a' + next)))
			next++
			position := rng.Intn(len(order) + 1)
			delta := querysubscription.MembershipDelta{Insertions: []querysubscription.Insertion{{Key: key, FinalIndex: position}}}
			if err := index.ApplyMembership(delta); err != nil {
				t.Fatal(err)
			}
			order = append(order, storage.EntityKey{})
			copy(order[position+1:], order[position:])
			order[position] = key
			isVisible := rng.Intn(2) == 0
			if _, err := index.SetVisible(key, isVisible); err != nil {
				t.Fatal(err)
			}
			visible[key] = isVisible
		case 1:
			if len(order) == 0 {
				continue
			}
			position := rng.Intn(len(order))
			key := order[position]
			if err := index.ApplyMembership(querysubscription.MembershipDelta{Removed: []storage.EntityKey{key}}); err != nil {
				t.Fatal(err)
			}
			order = append(order[:position], order[position+1:]...)
			delete(visible, key)
		case 2:
			if len(order) == 0 {
				continue
			}
			key := order[rng.Intn(len(order))]
			visible[key] = !visible[key]
			if _, err := index.SetVisible(key, visible[key]); err != nil {
				t.Fatal(err)
			}
		case 3:
			if len(order) < 2 {
				continue
			}
			from := rng.Intn(len(order))
			key := order[from]
			remaining := append([]storage.EntityKey(nil), order[:from]...)
			remaining = append(remaining, order[from+1:]...)
			to := rng.Intn(len(order))
			delta := querysubscription.MembershipDelta{
				Removed:    []storage.EntityKey{key},
				Insertions: []querysubscription.Insertion{{Key: key, FinalIndex: to}},
			}
			if err := index.ApplyMembership(delta); err != nil {
				t.Fatal(err)
			}
			order = append(remaining, storage.EntityKey{})
			copy(order[to+1:], order[to:])
			order[to] = key
		}
		if got := index.Keys(); !reflect.DeepEqual(got, order) {
			t.Fatalf("step %d: keys = %v, want %v", step, got, order)
		}
		wantVisible := make([]storage.EntityKey, 0)
		for _, key := range order {
			if visible[key] {
				wantVisible = append(wantVisible, key)
			}
		}
		if got := index.VisibleKeys(); !reflect.DeepEqual(got, wantVisible) {
			t.Fatalf("step %d: visible = %v, want %v", step, got, wantVisible)
		}
		for want, key := range wantVisible {
			if got, ok := index.VisibleRank(key); !ok || got != want {
				t.Fatalf("step %d: rank(%v) = %d, %v; want %d", step, key, got, ok, want)
			}
		}
	}
}
