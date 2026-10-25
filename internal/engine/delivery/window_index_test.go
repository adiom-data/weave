package delivery

import (
	"fmt"
	"testing"

	"github.com/adiom-data/weave/internal/datastore"
	"github.com/adiom-data/weave/internal/engine/querywindow"
	"github.com/adiom-data/weave/internal/storage"
)

func TestWindowIndexFindsOnlyOverlappingWindows(t *testing.T) {
	plan := fakePlan{fingerprint: "tasks", relation: storage.RelationKey{Source: "test", Name: "tasks"}}
	index := newWindowIndex(plan)
	windows := map[string]datastore.Bounds{
		"left":    {Left: hubCursor("a"), Right: hubCursor("d")},
		"overlap": {Left: hubCursor("c"), Right: hubCursor("f")},
		"right":   {Left: hubCursor("x"), Right: hubCursor("z")},
		"all":     {Left: datastore.Boundary{Kind: datastore.BoundaryStart}, Right: datastore.Boundary{Kind: datastore.BoundaryEnd}},
	}
	for id, bounds := range windows {
		if err := index.add(id, bounds); err != nil {
			t.Fatal(err)
		}
	}
	assertWindowIDs(t, index, "c", "all", "left")
	assertWindowIDs(t, index, "d", "all", "left", "overlap")
	assertWindowIDs(t, index, "w", "all")

	if err := index.replace("left", windows["left"], datastore.Bounds{Left: hubCursor("m"), Right: hubCursor("p")}); err != nil {
		t.Fatal(err)
	}
	assertWindowIDs(t, index, "c", "all")
	assertWindowIDs(t, index, "n", "all", "left")
	if err := index.delete("all", windows["all"]); err != nil {
		t.Fatal(err)
	}
	assertWindowIDs(t, index, "w")
}

func TestWindowIndexMatchesBruteForce(t *testing.T) {
	plan := fakePlan{fingerprint: "tasks", relation: storage.RelationKey{Source: "test", Name: "tasks"}}
	index := newWindowIndex(plan)
	windows := make(map[string]datastore.Bounds)
	for number := 0; number < 200; number++ {
		left := (number * 37) % 190
		width := 1 + (number*19)%20
		id := fmt.Sprintf("window-%03d", number)
		bounds := datastore.Bounds{Left: hubCursor(fmt.Sprintf("%03d", left)), Right: hubCursor(fmt.Sprintf("%03d", left+width))}
		windows[id] = bounds
		if err := index.add(id, bounds); err != nil {
			t.Fatal(err)
		}
	}
	for point := 0; point <= 220; point++ {
		value := fmt.Sprintf("%03d", point)
		actual, err := index.containing(datastore.EncodedSortKey(value))
		if err != nil {
			t.Fatal(err)
		}
		actualSet := make(map[string]struct{}, len(actual))
		for _, id := range actual {
			actualSet[id] = struct{}{}
		}
		for id, bounds := range windows {
			want, containsErr := querywindow.Contains(plan, datastore.EncodedSortKey(value), []datastore.Bounds{bounds})
			if containsErr != nil {
				t.Fatal(containsErr)
			}
			_, got := actualSet[id]
			if got != want {
				t.Fatalf("point %q window %q: index=%v, brute-force=%v", value, id, got, want)
			}
		}
	}
}

func hubCursor(value string) datastore.Boundary {
	return datastore.Boundary{Kind: datastore.BoundaryCursor, Key: datastore.EncodedSortKey(value)}
}

func assertWindowIDs(t *testing.T, index *windowIndex, value string, expected ...string) {
	t.Helper()
	actual, err := index.containing(datastore.EncodedSortKey(value))
	if err != nil {
		t.Fatal(err)
	}
	want := make(map[string]struct{}, len(expected))
	for _, id := range expected {
		want[id] = struct{}{}
	}
	if len(actual) != len(want) {
		t.Fatalf("windows containing %q = %v, want %v", value, actual, expected)
	}
	for _, id := range actual {
		if _, ok := want[id]; !ok {
			t.Fatalf("unexpected window %q for %q: %v", id, value, actual)
		}
	}
}
