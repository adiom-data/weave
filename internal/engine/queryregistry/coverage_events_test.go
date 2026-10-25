package queryregistry

import (
	"fmt"
	"testing"

	"github.com/adiom-data/weave/internal/datastore"
	"github.com/adiom-data/weave/internal/engine/querywindow"
	"github.com/adiom-data/weave/internal/storage"
)

func TestCoverageEventsIncrementallyMaintainOverlapAndLoss(t *testing.T) {
	plan := fakePlan{fingerprint: "coverage", relation: storage.RelationKey{Source: "test", Name: "tasks"}}
	coverage := newCoverageEvents(plan)
	first := datastore.Bounds{Left: orderedCursor("a"), Right: orderedCursor("f")}
	second := datastore.Bounds{Left: orderedCursor("c"), Right: orderedCursor("h")}
	if err := coverage.add(first); err != nil {
		t.Fatal(err)
	}
	if err := coverage.add(second); err != nil {
		t.Fatal(err)
	}
	assertCoverageIntervals(t, coverage, datastore.Bounds{Left: orderedCursor("a"), Right: orderedCursor("h")})
	lost, err := coverage.regionsWithCount(first, 1)
	if err != nil {
		t.Fatal(err)
	}
	assertBounds(t, lost, datastore.Bounds{Left: orderedCursor("a"), Right: orderedCursor("c")})
	if err := coverage.remove(first); err != nil {
		t.Fatal(err)
	}
	assertCoverageIntervals(t, coverage, second)
	for _, test := range []struct {
		value string
		want  bool
	}{{"c", false}, {"d", true}, {"h", true}, {"i", false}} {
		got, containsErr := coverage.contains(datastore.EncodedSortKey(test.value))
		if containsErr != nil {
			t.Fatal(containsErr)
		}
		if got != test.want {
			t.Fatalf("contains(%q)=%v, want %v", test.value, got, test.want)
		}
	}
}

func TestCoverageEventsMatchBruteForceAcrossWindowChurn(t *testing.T) {
	plan := fakePlan{fingerprint: "coverage", relation: storage.RelationKey{Source: "test", Name: "tasks"}}
	coverage := newCoverageEvents(plan)
	windows := make(map[int]datastore.Bounds)
	state := uint32(7)
	next := func(limit int) int {
		state = state*1664525 + 1013904223
		return int(state % uint32(limit))
	}
	for step := 0; step < 400; step++ {
		id := next(80)
		if previous, exists := windows[id]; exists && next(2) == 0 {
			if err := coverage.remove(previous); err != nil {
				t.Fatal(err)
			}
			delete(windows, id)
		} else {
			if previous, exists := windows[id]; exists {
				if err := coverage.remove(previous); err != nil {
					t.Fatal(err)
				}
			}
			left := next(95)
			right := left + 1 + next(100-left)
			bounds := datastore.Bounds{Left: orderedCursor(fmt.Sprintf("%03d", left)), Right: orderedCursor(fmt.Sprintf("%03d", right))}
			if err := coverage.add(bounds); err != nil {
				t.Fatal(err)
			}
			windows[id] = bounds
		}
		for point := 0; point <= 100; point++ {
			encoded := datastore.EncodedSortKey(fmt.Sprintf("%03d", point))
			actual, err := coverage.contains(encoded)
			if err != nil {
				t.Fatal(err)
			}
			want := false
			for _, bounds := range windows {
				left, _ := plan.Compare(encoded, bounds.Left.Key)
				right, _ := plan.Compare(encoded, bounds.Right.Key)
				if left > 0 && right <= 0 {
					want = true
					break
				}
			}
			if actual != want {
				t.Fatalf("step %d point %d: coverage=%v, want %v", step, point, actual, want)
			}
		}
	}
}

func TestCoverageEventsMergeTouchingIntervalsWithoutRebuild(t *testing.T) {
	plan := fakePlan{fingerprint: "coverage", relation: storage.RelationKey{Source: "test", Name: "tasks"}}
	coverage := newCoverageEvents(plan)
	left := datastore.Bounds{Left: orderedCursor("a"), Right: orderedCursor("c")}
	right := datastore.Bounds{Left: orderedCursor("c"), Right: orderedCursor("e")}
	if err := coverage.add(left); err != nil {
		t.Fatal(err)
	}
	if err := coverage.add(right); err != nil {
		t.Fatal(err)
	}
	assertCoverageIntervals(t, coverage, datastore.Bounds{Left: orderedCursor("a"), Right: orderedCursor("e")})
	covered, err := coverage.covered(datastore.Bounds{Left: orderedCursor("b"), Right: orderedCursor("d")})
	if err != nil || !covered {
		t.Fatalf("covered=%v, err=%v", covered, err)
	}
}

func TestCoverageEventsCoveredDetectsInteriorGapFromAggregates(t *testing.T) {
	plan := fakePlan{fingerprint: "coverage", relation: storage.RelationKey{Source: "test", Name: "tasks"}}
	coverage := newCoverageEvents(plan)
	for _, bounds := range []datastore.Bounds{
		{Left: orderedCursor("a"), Right: orderedCursor("c")},
		{Left: orderedCursor("d"), Right: orderedCursor("f")},
	} {
		if err := coverage.add(bounds); err != nil {
			t.Fatal(err)
		}
	}

	covered, err := coverage.covered(datastore.Bounds{Left: orderedCursor("b"), Right: orderedCursor("e")})
	if err != nil {
		t.Fatal(err)
	}
	if covered {
		t.Fatal("range crossing an interior coverage gap reported as covered")
	}

	if err := coverage.add(datastore.Bounds{Left: orderedCursor("c"), Right: orderedCursor("d")}); err != nil {
		t.Fatal(err)
	}
	covered, err = coverage.covered(datastore.Bounds{Left: orderedCursor("b"), Right: orderedCursor("e")})
	if err != nil {
		t.Fatal(err)
	}
	if !covered {
		t.Fatal("touching intervals should continuously cover the requested range")
	}
}

func TestCoverageEventsFindMaximalEdgeIntervals(t *testing.T) {
	plan := fakePlan{fingerprint: "coverage", relation: storage.RelationKey{Source: "test", Name: "tasks"}}
	coverage := newCoverageEvents(plan)
	start := datastore.Boundary{Kind: datastore.BoundaryStart}
	end := datastore.Boundary{Kind: datastore.BoundaryEnd}
	for _, bounds := range []datastore.Bounds{
		{Left: start, Right: orderedCursor("c")},
		{Left: orderedCursor("a"), Right: orderedCursor("f")},
		{Left: orderedCursor("h"), Right: orderedCursor("j")},
		{Left: orderedCursor("i"), Right: orderedCursor("m")},
		{Left: orderedCursor("k"), Right: end},
	} {
		if err := coverage.add(bounds); err != nil {
			t.Fatal(err)
		}
	}
	prefix, ok, err := coverage.edgeInterval(false)
	if err != nil || !ok {
		t.Fatalf("prefix: bounds=%+v ok=%v err=%v", prefix, ok, err)
	}
	assertBounds(t, []datastore.Bounds{prefix}, datastore.Bounds{Left: start, Right: orderedCursor("f")})
	suffix, ok, err := coverage.edgeInterval(true)
	if err != nil || !ok {
		t.Fatalf("suffix: bounds=%+v ok=%v err=%v", suffix, ok, err)
	}
	assertBounds(t, []datastore.Bounds{suffix}, datastore.Bounds{Left: orderedCursor("h"), Right: end})

	middleOnly := newCoverageEvents(plan)
	if err := middleOnly.add(datastore.Bounds{Left: orderedCursor("b"), Right: orderedCursor("d")}); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := middleOnly.edgeInterval(false); err != nil || ok {
		t.Fatalf("middle prefix: ok=%v err=%v", ok, err)
	}
	if _, ok, err := middleOnly.edgeInterval(true); err != nil || ok {
		t.Fatalf("middle suffix: ok=%v err=%v", ok, err)
	}
}

func TestCoverageEventsMaintainSubtreeAggregates(t *testing.T) {
	plan := fakePlan{fingerprint: "coverage", relation: storage.RelationKey{Source: "test", Name: "tasks"}}
	coverage := newCoverageEvents(plan)
	windows := []datastore.Bounds{
		{Left: orderedCursor("a"), Right: orderedCursor("f")},
		{Left: orderedCursor("b"), Right: orderedCursor("d")},
		{Left: orderedCursor("c"), Right: orderedCursor("h")},
	}
	for _, bounds := range windows {
		if err := coverage.add(bounds); err != nil {
			t.Fatal(err)
		}
		assertCoverageNodeAggregates(t, coverage.root)
	}
	for _, bounds := range windows {
		if err := coverage.remove(bounds); err != nil {
			t.Fatal(err)
		}
		assertCoverageNodeAggregates(t, coverage.root)
	}
}

func TestCoverageEventsFindOnlySinglyCoveredRegions(t *testing.T) {
	plan := fakePlan{fingerprint: "coverage", relation: storage.RelationKey{Source: "test", Name: "tasks"}}
	coverage := newCoverageEvents(plan)
	base := datastore.Bounds{Left: orderedCursor("a"), Right: orderedCursor("z")}
	for _, bounds := range []datastore.Bounds{
		base,
		{Left: orderedCursor("b"), Right: orderedCursor("d")},
		{Left: orderedCursor("f"), Right: orderedCursor("h")},
		{Left: orderedCursor("j"), Right: orderedCursor("l")},
	} {
		if err := coverage.add(bounds); err != nil {
			t.Fatal(err)
		}
	}

	regions, err := coverage.regionsWithCount(base, 1)
	if err != nil {
		t.Fatal(err)
	}
	assertBounds(t, regions,
		datastore.Bounds{Left: orderedCursor("a"), Right: orderedCursor("b")},
		datastore.Bounds{Left: orderedCursor("d"), Right: orderedCursor("f")},
		datastore.Bounds{Left: orderedCursor("h"), Right: orderedCursor("j")},
		datastore.Bounds{Left: orderedCursor("l"), Right: orderedCursor("z")},
	)
}

func assertCoverageNodeAggregates(t *testing.T, root *coverageNode) (sum, minPrefix int) {
	t.Helper()
	if root == nil {
		return 0, 0
	}
	leftSum, leftMin := assertCoverageNodeAggregates(t, root.left)
	rightSum, rightMin := assertCoverageNodeAggregates(t, root.right)
	afterNode := leftSum + root.delta
	wantSum := afterNode + rightSum
	wantMin := min(0, leftMin, afterNode, afterNode+rightMin)
	if root.sum != wantSum || root.minPrefix != wantMin {
		t.Fatalf("boundary %+v aggregates=(sum=%d,min=%d), want (sum=%d,min=%d)", root.boundary, root.sum, root.minPrefix, wantSum, wantMin)
	}
	return wantSum, wantMin
}

func assertCoverageIntervals(t *testing.T, coverage *coverageEvents, expected ...datastore.Bounds) {
	t.Helper()
	actual, err := coverage.intervals()
	if err != nil {
		t.Fatal(err)
	}
	assertBounds(t, actual, expected...)
}

func assertBounds(t *testing.T, actual []datastore.Bounds, expected ...datastore.Bounds) {
	t.Helper()
	if len(actual) != len(expected) {
		t.Fatalf("bounds=%+v, want %+v", actual, expected)
	}
	plan := fakePlan{}
	for index := range expected {
		left, err := querywindow.CompareBoundary(plan, actual[index].Left, expected[index].Left)
		if err != nil {
			t.Fatal(err)
		}
		right, err := querywindow.CompareBoundary(plan, actual[index].Right, expected[index].Right)
		if err != nil {
			t.Fatal(err)
		}
		if left != 0 || right != 0 || actual[index].Left.Kind != expected[index].Left.Kind || actual[index].Right.Kind != expected[index].Right.Kind {
			t.Fatalf("bounds[%d]=%+v, want %+v", index, actual[index], expected[index])
		}
	}
}
