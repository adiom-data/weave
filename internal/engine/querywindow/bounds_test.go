package querywindow

import (
	"bytes"
	"errors"
	"testing"

	"github.com/adiom-data/weave/internal/datastore"
	"github.com/adiom-data/weave/internal/storage"
)

type testPlan struct{ compareErr error }

func (testPlan) Fingerprint() datastore.QueryFingerprint { return "test" }
func (testPlan) Relation() storage.RelationKey           { return storage.RelationKey{} }
func (testPlan) EntityKey(storage.Record) (storage.EntityKey, error) {
	return storage.EntityKey{}, nil
}
func (testPlan) Matches(storage.Record) (bool, error) { return true, nil }
func (testPlan) Key(record storage.Record) (datastore.SortKey, error) {
	return datastore.EncodedSortKey(record["key"].Data), nil
}
func (p testPlan) Compare(left, right datastore.SortKey) (int, error) {
	if p.compareErr != nil {
		return 0, p.compareErr
	}
	return bytes.Compare(left.(datastore.EncodedSortKey), right.(datastore.EncodedSortKey)), nil
}

func TestContainsPropagatesComparisonErrors(t *testing.T) {
	want := errors.New("invalid native sort key")
	_, err := Contains(testPlan{compareErr: want}, datastore.EncodedSortKey("b"), []datastore.Bounds{{
		Left:  datastore.Boundary{Kind: datastore.BoundaryCursor, Key: datastore.EncodedSortKey("a")},
		Right: datastore.Boundary{Kind: datastore.BoundaryEnd},
	}})
	if !errors.Is(err, want) {
		t.Fatalf("Contains error = %v, want %v", err, want)
	}
}
func (testPlan) EncodeKey(key datastore.SortKey) ([]byte, error) {
	return key.(datastore.EncodedSortKey), nil
}
func (testPlan) DecodeKey(value []byte) (datastore.SortKey, error) {
	return datastore.EncodedSortKey(value), nil
}
func (testPlan) Query() storage.QuerySpec { return storage.QuerySpec{} }

func TestContainsUsesOpenLeftClosedRightBounds(t *testing.T) {
	plan := testPlan{}
	bounds := []datastore.Bounds{{
		Left:  datastore.Boundary{Kind: datastore.BoundaryCursor, Key: datastore.EncodedSortKey("b")},
		Right: datastore.Boundary{Kind: datastore.BoundaryCursor, Key: datastore.EncodedSortKey("d")},
	}}
	for _, test := range []struct {
		key  string
		want bool
	}{{"b", false}, {"c", true}, {"d", true}, {"e", false}} {
		got, err := Contains(plan, datastore.EncodedSortKey(test.key), bounds)
		if err != nil {
			t.Fatal(err)
		}
		if got != test.want {
			t.Fatalf("Contains(%q) = %v, want %v", test.key, got, test.want)
		}
	}
}

func TestCompareBoundaryOrdersSentinelsAndCursors(t *testing.T) {
	plan := testPlan{}
	start := datastore.Boundary{Kind: datastore.BoundaryStart}
	a := datastore.Boundary{Kind: datastore.BoundaryCursor, Key: datastore.EncodedSortKey("a")}
	b := datastore.Boundary{Kind: datastore.BoundaryCursor, Key: datastore.EncodedSortKey("b")}
	end := datastore.Boundary{Kind: datastore.BoundaryEnd}
	startA, err := CompareBoundary(plan, start, a)
	if err != nil {
		t.Fatal(err)
	}
	ab, err := CompareBoundary(plan, a, b)
	if err != nil {
		t.Fatal(err)
	}
	bEnd, err := CompareBoundary(plan, b, end)
	if err != nil {
		t.Fatal(err)
	}
	if startA >= 0 || ab >= 0 || bEnd >= 0 {
		t.Fatal("boundaries are not ordered START < cursor(a) < cursor(b) < END")
	}
}

func TestDifferencePreservesOpenLeftClosedRightPieces(t *testing.T) {
	plan := testPlan{}
	cursor := func(value string) datastore.Boundary {
		return datastore.Boundary{Kind: datastore.BoundaryCursor, Key: datastore.EncodedSortKey(value)}
	}
	pieces, err := Difference(plan,
		datastore.Bounds{Left: cursor("a"), Right: cursor("z")},
		datastore.Bounds{Left: cursor("c"), Right: cursor("x")},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(pieces) != 2 {
		t.Fatalf("difference = %+v, want two pieces", pieces)
	}
	for _, test := range []struct {
		key  string
		want bool
	}{{"a", false}, {"b", true}, {"c", true}, {"d", false}, {"x", false}, {"y", true}, {"z", true}} {
		got, containsErr := Contains(plan, datastore.EncodedSortKey(test.key), pieces)
		if containsErr != nil {
			t.Fatal(containsErr)
		}
		if got != test.want {
			t.Fatalf("difference contains %q = %v, want %v", test.key, got, test.want)
		}
	}
}
