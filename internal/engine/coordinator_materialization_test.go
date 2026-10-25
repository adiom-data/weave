package engine

import (
	"testing"

	"github.com/adiom-data/weave/internal/datastore"
	"github.com/adiom-data/weave/internal/storage"
)

func TestSameMaterializationReadsRejectsDifferentStagedRanges(t *testing.T) {
	relation := storage.RelationKey{Source: "test", Name: "tasks"}
	plan := fakePlan{fingerprint: "all", relation: relation}
	start := datastore.Boundary{Kind: datastore.BoundaryStart}
	end := datastore.Boundary{Kind: datastore.BoundaryEnd}
	one := datastore.Boundary{Kind: datastore.BoundaryCursor, Key: datastore.EncodedSortKey("1")}
	two := datastore.Boundary{Kind: datastore.BoundaryCursor, Key: datastore.EncodedSortKey("2")}

	equal, err := sameMaterializationReads(plan,
		[]datastore.Bounds{{Left: start, Right: one}, {Left: two, Right: end}},
		[]datastore.Bounds{{Left: start, Right: one}, {Left: two, Right: end}},
	)
	if err != nil || !equal {
		t.Fatalf("identical ranges: equal=%v err=%v", equal, err)
	}
	equal, err = sameMaterializationReads(plan,
		[]datastore.Bounds{{Left: start, Right: one}, {Left: two, Right: end}},
		[]datastore.Bounds{{Left: start, Right: two}},
	)
	if err != nil {
		t.Fatal(err)
	}
	if equal {
		t.Fatal("different staged ranges were accepted")
	}
}
