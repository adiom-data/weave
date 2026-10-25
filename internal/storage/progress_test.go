package storage

import "testing"

// Different concrete types deliberately share the same ordering contract.
type checkpointPosition struct {
	source SourceID
	order  uint64
}

func (p checkpointPosition) Source() SourceID { return p.source }
func (p checkpointPosition) Order() uint64    { return p.order }
func (checkpointPosition) StableID() string   { return "not-an-ordering-key" }
func (checkpointPosition) StorageCheckpoint() {}

type fencePosition struct {
	source SourceID
	order  uint64
}

func (p fencePosition) Source() SourceID { return p.source }
func (p fencePosition) Order() uint64    { return p.order }
func (fencePosition) StableID() string   { return "different-native-encoding" }
func (fencePosition) StorageFence()      {}

func TestCompareSourcePositionsAcrossRepresentations(t *testing.T) {
	for _, test := range []struct {
		left, right uint64
		want        int
	}{{0, 0, 0}, {1, 2, -1}, {2, 1, 1}, {2, 2, 0}, {0, ^uint64(0), -1}, {^uint64(0), 0, 1}} {
		checkpoint := checkpointPosition{"database", test.left}
		fence := fencePosition{"database", test.right}
		got, err := ComparePositions(checkpoint, fence)
		if err != nil || got != test.want {
			t.Fatalf("compare %d/%d = %d, %v", test.left, test.right, got, err)
		}
		covered, err := CheckpointCovers(checkpoint, fence)
		if err != nil || covered != (test.want >= 0) {
			t.Fatalf("covers %d/%d = %v, %v", test.left, test.right, covered, err)
		}
	}
}

func TestSourcePositionComparisonsRejectMissingAndForeignSources(t *testing.T) {
	valid := checkpointPosition{"database", 1}
	for _, other := range []SourcePosition{nil, fencePosition{"", 1}, fencePosition{"other", 1}} {
		if _, err := ComparePositions(valid, other); err == nil {
			t.Fatalf("invalid right position accepted: %#v", other)
		}
		if _, err := ComparePositions(other, valid); err == nil {
			t.Fatalf("invalid left position accepted: %#v", other)
		}
	}
	if covered, err := CheckpointCovers(nil, fencePosition{"database", 0}); err == nil || covered {
		t.Fatal("missing checkpoint fabricated coverage")
	}
	if covered, err := CheckpointCovers(valid, nil); err == nil || covered {
		t.Fatal("missing fence fabricated coverage")
	}
}
