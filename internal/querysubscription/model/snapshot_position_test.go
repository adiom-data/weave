package querysubscription

import "testing"

func TestSnapshotPositionPreservesNewestEvidenceAndSource(t *testing.T) {
	point := func(value uint64) *Position { return &Position{SourceID: "db", Value: value} }
	for _, pair := range [][2]*Position{{nil, point(20)}, {point(10), point(20)}, {point(20), point(10)}, {point(20), nil}} {
		floor, err := SnapshotPosition(pair[0], pair[1])
		if err != nil || !SamePosition(floor, point(20)) {
			t.Fatalf("floor=%+v err=%v", floor, err)
		}
		floor.Value = 0
		if pair[0] != nil && pair[0].Value == 0 || pair[1] != nil && pair[1].Value == 0 {
			t.Fatal("returned caller-owned pointer")
		}
	}
	for _, pair := range [][2]*Position{{nil, nil}, {point(10), {SourceID: "other", Value: 20}}, {{Value: 10}, nil}} {
		if _, err := SnapshotPosition(pair[0], pair[1]); err == nil {
			t.Fatal("invalid positions accepted")
		}
	}
}
