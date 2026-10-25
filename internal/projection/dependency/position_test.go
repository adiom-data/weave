package dependency

import (
	"testing"

	q "github.com/adiom-data/weave/internal/querysubscription/model"
)

func TestSnapshotKeepsItsOwnFreshnessFloor(t *testing.T) {
	for _, checkpoint := range []*q.Position{nil, position(15)} {
		for _, through := range []uint64{10, 19, 20, 25} {
			g := &generation{revision: 2, checkpoint: position(through)}
			e := &entry{state: result{revision: 1, checkpoint: checkpoint, fence: position(20)}}
			snapshot := g.snapshot(e)
			if snapshot.Revision != 2 || !q.SamePosition(snapshot.Fence, position(20)) {
				t.Fatalf("lost revision or fence: %+v", snapshot)
			}
			if through < 20 {
				if !q.SamePosition(snapshot.Checkpoint, checkpoint) {
					t.Fatalf("progress %d weakened snapshot checkpoint: %+v", through, snapshot)
				}
			} else if !q.SamePosition(snapshot.Checkpoint, position(through)) {
				t.Fatalf("covered snapshot did not advance: %+v", snapshot)
			}
		}
	}
}

func TestLocalDeliveriesDoNotAdvanceSharedSourceProgress(t *testing.T) {
	g := &generation{owner: &Cache{}}
	if err := g.dispatch(q.Event{Progress: &q.Progress{Position: position(10), Revision: 1}}); err != nil {
		t.Fatal(err)
	}
	for index, kind := range []q.DeliveryKind{q.DeliveryWindow, q.DeliveryHydration} {
		revision := uint64(index + 2)
		err := g.dispatch(q.Event{Delivery: &q.DeliveryEnvelope{Delivery: q.DeliveryMetadata{Kind: kind, Revision: revision, PreviousRevision: revision - 1, Checkpoint: position(20)}}})
		if err != nil {
			t.Fatal(err)
		}
		if !q.SamePosition(g.checkpoint, position(10)) {
			t.Fatalf("local delivery advanced global progress: %+v", g.checkpoint)
		}
	}
	// An actual source transaction between the old progress and the local
	// snapshot is still valid and must not be rejected or skipped.
	if err := g.dispatch(q.Event{Delivery: &q.DeliveryEnvelope{Delivery: q.DeliveryMetadata{Kind: q.DeliveryCommit, Revision: 4, PreviousRevision: 3, Checkpoint: position(15)}}}); err != nil {
		t.Fatal(err)
	}
	if !q.SamePosition(g.checkpoint, position(15)) {
		t.Fatal("commit did not advance source progress")
	}
	if err := g.dispatch(q.Event{Progress: &q.Progress{Position: position(14), Revision: 5, PreviousRevision: 4}}); err == nil {
		t.Fatal("regressing source report accepted")
	}
}
