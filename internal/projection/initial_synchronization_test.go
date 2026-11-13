package projection

import (
	"testing"

	q "github.com/adiom-data/weave/internal/querysubscription/model"
	"github.com/adiom-data/weave/internal/storage"
)

func TestInitialSynchronizationRequestsProgressForNewTargetsOnly(t *testing.T) {
	position := func(value uint64) *q.Position { return &q.Position{SourceID: "test", Value: value} }
	graph := projectionGraph{instances: map[string]*projectionInstance{
		"parent": {checkpoint: position(10)},
		"child":  {checkpoint: position(20)},
	}}
	observed := map[storage.SourceID]*q.Position{"test": position(5)}
	synchronizer := initialSynchronization{requested: make(map[storage.SourceID]*q.Position)}
	plan, err := synchronizer.inspectLocked(&graph, observed)
	if err != nil || plan.covered || plan.progressTargets["test"] == nil || plan.progressTargets["test"].Value != 20 {
		t.Fatalf("initial snapshot target: %+v %v", plan, err)
	}
	synchronizer.recordRequest(plan.progressTargets)
	// A response behind the snapshot does not justify another request for the
	// same target. It must remain blocked until ordered progress catches up.
	observed["test"] = position(10)
	plan, err = synchronizer.inspectLocked(&graph, observed)
	if err != nil || plan.covered || len(plan.progressTargets) != 0 {
		t.Fatalf("repeated request for an unchanged snapshot target: %+v %v", plan, err)
	}
	// Further expansion can introduce a newer snapshot and a new request reason.
	graph.instances["child"].checkpoint = position(30)
	plan, err = synchronizer.inspectLocked(&graph, observed)
	if err != nil || plan.covered || plan.progressTargets["test"] == nil || plan.progressTargets["test"].Value != 30 {
		t.Fatalf("new snapshot target was not requested: %+v %v", plan, err)
	}
	synchronizer.recordRequest(plan.progressTargets)
	observed["test"] = position(30)
	plan, err = synchronizer.inspectLocked(&graph, observed)
	if err != nil || !plan.covered || len(plan.progressTargets) != 0 || plan.cut["test"] == nil || plan.cut["test"].Value != 30 {
		t.Fatalf("covered snapshots did not establish the common cut: %+v %v", plan, err)
	}
}
