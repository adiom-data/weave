package projectionserver

import (
	"context"
	"errors"
	"testing"

	"github.com/adiom-data/weave/internal/accesscontrol"
	proj "github.com/adiom-data/weave/internal/projection"
	q "github.com/adiom-data/weave/internal/querysubscription/model"
)

type partialPrepared struct {
	visibilityTestPrepared
	decisions func(accesscontrol.Candidate) accesscontrol.Decision
}

func (p partialPrepared) Evaluate(_ context.Context, _ accesscontrol.View, candidates []accesscontrol.Candidate) ([]accesscontrol.Decision, error) {
	result := make([]accesscontrol.Decision, len(candidates))
	for i, c := range candidates {
		result[i] = p.decisions(c)
	}
	return result, nil
}

func TestDirectVisibilityDoesNotPublishPendingAllow(t *testing.T) {
	prepared := partialPrepared{decisions: func(accesscontrol.Candidate) accesscontrol.Decision {
		return accesscontrol.Decision{Visible: true, Pending: true}
	}}
	filtered, err := newSubscriptionVisibility().filterSnapshot(t.Context(), prepared, visibilityTestView{}, policySnapshot(10, policyRow("projects", "p1", "Pending")))
	if err != nil || len(filtered.Rows) != 0 {
		t.Fatalf("pending allow escaped: %+v %v", filtered, err)
	}
	prepared.decisions = func(accesscontrol.Candidate) accesscontrol.Decision { return accesscontrol.Decision{Visible: true} }
	visible, err := evaluateVisible(t.Context(), prepared, visibilityTestView{}, []accesscontrol.Candidate{{}})
	if err != nil || len(visible) != 1 || !visible[0] {
		t.Fatal("resolved allow did not become visible", visible, err)
	}
}

func TestPartialEvaluationFailureWithdrawsOnlyFailedReferencesAndRecoversWithoutRewindingSharedEntity(t *testing.T) {
	ctx := t.Context()
	gate := newProjectionVisibility()
	gate.partial = true
	model := newConnectionModel()
	prepared := partialPrepared{decisions: func(c accesscontrol.Candidate) accesscontrol.Decision {
		if string(c.Record["title"].Data) == "broken" {
			return accesscontrol.Decision{Err: errors.New("private fact details")}
		}
		return accesscontrol.Decision{Visible: true}
	}}
	apply := func(batch proj.ProjectionBatch, invalidation accesscontrol.Invalidation) (*proj.ProjectionBatch, error) {
		filtered, err := gate.filterTransaction(ctx, prepared, visibilityTestView{}, batch, invalidation)
		return &filtered, err
	}
	initial := policyContextFixture()
	for i := range initial {
		initial[i].Version = q.RecordVersion{Revision: 1}
		if initial[i].Type == proj.ProjectionInstanceOpened {
			initial[i].Bounds = modelQueryBounds()
		}
		if initial[i].Node == "task" {
			initial[i].EntityNode = true
		}
	}
	filtered, err := apply(proj.ProjectionBatch{Delivery: testDelivery(1), Events: initial}, accesscontrol.Invalidation{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = model.applyProjection("view", *filtered); err != nil {
		t.Fatal(err)
	}
	failed := initial[5]
	failed.Record = modelRecord("shared", "1", "broken")
	failed.Kind, failed.Version = q.Updated, q.RecordVersion{Revision: 2}
	filtered, err = apply(proj.ProjectionBatch{Delivery: testDelivery(2), Events: []proj.ProjectionEvent{failed}}, accesscontrol.Invalidation{})
	if err != nil {
		t.Fatal(err)
	}
	batch, err := model.applyProjection("view", *filtered)
	if err != nil {
		t.Fatal(err)
	}
	if len(batch.Branches) != 1 || batch.Branches[0].GetError() == nil || batch.Branches[0].GetError().Message == "private fact details" {
		t.Fatalf("missing or unsafe error: %v", batch)
	}
	id := publicEntityID(failed.Key)
	if model.entityRefs[id] != 1 || model.occurrences["left:task"].entityID != "" || model.occurrences["right:task"].entityID != id {
		t.Fatal("failed reference poisoned or retained shared coverage")
	}
	// Another healthy consumer advances the shared image while this path is down.
	if _, err = model.openSubscription("other", "other-query", "", q.QuerySnapshot{Bounds: modelQueryBounds(), Revision: 4, Rows: []q.QueryRow{{Row: q.Row{Key: failed.Key, Record: modelRecord("shared", "1", "latest")}}}}, nil); err != nil {
		t.Fatal(err)
	}
	failed.Record = modelRecord("shared", "1", "recovered-but-older")
	failed.Version = q.RecordVersion{Revision: 3}
	filtered, err = apply(proj.ProjectionBatch{Delivery: testDelivery(4), Events: []proj.ProjectionEvent{failed}}, accesscontrol.Invalidation{})
	if err != nil {
		t.Fatal(err)
	}
	batch, err = model.applyProjection("view", *filtered)
	if err != nil {
		t.Fatal(err)
	}
	if len(batch.Branches) != 1 || batch.Branches[0].GetRecovered() == nil || len(model.branches) != 0 {
		t.Fatalf("recovery missing: %v", batch)
	}
	if got := string(model.entities[id].record["title"].Data); got != "latest" {
		t.Fatalf("recovery rewound shared image to %q", got)
	}
	if model.occurrences["left:task"].entityID != id {
		t.Fatal("recovered branch not installed")
	}
}

func TestPartialFailuresAreOptInAndDoNotTurnErrorsIntoDenials(t *testing.T) {
	prepared := partialPrepared{decisions: func(accesscontrol.Candidate) accesscontrol.Decision {
		return accesscontrol.Decision{Err: errors.New("evaluation failed")}
	}}
	for _, partial := range []bool{false, true} {
		gate := newProjectionVisibility()
		gate.partial = partial
		filtered, err := gate.filterBatch(t.Context(), prepared, visibilityTestView{}, proj.ProjectionBatch{Events: policyContextFixture()})
		if !partial {
			if err == nil {
				t.Fatal("default failure behavior changed")
			}
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		failures := 0
		for _, event := range filtered.Events {
			if event.Type == proj.ProjectionBranchChanged && event.Failure != nil {
				failures++
				if event.InstanceID != "projects" {
					t.Fatal("inherited failure leaked as an independent child error")
				}
			}
		}
		if failures != 1 {
			t.Fatalf("got %d failures, want topmost branch only", failures)
		}
	}
}

func TestPartialParentRecoveryPublishesCompleteBranchWithRemainingChildError(t *testing.T) {
	gate := newProjectionVisibility()
	gate.partial = true
	badParent, badChild := true, true
	prepared := partialPrepared{decisions: func(c accesscontrol.Candidate) accesscontrol.Decision {
		if c.Context.InstanceID == "projects" && badParent || c.Context.InstanceID == "left:task" && badChild {
			return accesscontrol.Decision{Err: errors.New("failed")}
		}
		return accesscontrol.Decision{Visible: true}
	}}
	initial := policyContextFixture()
	if _, err := gate.filterBatch(t.Context(), prepared, visibilityTestView{}, proj.ProjectionBatch{Events: initial}); err != nil {
		t.Fatal(err)
	}
	badParent = false
	changed := initial[1]
	changed.Kind = q.Updated
	batch, err := gate.filterBatch(t.Context(), prepared, visibilityTestView{}, proj.ProjectionBatch{Delivery: testDelivery(2), Events: []proj.ProjectionEvent{changed}})
	if err != nil {
		t.Fatal(err)
	}
	opened := map[string]bool{}
	failed, recovered := map[string]bool{}, map[string]bool{}
	for _, event := range batch.Events {
		if event.Type == proj.ProjectionInstanceOpened {
			opened[event.InstanceID] = true
		}
		if event.Type == proj.ProjectionBranchChanged {
			if event.Failure != nil {
				failed[event.InstanceID] = true
			} else {
				recovered[event.InstanceID] = true
			}
		}
	}
	if !recovered["projects"] || !failed["left:task"] || !opened["left:links"] || !opened["right:links"] || !opened["right:task"] {
		t.Fatalf("incomplete parent recovery: %+v", batch.Events)
	}
	if gate.instances["left:task"].rows[initial[5].Key].effective {
		t.Fatal("errored child was exposed")
	}
	badChild = false
	batch, err = gate.filterTransaction(t.Context(), prepared, visibilityTestView{}, proj.ProjectionBatch{Delivery: testDelivery(3)}, accesscontrol.Invalidation{Targets: []accesscontrol.InvalidationTarget{{InstanceID: "left:task", Key: initial[5].Key}}})
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, event := range batch.Events {
		if event.Type == proj.ProjectionBranchChanged && event.InstanceID == "left:task" && event.Failure == nil {
			found = true
		}
	}
	if !found || !gate.instances["left:task"].rows[initial[5].Key].effective {
		t.Fatal("dependency invalidation did not recover child")
	}
}

func TestPartialFailureReleasesSoleEntityAndCleanupClearsErrors(t *testing.T) {
	model := newConnectionModel()
	key := modelKey(snapshotTestRelation(), "one")
	gate := newProjectionVisibility()
	gate.partial = true
	broken := false
	prepared := partialPrepared{decisions: func(accesscontrol.Candidate) accesscontrol.Decision {
		if broken {
			return accesscontrol.Decision{Err: errors.New("failed")}
		}
		return accesscontrol.Decision{Visible: true}
	}}
	initial := []proj.ProjectionEvent{{Type: proj.ProjectionInstanceOpened, InstanceID: "root", Node: "projects", Bounds: modelQueryBounds()}, {Type: proj.ProjectionRowChanged, InstanceID: "root", Node: "projects", Kind: q.Added, Key: key, Record: modelRecord("one", "1", "old")}}
	filtered, err := gate.filterBatch(t.Context(), prepared, visibilityTestView{}, proj.ProjectionBatch{Events: initial})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = model.applyProjection("p", filtered); err != nil {
		t.Fatal(err)
	}
	broken = true
	filtered, err = gate.filterTransaction(t.Context(), prepared, visibilityTestView{}, proj.ProjectionBatch{Delivery: testDelivery(2)}, accesscontrol.Invalidation{Targets: []accesscontrol.InvalidationTarget{{InstanceID: "root", Key: key}}})
	if err != nil {
		t.Fatal(err)
	}
	batch, err := model.applyProjection("p", filtered)
	if err != nil {
		t.Fatal(err)
	}
	if len(batch.Entities) != 1 || batch.Entities[0].GetReleased() == nil || model.entities[publicEntityID(key)] != nil || len(model.branches) != 1 {
		t.Fatalf("failure retained live cache ownership: %v", batch)
	}
	cleanup, err := model.closeScope("p")
	if err != nil {
		t.Fatal(err)
	}
	if len(cleanup.Branches) != 1 || cleanup.Branches[0].GetRecovered() == nil || len(model.branches) != 0 {
		t.Fatalf("error survived cleanup: %v", cleanup)
	}
}
