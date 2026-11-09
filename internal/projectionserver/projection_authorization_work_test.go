package projectionserver

import (
	"context"
	"testing"

	"github.com/adiom-data/weave/internal/accesscontrol"
	proj "github.com/adiom-data/weave/internal/projection"
	q "github.com/adiom-data/weave/internal/querysubscription/model"
)

type recordedAuthorization struct {
	visibilityTestPrepared
	calls      int
	candidates []accesscontrol.Candidate
}

func (p *recordedAuthorization) Evaluate(ctx context.Context, view accesscontrol.View, candidates []accesscontrol.Candidate) ([]accesscontrol.Decision, error) {
	p.calls++
	p.candidates = append(p.candidates, candidates...)
	return p.visibilityTestPrepared.Evaluate(ctx, view, candidates)
}

func TestProjectionAuthorizationCombinesTargetsInOneParentFirstPass(t *testing.T) {
	ctx := context.Background()
	gate := newProjectionVisibility()
	prepared := &recordedAuthorization{visibilityTestPrepared: visibilityTestPrepared{decide: func(accesscontrol.Candidate) bool { return true }}}
	initial := policyContextFixture()
	if _, err := gate.filterBatch(ctx, prepared, visibilityTestView{}, proj.ProjectionBatch{Events: initial}); err != nil {
		t.Fatal(err)
	}
	if prepared.calls != 1 || len(prepared.candidates) != 5 {
		t.Fatalf("initial authorization repeated: calls=%d candidates=%d", prepared.calls, len(prepared.candidates))
	}
	prepared.calls, prepared.candidates = 0, nil
	// One parent change invalidates both branches. A directly changed task and
	// repeated explicit targets must still each have only one final check.
	project, task := initial[1], initial[5]
	project.Kind, project.Version.Revision = q.Updated, 2
	task.Kind, task.Version.Revision = q.Updated, 2
	task.Record = modelRecord("shared", "1", "new data")
	inv := accesscontrol.Invalidation{Targets: []accesscontrol.InvalidationTarget{
		{InstanceID: "left:task", Key: task.Key}, {InstanceID: "left:task", Key: task.Key},
	}, Candidates: []accesscontrol.Candidate{{Key: task.Key, Record: modelRecord("shared", "1", "policy only"), Complete: true}}}
	if _, err := gate.filterTransaction(ctx, prepared, visibilityTestView{}, proj.ProjectionBatch{Delivery: testDelivery(2), Events: []proj.ProjectionEvent{task, project}}, inv); err != nil {
		t.Fatal(err)
	}
	if prepared.calls != 1 || len(prepared.candidates) != 5 {
		t.Fatalf("combined authorization repeated: calls=%d candidates=%d", prepared.calls, len(prepared.candidates))
	}
	seen := make(map[occurrenceRow]bool)
	depth := 0
	for _, candidate := range prepared.candidates {
		key := occurrenceRow{candidate.Context.InstanceID, candidate.Key}
		if seen[key] || candidate.Context.Projection.Len() < depth {
			t.Fatal("authorization is duplicated or not parent first")
		}
		seen[key], depth = true, candidate.Context.Projection.Len()
		if candidate.Key == task.Key && string(candidate.Record["title"].Data) != "policy only" {
			t.Fatal("combined pass lost the explicit policy input")
		}
	}
	if string(gate.instances["left:task"].rows[task.Key].event.Record["title"].Data) != "new data" || string(gate.instances["right:task"].rows[task.Key].event.Record["title"].Data) != "Task" {
		t.Fatal("policy input replaced authoritative occurrence data")
	}
}

func TestProjectionAuthorizationUsesFinalImageForRepeatedRawChanges(t *testing.T) {
	gate := newProjectionVisibility()
	prepared := &recordedAuthorization{visibilityTestPrepared: visibilityTestPrepared{decide: func(c accesscontrol.Candidate) bool { return string(c.Record["title"].Data) == "final" }}}
	events := policyContextFixture()[:2]
	first := events[1]
	last := first
	last.Record = modelRecord("p1", "1", "final")
	last.Kind, last.Version.Revision = q.Updated, 2
	events = append(events, last)
	batch, err := gate.filterBatch(context.Background(), prepared, visibilityTestView{}, proj.ProjectionBatch{Events: events})
	if err != nil {
		t.Fatal(err)
	}
	if len(prepared.candidates) != 1 || string(prepared.candidates[0].Record["title"].Data) != "final" || !gate.instances[first.InstanceID].rows[first.Key].allowed {
		t.Fatal("multiple raw events were not coalesced into one final authorization")
	}
	for _, event := range batch.Events {
		if event.Type == proj.ProjectionRowChanged && event.Kind != q.Removed && string(event.Record["title"].Data) != "final" {
			t.Fatal("final authorization was incorrectly applied to an earlier unauthorized image")
		}
	}
}

func TestProjectionAuthorizationInvalidatedParentReevaluatesPropagation(t *testing.T) {
	gate := newProjectionVisibility()
	prepared := &recordedAuthorization{visibilityTestPrepared: visibilityTestPrepared{decide: func(accesscontrol.Candidate) bool { return true }}}
	initial := policyContextFixture()
	if _, err := gate.filterBatch(context.Background(), prepared, visibilityTestView{}, proj.ProjectionBatch{Events: initial}); err != nil {
		t.Fatal(err)
	}
	prepared.calls, prepared.candidates = 0, nil
	inv := accesscontrol.Invalidation{Targets: []accesscontrol.InvalidationTarget{{InstanceID: initial[1].InstanceID, Key: initial[1].Key}}}
	if _, err := gate.filterTransaction(context.Background(), prepared, visibilityTestView{}, proj.ProjectionBatch{Delivery: testDelivery(2)}, inv); err != nil {
		t.Fatal(err)
	}
	if prepared.calls != 1 || len(prepared.candidates) != 5 {
		t.Fatalf("policy-only parent invalidation lost descendants: calls=%d candidates=%d", prepared.calls, len(prepared.candidates))
	}
}

func TestProjectionAuthorizationNewRowInvalidationDoesNotReplaceData(t *testing.T) {
	gate := newProjectionVisibility()
	prepared := &recordedAuthorization{visibilityTestPrepared: visibilityTestPrepared{decide: func(c accesscontrol.Candidate) bool {
		return string(c.Record["title"].Data) == "policy only"
	}}}
	events := policyContextFixture()[:2]
	key := events[1].Key
	if _, err := gate.filterTransaction(context.Background(), prepared, visibilityTestView{}, proj.ProjectionBatch{Delivery: testDelivery(2), Events: events}, accesscontrol.Invalidation{Candidates: []accesscontrol.Candidate{{Key: key, Record: modelRecord("p1", "1", "policy only"), Complete: true}}}); err != nil {
		t.Fatal(err)
	}
	row := gate.instances[events[1].InstanceID].rows[key]
	if len(prepared.candidates) != 1 || !row.allowed || string(row.event.Record["title"].Data) != "Project" {
		t.Fatal("new-row invalidation was lost, duplicated, or replaced authoritative data")
	}
}

func TestInitialPolicyImageBorrowsImmutableDataUntilModelOwnership(t *testing.T) {
	ctx := context.Background()
	image := newInitialPolicyImage()
	events := policyContextFixture()[:2]
	events[0].Bounds = modelQueryBounds()
	prepared := visibilityTestPrepared{decide: func(accesscontrol.Candidate) bool { return true }}
	if _, err := image.visibility.filterBatch(ctx, prepared, visibilityTestView{}, proj.ProjectionBatch{Events: events}); err != nil {
		t.Fatal(err)
	}
	borrowed := image.borrowEvents()
	retained := image.visibility.instances["projects"].rows[events[1].Key]
	if &borrowed[1].Record["title"].Data[0] != &retained.event.Record["title"].Data[0] {
		t.Fatal("read-only image unexpectedly deep copied its retained payload")
	}
	model := newConnectionModel()
	if _, err := model.applyProjection("snapshot", proj.ProjectionBatch{Events: borrowed}); err != nil {
		t.Fatal(err)
	}
	// The consumer owns its mutable model, independently of the borrowed image.
	model.entities[publicEntityID(events[1].Key)].record["title"].Data[0] = 'X'
	if string(retained.event.Record["title"].Data) != "Project" {
		t.Fatal("model mutation escaped its ownership boundary")
	}
	updated := events[1]
	updated.Kind, updated.Version.Revision = q.Updated, 2
	updated.Record = modelRecord("p1", "1", "replacement")
	if _, err := image.visibility.filterBatch(ctx, prepared, visibilityTestView{}, proj.ProjectionBatch{Events: []proj.ProjectionEvent{updated}}); err != nil {
		t.Fatal(err)
	}
	if string(borrowed[1].Record["title"].Data) != "Project" || string(image.borrowEvents()[1].Record["title"].Data) != "replacement" {
		t.Fatal("replacing current visibility state mutated the borrowed image")
	}
}
