package projectionserver

import (
	"context"
	"testing"
	"time"

	"github.com/adiom-data/weave/internal/accesscontrol"
	proj "github.com/adiom-data/weave/internal/projection"
	"github.com/adiom-data/weave/internal/projection/evaluation"
	q "github.com/adiom-data/weave/internal/querysubscription/model"
	relationdef "github.com/adiom-data/weave/internal/relation"
	"github.com/adiom-data/weave/internal/storage"
)

type recordingReleasePrepared struct {
	visibilityTestPrepared
	released map[string]int
}

func (p *recordingReleasePrepared) Release(_ context.Context, id string, _ []storage.EntityKey) error {
	if p.released == nil {
		p.released = make(map[string]int)
	}
	p.released[id]++
	return nil
}

func TestProjectionCleanupReleasesUnpublishedAttemptDependencies(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	backend := newPolicyBackend(func(ctx context.Context, _ q.QuerySubscribeRequest) q.QuerySnapshot {
		<-ctx.Done() // keep the newly discovered grant unresolved
		return policySnapshot(10)
	})
	feed, err := backend.OpenFeed(ctx, q.FeedOptions{BufferLimit: 100})
	if err != nil {
		t.Fatal(err)
	}
	defer feed.Close(context.Background())
	evaluator, err := relationdef.Compile(policyOptions().Relations.Definitions, policyOptions().Relations.Limits)
	if err != nil {
		t.Fatal(err)
	}
	controller, err := newRelationAccess(ctx, feed, evaluator, accesscontrol.Claims{}, evaluation.Options{MaxQueries: 10, MaxRows: 10, LoadConcurrency: 1}, false)
	if err != nil {
		t.Fatal(err)
	}
	defer controller.Close()
	prepared, err := controller.Prepare(ctx, accesscontrol.PrepareRequest{ID: "connection"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := controller.session.Observe(q.Event{Progress: &q.Progress{Position: &q.Position{SourceID: "test", Value: 10}, Revision: 1}}); err != nil {
		t.Fatal(err)
	}
	row := policyRow("projects", "p1", "project")
	events := []proj.ProjectionEvent{
		{Type: proj.ProjectionInstanceOpened, InstanceID: "new-child", Checkpoint: &q.Position{SourceID: "test", Value: 10}},
		{Type: proj.ProjectionRowChanged, InstanceID: "new-child", Kind: q.Added, Key: row.Key, Record: row.Record},
	}
	state := &sharedProjection{visibility: newProjectionVisibility()}
	if err := state.policyOwnership.capture(ctx, prepared, state.visibility, events); err != nil {
		t.Fatal(err)
	}
	fresh := newProjectionVisibility()
	if _, err := fresh.filterInitial(ctx, controller, prepared, events); err != nil {
		t.Fatal(err)
	}
	receivePolicyRequest(t, ctx, backend, "grants")
	if !controller.session.Pending([]string{"new-child"}) {
		t.Fatal("test did not create an unresolved attempt")
	}
	if len(state.visibility.instances) != 0 {
		t.Fatal("attempt was published")
	}
	if err := state.releaseVisibility(ctx, prepared); err != nil {
		t.Fatal(err)
	}
	if controller.session.Pending([]string{"new-child"}) || len(controller.session.Invalidated()) != 0 {
		t.Fatal("unpublished authorization dependency survived projection cleanup")
	}
	if err := state.releaseVisibility(ctx, prepared); err != nil {
		t.Fatal("cleanup is not idempotent:", err)
	}
}

func TestNewAttemptRetiresDependenciesFromDiscardedAttempt(t *testing.T) {
	ctx := context.Background()
	prepared := &recordingReleasePrepared{}
	owner := projectionPolicyOwnership{}
	retained := newProjectionVisibility()
	first := []proj.ProjectionEvent{{Type: proj.ProjectionInstanceOpened, InstanceID: "obsolete"}}
	second := []proj.ProjectionEvent{{Type: proj.ProjectionInstanceOpened, InstanceID: "replacement"}}
	if err := owner.capture(ctx, prepared, retained, first); err != nil {
		t.Fatal(err)
	}
	if err := owner.capture(ctx, prepared, retained, second); err != nil {
		t.Fatal(err)
	}
	if prepared.released["obsolete"] != 1 || prepared.released["replacement"] != 0 {
		t.Fatal("incorrect attempt retirement", prepared.released)
	}
	if err := owner.release(ctx, prepared); err != nil {
		t.Fatal(err)
	}
	if prepared.released["replacement"] != 1 {
		t.Fatal("replacement ownership lost")
	}
}
