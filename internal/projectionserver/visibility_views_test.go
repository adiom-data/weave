package projectionserver

import (
	"context"
	"testing"

	"github.com/adiom-data/weave/internal/accesscontrol"
	proj "github.com/adiom-data/weave/internal/projection"
	q "github.com/adiom-data/weave/internal/querysubscription/model"
	"github.com/adiom-data/weave/internal/storage"
)

type countedVisibilityView struct{ closed int }

func (v *countedVisibilityView) Close() error { v.closed++; return nil }

type countedVisibilityController struct {
	visibilityPointController
	views []*countedVisibilityView
}

func (c *countedVisibilityController) view() *countedVisibilityView {
	v := &countedVisibilityView{}
	c.views = append(c.views, v)
	return v
}
func (c *countedVisibilityController) Synchronize(_ context.Context, _ accesscontrol.PreparedID, position storage.Checkpoint) (accesscontrol.Checkpoint, error) {
	return accesscontrol.Checkpoint{Position: position, View: c.view()}, nil
}
func (c *countedVisibilityController) AcquireSnapshot(_ context.Context, fence storage.Fence) (accesscontrol.SnapshotCheckpoint, error) {
	return accesscontrol.SnapshotCheckpoint{Fence: fence, View: c.view()}, nil
}

func TestInitialVisibilitySharesEvidenceOnlyAtIdenticalBoundaries(t *testing.T) {
	controller := &countedVisibilityController{}
	prepared := visibilityTestPrepared{decide: func(accesscontrol.Candidate) bool { return true }}
	point := func(value uint64) *q.Position { return &q.Position{SourceID: "test", Value: value} }
	events := []proj.ProjectionEvent{
		{Type: proj.ProjectionInstanceOpened, InstanceID: "a", Checkpoint: point(10)},
		{Type: proj.ProjectionInstanceOpened, InstanceID: "b", Checkpoint: point(10), Fence: point(5)},
		{Type: proj.ProjectionInstanceOpened, InstanceID: "c", Checkpoint: point(20)},
		{Type: proj.ProjectionInstanceOpened, InstanceID: "d", Fence: point(10)},
		{Type: proj.ProjectionInstanceOpened, InstanceID: "e", Checkpoint: point(5), Fence: point(10)},
	}
	if _, err := newProjectionVisibility().filterInitial(context.Background(), controller, prepared, events); err != nil {
		t.Fatal(err)
	}
	// Two committed positions, plus a snapshot view at 10. The older fence of
	// b is irrelevant; d/e share snapshot evidence but not committed evidence.
	if len(controller.views) != 3 {
		t.Fatalf("opened %d views for 3 boundaries", len(controller.views))
	}
	for _, view := range controller.views {
		if view.closed != 1 {
			t.Fatalf("shared view closed %d times", view.closed)
		}
	}
}

func TestVisibilityDoesNotEvaluateBeforeNewerSnapshotFence(t *testing.T) {
	point := func(value uint64) *q.Position { return &q.Position{SourceID: "test", Value: value} }
	view, err := acquireVisibilityAt(context.Background(), visibilityPointController{}, visibilityPointPrepared{}, point(10), point(20))
	if err != nil {
		t.Fatal(err)
	}
	defer view.Close()
	if view.(visibilityPointView).checkpoint {
		t.Fatal("used old committed view rather than snapshot fence")
	}
}
