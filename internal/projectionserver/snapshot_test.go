package projectionserver

import (
	"context"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/adiom-data/weave/internal/querysubscription/model"
	"github.com/adiom-data/weave/internal/storage"
	reactivev1 "github.com/adiom-data/weave/pkg/gen/reactive/v1"
)

func TestGetProjectionSnapshotRejectsLayoutBeforeOpeningFeed(t *testing.T) {
	request := snapshotTestRequest(false, true)
	request.Root.Children[0].NodeId = "title"
	// This client errors if a feed is opened. Layout errors must instead be
	// returned immediately, even if the table would have had zero rows.
	response, err := newWithDefaults(constructorQueries{}).GetProjectionSnapshot(context.Background(), request)
	if response != nil || status.Code(err) != codes.InvalidArgument {
		t.Fatalf("response=%v err=%v", response, err)
	}
}

func TestGetProjectionSnapshotReturnsInitialImageWithoutWaitingForCommonProgress(t *testing.T) {
	key := modelKey(snapshotTestRelation(), "project-1")
	client := newProjectionLifecycleClient(querysubscription.QuerySnapshot{
		Bounds:     snapshotTestBounds(),
		Rows:       []querysubscription.QueryRow{{Row: querysubscription.Row{Key: key, Record: modelRecord("project-1", "1", "Project"), Cursor: []byte("project-1")}}},
		Checkpoint: &querysubscription.Position{SourceID: "test", Value: 10},
		Fence:      &querysubscription.Position{SourceID: "test", Value: 10},
	})
	response, err := newWithDefaults(client).GetProjectionSnapshot(context.Background(), snapshotTestRequest(false, false))
	if err != nil {
		t.Fatal(err)
	}
	if response.Extensions.Synchronized {
		t.Fatal("initial-only snapshot was marked synchronized")
	}
	rows := response.Data.Fields["projects"].GetListValue().Values
	if len(rows) != 1 || rows[0].GetStructValue().Fields["title"].GetStringValue() != "Project" {
		t.Fatalf("snapshot data = %v", response.Data)
	}
	if response.Extensions.Checkpoint != "" {
		t.Fatalf("initial-only snapshot advertised common checkpoint %q", response.Extensions.Checkpoint)
	}
}

func TestGetProjectionSnapshotUsesSingleInstanceCheckpointWithoutRequestingProgress(t *testing.T) {
	key := modelKey(snapshotTestRelation(), "project-1")
	client := newProjectionLifecycleClient(querysubscription.QuerySnapshot{
		Bounds:     snapshotTestBounds(),
		Rows:       []querysubscription.QueryRow{{Row: querysubscription.Row{Key: key, Record: modelRecord("project-1", "1", "Project"), Cursor: []byte("project-1")}}},
		Checkpoint: &querysubscription.Position{SourceID: "test", Value: 10},
		Fence:      &querysubscription.Position{SourceID: "test", Value: 10},
	})
	response, err := newWithDefaults(client).GetProjectionSnapshot(context.Background(), snapshotTestRequest(true, false))
	if err != nil {
		t.Fatal(err)
	}
	if !response.Extensions.Synchronized || response.Extensions.Checkpoint != "10" {
		t.Fatalf("single-instance snapshot = %#v", response)
	}
	select {
	case <-client.progress:
		t.Fatal("single-instance snapshot requested unnecessary progress")
	default:
	}
}

func TestGetProjectionSnapshotWaitsForFeedToCrossSnapshotFence(t *testing.T) {
	key := modelKey(snapshotTestRelation(), "project-1")
	client := newProjectionLifecycleClient(querysubscription.QuerySnapshot{
		Bounds:     snapshotTestBounds(),
		Rows:       []querysubscription.QueryRow{{Row: querysubscription.Row{Key: key, Record: modelRecord("project-1", "1", "Project"), Cursor: []byte("project-1")}}},
		Checkpoint: &querysubscription.Position{SourceID: "test", Value: 10},
		Fence:      &querysubscription.Position{SourceID: "test", Value: 10},
	})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	type result struct {
		response *reactivev1.ProjectionSnapshotResponse
		err      error
	}
	done := make(chan result, 1)
	go func() {
		response, err := newWithDefaults(client).GetProjectionSnapshot(ctx, snapshotTestRequest(true, true))
		done <- result{response: response, err: err}
	}()
	subscription := <-client.opened
	<-client.opened
	<-client.progress
	select {
	case outcome := <-done:
		t.Fatalf("synchronized snapshot returned before progress: response=%#v err=%v", outcome.response, outcome.err)
	case <-time.After(20 * time.Millisecond):
	}
	if err := subscription.offer(querysubscription.DeliveryEnvelope{Delivery: querysubscription.DeliveryMetadata{
		Kind: querysubscription.DeliveryCommit, Checkpoint: &querysubscription.Position{SourceID: "test", Value: 11},
		Atomic: true, Revision: 1,
	}}); err != nil {
		t.Fatal(err)
	}
	outcome := <-done
	if outcome.err != nil {
		t.Fatal(outcome.err)
	}
	if !outcome.response.Extensions.Synchronized || outcome.response.Extensions.Checkpoint != "11" {
		t.Fatalf("synchronized response = %#v", outcome.response)
	}
}

func snapshotTestRequest(wait, child bool) *reactivev1.ProjectionSnapshotRequest {
	request := &reactivev1.ProjectionSnapshotRequest{
		WaitForSync: wait,
		Root: &reactivev1.ProjectionNode{NodeId: "projects", Selection: &reactivev1.ProjectionNode_List{List: &reactivev1.ProjectionList{
			Query: &reactivev1.Query{
				Schema: "app", Table: "projects",
				Columns: []*reactivev1.FieldPath{{Segments: []string{"id"}}, {Segments: []string{"position"}}, {Segments: []string{"title"}}},
				Order:   []*reactivev1.OrderTerm{{Column: &reactivev1.FieldPath{Segments: []string{"id"}}}},
			},
			Window: &reactivev1.InitialWindow{From: reactivev1.InitialWindow_START, Count: 10},
		}}},
	}
	if child {
		request.Root.Children = []*reactivev1.ProjectionNode{
			{
				NodeId: "tasks",
				Selection: &reactivev1.ProjectionNode_Entity{Entity: &reactivev1.ProjectionEntity{
					Query: &reactivev1.Query{
						Schema: "app", Table: "tasks",
						Columns: []*reactivev1.FieldPath{{Segments: []string{"id"}}, {Segments: []string{"position"}}, {Segments: []string{"title"}}},
						Order:   []*reactivev1.OrderTerm{{Column: &reactivev1.FieldPath{Segments: []string{"id"}}}},
					},
				}},
			},
		}
	}
	return request
}

func snapshotTestBounds() querysubscription.Bounds {
	return querysubscription.Bounds{
		Left:  querysubscription.Boundary{Kind: querysubscription.BoundaryStart},
		Right: querysubscription.Boundary{Kind: querysubscription.BoundaryEnd},
	}
}

func snapshotTestRelation() storage.RelationKey {
	return storage.RelationKey{Source: "test", Namespace: "app", Name: "projects", OpaqueID: "projects"}
}
