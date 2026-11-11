package projectionserver

import (
	"context"
	"fmt"
	"testing"
	"time"

	q "github.com/adiom-data/weave/internal/querysubscription/model"
	relationdef "github.com/adiom-data/weave/internal/relation"
	"github.com/adiom-data/weave/internal/storage"
	pb "github.com/adiom-data/weave/pkg/gen/reactive/v1"
	"google.golang.org/protobuf/types/known/structpb"
)

func nullableProjectionRequest(wait, entity bool) *pb.ProjectionSnapshotRequest {
	request := snapshotTestRequest(wait, true)
	child := request.Root.Children[0]
	bindings := []*pb.ProjectionBinding{{Column: &pb.FieldPath{Segments: []string{"id"}}, Operator: pb.Comparison_OPERATOR_EQUAL,
		Value: &pb.ProjectionBinding_ParentColumn{ParentColumn: &pb.FieldPath{Segments: []string{"target"}}}}}
	if entity {
		child.GetEntity().Bindings = bindings
	} else {
		child.Selection = &pb.ProjectionNode_List{List: &pb.ProjectionList{Query: child.GetEntity().Query, Bindings: bindings,
			Window: &pb.InitialWindow{From: pb.InitialWindow_FROM_START, Count: 10}}}
	}
	return request
}

func nullableProjectionBackend(t *testing.T) *policyBackend {
	t.Helper()
	return newPolicyBackend(func(_ context.Context, request q.QuerySubscribeRequest) q.QuerySnapshot {
		if request.Query.Relation.Name != "projects" {
			t.Errorf("null binding opened a query for %s", request.Query.Relation.Name)
		}
		row := policyRow("projects", "p1", "Project")
		row.Record["target"] = storage.Value{Type: storage.TypeText, State: storage.ValueNull}
		return policySnapshot(10, row)
	})
}

func nullableProjectionOptions(policy bool) Options {
	options := policyOptions()
	if policy {
		options.Relations.Definitions = relationdef.Definitions{
			{Namespace: "app", Name: "projects"}: {Visibility: "true"},
			{Namespace: "app", Name: "tasks"}:    {Visibility: "true"},
		}
	} else {
		options.Relations = nil
	}
	return options
}

func TestNullableProjectionSnapshotsWithAndWithoutPolicy(t *testing.T) {
	for _, policy := range []bool{false, true} {
		for _, wait := range []bool{false, true} {
			for _, entity := range []bool{false, true} {
				t.Run(fmt.Sprintf("policy=%v/sync=%v/entity=%v", policy, wait, entity), func(t *testing.T) {
					ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
					defer cancel()
					service, err := New(ctx, nullableProjectionBackend(t), nullableProjectionOptions(policy))
					if err != nil {
						t.Fatal(err)
					}
					defer service.Close()
					response, err := service.(*server).GetProjectionSnapshot(ctx, nullableProjectionRequest(wait, entity))
					if err != nil {
						t.Fatal(err)
					}
					rows := response.Data.Fields["projects"].GetListValue().Values
					if len(rows) != 1 || response.Extensions.Synchronized != wait {
						t.Fatalf("nullable projection snapshot = %v", response)
					}
					related := rows[0].GetStructValue().Fields["tasks"]
					if related == nil {
						t.Fatal("empty child missing from snapshot")
					}
					if entity {
						if _, null := related.Kind.(*structpb.Value_NullValue); !null {
							t.Fatalf("empty entity = %v", related)
						}
					} else if list := related.GetListValue(); list == nil || len(list.Values) != 0 {
						t.Fatalf("empty list = %v", related)
					}
				})
			}
		}
	}
}

func TestNullablePolicyProjectionStreamReachesReady(t *testing.T) {
	for _, wait := range []bool{false, true} {
		t.Run(fmt.Sprintf("sync=%v", wait), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			stream, done := openPolicyStreamOptions(t, ctx, nullableProjectionBackend(t), nullableProjectionOptions(true))
			defer func() { cancel(); <-done }()
			stream.received <- &pb.ClientMessage{SubscriptionId: "view", RequestId: "open", Operation: &pb.ClientMessage_SubscribeProjection{
				SubscribeProjection: &pb.SubscribeProjection{Root: nullableProjectionRequest(wait, true).Root, WaitForSync: wait},
			}}
			batch := receivePolicyBatch(t, ctx, stream, pb.ModelBatch_KIND_SNAPSHOT, "")
			if len(batch.Entities) != 1 {
				t.Fatalf("nullable projection published extra entities: %v", batch)
			}
			if ready := receiveAuthorizationMessage(t, stream.sent); ready.GetReady() == nil {
				t.Fatalf("nullable projection did not reach Ready: %v", ready)
			}
		})
	}
}
