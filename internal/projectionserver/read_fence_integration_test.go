package projectionserver

import (
	"fmt"
	"testing"

	"github.com/adiom-data/weave/internal/postgres"
	"github.com/adiom-data/weave/internal/projection/plan"
	relationdef "github.com/adiom-data/weave/internal/relation"
	"github.com/adiom-data/weave/internal/storage"
	pb "github.com/adiom-data/weave/pkg/gen/reactive/v1"
)

// An ACL-only update must be visible through both warm projection rows and
// lazily loaded authorization facts, without an extra direct entity read.
func TestReadFenceCoversWarmContextualComputedFactsIntegration(t *testing.T) {
	for _, remote := range []bool{false, true} {
		for _, sync := range []bool{false, true} {
			t.Run(fmt.Sprintf("remote=%v/sync=%v", remote, sync), func(t *testing.T) {
				ctx, pool, schema, _, _, queries := newReferenceIntegration(t, remote, false)
				projects := storage.RelationName{Namespace: schema, Name: "projects"}
				links := storage.RelationName{Namespace: schema, Name: "links"}
				tasks := storage.RelationName{Namespace: schema, Name: "tasks"}
				definitions := relationdef.Definitions{
					projects: {Visibility: "true", Computed: map[string]relationdef.Computed{"canEdit": {Type: storage.TypeBool, Expression: "false", Propagate: []relationdef.Propagation{{To: links, When: "true"}}}}},
					links:    {Visibility: "true", Computed: map[string]relationdef.Computed{"canEdit": {Type: storage.TypeBool, Expression: "has(inherited.canEdit) && inherited.canEdit", Propagate: []relationdef.Propagation{{To: tasks, When: "parent.task_id == item.id"}}}}},
					tasks: {Visibility: "true", Facts: []relationdef.Fact{{ID: "acl", Projection: &plan.Node{Name: "acl", Query: storage.QuerySpec{Relation: tasks, Filter: &storage.Predicate{Comparison: &storage.Comparison{Field: "id", Operator: storage.ComparisonEqual, Value: storage.Literal{Type: storage.TypeInteger, Data: []byte("2")}}}}}}}, Computed: map[string]relationdef.Computed{
						"canEdit":         {Type: storage.TypeBool, Expression: `(has(inherited.canEdit) && inherited.canEdit) || facts.acl.exists(g, g.item.title == "editor")`},
						"canManageAccess": {Type: storage.TypeBool, Expression: "computed.canEdit"},
					}},
				}
				service, err := New(ctx, queries, Options{Relations: &RelationOptions{Definitions: definitions}})
				if err != nil {
					t.Fatal(err)
				}
				defer service.Close()
				s := service.(*server)
				root := referenceProjection(schema, false)
				root.Children[0].Children[0].Computed = []string{"canEdit", "canManageAccess"}
				check := func(fence string, want bool) {
					t.Helper()
					response, err := s.GetProjectionSnapshot(ctx, &pb.ProjectionSnapshotRequest{Root: root, WaitForSync: sync, AtLeastFence: fence})
					if err != nil {
						t.Fatal(err)
					}
					fields := response.Data.Fields["project"].GetStructValue().Fields["links"].GetListValue().Values[0].GetStructValue().Fields["task"].GetStructValue().Fields
					for _, name := range []string{"canEdit", "canManageAccess"} {
						if fields[name] == nil || fields[name].GetBoolValue() != want {
							t.Fatalf("%s: %v", name, response)
						}
					}
				}
				check("", false)
				for _, title := range []string{"editor", "viewer"} {
					if _, err := pool.Exec(ctx, "UPDATE "+schema+".tasks SET title=$1 WHERE id=2", title); err != nil {
						t.Fatal(err)
					}
					fence, err := postgres.CaptureReadFence(ctx, pool)
					if err != nil {
						t.Fatal(err)
					}
					check(fence, title == "editor")
				}
			})
		}
	}
}
