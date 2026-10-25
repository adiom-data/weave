package projectionserver

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/adiom-data/weave/internal/projectioncompiler"
	q "github.com/adiom-data/weave/internal/querysubscription/model"
	"github.com/adiom-data/weave/internal/storage"
	pb "github.com/adiom-data/weave/pkg/gen/reactive/v1"
	"github.com/vektah/gqlparser/v2/ast"
)

func TestAllFieldsValidationRemainsExplicit(t *testing.T) {
	for _, test := range []struct {
		name   string
		all    bool
		query  *pb.Query
		fields *pb.ObjectSelection
	}{
		{"empty_is_not_all", false, &pb.Query{Schema: "app", Table: "tasks"}, nil},
		{"all_with_columns", true, &pb.Query{Schema: "app", Table: "tasks", Columns: []*pb.FieldPath{{Segments: []string{"id"}}}}, nil},
		{"all_with_mask", true, &pb.Query{Schema: "app", Table: "tasks"}, &pb.ObjectSelection{}},
		{"missing_query", true, nil, nil},
		{"missing_schema", true, &pb.Query{Table: "tasks"}, nil},
		{"invalid_order", true, &pb.Query{Schema: "app", Table: "tasks", Order: []*pb.OrderTerm{{}}}, nil},
		{"nil_order", true, &pb.Query{Schema: "app", Table: "tasks", Order: []*pb.OrderTerm{nil}}, nil},
		{"invalid_filter", true, &pb.Query{Schema: "app", Table: "tasks", Filter: &pb.Predicate{}}, nil},
	} {
		t.Run(test.name, func(t *testing.T) {
			root := &pb.ProjectionNode{NodeId: "task", AllFields: test.all, Fields: test.fields, Selection: &pb.ProjectionNode_Entity{Entity: &pb.ProjectionEntity{Query: test.query}}}
			if _, _, err := projectionNode(root, nil, 100, 8, 64); err == nil {
				t.Fatal("invalid selection accepted")
			}
		})
	}
	root := &pb.ProjectionNode{NodeId: "task", AllFields: true, Selection: &pb.ProjectionNode_Entity{Entity: &pb.ProjectionEntity{Query: &pb.Query{Schema: "app", Table: "tasks"}}}}
	_, masks, err := projectionNode(root, nil, 100, 8, 64)
	if err != nil {
		t.Fatal(err)
	}
	if mask, present := masks["task"]; !present || mask != nil {
		t.Fatal("all_fields did not resolve to whole-record output")
	}
	// Raw query subscription requests keep requiring explicit columns.
	if _, _, err := storageQuery(root.GetEntity().Query, 100, 8, 64); err == nil {
		t.Fatal("empty raw query selection became all fields")
	}
}

func TestAllFieldsSnapshotWithIndependentRelationshipSelection(t *testing.T) {
	schema := `type Query { task: Task tasks: [Task!]! }
type Task @table(schema:"app",name:"tasks") {
 id: String! ownerId: String! @column(path:["owner_id"])
 owner: User @relation(parent:["ownerId"],child:["id"])
}
type User @table(schema:"app",name:"users") { id: String! name: String! secret: String }`
	compiler, err := projectioncompiler.New(&ast.Source{Name: "schema", Input: schema})
	if err != nil {
		t.Fatal(err)
	}
	text := func(s string) storage.Value {
		return storage.Value{Type: storage.TypeText, State: storage.ValuePresent, Data: []byte(s)}
	}
	taskRecord := storage.Record{"id": text("task-1"), "owner_id": text("user-1"), "extra": text("not declared in the schema"), "metadata": {Type: storage.TypeObject, State: storage.ValuePresent, Object: storage.Record{"hidden": text("whole embedded value")}}}
	userRecord := storage.Record{"id": text("user-1"), "name": text("Alice"), "secret": text("not selected by default")}
	for _, list := range []bool{false, true} {
		for _, childAll := range []bool{false, true} {
			t.Run(fmt.Sprintf("list=%t/childAll=%t", list, childAll), func(t *testing.T) {
				rootName, window, directive := "task", "", ""
				if list {
					rootName, window = "tasks", " @window(initialCount:1)"
				}
				if childAll {
					directive = " @allFields"
				}
				ops, err := compiler.Compile(&ast.Source{Name: "projection", Input: fmt.Sprintf("query Details { %s @allFields%s { owner%s { name } } }", rootName, window, directive)})
				if err != nil {
					t.Fatal(err)
				}
				backend := newProjectionLifecycleClient(q.QuerySnapshot{})
				backend.snapshotFor = func(request q.QuerySubscribeRequest) q.QuerySnapshot {
					table, record, id := request.Query.Relation.Name, taskRecord, "task-1"
					if table == "users" {
						record, id = userRecord, "user-1"
					}
					key := modelKey(storage.RelationKey{Source: "test", Namespace: "app", Name: table, OpaqueID: table}, id)
					return q.QuerySnapshot{Bounds: snapshotTestBounds(), Rows: []q.QueryRow{{Row: q.Row{Key: key, Record: record}}}, Checkpoint: &q.Position{SourceID: "test", Value: 1}, Fence: &q.Position{SourceID: "test", Value: 1}}
				}
				service := newWithDefaults(backend)
				defer service.Close()
				response, err := service.GetProjectionSnapshot(context.Background(), &pb.ProjectionSnapshotRequest{Root: ops[0].Root})
				if err != nil {
					t.Fatal(err)
				}
				root := response.Data.Fields[rootName]
				if list {
					if len(root.GetListValue().Values) != 1 {
						t.Fatal("lost list item")
					}
					root = root.GetListValue().Values[0]
				}
				wantOwner := map[string]any{"name": "Alice"}
				if childAll {
					wantOwner["id"], wantOwner["secret"] = "user-1", "not selected by default"
				}
				want := map[string]any{"id": "task-1", "owner_id": "user-1", "extra": "not declared in the schema", "metadata": map[string]any{"hidden": "whole embedded value"}, rootName + ".owner": wantOwner}
				if !reflect.DeepEqual(root.GetStructValue().AsMap(), want) {
					t.Fatalf("got=%v want=%v", root, want)
				}
				if len(ops[0].Shape.Fields) != 0 || len(ops[0].Shape.Children["owner"].Fields) != 1 {
					t.Fatal("allFields widened the typed reader")
				}
			})
		}
	}
}

func TestAllFieldsReferenceRestoresNarrowOutputAfterClosing(t *testing.T) {
	m := newConnectionModel()
	record := modelRecord("one", "1", "Title")
	applyEntityImage(t, m, "narrow", 2, record, false, true, storage.MustFieldPath("title"))
	batch := applyEntityImage(t, m, "all", 1, record, false, true)
	if len(batch.Entities) != 1 || len(batch.Entities[0].GetUpsert().Values) != len(record) {
		t.Fatal("whole-record reference did not expand selected output")
	}
	record["new_column"] = record["title"]
	batch = applyEntityImage(t, m, "all", 3, record, false, false)
	if len(batch.Entities) != 1 || batch.Entities[0].GetUpsert().Values["new_column"] == nil {
		t.Fatal("allFields did not include a newly observed column")
	}
	batch, err := m.closeScope("all")
	if err != nil {
		t.Fatal(err)
	}
	if len(batch.Entities) != 1 || len(batch.Entities[0].GetUpsert().Values) != 1 || batch.Entities[0].GetUpsert().Values["title"] == nil {
		t.Fatal("closing allFields did not restore the remaining narrow selection")
	}
}
