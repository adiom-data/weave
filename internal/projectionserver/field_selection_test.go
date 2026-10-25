package projectionserver

import (
	"context"
	"reflect"
	"testing"

	proj "github.com/adiom-data/weave/internal/projection"
	"github.com/adiom-data/weave/internal/projectioncompiler"
	q "github.com/adiom-data/weave/internal/querysubscription/model"
	"github.com/adiom-data/weave/internal/storage"
	pb "github.com/adiom-data/weave/pkg/gen/reactive/v1"
	"github.com/vektah/gqlparser/v2/ast"
)

func selectionField(name string, value *pb.ValueSelection) *pb.SelectedField {
	return &pb.SelectedField{Path: &pb.FieldPath{Segments: []string{name}}, Selection: value}
}
func selectionObject(fields ...*pb.SelectedField) *pb.ValueSelection {
	return &pb.ValueSelection{Kind: &pb.ValueSelection_Object{Object: &pb.ObjectSelection{Fields: fields}}}
}
func metadataSelection(t *testing.T, child string) *fieldSelection {
	t.Helper()
	selected, err := projectionFields(&pb.ObjectSelection{Fields: []*pb.SelectedField{selectionField("metadata", selectionObject(selectionField(child, nil)))}}, []storage.FieldPath{storage.MustFieldPath("metadata")}, 100)
	if err != nil {
		t.Fatal(err)
	}
	return selected
}

func TestRecursiveSelectionSuppressesUnselectedChangesAndShrinksSharedImage(t *testing.T) {
	m := newConnectionModel()
	owner, other := metadataSelection(t, "owner"), metadataSelection(t, "other")
	text := func(value string) storage.Value {
		return storage.Value{Type: storage.TypeText, State: storage.ValuePresent, Data: []byte(value)}
	}
	record := storage.Record{"metadata": {Type: storage.TypeObject, State: storage.ValuePresent, Object: storage.Record{"owner": text("Alice"), "other": text("Bob"), "secret": text("hidden")}}}
	key := modelKey(snapshotTestRelation(), "one")
	apply := func(scope string, version uint64, open bool, mask *fieldSelection) *pb.ModelBatch {
		t.Helper()
		events := []proj.ProjectionEvent{}
		if open {
			events = append(events, proj.ProjectionEvent{Type: proj.ProjectionInstanceOpened, Node: scope, InstanceID: scope, EntityNode: true})
		}
		events = append(events, proj.ProjectionEvent{Type: proj.ProjectionRowChanged, Node: scope, InstanceID: scope, EntityNode: true, Kind: q.Updated, Key: key, Record: record, Version: q.RecordVersion{Revision: version}})
		batch, err := m.applyProjection(scope, proj.ProjectionBatch{Events: events}, map[string]*fieldSelection{scope: mask})
		if err != nil {
			t.Fatal(err)
		}
		return batch
	}
	check := func(batch *pb.ModelBatch, want ...string) {
		t.Helper()
		if len(batch.Entities) != 1 {
			t.Fatalf("expected one entity event, got %v", batch)
		}
		fields := batch.Entities[0].GetUpsert().Values["metadata"].GetObjectValue().Fields
		if len(fields) != len(want) {
			t.Fatalf("fields=%v want=%v", fields, want)
		}
		for _, name := range want {
			if fields[name] == nil {
				t.Fatalf("missing %q", name)
			}
		}
	}
	check(apply("owner", 1, true, owner), "owner")
	record["metadata"].Object["secret"] = text("changed but not selected")
	if batch := apply("owner", 2, false, owner); len(batch.Entities) != 0 {
		t.Fatalf("unselected change emitted: %v", batch)
	}
	check(apply("other", 1, true, other), "owner", "other")
	record["metadata"].Object["owner"] = text("Changed")
	check(apply("owner", 3, false, owner), "owner", "other")
	closed, err := m.closeScope("other")
	if err != nil {
		t.Fatal(err)
	}
	check(closed, "owner")
	whole := selectColumns([]storage.FieldPath{storage.MustFieldPath("metadata")})
	check(apply("whole", 1, true, whole), "owner", "other", "secret")
	closed, err = m.closeScope("whole")
	if err != nil {
		t.Fatal(err)
	}
	check(closed, "owner")
}

func TestCompiledSnapshotNarrowsEmbeddedObjectsAndArrayElements(t *testing.T) {
	schema := `type Query { project: Project }
type Project @table(schema:"app",name:"projects") { id: String! meta: Metadata @column(path:["metadata"]) }
type Metadata { owner: String members: [Member] }
type Member { name: String email: String }`
	compiler, err := projectioncompiler.New(&ast.Source{Name: "schema", Input: schema})
	if err != nil {
		t.Fatal(err)
	}
	ops, err := compiler.Compile(&ast.Source{Name: "projection", Input: `query Example { project { id first:meta { owner } second:meta { members { name } } } }`})
	if err != nil {
		t.Fatal(err)
	}
	text := func(value string) storage.Value {
		return storage.Value{Type: storage.TypeText, State: storage.ValuePresent, Data: []byte(value)}
	}
	record := storage.Record{"id": text("one"), "metadata": {Type: storage.TypeObject, State: storage.ValuePresent, Object: storage.Record{
		"owner": text("Alice"), "secret": text("hidden"), "members": {Type: storage.TypeList, State: storage.ValuePresent, List: []storage.Value{
			{Type: storage.TypeObject, State: storage.ValuePresent, Object: storage.Record{"name": text("Bob"), "email": text("hidden")}},
			{State: storage.ValueNull},
		}},
	}}}
	client := newProjectionLifecycleClient(q.QuerySnapshot{Bounds: snapshotTestBounds(), Checkpoint: &q.Position{SourceID: "test", Value: 1}, Fence: &q.Position{SourceID: "test", Value: 1}, Rows: []q.QueryRow{{Row: q.Row{Key: modelKey(snapshotTestRelation(), "one"), Record: record}}}})
	response, err := newWithDefaults(client).GetProjectionSnapshot(context.Background(), &pb.ProjectionSnapshotRequest{Root: ops[0].Root})
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]any{"project": map[string]any{"id": "one", "metadata": map[string]any{"owner": "Alice", "members": []any{map[string]any{"name": "Bob"}, nil}}}}
	if !reflect.DeepEqual(response.Data.AsMap(), want) {
		t.Fatalf("data=%v want=%v", response.Data.AsMap(), want)
	}
}

func TestProjectionFieldsValidation(t *testing.T) {
	columns := []storage.FieldPath{storage.MustFieldPath("metadata")}
	for _, test := range []struct {
		name   string
		fields *pb.ObjectSelection
		limit  int
	}{
		{"empty", &pb.ObjectSelection{}, 100},
		{"unselected_column", &pb.ObjectSelection{Fields: []*pb.SelectedField{selectionField("other", nil)}}, 100},
		{"empty_kind", &pb.ObjectSelection{Fields: []*pb.SelectedField{selectionField("metadata", &pb.ValueSelection{})}}, 100},
		{"false_whole", &pb.ObjectSelection{Fields: []*pb.SelectedField{selectionField("metadata", &pb.ValueSelection{Kind: &pb.ValueSelection_Whole{Whole: false}})}}, 100},
		{"empty_object", &pb.ObjectSelection{Fields: []*pb.SelectedField{selectionField("metadata", selectionObject())}}, 100},
		{"limit", &pb.ObjectSelection{Fields: []*pb.SelectedField{selectionField("metadata", selectionObject(selectionField("owner", nil)))}}, 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := projectionFields(test.fields, columns, test.limit); err == nil {
				t.Fatal("invalid selection accepted")
			}
		})
	}
	if _, _, err := storageQuery(&pb.Query{Schema: "app", Table: "rows", Columns: []*pb.FieldPath{{Segments: []string{"a", "b"}}}}, 1, 8, 64); err == nil {
		t.Fatal("deep selected path bypassed the selection depth bound")
	}
}

func TestArraySelectionSuppressesUnusedFieldsButPublishesOrderAndLength(t *testing.T) {
	mask, err := projectionFields(&pb.ObjectSelection{Fields: []*pb.SelectedField{
		selectionField("items", &pb.ValueSelection{Kind: &pb.ValueSelection_Elements{Elements: selectionObject(selectionField("name", nil))}}),
	}}, []storage.FieldPath{storage.MustFieldPath("items")}, 100)
	if err != nil {
		t.Fatal(err)
	}
	text := func(value string) storage.Value {
		return storage.Value{Type: storage.TypeText, State: storage.ValuePresent, Data: []byte(value)}
	}
	item := func(name string) storage.Value {
		return storage.Value{Type: storage.TypeObject, State: storage.ValuePresent, Object: storage.Record{"name": text(name), "extra": text("hidden")}}
	}
	record := storage.Record{"items": {Type: storage.TypeList, State: storage.ValuePresent, List: []storage.Value{item("A"), item("B")}}}
	m := newConnectionModel()
	key := modelKey(snapshotTestRelation(), "one")
	apply := func(revision uint64) *pb.ModelBatch {
		t.Helper()
		events := []proj.ProjectionEvent{}
		if revision == 1 {
			events = append(events, proj.ProjectionEvent{Type: proj.ProjectionInstanceOpened, Node: "items", InstanceID: "root", EntityNode: true})
		}
		events = append(events, proj.ProjectionEvent{Type: proj.ProjectionRowChanged, Node: "items", InstanceID: "root", EntityNode: true, Kind: q.Updated, Key: key, Record: record, Version: q.RecordVersion{Revision: revision}})
		batch, err := m.applyProjection("scope", proj.ProjectionBatch{Events: events}, map[string]*fieldSelection{"items": mask})
		if err != nil {
			t.Fatal(err)
		}
		return batch
	}
	if len(apply(1).Entities) != 1 {
		t.Fatal("missing initial image")
	}
	record["items"].List[0].Object["extra"] = text("changed")
	if len(apply(2).Entities) != 0 {
		t.Fatal("unselected array element field emitted an update")
	}
	record["items"].List[0], record["items"].List[1] = record["items"].List[1], record["items"].List[0]
	batch := apply(3)
	if len(batch.Entities) != 1 {
		t.Fatal("reordering selected elements was suppressed")
	}
	values := batch.Entities[0].GetUpsert().Values["items"].GetListValue().Values
	if values[0].GetObjectValue().Fields["name"].GetTextValue() != "B" || len(values[0].GetObjectValue().Fields) != 1 {
		t.Fatal("array selection or ordering changed")
	}
	value := record["items"]
	value.List = value.List[:1]
	record["items"] = value
	if len(apply(4).Entities) != 1 {
		t.Fatal("array length change was suppressed")
	}
}
