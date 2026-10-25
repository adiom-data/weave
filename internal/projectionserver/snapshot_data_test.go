package projectionserver

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	proj "github.com/adiom-data/weave/internal/projection"
	q "github.com/adiom-data/weave/internal/querysubscription/model"
	"github.com/adiom-data/weave/internal/storage"
	pb "github.com/adiom-data/weave/pkg/gen/reactive/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Traverse response objects, excluding the outer data envelope. Useful for
// integration fixtures whose rows contain only scalar stored fields.
func snapshotObjects(data *structpb.Struct) []*structpb.Struct {
	var out []*structpb.Struct
	var visit func(*structpb.Value)
	visit = func(value *structpb.Value) {
		if object := value.GetStructValue(); object != nil {
			out = append(out, object)
			for _, child := range object.Fields {
				visit(child)
			}
		}
		if list := value.GetListValue(); list != nil {
			for _, child := range list.Values {
				visit(child)
			}
		}
	}
	for _, value := range data.Fields {
		visit(value)
	}
	return out
}

func snapshotObjectCount(data *structpb.Struct) int { return len(snapshotObjects(data)) }

func TestSnapshotDataRendersSelectionsPerDiamondOccurrence(t *testing.T) {
	m := newConnectionModel()
	key := modelKey(snapshotTestRelation(), "shared")
	entityID := publicEntityID(key)
	m.entities[entityID] = &modelEntity{hasImage: true, record: modelRecord("shared", "1", "Title")}
	m.occurrences["root"] = modelOccurrence{node: "project", entityID: entityID, selected: selectColumns([]storage.FieldPath{storage.MustFieldPath("title")})}
	m.occurrences["child"] = modelOccurrence{node: "same", parentOccurrenceID: "root", parentEntityID: entityID, entityID: entityID, selected: selectColumns([]storage.FieldPath{storage.MustFieldPath("position")})}
	root := &pb.ProjectionNode{NodeId: "project", Selection: &pb.ProjectionNode_Entity{Entity: &pb.ProjectionEntity{}}, Children: []*pb.ProjectionNode{
		{NodeId: "same", Selection: &pb.ProjectionNode_Entity{Entity: &pb.ProjectionEntity{}}},
		{NodeId: "missing", Selection: &pb.ProjectionNode_Entity{Entity: &pb.ProjectionEntity{}}},
		{NodeId: "empty", Selection: &pb.ProjectionNode_List{List: &pb.ProjectionList{}}},
	}}
	data, err := snapshotData(m, root)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]any{"project": map[string]any{"title": "Title", "same": map[string]any{"position": "1"}, "missing": nil, "empty": []any{}}}
	if !reflect.DeepEqual(data.AsMap(), want) {
		t.Fatalf("data=%#v want=%#v", data.AsMap(), want)
	}
	// A one-shot response cannot silently claim that unresolved data is complete.
	m.entities[entityID].pending = true
	if _, err := snapshotData(m, root); err == nil {
		t.Fatal("pending image returned as successful data")
	}
	// Fields and child expansions cannot silently overwrite one another.
	m.entities[entityID].pending = false
	root.Children[0].NodeId = "title"
	if _, err := snapshotData(m, root); err == nil {
		t.Fatal("field/expansion collision accepted")
	}
}

func TestSnapshotResponseUsesPlainJSONAndExactScalars(t *testing.T) {
	request := snapshotTestRequest(false, false)
	events := []proj.ProjectionEvent{
		{Type: proj.ProjectionInstanceOpened, Node: "projects", InstanceID: "root", Bounds: snapshotTestBounds()},
		{Type: proj.ProjectionRowChanged, Node: "projects", InstanceID: "root", Kind: q.Added, Key: modelKey(snapshotTestRelation(), "one"), Record: storage.Record{
			"large":  {Type: storage.TypeInteger, State: storage.ValuePresent, Data: []byte("9223372036854775807")},
			"secret": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("not selected")},
		}},
	}
	response, err := buildSnapshotResponse(request, "test", events, nil, map[string]*fieldSelection{"projects": selectColumns([]storage.FieldPath{storage.MustFieldPath("large")})})
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := protojson.Marshal(response)
	if err != nil {
		t.Fatal(err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	want := map[string]any{"data": map[string]any{"projects": []any{map[string]any{"large": "9223372036854775807"}}}, "extensions": map[string]any{}}
	if !reflect.DeepEqual(decoded, want) {
		t.Fatalf("JSON=%s", encoded)
	}
	for _, test := range []struct {
		value *pb.Value
		want  any
	}{
		{&pb.Value{Kind: &pb.Value_DecimalValue{DecimalValue: "123.4567890123456789"}}, "123.4567890123456789"},
		{&pb.Value{Kind: &pb.Value_FloatValue{FloatValue: "NaN"}}, "NaN"},
		{&pb.Value{Kind: &pb.Value_FloatValue{FloatValue: "1.25"}}, 1.25},
		{&pb.Value{Kind: &pb.Value_BytesValue{BytesValue: []byte{0, 255}}}, "AP8="},
		{&pb.Value{Kind: &pb.Value_DatetimeValue{DatetimeValue: timestamppb.New(time.Date(2026, 11, 1, 0, 0, 0, 123000, time.UTC))}}, "2026-11-01T00:00:00.000123Z"},
	} {
		got, err := snapshotValue(test.value)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got.AsInterface(), test.want) {
			t.Fatalf("got=%v want=%v", got, test.want)
		}
	}
}

func TestSnapshotDataPreservesListOrderAndEmbeddedPaths(t *testing.T) {
	m := newConnectionModel()
	path := storage.MustFieldPath("metadata", "odd.name")
	keys := []storage.EntityKey{modelKey(snapshotTestRelation(), "b"), modelKey(snapshotTestRelation(), "a")}
	query := &modelQuery{selected: selectColumns([]storage.FieldPath{path}), ordered: keys, members: make(map[storage.EntityKey]struct{})}
	m.queries["list"] = query
	m.occurrences["list"] = modelOccurrence{node: "literal.node", queryID: "list"}
	for _, key := range keys {
		query.members[key] = struct{}{}
		m.entities[publicEntityID(key)] = &modelEntity{hasImage: true, record: storage.Record{"metadata": {
			Type: storage.TypeObject, State: storage.ValuePresent, Object: storage.Record{
				"odd.name": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte(key.Value)},
				"hidden":   {Type: storage.TypeBool, State: storage.ValuePresent, Data: []byte("true")},
			},
		}}}
	}
	data, err := snapshotData(m, &pb.ProjectionNode{NodeId: "literal.node", Selection: &pb.ProjectionNode_List{List: &pb.ProjectionList{}}})
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]any{"literal.node": []any{
		map[string]any{"metadata": map[string]any{"odd.name": "b"}},
		map[string]any{"metadata": map[string]any{"odd.name": "a"}},
	}}
	if !reflect.DeepEqual(data.AsMap(), want) {
		t.Fatalf("got=%v want=%v", data.AsMap(), want)
	}
	for _, key := range keys {
		if _, present := m.entities[publicEntityID(key)].record["metadata"].Object["hidden"]; !present {
			t.Fatal("rendering changed the source record")
		}
	}
}

func TestSnapshotLayoutUsesExactTopLevelOutputNames(t *testing.T) {
	request := snapshotTestRequest(false, true)
	request.Root.Children[0].NodeId = "metadata"
	for _, test := range []struct {
		name    string
		path    storage.FieldPath
		invalid bool
	}{
		{"nested", storage.MustFieldPath("metadata", "owner"), true},
		{"literal_dot", storage.MustFieldPath("metadata.owner"), false},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := validateSnapshotLayout(request.Root, map[string]*fieldSelection{"projects": selectColumns([]storage.FieldPath{test.path})})
			if test.invalid && status.Code(err) != codes.InvalidArgument || !test.invalid && err != nil {
				t.Fatalf("err=%v", err)
			}
		})
	}
}

func TestSnapshotSelectAllCollisionRetainsInvalidArgumentStatus(t *testing.T) {
	request := snapshotTestRequest(false, true)
	request.Root.Children[0].NodeId = "title"
	events := []proj.ProjectionEvent{
		{Type: proj.ProjectionInstanceOpened, Node: "projects", InstanceID: "root", Bounds: snapshotTestBounds()},
		{Type: proj.ProjectionRowChanged, Node: "projects", InstanceID: "root", Kind: q.Added, Key: modelKey(snapshotTestRelation(), "one"), Record: modelRecord("one", "1", "Title")},
	}
	response, err := buildSnapshotResponse(request, "test", events, nil, nil)
	if response != nil || status.Code(err) != codes.InvalidArgument {
		t.Fatalf("response=%v err=%v", response, err)
	}
}
