package projectioncompiler

import (
	"fmt"
	"reflect"
	"strings"
	"testing"

	reactivev1 "github.com/adiom-data/weave/pkg/gen/reactive/v1"
	"github.com/vektah/gqlparser/v2/ast"
)

const testSchema = `
type Query { project(id: UUID!): Project @bind(argument:"id",field:"id") }
type Project @table(schema:"app",name:"projects",softDelete:"deletedAt") {
  id: UUID!
  title: String!
  deletedAt: DateTime @column(path:["deleted_at"])
  tasks: [Task!]! @relation(parent:["id"],child:["projectId"])
}
type Task @table(schema:"app",name:"tasks") {
  id: UUID!
  projectId: UUID! @column(path:["project_id"])
  title: String!
  completed: Boolean!
  position: Decimal!
  meta: Metadata @column(path:["metadata"])
  children: [Task!]! @relation(parent:["id"],child:["projectId"])
}
type Metadata { labels: [String!]! owner: String @column(path:["odd.name"]) }
`

func compileTest(t *testing.T, schema, document string) ([]Operation, error) {
	t.Helper()
	c, err := New(&ast.Source{Name: "schema.graphql", Input: schema})
	if err != nil {
		return nil, err
	}
	return c.Compile(&ast.Source{Name: "projection.graphql", Input: document})
}

func TestCompileReferenceFetchHint(t *testing.T) {
	schema := strings.Replace(testSchema, "  children: [Task!]!", "  parent: Task @relation(parent:[\"projectId\"],child:[\"id\"])\n  children: [Task!]!", 1)
	ops, err := compileTest(t, schema, `query Details($id: UUID!) {
	  project(id:$id) { id tasks @window(initialCount:2) @fetchReference(child:"owner") { id owner:parent { id children @window(initialCount:1) { id } } } }
	}`)
	if err != nil {
		t.Fatal(err)
	}
	list := ops[0].Root.Children[0]
	if list.GetList().FetchReference != "project.tasks.owner" || list.Children[0].NodeId != list.GetList().FetchReference || len(list.Children[0].Children) != 1 {
		t.Fatal("hint changed projection topology or lost its immediate child alias")
	}
	for _, document := range []string{
		`query Details($id: UUID!) { project(id:$id) @fetchReference(child:"tasks") { id } }`,
		`query Details($id: UUID!) { project(id:$id) { id tasks @window(initialCount:2) @fetchReference(child:"absent") { id } } }`,
		`query Details($id: UUID!) { project(id:$id) { id tasks @window(initialCount:2) @fetchReference(child:"children") { id children @window(initialCount:1) { id } } } }`,
	} {
		if _, err := compileTest(t, schema, document); err == nil {
			t.Fatalf("invalid fetch hint accepted: %s", document)
		}
	}
}

func TestEmbeddedMasksUseStoredNamesAndTraverseArrays(t *testing.T) {
	schema := strings.Replace(testSchema, "type Metadata {", "type Member { name: String secret: String }\ntype Metadata { members: [[Member]]", 1)
	ops, err := compileTest(t, schema, `query Details($id: UUID!) { project(id:$id) { id tasks @window(initialCount:1) { id first:meta { owner } second:meta { members { display:name } } } } }`)
	if err != nil {
		t.Fatal(err)
	}
	node := ops[0].Root.Children[0]
	if len(node.Fields.GetFields()) != 3 {
		t.Fatalf("output fields=%v", node.Fields)
	}
	fields := node.Fields.Fields
	if fields[0].Path.Segments[0] != "id" || fields[0].Selection != nil {
		t.Fatal("scalar was not selected whole")
	}
	if fields[1].Path.Segments[0] != "metadata" || fields[1].Selection.GetObject().Fields[0].Path.Segments[0] != "odd.name" {
		t.Fatal("output mask used application aliases instead of stored names")
	}
	member := fields[2].Selection.GetObject().Fields[0].Selection.GetElements().GetElements().GetObject()
	if member == nil || member.Fields[0].Path.Segments[0] != "name" {
		t.Fatal("nested arrays lost selection or retained display alias")
	}
	if node.GetList().Query.Columns[1].Segments[0] != "metadata" || len(node.GetList().Query.Columns) != 2 {
		t.Fatal("publication mask changed storage paths")
	}
}

func TestAllFieldsControlsPublicationWithoutChangingTypedShape(t *testing.T) {
	ops, err := compileTest(t, testSchema, `query Details($id:UUID!) {
  project(id:$id) @allFields {
    tasks @window(initialCount:2) @allFields {
      first:meta { owner }
      children @window(initialCount:1) { title }
    }
  }
}`)
	if err != nil {
		t.Fatal(err)
	}
	root := ops[0].Root
	tasks := root.Children[0]
	for _, node := range []*reactivev1.ProjectionNode{root, tasks} {
		var query *reactivev1.Query
		if node.GetEntity() != nil {
			query = node.GetEntity().Query
		} else {
			query = node.GetList().Query
		}
		if !node.AllFields || node.Fields != nil || len(query.Columns) != 0 {
			t.Fatalf("invalid whole-record publication: %v", node)
		}
	}
	child := tasks.Children[0]
	if child.AllFields || len(child.GetList().Query.Columns) != 1 || child.GetList().Query.Columns[0].Segments[0] != "title" {
		t.Fatal("parent allFields changed child selection")
	}
	shape := ops[0].Shape
	if len(shape.Fields) != 0 || len(shape.Children) != 1 || len(shape.Children["tasks"].Fields) != 1 || shape.Children["tasks"].Fields[0].Name != "first" {
		t.Fatalf("directive widened typed shape: %+v", shape)
	}
	core, _, err := GenerateTypeScript(ops, "@adiom-data/weave", "./index.js")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(core), `"allFields":true`) {
		t.Fatal("generated request lost allFields")
	}
	for _, doc := range []string{
		`query X($id:UUID!){project(id:$id){title @allFields}}`,
		`query X($id:UUID!){project(id:$id){id tasks @window(initialCount:1){meta @allFields { owner }}}}`,
		`query X($id:UUID!){project(id:$id) @allFields(extra:true){title}}`,
		`query X($id:UUID!){project(id:$id) @allFields { tasks @allFields { title } }}`,
	} {
		if _, err := compileTest(t, testSchema, doc); err == nil {
			t.Fatalf("invalid directive accepted: %s", doc)
		}
	}
}

func TestCompileNestedWindowsAliasesFragmentsAndFilters(t *testing.T) {
	ops, err := compileTest(t, testSchema, `
query Details($id: UUID!) {
  current: project(id:$id) {
    title
    tasks @window(initialCount:10) @orderBy(terms:[{field:"position",descending:true}])
      @where(predicate:{and:[{field:"completed",op:EQUAL,value:false},{not:{field:"title",isNull:true}}]}) {
      ...TaskFields
      children @window(initialCount:3,from:END) { id }
    }
  }
}
fragment TaskFields on Task { id title position meta { labels owner } }
`)
	if err != nil {
		t.Fatal(err)
	}
	root := ops[0].Root
	if root.NodeId != "current" || root.GetEntity().Bindings[0].GetParameter() != "id" || root.GetEntity().Query.Filter.GetNullTest().Column.Segments[0] != "deleted_at" {
		t.Fatalf("root = %v", root)
	}
	tasks := root.Children[0]
	if tasks.NodeId != "current.tasks" || tasks.GetList().Window.Count != 10 || tasks.GetList().Query.Order[0].Descending != true {
		t.Fatalf("tasks = %v", tasks)
	}
	if tasks.GetList().Bindings[0].Column.Segments[0] != "project_id" || tasks.GetList().Bindings[0].GetParentColumn().Segments[0] != "id" {
		t.Fatal("relationship binding was not mapped")
	}
	if len(tasks.GetList().Query.Filter.GetJunction().Operands) != 2 || tasks.Children[0].GetList().Window.Count != 3 {
		t.Fatal("nested filters/windows lost")
	}
	shape := ops[0].Shape.Children["tasks"]
	if shape.Fields[2].Type.Kind != "decimal" || shape.Fields[3].Type.Kind != "object" || shape.Fields[3].Type.Fields[1].Path[0] != "odd.name" {
		t.Fatalf("shape = %+v", shape)
	}
}

func TestRepeatedFragmentFieldsMergeWithoutDuplicateNodes(t *testing.T) {
	ops, err := compileTest(t, testSchema, `query Details($id:UUID!){project(id:$id){...A ...A title}} fragment A on Project {tasks @window(initialCount:1){id}}`)
	if err != nil {
		t.Fatal(err)
	}
	if len(ops[0].Root.Children) != 1 || len(ops[0].Shape.Fields) != 1 {
		t.Fatal("repeated selections were duplicated")
	}
}

func TestCollectSharedFragmentsOnce(t *testing.T) {
	leaf := &ast.Field{Name: "tasks", Alias: "tasks", SelectionSet: ast.SelectionSet{&ast.Field{Name: "id", Alias: "id"}}}
	fragment := &ast.FragmentDefinition{Name: "Leaf", TypeCondition: "Project", SelectionSet: ast.SelectionSet{leaf}}
	for i := 0; i < 20; i++ {
		fragment = &ast.FragmentDefinition{Name: fmt.Sprintf("F%d", i), TypeCondition: "Project", SelectionSet: ast.SelectionSet{
			&ast.FragmentSpread{Definition: fragment}, &ast.FragmentSpread{Definition: fragment},
		}}
	}
	fields, err := collectFields(ast.SelectionSet{&ast.FragmentSpread{Definition: fragment}}, "Project")
	if err != nil {
		t.Fatal(err)
	}
	if len(fields) != 1 || len(fields[0].SelectionSet) != 1 {
		t.Fatal("shared fragments duplicated nested selections")
	}
	if len(leaf.SelectionSet) != 1 {
		t.Fatal("collection mutated the source fragment")
	}
}

func TestSharedFragmentsRemainAvailableAcrossOccurrences(t *testing.T) {
	ops, err := compileTest(t, testSchema, `
query Shared($id:UUID!) {
  project(id:$id) {
    title
    first:tasks @window(initialCount:1) { ...TaskFields }
    second:tasks @window(initialCount:2) {
      ...TaskFields
      children @window(initialCount:1) { ...TaskFields }
    }
  }
}
fragment TaskFields on Task { id title }
`)
	if err != nil {
		t.Fatal(err)
	}
	root := ops[0].Shape
	for _, shape := range []*Shape{root.Children["first"], root.Children["second"], root.Children["second"].Children["children"]} {
		if shape == nil || len(shape.Fields) != 2 || shape.Fields[0].Name != "id" || shape.Fields[1].Name != "title" {
			t.Fatalf("fragment fields lost in occurrence: %+v", shape)
		}
	}
}

func TestAliasesShareStorageColumnsButRetainIndependentShapes(t *testing.T) {
	schema := strings.Replace(testSchema, "  title: String!", "  title: String!\n  displayTitle: String! @column(path:[\"title\"])", 1)
	schema = strings.Replace(schema, "  meta: Metadata", "  flat: String @column(path:[\"a.b\"])\n  nested: String @column(path:[\"a\",\"b\"])\n  meta: Metadata", 1)
	ops, err := compileTest(t, schema, `
query Aliases($id:UUID!) {
  project(id:$id) {
    first:title second:title third:displayTitle
    tasks @window(initialCount:1) {
      labels:meta { labels }
      owner:meta { owner }
      flat nested
    }
  }
}`)
	if err != nil {
		t.Fatal(err)
	}
	root := ops[0].Root
	if columns := root.GetEntity().Query.Columns; len(columns) != 1 || !reflect.DeepEqual(columns[0].Segments, []string{"title"}) {
		t.Fatalf("root columns = %v, want title once", columns)
	}
	columns := root.Children[0].GetList().Query.Columns
	wantPaths := [][]string{{"metadata"}, {"a.b"}, {"a", "b"}}
	if len(columns) != len(wantPaths) {
		t.Fatalf("list columns = %v, want %v", columns, wantPaths)
	}
	for i, want := range wantPaths {
		if !reflect.DeepEqual(columns[i].Segments, want) {
			t.Fatalf("column %d = %v, want %v", i, columns[i].Segments, want)
		}
	}
	for i, name := range []string{"first", "second", "third"} {
		if fields := ops[0].Shape.Fields; len(fields) != 3 || fields[i].Name != name || !reflect.DeepEqual(fields[i].Path, []string{"title"}) {
			t.Fatalf("root shape fields = %+v", fields)
		}
	}
	fields := ops[0].Shape.Children["tasks"].Fields
	if len(fields) != 4 || fields[0].Name != "labels" || fields[0].Type.Fields[0].Name != "labels" || fields[1].Name != "owner" || fields[1].Type.Fields[0].Name != "owner" {
		t.Fatalf("embedded aliases lost independent selections: %+v", fields)
	}
}

func TestRejectInvalidOrUnsupportedDefinitions(t *testing.T) {
	tests := []struct{ name, schema, document, want string }{
		{"unknown field", testSchema, `query X{project(id:"10000000-0000-4000-8000-000000000001"){missing}}`, "Cannot query field"},
		{"missing window", testSchema, `query X($id:UUID!){project(id:$id){tasks{id}}}`, "requires @window"},
		{"relationship only", testSchema, `query X($id:UUID!){project(id:$id){tasks @window(initialCount:1){id}}}`, "select at least one stored field"},
		{"wrong parameter type", testSchema, `query X($id:String!){project(id:$id){id}}`, "used in position expecting"},
		{"nullable parameter", testSchema, `query X($id:UUID = "10000000-0000-4000-8000-000000000001"){project(id:$id){id}}`, "required scalar"},
		{"bad relationship", strings.ReplaceAll(testSchema, `child:["projectId"]`, `child:["title"]`), `query X{project(id:"10000000-0000-4000-8000-000000000001"){id}}`, "binding types differ"},
		{"wrong filter type", testSchema, `query X($id:UUID!){project(id:$id){tasks @window(initialCount:2) @where(predicate:{field:"completed",op:EQUAL,value:"false"}){id}}}`, "Boolean literal"},
		{"unknown filter field", testSchema, `query X($id:UUID!){project(id:$id){tasks @window(initialCount:2) @where(predicate:{field:"missing",isNull:true}){id}}}`, "not a stored field"},
		{"conditional fields", testSchema, `query X($id:UUID!){project(id:$id){title @skip(if:true)}}`, "cannot have arguments or directives"},
		{"conditional repeated fragment", testSchema, `query X($id:UUID!){project(id:$id){...Fields ...Fields @skip(if:true)}} fragment Fields on Project {title}`, "fragment directives are not supported"},
		{"multiple roots", testSchema, `query X($id:UUID!){a:project(id:$id){id} b:project(id:$id){id}}`, "exactly one root"},
		{"reserved alias", testSchema, `query X($id:UUID!){project(id:$id){__proto__:title}}`, "reserved"},
		{"empty window", testSchema, `query X($id:UUID!){project(id:$id){tasks @window(initialCount:0){id}}}`, "must be positive"},
		{"conflicting windows", testSchema, `query X($id:UUID!){project(id:$id){tasks @window(initialCount:1){id} tasks @window(initialCount:2){title}}}`, "identical reactive directives"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := compileTest(t, test.schema, test.document)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestPreciseNumericLiteralsAndEveryCanonicalScalar(t *testing.T) {
	schema := `type Query { rows: [Row!]! } type Row @table(schema:"app",name:"rows") {
 id: Int64! amount: Decimal! enabled: Boolean! small: Int! finite: Float! number: Float64!
 text: String! bytes: Bytes! uuid: UUID! instant: DateTime! structured: StructuredValue! values: [Int64!]
}`
	ops, err := compileTest(t, schema, `query Scalars{rows @window(initialCount:1) @where(predicate:{field:"amount",op:EQUAL,value:12345678901234567890.123456789}){id amount enabled small finite number text bytes uuid instant structured values}}`)
	if err != nil {
		t.Fatal(err)
	}
	value := ops[0].Root.GetList().Query.Filter.GetComparison().Value.GetDecimalValue()
	if value != "1.2345678901234567890123456789e19" {
		t.Fatalf("decimal lost precision: %s", value)
	}
	core, _, err := GenerateTypeScript(ops, "@adiom-data/weave", "./index.js")
	if err != nil {
		t.Fatal(err)
	}
	for _, expected := range []string{`"id": bigint`, `"amount": string`, `"enabled": boolean`, `"small": number`, `"structured": $runtime.Value`, `ReadonlyArray<bigint>`} {
		if !strings.Contains(string(core), expected) {
			t.Fatalf("missing generated type %s", expected)
		}
	}
}

func TestGeneratedImportsCannotCollideWithOperationOrHookNames(t *testing.T) {
	var document strings.Builder
	for _, name := range []string{"runtime", "ReactiveClient", "ProjectionOptions", "TypedProjection", "useTypedProjection", "Foo", "useFoo", "typeofQuery", "voidQuery"} {
		document.WriteString("query " + name + " { project(id: \"10000000-0000-4000-8000-000000000001\") { id } }\n")
	}
	operations, err := compileTest(t, testSchema, document.String())
	if err != nil {
		t.Fatal(err)
	}
	core, react, err := GenerateTypeScript(operations, "@adiom-data/weave", "./index.js")
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"import * as $runtime", "export const runtime = $runtime.defineProjection", "$runtime.ProjectedEntity"} {
		if !strings.Contains(string(core), want) {
			t.Fatalf("missing collision-safe output %q", want)
		}
	}
	for _, want := range []string{"useTypedProjection as $useTypedProjection", "ReactiveClient as $ReactiveClient", "import * as $projections", "export function useTypedProjection(", "$projections.ReactiveClient"} {
		if !strings.Contains(string(react), want) {
			t.Fatalf("missing collision-safe React output %q", want)
		}
	}
	// Real collisions between public generated declarations still fail clearly.
	operations[1].Name = "runtimeData"
	if _, _, err := GenerateTypeScript(operations, "@adiom-data/weave", "./index.js"); err == nil {
		t.Fatal("public symbol collision was accepted")
	}
}

func TestRejectReservedOperationNames(t *testing.T) {
	// Keep this exhaustive independently of the production lookup table, so
	// missing keywords cannot silently generate syntactically invalid bindings.
	for _, name := range strings.Fields(`
arguments eval await break case catch class const continue debugger default
delete do else enum export extends false finally for function if import in
instanceof new null return super switch this throw true try typeof var void
while with yield implements interface let package private protected public static
`) {
		t.Run(name, func(t *testing.T) {
			operations, err := compileTest(t, testSchema, `query `+name+`($id:UUID!){project(id:$id){title}}`)
			if err != nil {
				t.Fatal(err)
			}
			if _, _, err := GenerateTypeScript(operations, "@adiom-data/weave", "./index.js"); err == nil || !strings.Contains(err.Error(), "reserved name: "+name) {
				t.Fatalf("reserved binding %s: error = %v", name, err)
			}
		})
	}
}
