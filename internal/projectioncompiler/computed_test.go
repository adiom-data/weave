package projectioncompiler

import (
	"strings"
	"testing"
)

func TestComputedSelectionsStayOutsideStoredQueries(t *testing.T) {
	schema := strings.Replace(testSchema, "  title: String!", "  title: String!\n  role: Int64! @computed\n  canEdit: Boolean! @computed(name:\"can_edit\")", 1)
	ops, err := compileTest(t, schema, `query Details($id:UUID!) { project(id:$id) { permission:role canEdit } }`)
	if err != nil {
		t.Fatal(err)
	}
	root := ops[0].Root
	if len(root.GetEntity().Query.Columns) != 0 || len(root.Computed) != 2 || root.Computed[0] != "role" || root.Computed[1] != "can_edit" {
		t.Fatalf("request: %v", root)
	}
	fields := ops[0].Shape.Fields
	if len(fields) != 2 || !fields[0].Computed || fields[0].Name != "permission" || fields[0].Path[0] != "role" || !fields[1].Computed {
		t.Fatalf("shape: %+v", fields)
	}
	code, _, err := GenerateTypeScript(ops, "@adiom-data/weave", "./index.js")
	if err != nil || !strings.Contains(string(code), `"computed": true`) {
		t.Fatalf("code=%s err=%v", code, err)
	}
	if !strings.Contains(string(code), `$runtime.ProjectionFields<{  }, { readonly "permission": bigint; readonly "canEdit": boolean }>`) {
		t.Fatalf("generated row does not classify stored/computed field types: %s", code)
	}
	for _, doc := range []string{
		`query X($id:UUID!) { project(id:$id) @orderBy(terms:[{field:"role"}]) { id } }`,
		`query X($id:UUID!) { project(id:$id) @where(predicate:{field:"role",op:EQUAL,value:1}) { id } }`,
	} {
		if _, err := compileTest(t, schema, doc); err == nil {
			t.Fatalf("computed query field accepted: %s", doc)
		}
	}
}

func TestPrivateComputedValuesCannotBeExposedBySchemasOrAliases(t *testing.T) {
	for _, declaration := range []string{
		`_role: Int64! @computed`,
		`publicRole: Int64! @computed(name:"_role")`,
		`publicRole: Boolean! @computed(name:"$visibility")`,
	} {
		schema := strings.Replace(testSchema, "  title: String!", "  title: String!\n  "+declaration, 1)
		if _, err := compileTest(t, schema, `query X($id:UUID!) { project(id:$id) { id } }`); err == nil || !strings.Contains(err.Error(), "private computed") {
			t.Fatalf("schema exposed private computation %s: %v", declaration, err)
		}
	}
	// Privacy applies to computations, not application-defined column names.
	schema := strings.Replace(testSchema, "  title: String!", "  title: String!\n  _stored: String", 1)
	if _, err := compileTest(t, schema, `query X($id:UUID!) { project(id:$id) { _stored } }`); err != nil {
		t.Fatal(err)
	}
}
