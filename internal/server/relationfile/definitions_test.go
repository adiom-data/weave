package relationfile

import (
	"os"
	"path/filepath"
	"testing"

	relationdef "github.com/adiom-data/weave/pkg/relation"
	"github.com/adiom-data/weave/pkg/storage"
)

func writeTestFile(t *testing.T, name, contents string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, []byte(contents), 0600); err != nil {
		t.Fatal(err)
	}
	return path
}
func TestRelationsFromYAML(t *testing.T) {
	path := writeTestFile(t, "policies.yaml", `
relations:
  - schema: app
    table: projects
    facts:
      - id: grants
        projection:
          name: grants
          kind: list
          relation: {schema: app, table: grants}
          filter: {field: [deleted_at], operator: is_null}
          bindings:
            - field: [project_id]
              operator: eq
              parameter: [item, id]
          children:
            - name: user
              kind: entity
              relation: {schema: app, table: users}
              bindings:
                - field: [id]
                  operator: eq
                  parent: [user_id]
    receive: []
    computed:
      _canView:
        type: bool
        expression: item.deleted_at == null && item.organization_id == claims.organization_id
        propagate:
          - to: {schema: app, table: project_tasks}
            when: parent.id == item.project_id
    visibility: computed._canView
  - schema: app
    table: project_tasks
    visibility: has(inherited._canView) && inherited._canView
`)
	if _, err := Load(path, relationdef.DefaultLimits()); err != nil {
		t.Fatal(err)
	}
	definitions, err := loadDefinitions(path)
	if err != nil {
		t.Fatal(err)
	}
	s := definitions[storage.RelationName{Namespace: "app", Name: "projects"}]
	if s.Receive == nil || len(s.Receive) != 0 || len(s.Facts) != 1 || len(s.Computed["_canView"].Propagate) != 1 {
		t.Fatalf("lost policy structure: %+v", s)
	}
	if s.Visibility != "computed._canView" || s.Computed["_canView"].Expression != "item.deleted_at == null && item.organization_id == claims.organization_id" {
		t.Fatalf("lost expressions: %+v", s)
	}
	if s.Facts[0].Projection.Bindings[0].Parameter != storage.MustFieldPath("item", "id") {
		t.Fatal("lost parameter path")
	}
	s = definitions[storage.RelationName{Namespace: "app", Name: "project_tasks"}]
	if s.Receive != nil {
		t.Fatal("omitted receive must remain unrestricted")
	}
}

func TestComputedRelationDefinitionsFromYAML(t *testing.T) {
	path := writeTestFile(t, "relations.yaml", `
relations:
  - schema: app
    table: projects
    computed:
      _role:
        type: integer
        expression: '2'
        propagate:
          - to: {schema: app, table: tasks}
            when: parent.id == item.project_id
  - schema: app
    table: tasks
    receive:
      - from: {schema: app, table: projects}
        field: _role
    computed:
      canEdit:
        type: bool
        expression: 'has(inherited._role) && inherited._role >= 2'
    visibility: has(inherited._role) && inherited._role >= 1
`)
	if _, err := Load(path, relationdef.DefaultLimits()); err != nil {
		t.Fatal(err)
	}
	definitions, err := loadDefinitions(path)
	if err != nil {
		t.Fatal(err)
	}
	definition := definitions[storage.RelationName{Namespace: "app", Name: "projects"}]
	if definition.Visibility != "" || definition.Computed["_role"].Propagate[0].To.Name != "tasks" {
		t.Fatalf("definition: %+v", definition)
	}
	definition = definitions[storage.RelationName{Namespace: "app", Name: "tasks"}]
	if definition.Receive[0].Field != "_role" || definition.Visibility != "has(inherited._role) && inherited._role >= 1" {
		t.Fatalf("definition: %+v", definition)
	}
}

func TestRejectBrokenDefinitions(t *testing.T) {
	for _, raw := range []string{
		"relations: [{schema: app, table: projects, visibility: 'item.('}]\n",
		"relations: [{schema: app, table: projects}, {schema: app, table: projects}]\n",
		"relations: [{schema: app, table: projects, typo: true}]\n",
		"relations: [{schema: app, table: projects, visibility: {allow: [{id: old, when: 'true'}]}}]\n",
		"relations: [{schema: app, table: projects, visibility: '42'}]\n",
		"relations: [{schema: app, table: projects, computed: {role: {type: integer, expression: '1', receive: []}}}]\n",
		"relations: [{schema: app, table: projects, computed: {role: {type: integer, expression: '1', propagate: [{id: old, to: {schema: app, table: projects}, when: 'true'}]}}}]\n",
		"relations: [{schema: app, table: projects, computed: {role: {type: integer, expression: '1', propagate: [{field: renamed, to: {schema: app, table: projects}, when: 'true'}]}}}]\n",
		"relations: [{schema: app, table: projects, facts: [{id: f, projection: {name: f, kind: list, relation: {schema: app, table: grants}, bindings: [{field: [id], operator: eq, parent: [id], parameter: [item, id]}]}}]}]\n",
	} {
		if _, err := Load(writeTestFile(t, "bad.yaml", raw), relationdef.DefaultLimits()); err == nil {
			t.Fatalf("accepted broken policy: %s", raw)
		}
	}
}
