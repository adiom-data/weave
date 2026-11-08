package policyfile

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/adiom-data/weave/pkg/accesscontrol/policy"
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
func TestPoliciesFromYAML(t *testing.T) {
	path := writeTestFile(t, "policies.yaml", `
relations:
  - schema: app
    table: projects
    receive: []
    require:
      - id: active
        when: item.deleted_at == null
    allow:
      - id: organization
        when: item.organization_id == claims.organization_id
    propagate:
      - id: tasks
        to: {schema: app, table: project_tasks}
        when: parent.id == item.project_id
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
  - schema: app
    table: project_tasks
    allow: []
`)
	provider, err := Load(path, policy.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	s, err := provider.GetPolicy(t.Context(), storage.RelationName{Namespace: "app", Name: "projects"})
	if err != nil {
		t.Fatal(err)
	}
	if s.Policy.Receive == nil || len(s.Policy.Receive) != 0 || len(s.Policy.Facts) != 1 || len(s.Policy.Propagate) != 1 {
		t.Fatalf("lost policy structure: %+v", s.Policy)
	}
	if len(s.Policy.Require) != 1 || s.Policy.Require[0] != (policy.Rule{ID: "active", When: "item.deleted_at == null"}) {
		t.Fatalf("lost requirement: %+v", s.Policy.Require)
	}
	if s.Policy.Facts[0].Projection.Bindings[0].Parameter != storage.MustFieldPath("item", "id") {
		t.Fatal("lost parameter path")
	}
	s, err = provider.GetPolicy(t.Context(), storage.RelationName{Namespace: "app", Name: "project_tasks"})
	if err != nil {
		t.Fatal(err)
	}
	if s.Policy.Receive != nil {
		t.Fatal("omitted receive must remain unrestricted")
	}
}

func TestRejectBrokenPolicies(t *testing.T) {
	for _, raw := range []string{
		"relations: [{schema: app, table: projects, allow: [{id: broken, when: 'item.('}]}]",
		"relations: [{schema: app, table: projects}, {schema: app, table: projects}]",
		"relations: [{schema: app, table: projects, typo: true}]",
		"relations: [{schema: app, table: projects, deny: [{id: removed, when: 'true'}]}]",
		"relations: [{schema: app, table: projects, require: [{id: broken, when: '42'}]}]",
		"relations: [{schema: app, table: projects, facts: [{id: f, projection: {name: f, kind: list, relation: {schema: app, table: grants}, bindings: [{field: [id], operator: eq, parent: [id], parameter: [item, id]}]}}]}]",
	} {
		if _, err := Load(writeTestFile(t, "bad.yaml", raw), policy.DefaultLimits()); err == nil {
			t.Fatalf("accepted broken policy: %s", raw)
		}
	}
}
