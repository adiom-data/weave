package policy

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/adiom-data/weave/internal/accesscontrol"
	"github.com/adiom-data/weave/internal/projection/plan"
	"github.com/adiom-data/weave/internal/storage"
)

var projects = storage.RelationName{Namespace: "demo", Name: "projects"}
var tasks = storage.RelationName{Namespace: "demo", Name: "tasks"}
var links = storage.RelationName{Namespace: "demo", Name: "project_tasks"}
var grants = storage.RelationName{Namespace: "demo", Name: "project_user_grants"}

func text(v string) storage.Value {
	return storage.Value{Type: storage.TypeText, State: storage.ValuePresent, Data: []byte(v)}
}
func candidate(relation storage.RelationName, id string, record storage.Record) accesscontrol.Candidate {
	return accesscontrol.Candidate{Key: storage.EntityKey{Relation: storage.RelationKey{Source: "db", Namespace: relation.Namespace, Name: relation.Name}, Value: id}, Record: record, Complete: true}
}
func compile(t *testing.T, definitions Map) *Evaluator {
	t.Helper()
	e, err := Compile(definitions, Limits{MaxNodes: 1000, MaxDepth: 50, MaxEvaluationSteps: 100000})
	if err != nil {
		t.Fatal(err)
	}
	return e
}
func grantFact() Fact {
	return Fact{ID: "grant", Projection: &plan.Node{Name: "grants", Query: storage.QuerySpec{Relation: grants}, Bindings: []plan.Binding{{Field: "project_id", Operator: storage.ComparisonEqual, Parameter: storage.MustFieldPath("item", "id")}}}}
}
func list(values ...storage.Value) storage.Value {
	return storage.Value{Type: storage.TypeList, State: storage.ValuePresent, List: values}
}
func object(record storage.Record) storage.Value {
	return storage.Value{Type: storage.TypeObject, State: storage.ValuePresent, Object: map[string]storage.Value(record)}
}

type resolveFunc func(context.Context, FactRequest) (FactResult, error)

func (f resolveFunc) Resolve(ctx context.Context, r FactRequest) (FactResult, error) {
	return f(ctx, r)
}

func TestLazyCELRequirementsAllowsAndUnknown(t *testing.T) {
	for _, test := range []struct {
		name           string
		allow, require []Rule
		result         FactResult
		want           Decision
		calls          int
	}{
		{"local allow skips facts", []Rule{{"local", "true"}, {"remote", `facts.grant.exists(g, true)`}}, nil, FactResult{}, Decision{true, true}, 0},
		{"failed requirement skips allow facts", []Rule{{"remote", `facts.grant.exists(g, true)`}}, []Rule{{"required", "false"}}, FactResult{}, Decision{false, true}, 0},
		{"failed requirement skips later requirements", []Rule{{"local", "true"}}, []Rule{{"first", "false"}, {"remote", `facts.grant.exists(g, true)`}}, FactResult{}, Decision{false, true}, 0},
		{"requirements alone cannot grant", nil, []Rule{{"required", "true"}}, FactResult{}, Decision{false, true}, 0},
		{"all requirements pass", []Rule{{"local", "true"}}, []Rule{{"first", "true"}, {"second", "true"}}, FactResult{}, Decision{true, true}, 0},
		{"requirements are conjunctive", []Rule{{"local", "true"}}, []Rule{{"first", "true"}, {"second", "false"}}, FactResult{}, Decision{false, true}, 0},
		{"unknown grant", []Rule{{"grant", `facts.grant.exists(g, true)`}}, nil, FactResult{}, Decision{}, 1},
		{"known absent grant", []Rule{{"grant", `facts.grant.exists(g, true)`}}, nil, FactResult{Value: list(), Complete: true}, Decision{false, true}, 1},
		{"known present grant", []Rule{{"grant", `facts.grant.exists(g, true)`}}, nil, FactResult{Value: list(object(storage.Record{})), Complete: true}, Decision{true, true}, 1},
		{"unknown requirement withholds", []Rule{{"local", "true"}}, []Rule{{"required", `facts.grant.exists(g, true)`}}, FactResult{}, Decision{}, 1},
		{"known failed requirement overrides allow", []Rule{{"local", "true"}}, []Rule{{"required", `facts.grant.exists(g, true)`}}, FactResult{Value: list(), Complete: true}, Decision{false, true}, 1},
		{"known satisfied requirement permits allow", []Rule{{"local", "true"}}, []Rule{{"required", `facts.grant.exists(g, true)`}}, FactResult{Value: list(object(storage.Record{})), Complete: true}, Decision{true, true}, 1},
		{"failure after unknown resolves rejection", []Rule{{"local", "true"}}, []Rule{{"remote", `facts.grant.exists(g, true)`}, {"required", "false"}}, FactResult{}, Decision{false, true}, 1},
		{"no grant resolves rejection despite unknown requirement", nil, []Rule{{"required", `facts.grant.exists(g, true)`}}, FactResult{}, Decision{false, true}, 1},
		{"memoize requirement and allow fact", []Rule{{"grant", `facts.grant.exists(g, true)`}}, []Rule{{"required", `facts.grant.exists(g, true)`}}, FactResult{Value: list(object(storage.Record{})), Complete: true}, Decision{true, true}, 1},
		{"CEL skips unused branch", []Rule{{"grant", `false && facts.grant.exists(g, true)`}}, nil, FactResult{}, Decision{false, true}, 0},
		{"unknown negation cannot grant", []Rule{{"grant", `!facts.grant.exists(g, true)`}}, nil, FactResult{}, Decision{}, 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			e := compile(t, Map{projects: {Facts: []Fact{grantFact()}, Require: test.require, Allow: test.allow}})
			calls := 0
			got, err := e.Evaluate(context.Background(), Input{Candidate: candidate(projects, "p1", storage.Record{"id": text("p1")})}, resolveFunc(func(_ context.Context, r FactRequest) (FactResult, error) {
				calls++
				if r.Projection.Query.Relation != grants {
					t.Fatal("wrong fact projection")
				}
				return test.result, nil
			}))
			if err != nil || got != test.want || calls != test.calls {
				t.Fatalf("got %+v, %v, calls=%d; want %+v calls=%d", got, err, calls, test.want, test.calls)
			}
		})
	}
}

func TestPendingRequirementStillDiscoversAllowFacts(t *testing.T) {
	explicit := grantFact()
	explicit.ID = "required"
	e := compile(t, Map{projects: {
		Facts:   []Fact{explicit, grantFact()},
		Require: []Rule{{"required", `facts.required.exists(g, true)`}},
		Allow:   []Rule{{"grant", `facts.grant.exists(g, true)`}},
	}})
	var calls []string
	got, err := e.Evaluate(t.Context(), Input{Candidate: candidate(projects, "p1", storage.Record{})}, resolveFunc(func(_ context.Context, r FactRequest) (FactResult, error) {
		calls = append(calls, r.ID)
		if r.ID == "required" {
			return FactResult{}, nil
		}
		return FactResult{Value: list(object(storage.Record{})), Complete: true}, nil
	}))
	if err != nil || got != (Decision{}) || strings.Join(calls, ",") != "required,grant" {
		t.Fatalf("pending requirement did not preserve ordered, concurrent discovery: %+v, %v, %v", got, err, calls)
	}
}

func TestConditionalRequirementLazilyChecksPropagatedAccess(t *testing.T) {
	explicit := grantFact()
	explicit.ID = "explicit"
	e := compile(t, Map{
		projects: {Propagate: []Edge{{"task", tasks, `parent.id == item.project_id`}}},
		tasks: {
			Facts:   []Fact{explicit, grantFact()},
			Require: []Rule{{"restricted", `item.restricted != true || facts.explicit.exists(g, true)`}},
			Allow:   []Rule{{"direct", `facts.grant.exists(g, true)`}},
		},
	})
	for _, test := range []struct {
		name       string
		restricted storage.Value
		result     FactResult
		want       Decision
		calls      int
	}{
		{"unrestricted", storage.Value{Type: storage.TypeBool, State: storage.ValuePresent, Data: []byte("false")}, FactResult{}, Decision{true, true}, 0},
		{"optional restricted absent", storage.Value{State: storage.ValueNull}, FactResult{}, Decision{true, true}, 0},
		{"restricted pending", storage.Value{Type: storage.TypeBool, State: storage.ValuePresent, Data: []byte("true")}, FactResult{}, Decision{}, 1},
		{"restricted without explicit grant", storage.Value{Type: storage.TypeBool, State: storage.ValuePresent, Data: []byte("true")}, FactResult{Value: list(), Complete: true}, Decision{false, true}, 1},
		{"restricted with explicit grant", storage.Value{Type: storage.TypeBool, State: storage.ValuePresent, Data: []byte("true")}, FactResult{Value: list(object(storage.Record{})), Complete: true}, Decision{true, true}, 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			calls := 0
			record := storage.Record{"project_id": text("p1")}
			if test.restricted.State != storage.ValueNull {
				record["restricted"] = test.restricted
			}
			got, err := e.Evaluate(t.Context(), Input{
				Candidate: candidate(tasks, "t1", record),
				Parent:    &ParentState{Candidate: candidate(projects, "p1", storage.Record{"id": text("p1")}), Visible: true},
			}, resolveFunc(func(_ context.Context, r FactRequest) (FactResult, error) {
				if r.ID != "explicit" {
					t.Fatalf("propagated access evaluated individual allow: %s", r.ID)
				}
				calls++
				return test.result, nil
			}))
			if err != nil || got != test.want || calls != test.calls {
				t.Fatalf("got %+v, %v, calls=%d; want %+v calls=%d", got, err, calls, test.want, test.calls)
			}
		})
	}
}

func TestRequirementsPrecedePropagation(t *testing.T) {
	e := compile(t, Map{
		projects: {Propagate: []Edge{{"task", tasks, `parent.id < 0`}}},
		tasks:    {Require: []Rule{{"enabled", `item.enabled == true`}}},
	})
	input := Input{
		Candidate: candidate(tasks, "t1", storage.Record{}),
		Parent:    &ParentState{Candidate: candidate(projects, "p1", storage.Record{"id": text("p1")}), Visible: true},
	}
	got, err := e.Evaluate(t.Context(), input, nil)
	if err != nil || got != (Decision{Resolved: true}) {
		t.Fatalf("failed requirement evaluated propagation: %+v, %v", got, err)
	}
	input.Candidate.Record["enabled"] = storage.Value{Type: storage.TypeBool, State: storage.ValuePresent, Data: []byte("true")}
	if _, err := e.Evaluate(t.Context(), input, nil); err == nil || !strings.Contains(err.Error(), "propagation task:") {
		t.Fatalf("test propagation should fail once requirements pass: %v", err)
	}
}

func TestRequirementErrorsRetainRuleIdentity(t *testing.T) {
	e := compile(t, Map{projects: {
		Facts:   []Fact{grantFact()},
		Require: []Rule{{"explicit-access", `facts.grant.exists(g, true)`}},
		Allow:   []Rule{{"local", "true"}},
	}})
	failure := errors.New("fact unavailable")
	got, err := e.Evaluate(t.Context(), Input{Candidate: candidate(projects, "p1", storage.Record{})}, resolveFunc(func(context.Context, FactRequest) (FactResult, error) {
		return FactResult{}, failure
	}))
	if !errors.Is(err, failure) || !strings.Contains(err.Error(), "require explicit-access:") || got.Visible {
		t.Fatalf("requirement failure lost identity or became authorization: %+v, %v", got, err)
	}
}

func TestCELPropagationRequiresDeclaringRelationAndImmediateLink(t *testing.T) {
	definitions := Map{
		projects: {Allow: []Rule{{"root", "true"}}, Propagate: []Edge{{"links", links, `parent.id == item.project_id`}}},
		links:    {Receive: []EdgeReference{{From: projects, ID: "links"}}, Propagate: []Edge{{"task", tasks, `parent.task_id == item.id`}}},
		tasks:    {Receive: []EdgeReference{{From: links, ID: "task"}}, Require: []Rule{{"not-blocked", `item.blocked != "yes"`}}},
	}
	e := compile(t, definitions)
	project := candidate(projects, "p1", storage.Record{"id": text("p1")})
	link := candidate(links, "l1", storage.Record{"project_id": text("p1"), "task_id": text("t1")})
	task := candidate(tasks, "t1", storage.Record{"id": text("t1")})
	check := func(parent, child accesscontrol.Candidate, visible, want bool) {
		t.Helper()
		got, err := e.Evaluate(context.Background(), Input{Candidate: child, Parent: &ParentState{Candidate: parent, Visible: visible}}, nil)
		if err != nil || !got.Resolved || got.Visible != want {
			t.Fatalf("got %+v %v, want visible=%v", got, err, want)
		}
	}
	check(project, link, true, true)
	check(link, task, true, true)
	check(project, task, true, false)
	check(project, link, false, false)
	other := project
	other.Record = storage.Record{"id": text("p2")}
	check(other, link, true, false)
	other = project
	other.Key.Relation.Source = "other"
	check(other, link, true, false)
	task.Record["blocked"] = text("yes")
	check(link, task, true, false)
	// A second parent can reuse an edge's local ID, but does not satisfy a
	// receive allowlist scoped to the first parent's relation.
	otherRelation := storage.RelationName{Namespace: "demo", Name: "other"}
	definitions[otherRelation] = Relation{Propagate: []Edge{{"links", links, "true"}}}
	e = compile(t, definitions)
	check(candidate(otherRelation, "x", storage.Record{}), link, true, false)
}

func TestCELMissingOptionalFieldsPendingRowsAndStrictScalarTypes(t *testing.T) {
	e := compile(t, Map{projects: {
		Require: []Rule{{"active", `item.deleted_at == null`}},
		Allow:   []Rule{{"visible", `item.organization_id == claims.organization_id`}},
	}})
	input := Input{Candidate: candidate(projects, "p1", storage.Record{"organization_id": text("org")}), Claims: accesscontrol.Claims{"organization_id": text("org")}}
	got, err := e.Evaluate(context.Background(), input, nil)
	if err != nil || !got.Visible {
		t.Fatalf("optional null: %+v %v", got, err)
	}
	input.Candidate.Complete = false
	got, err = e.Evaluate(context.Background(), input, nil)
	if err != nil || got.Resolved || got.Visible {
		t.Fatalf("pending row authorized: %+v %v", got, err)
	}
	input.Candidate.Complete = true
	input.Candidate.Record["organization_id"] = storage.Value{Type: storage.TypeInteger, State: storage.ValuePresent, Data: []byte("123")}
	input.Claims["organization_id"] = text("123")
	got, err = e.Evaluate(context.Background(), input, nil)
	if err != nil || got.Visible {
		t.Fatalf("scalar types coerced: %+v %v", got, err)
	}
}

func TestCELCompileRejectsInvalidDefinitions(t *testing.T) {
	for name, definition := range map[string]Map{
		"missing expression":  {projects: {Allow: []Rule{{ID: "x"}}}},
		"unknown fact":        {projects: {Allow: []Rule{{"x", `facts.missing.exists(g,true)`}}}},
		"dynamic fact name":   {projects: {Facts: []Fact{grantFact()}, Allow: []Rule{{"x", `facts[item.id].exists(g,true)`}}}},
		"whole fact map":      {projects: {Facts: []Fact{grantFact()}, Allow: []Rule{{"x", `size(facts) > 0`}}}},
		"fact presence":       {projects: {Facts: []Fact{grantFact()}, Allow: []Rule{{"x", `has(facts.grant)`}}}},
		"parent outside edge": {projects: {Allow: []Rule{{"x", `parent.id == item.id`}}}},
		"non boolean":         {projects: {Allow: []Rule{{"x", `item.id`}}}},
		"duplicate rule":      {projects: {Allow: []Rule{{"x", "true"}}, Require: []Rule{{"x", "false"}}}},
		"invalid requirement": {projects: {Require: []Rule{{"x", "42"}}}},
		"duplicate fact":      {projects: {Facts: []Fact{grantFact(), grantFact()}}},
		"unknown target":      {projects: {Propagate: []Edge{{"edge", tasks, "true"}}}},
		"unknown receiver":    {projects: {Receive: []EdgeReference{{From: tasks, ID: "missing"}}}},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := Compile(definition, Limits{MaxNodes: 1000, MaxDepth: 30, MaxEvaluationSteps: 1000}); err == nil {
				t.Fatal("accepted invalid policy")
			}
		})
	}
	if _, err := Compile(Map{projects: {Allow: []Rule{{"x", `item.a && item.b`}}}}, Limits{MaxNodes: 2, MaxDepth: 30, MaxEvaluationSteps: 1000}); err == nil {
		t.Fatal("node limit ignored")
	}
}

func TestCELPolicyCopiesAndConcurrentEvaluation(t *testing.T) {
	rules := []Rule{{"local", `item.enabled == "yes"`}}
	requirements := []Rule{{"active", `item.deleted_at == null`}}
	definitions := Map{projects: {Require: requirements, Allow: rules}}
	e := compile(t, definitions)
	rules[0].When = "false"
	requirements[0].When = "false"
	delete(definitions, projects)
	input := Input{Candidate: candidate(projects, "p1", storage.Record{"enabled": text("yes")})}
	var wg sync.WaitGroup
	for range 30 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			got, err := e.Evaluate(context.Background(), input, nil)
			if err != nil || !got.Visible {
				t.Errorf("%+v %v", got, err)
			}
		}()
	}
	wg.Wait()
}

func TestCELResolverErrorCancellationAndWorkLimit(t *testing.T) {
	e := compile(t, Map{projects: {Facts: []Fact{grantFact()}, Allow: []Rule{{"grant", `facts.grant.exists(g,true)`}}}})
	failure := errors.New("provider failed")
	input := Input{Candidate: candidate(projects, "p1", storage.Record{})}
	got, err := e.Evaluate(context.Background(), input, resolveFunc(func(context.Context, FactRequest) (FactResult, error) { return FactResult{}, failure }))
	if !errors.Is(err, failure) || got.Visible {
		t.Fatalf("error lost: %+v %v", got, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := e.Evaluate(ctx, input, nil); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	limited, err := Compile(Map{projects: {Allow: []Rule{{"cost", `item.values.all(v, v > 0)`}}}}, Limits{MaxNodes: 1000, MaxDepth: 30, MaxEvaluationSteps: 10})
	if err != nil {
		t.Fatal(err)
	}
	values := make([]storage.Value, 100)
	for i := range values {
		values[i] = storage.Value{Type: storage.TypeInteger, State: storage.ValuePresent, Data: []byte("1")}
	}
	input.Candidate.Record = storage.Record{"values": list(values...)}
	got, err = limited.Evaluate(context.Background(), input, nil)
	if err == nil || got.Visible {
		t.Fatal("CEL work budget not enforced")
	}
}
