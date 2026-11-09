package relation

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

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
func candidate(relation storage.RelationName, id string, record storage.Record) Candidate {
	return Candidate{Key: storage.EntityKey{Relation: storage.RelationKey{Source: "db", Namespace: relation.Namespace, Name: relation.Name}, Value: id}, Record: record, Complete: true}
}
func compile(t *testing.T, definitions Definitions) *Evaluator {
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

func TestVisibilityExpressionsAreLazyAndPreserveUnknown(t *testing.T) {
	for _, test := range []struct {
		name, expression string
		result           FactResult
		want             Decision
		calls            int
	}{
		{"local grant", `true || facts.grant.exists(g,true)`, FactResult{}, Decision{true, true}, 0},
		{"local gate", `false && facts.grant.exists(g,true)`, FactResult{}, Decision{false, true}, 0},
		{"unknown grant", `facts.grant.exists(g,true)`, FactResult{}, Decision{}, 1},
		{"absent grant", `facts.grant.exists(g,true)`, FactResult{Value: list(), Complete: true}, Decision{false, true}, 1},
		{"present grant", `facts.grant.exists(g,true)`, FactResult{Value: list(object(storage.Record{})), Complete: true}, Decision{true, true}, 1},
		{"unknown gate", `facts.grant.exists(g,true) && true`, FactResult{}, Decision{}, 1},
		{"failure after unknown", `facts.grant.exists(g,true) && false`, FactResult{}, Decision{false, true}, 1},
		{"grant after unknown", `facts.grant.exists(g,true) || true`, FactResult{}, Decision{true, true}, 1},
		{"repeated fact", `facts.grant.exists(g,true) && facts.grant.exists(g,true)`, FactResult{Value: list(object(storage.Record{})), Complete: true}, Decision{true, true}, 1},
		{"unknown negation", `!facts.grant.exists(g,true)`, FactResult{}, Decision{}, 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			e := compile(t, Definitions{projects: {Facts: []Fact{grantFact()}, Visibility: test.expression}})
			calls := 0
			got, err := e.Evaluate(t.Context(), Input{Candidate: candidate(projects, "p1", storage.Record{"id": text("p1")})}, resolveFunc(func(_ context.Context, r FactRequest) (FactResult, error) {
				calls++
				if r.Projection.Query.Relation != grants {
					t.Fatal("wrong fact projection")
				}
				return test.result, nil
			}))
			if err != nil || got != test.want || calls != test.calls {
				t.Fatalf("got %+v %v calls=%d, want %+v calls=%d", got, err, calls, test.want, test.calls)
			}
		})
	}
}

func TestPendingGateStillDiscoversOtherRequiredFacts(t *testing.T) {
	explicit := grantFact()
	explicit.ID = "required"
	e := compile(t, Definitions{projects: {Facts: []Fact{explicit, grantFact()}, Visibility: `facts.required.exists(g,true) && facts.grant.exists(g,true)`}})
	var calls []string
	got, err := e.Evaluate(t.Context(), Input{Candidate: candidate(projects, "p1", storage.Record{})}, resolveFunc(func(_ context.Context, r FactRequest) (FactResult, error) {
		calls = append(calls, r.ID)
		if r.ID == "required" {
			return FactResult{}, nil
		}
		return FactResult{Value: list(object(storage.Record{})), Complete: true}, nil
	}))
	if err != nil || got != (Decision{}) || strings.Join(calls, ",") != "required,grant" {
		t.Fatalf("pending gate serialized discovery: %+v %v %v", got, err, calls)
	}
}

func TestVisibilityGateShortCircuitsPropagationAndLazyFacts(t *testing.T) {
	e := compile(t, Definitions{
		projects: {Computed: map[string]Computed{"_role": {Type: storage.TypeInteger, Expression: "2", Propagate: []Propagation{{To: tasks, When: `parent.id == item.project_id`}}}}},
		tasks:    {Facts: []Fact{grantFact()}, Visibility: `item.enabled == true && (item.restricted != true || facts.grant.exists(g,true)) && has(inherited._role) && inherited._role >= 1`},
	})
	for _, test := range []struct {
		enabled, restricted bool
		result              FactResult
		want                Decision
		calls               int
	}{
		{false, true, FactResult{}, Decision{false, true}, 0}, {true, false, FactResult{}, Decision{true, true}, 0},
		{true, true, FactResult{}, Decision{}, 1}, {true, true, FactResult{Value: list(), Complete: true}, Decision{false, true}, 1},
		{true, true, FactResult{Value: list(object(storage.Record{})), Complete: true}, Decision{true, true}, 1},
	} {
		calls := 0
		in := Input{Candidate: candidate(tasks, "t1", storage.Record{"enabled": boolean(test.enabled), "restricted": boolean(test.restricted), "project_id": text("p1")}), Parent: &ParentState{Candidate: candidate(projects, "p1", storage.Record{"id": text("p1")}), Visible: true}}
		got, err := e.Evaluate(t.Context(), in, resolveFunc(func(context.Context, FactRequest) (FactResult, error) { calls++; return test.result, nil }))
		if err != nil || got != test.want || calls != test.calls {
			t.Fatalf("got %+v %v calls=%d, want %+v calls=%d", got, err, calls, test.want, test.calls)
		}
	}
	// A failed local gate must not evaluate even an invalid export guard.
	e = compile(t, Definitions{projects: {Computed: map[string]Computed{"_role": {Type: storage.TypeInteger, Expression: "2", Propagate: []Propagation{{To: tasks, When: `parent.id < 0`}}}}}, tasks: {Visibility: `item.enabled == true && has(inherited._role)`}})
	in := Input{Candidate: candidate(tasks, "t1", storage.Record{}), Parent: &ParentState{Candidate: candidate(projects, "p1", storage.Record{"id": text("p1")}), Visible: true}}
	if got, err := e.Evaluate(t.Context(), in, nil); err != nil || got != (Decision{Resolved: true}) {
		t.Fatalf("gate evaluated invalid guard: %+v %v", got, err)
	}
	in.Candidate.Record["enabled"] = boolean(true)
	if _, err := e.Evaluate(t.Context(), in, nil); err == nil || !strings.Contains(err.Error(), "export demo.projects._role") {
		t.Fatalf("missing export error: %v", err)
	}
}

func TestNamedPropagationRequiresDeclaringRelationAndImmediateLink(t *testing.T) {
	defs := Definitions{
		projects: {Computed: map[string]Computed{"_role": {Type: storage.TypeInteger, Expression: "2", Propagate: []Propagation{{To: links, When: `parent.id == item.project_id`}}}}},
		links:    {Receive: []Incoming{{From: projects, Field: "_role"}}, Computed: map[string]Computed{"_role": {Type: storage.TypeInteger, Expression: `has(inherited._role) ? inherited._role : 0`, Propagate: []Propagation{{To: tasks, When: `parent.task_id == item.id`}}}}, Visibility: `computed._role >= 1`},
		tasks:    {Receive: []Incoming{{From: links, Field: "_role"}}, Visibility: `item.blocked != "yes" && has(inherited._role) && inherited._role >= 1`},
	}
	e := compile(t, defs)
	pass := e.Begin()
	project := candidate(projects, "p1", storage.Record{"id": text("p1")})
	link := candidate(links, "l1", storage.Record{"project_id": text("p1"), "task_id": text("t1")})
	task := candidate(tasks, "t1", storage.Record{"id": text("t1")})
	parent := pass.NewContext(Input{Candidate: project}, nil, nil, false)
	linked := pass.NewContext(Input{Candidate: link, Parent: &ParentState{Candidate: project, Visible: true}}, nil, parent, false)
	check := func(parent *Context, child Candidate, visible, want bool) {
		t.Helper()
		c := pass.NewContext(Input{Candidate: child, Parent: &ParentState{Candidate: parent.input.Candidate, Visible: visible}}, nil, parent, false)
		got, err := c.Visibility(t.Context())
		if err != nil || !decision(got).Resolved || decision(got).Visible != want {
			t.Fatalf("got %+v %v want %v", got, err, want)
		}
	}
	check(parent, link, true, true)
	check(linked, task, true, true)
	check(parent, task, true, false)
	check(parent, link, false, false)
	other := project
	other.Record = storage.Record{"id": text("p2")}
	check(pass.NewContext(Input{Candidate: other}, nil, nil, false), link, true, false)
	other = project
	other.Key.Relation.Source = "other"
	check(pass.NewContext(Input{Candidate: other}, nil, nil, false), link, true, false)
	task.Record["blocked"] = text("yes")
	check(linked, task, true, false)
	otherType := storage.RelationName{Namespace: "demo", Name: "other"}
	defs[otherType] = Relation{Computed: map[string]Computed{"_role": {Type: storage.TypeInteger, Expression: "2", Propagate: []Propagation{{To: links, When: "true"}}}}}
	e = compile(t, defs)
	pass = e.Begin()
	check(pass.NewContext(Input{Candidate: candidate(otherType, "x", storage.Record{})}, nil, nil, false), link, true, false)
}

func TestMissingOptionalFieldsPendingRowsAndStrictScalarTypes(t *testing.T) {
	e := compile(t, Definitions{projects: {Visibility: `item.deleted_at == null && item.organization_id == claims.organization_id`}})
	in := Input{Candidate: candidate(projects, "p1", storage.Record{"organization_id": text("org")}), Claims: Claims{"organization_id": text("org")}}
	if got, err := e.Evaluate(t.Context(), in, nil); err != nil || !got.Visible {
		t.Fatalf("optional null: %+v %v", got, err)
	}
	in.Candidate.Complete = false
	if got, err := e.Evaluate(t.Context(), in, nil); err != nil || got.Resolved {
		t.Fatalf("pending: %+v %v", got, err)
	}
	in.Candidate.Complete = true
	in.Candidate.Record["organization_id"] = storage.Value{Type: storage.TypeInteger, State: storage.ValuePresent, Data: []byte("123")}
	in.Claims["organization_id"] = text("123")
	if got, err := e.Evaluate(t.Context(), in, nil); err != nil || got.Visible {
		t.Fatalf("implicit coercion: %+v %v", got, err)
	}
}

func TestCompileRejectsInvalidDefinitionExpressionsAndExports(t *testing.T) {
	for name, defs := range map[string]Definitions{
		"unknown fact":          {projects: {Visibility: `facts.missing.exists(g,true)`}},
		"dynamic fact":          {projects: {Facts: []Fact{grantFact()}, Visibility: `facts[item.id].exists(g,true)`}},
		"fact enumeration":      {projects: {Facts: []Fact{grantFact()}, Visibility: `size(facts)>0`}},
		"fact presence":         {projects: {Facts: []Fact{grantFact()}, Visibility: `has(facts.grant)`}},
		"parent outside guard":  {projects: {Visibility: `parent.id==item.id`}},
		"non boolean":           {projects: {Visibility: "42"}},
		"duplicate fact":        {projects: {Facts: []Fact{grantFact(), grantFact()}}},
		"dynamic inherited":     {projects: {Visibility: `inherited[item.field] == true`}},
		"inherited enumeration": {projects: {Visibility: `size(inherited)>0`}},
		"unknown target":        {projects: {Computed: map[string]Computed{"_role": {Type: storage.TypeInteger, Expression: "1", Propagate: []Propagation{{To: tasks, When: "true"}}}}}},
		"unknown receiver":      {projects: {Receive: []Incoming{{From: tasks, Field: "_role"}}}},
		"duplicate export":      {projects: {Computed: map[string]Computed{"_role": {Type: storage.TypeInteger, Expression: "1", Propagate: []Propagation{{To: tasks, When: "true"}, {To: tasks, When: "false"}}}}}, tasks: {}},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := Compile(defs, DefaultLimits()); err == nil {
				t.Fatal("accepted invalid definition")
			}
		})
	}
	if _, err := Compile(Definitions{projects: {Visibility: `item.a && item.b`}}, Limits{MaxNodes: 2, MaxDepth: 30, MaxEvaluationSteps: 1000}); err == nil {
		t.Fatal("node limit ignored")
	}
	e := compile(t, Definitions{projects: {Visibility: `item.visible`}})
	if _, err := e.Evaluate(t.Context(), Input{Candidate: candidate(projects, "p1", storage.Record{"visible": text("yes")})}, nil); err == nil {
		t.Fatal("non-boolean runtime result accepted")
	}
}

func TestDefinitionCopiesAndConcurrentEvaluation(t *testing.T) {
	defs := Definitions{projects: {Visibility: `item.deleted_at == null && item.enabled == "yes"`}}
	e := compile(t, defs)
	defs[projects] = Relation{Visibility: "false"}
	delete(defs, projects)
	in := Input{Candidate: candidate(projects, "p1", storage.Record{"enabled": text("yes")})}
	var group sync.WaitGroup
	for range 30 {
		group.Go(func() {
			if got, err := e.Evaluate(t.Context(), in, nil); err != nil || !got.Visible {
				t.Errorf("%+v %v", got, err)
			}
		})
	}
	group.Wait()
}

func TestResolverErrorsCancellationAndWorkLimit(t *testing.T) {
	e := compile(t, Definitions{projects: {Facts: []Fact{grantFact()}, Visibility: `facts.grant.exists(g,true)`}})
	failure := errors.New("fact unavailable")
	in := Input{Candidate: candidate(projects, "p1", storage.Record{})}
	got, err := e.Evaluate(t.Context(), in, resolveFunc(func(context.Context, FactRequest) (FactResult, error) { return FactResult{}, failure }))
	if !errors.Is(err, failure) || got.Visible {
		t.Fatalf("error became visibility: %+v %v", got, err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := e.Evaluate(ctx, in, nil); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	limited, err := Compile(Definitions{projects: {Visibility: `item.values.all(v,v>0)`}}, Limits{MaxNodes: 1000, MaxDepth: 30, MaxEvaluationSteps: 10})
	if err != nil {
		t.Fatal(err)
	}
	values := make([]storage.Value, 100)
	for i := range values {
		values[i] = storage.Value{Type: storage.TypeInteger, State: storage.ValuePresent, Data: []byte("1")}
	}
	in.Candidate.Record = storage.Record{"values": list(values...)}
	if got, err := limited.Evaluate(t.Context(), in, nil); err == nil || got.Visible {
		t.Fatal("work budget ignored")
	}
}
