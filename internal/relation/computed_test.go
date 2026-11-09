package relation

import (
	"context"
	"strings"
	"testing"

	"github.com/adiom-data/weave/internal/storage"
)

func TestComputedValuesAreLazyTypedAndReusedByVisibility(t *testing.T) {
	e := compile(t, Definitions{projects: {Facts: []Fact{grantFact()}, Computed: map[string]Computed{
		"role":    {Type: storage.TypeInteger, Expression: `item.owner == claims.user_id ? 2 : facts.grant.exists(g,true) ? 1 : 0`},
		"canEdit": {Type: storage.TypeBool, Expression: `computed.role >= 2`},
		"unused":  {Type: storage.TypeText, Expression: `facts.grant.exists(g,true) ? "yes" : "no"`},
	}, Visibility: `computed.role >= 1`}})
	calls := 0
	resolver := resolveFunc(func(context.Context, FactRequest) (FactResult, error) {
		calls++
		return FactResult{Value: list(object(storage.Record{})), Complete: true, Dependencies: map[string]struct{}{"grant-selector": {}}}, nil
	})
	for _, owner := range []bool{true, false} {
		in := Input{Candidate: candidate(projects, "p1", storage.Record{}), Claims: Claims{"user_id": text("u1")}}
		if owner {
			in.Candidate.Record["owner"] = text("u1")
		}
		c := e.Begin().NewContext(in, resolver, nil, false)
		visible, err := c.Visibility(t.Context())
		if err != nil || !decision(visible).Visible {
			t.Fatalf("visibility: %+v %v", visible, err)
		}
		role, err := c.Compute(t.Context(), "role")
		if err != nil || !role.Complete {
			t.Fatalf("role: %+v %v", role, err)
		}
		edit, err := c.Compute(t.Context(), "canEdit")
		if err != nil || string(edit.Value.Data) != map[bool]string{true: "true", false: "false"}[owner] {
			t.Fatalf("edit: %+v %v", edit, err)
		}
		want := "1"
		if owner {
			want = "2"
		}
		if string(role.Value.Data) != want {
			t.Fatalf("role %s want %s", role.Value.Data, want)
		}
		if owner && calls != 0 {
			t.Fatal("local grant expanded facts")
		}
		if !owner {
			if calls != 1 {
				t.Fatalf("visibility and selected values loaded fact %d times", calls)
			}
			if _, ok := edit.Dependencies["grant-selector"]; !ok {
				t.Fatal("transitive fact dependency missing")
			}
		}
	}
}

func TestComputedPropagationUsesDeclaredImmediateEdges(t *testing.T) {
	e := compile(t, Definitions{
		projects: {Computed: map[string]Computed{"role": {Type: storage.TypeInteger, Expression: `item.role`, Propagate: []Propagation{{To: tasks, When: `parent.id == item.project_id`}}}}},
		tasks:    {Receive: []Incoming{{From: projects, Field: "role"}}, Computed: map[string]Computed{"role": {Type: storage.TypeInteger, Expression: `item.restricted == true ? 0 : has(inherited.role) ? inherited.role : 0`}, "canEdit": {Type: storage.TypeBool, Expression: `computed.role >= 2`}}, Visibility: `computed.role >= 1`},
	})
	pass := e.Begin()
	parent := pass.NewContext(Input{Candidate: candidate(projects, "p1", storage.Record{"id": text("p1"), "role": {Type: storage.TypeInteger, State: storage.ValuePresent, Data: []byte("2")}})}, nil, nil, false)
	for _, test := range []struct {
		name, project string
		restricted    bool
		want          string
	}{{"matching", "p1", false, "2"}, {"unrelated", "p2", false, "0"}, {"restricted", "p1", true, "0"}} {
		t.Run(test.name, func(t *testing.T) {
			record := storage.Record{"project_id": text(test.project)}
			if test.restricted {
				record["restricted"] = boolean(true)
			}
			in := Input{Candidate: candidate(tasks, "t1", record), Parent: &ParentState{Candidate: parent.input.Candidate, Visible: true}}
			child := pass.NewContext(in, nil, parent, false)
			r, err := child.Compute(t.Context(), "role")
			if err != nil || !r.Complete || string(r.Value.Data) != test.want {
				t.Fatalf("got %+v %v", r, err)
			}
			visible, err := child.Visibility(t.Context())
			if err != nil || decision(visible).Visible != (test.want != "0") {
				t.Fatalf("visibility %+v %v", visible, err)
			}
		})
	}
	if len(parent.values) != 1 {
		t.Fatalf("parent computation was not shared: %v", parent.values)
	}
}

func TestComputedPendingIsNotFalseOrAbsent(t *testing.T) {
	e := compile(t, Definitions{projects: {Facts: []Fact{grantFact()}, Computed: map[string]Computed{"role": {Type: storage.TypeInteger, Expression: `facts.grant.exists(g,true) ? 1 : 0`}}, Visibility: `computed.role >= 1`}})
	c := e.Begin().NewContext(Input{Candidate: candidate(projects, "p1", storage.Record{})}, resolveFunc(func(context.Context, FactRequest) (FactResult, error) { return FactResult{}, nil }), nil, false)
	role, err := c.Compute(t.Context(), "role")
	if err != nil || role.Complete || role.Value.State != storage.ValueUnavailable {
		t.Fatalf("pending role became a value: %+v %v", role, err)
	}
	visible, err := c.Visibility(t.Context())
	if err != nil || decision(visible).Resolved {
		t.Fatalf("pending role resolved visibility: %+v %v", visible, err)
	}
}

func TestComputedCanonicalOutputs(t *testing.T) {
	for _, test := range []struct {
		expression string
		kind       storage.ValueType
		want       string
	}{
		{`true`, storage.TypeBool, "true"}, {`42`, storage.TypeInteger, "42"}, {`1.5`, storage.TypeFloat, "1.5"}, {`"hello"`, storage.TypeText, "hello"},
		{`-0.0`, storage.TypeFloat, "0"},
		{`double("Infinity")`, storage.TypeFloat, "Infinity"}, {`double("-Infinity")`, storage.TypeFloat, "-Infinity"}, {`double("NaN")`, storage.TypeFloat, "NaN"},
		{`[1,2]`, storage.TypeList, ""}, {`{"value": [1,2], "nested": {"ok": true}}`, storage.TypeObject, ""},
		{`[-0.0, 1.5]`, storage.TypeList, ""}, {`[-0.0, double("Infinity")]`, storage.TypeList, ""},
		{`{"zero": -0.0, "negative": double("-Infinity")}`, storage.TypeObject, ""},
		{`decimal("1.25")`, storage.TypeDecimal, "1.25"}, {`uuid("00000000-0000-0000-0000-000000000001")`, storage.TypeUUID, "00000000-0000-0000-0000-000000000001"},
	} {
		t.Run(test.expression, func(t *testing.T) {
			e := compile(t, Definitions{projects: {Computed: map[string]Computed{"v": {Type: test.kind, Expression: test.expression}}}})
			c := e.Begin().NewContext(Input{Candidate: candidate(projects, "p1", storage.Record{})}, nil, nil, true)
			r, err := c.Compute(t.Context(), "v")
			if err != nil || !r.Complete || r.Value.Type != test.kind || test.want != "" && string(r.Value.Data) != test.want {
				t.Fatalf("got %+v %v", r, err)
			}
			if err := r.Value.ValidateComplete(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestComputedFloatCopiesPreserveCanonicalValues(t *testing.T) {
	e := compile(t, Definitions{projects: {Computed: map[string]Computed{
		"direct":  {Type: storage.TypeFloat, Expression: `item.value`},
		"copy":    {Type: storage.TypeFloat, Expression: `computed.direct`},
		"derived": {Type: storage.TypeFloat, Expression: `computed.copy + 0.0`},
	}}})
	for _, input := range []string{"Infinity", "-Infinity", "NaN", "-0", "0", "1.5", "5e-324", "1.7976931348623157e308"} {
		t.Run(input, func(t *testing.T) {
			want, err := storage.ParseScalar(storage.TypeFloat, []byte(input))
			if err != nil {
				t.Fatal(err)
			}
			c := e.Begin().NewContext(Input{Candidate: candidate(projects, "p1", storage.Record{"value": want})}, nil, nil, true)
			for _, name := range []string{"direct", "copy", "derived"} {
				got, err := c.Compute(t.Context(), name)
				if err != nil || !got.Complete || !storage.EqualValues(got.Value, want) {
					t.Fatalf("%s: got %+v, error %v; want %+v", name, got, err, want)
				}
			}
		})
	}
}

func TestComputedRejectsCyclesAndUndeclaredReads(t *testing.T) {
	for _, fields := range []map[string]Computed{
		{"a": {Type: storage.TypeBool, Expression: `computed.a`}},
		{"a": {Type: storage.TypeBool, Expression: `computed.b`}, "b": {Type: storage.TypeBool, Expression: `computed.a`}},
		{"a": {Type: storage.TypeBool, Expression: `computed.missing == true`}},
		{"a": {Type: storage.TypeBool, Expression: `size(computed) > 0`}},
		{"a": {Type: storage.TypeBool, Expression: `has(computed.a)`}},
	} {
		if _, err := Compile(Definitions{projects: {Computed: fields}}, DefaultLimits()); err == nil {
			t.Fatalf("accepted invalid computation: %v", fields)
		}
	}
	e := compile(t, Definitions{projects: {Computed: map[string]Computed{"a": {Type: storage.TypeBool, Expression: `item.value`}}}})
	_, err := e.Begin().NewContext(Input{Candidate: candidate(projects, "p1", storage.Record{"value": text("not boolean")})}, nil, nil, true).Compute(t.Context(), "a")
	if err == nil || !strings.Contains(err.Error(), "expected bool") {
		t.Fatalf("type mismatch: %v", err)
	}
}

func TestNamedExportsCanFeedDifferentChildComputations(t *testing.T) {
	defs := Definitions{
		projects: {Computed: map[string]Computed{"_role": {Type: storage.TypeInteger, Expression: `2`, Propagate: []Propagation{{To: tasks, When: `parent.id == item.project_id`}}}}},
		tasks:    {Computed: map[string]Computed{"canEdit": {Type: storage.TypeBool, Expression: `has(inherited._role) && inherited._role >= 2`}}, Visibility: `has(inherited._role) && inherited._role >= 1`},
	}
	e := compile(t, defs)
	pass := e.Begin()
	parentInput := Input{Candidate: candidate(projects, "p1", storage.Record{"id": text("p1")})}
	parent := pass.NewContext(parentInput, nil, nil, true)
	child := pass.NewContext(Input{Candidate: candidate(tasks, "t1", storage.Record{"project_id": text("p1")}), Parent: &ParentState{Candidate: parentInput.Candidate, Visible: true}}, nil, parent, true)
	value, err := child.Compute(t.Context(), "canEdit")
	if err != nil || !value.Complete || string(value.Value.Data) != "true" {
		t.Fatalf("transformation: %+v %v", value, err)
	}
	visible, err := child.Visibility(t.Context())
	if err != nil || !decision(visible).Visible {
		t.Fatalf("visibility did not consume named export: %+v %v", visible, err)
	}
}

func TestDefinitionStorePreservesOmittedAndExplicitVisibility(t *testing.T) {
	store, err := NewStore(Definitions{projects: {}, tasks: {Visibility: "false"}}, DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	e, err := NewEvaluator(store)
	if err != nil {
		t.Fatal(err)
	}
	for _, relation := range []storage.RelationName{projects, tasks} {
		value, err := e.Begin().NewContext(Input{Candidate: candidate(relation, "id", storage.Record{})}, nil, nil, false).Visibility(t.Context())
		if err != nil || !value.Complete || decision(value).Visible != (relation == projects) {
			t.Fatalf("%v: %+v %v", relation, value, err)
		}
	}
}

func TestComputedDiamondsDoNotShareContextualValues(t *testing.T) {
	e := compile(t, Definitions{
		projects: {Computed: map[string]Computed{"role": {Type: storage.TypeInteger, Expression: `item.role`, Propagate: []Propagation{{To: tasks, When: `true`}}}}},
		tasks:    {Computed: map[string]Computed{"role": {Type: storage.TypeInteger, Expression: `has(inherited.role) ? inherited.role : 0`}}},
	})
	pass := e.Begin()
	for _, role := range []string{"1", "2"} {
		parentInput := Input{Candidate: candidate(projects, "p"+role, storage.Record{"role": {Type: storage.TypeInteger, State: storage.ValuePresent, Data: []byte(role)}})}
		parent := pass.NewContext(parentInput, nil, nil, true)
		child := pass.NewContext(Input{Candidate: candidate(tasks, "shared", storage.Record{}), Parent: &ParentState{Candidate: parentInput.Candidate, Visible: true}}, nil, parent, true)
		value, err := child.Compute(t.Context(), "role")
		if err != nil || !value.Complete || string(value.Value.Data) != role {
			t.Fatalf("role=%s: %+v %v", role, value, err)
		}
	}
}

func TestInheritedPresenceDoesNotComputeValueAndRetainsDependencies(t *testing.T) {
	e := compile(t, Definitions{
		projects: {Facts: []Fact{grantFact()}, Computed: map[string]Computed{"_role": {Type: storage.TypeInteger, Expression: `facts.grant.exists(g,true) ? 2 : 0`, Propagate: []Propagation{{To: tasks, When: `parent.id == item.project_id`}}}}},
		tasks: {Computed: map[string]Computed{
			"hasRole": {Type: storage.TypeBool, Expression: `has(inherited._role)`},
			"role":    {Type: storage.TypeInteger, Expression: `has(inherited._role) ? inherited._role : 0`},
		}},
	})
	for _, complete := range []bool{true, false} {
		calls := 0
		parent := candidate(projects, "p1", storage.Record{"id": text("p1")})
		input := Input{Candidate: candidate(tasks, "t1", storage.Record{"project_id": text("p1")}), Parent: &ParentState{Candidate: parent, Visible: true}}
		input.Candidate.Complete = complete
		c := e.Begin().NewContext(input, resolveFunc(func(context.Context, FactRequest) (FactResult, error) { calls++; return FactResult{}, nil }), nil, true)
		present, err := c.Compute(t.Context(), "hasRole")
		if err != nil || present.Complete != complete || complete && !decision(present).Visible || calls != 0 {
			t.Fatalf("presence loaded a value or confused pending/absent: %+v %v calls=%d", present, err, calls)
		}
		if _, found := present.Definitions[projects]; !found {
			t.Fatal("presence lost parent definition dependency")
		}
		role, err := c.Compute(t.Context(), "role")
		if err != nil || role.Complete || calls != map[bool]int{true: 1, false: 0}[complete] {
			t.Fatalf("pending export: %+v %v calls=%d", role, err, calls)
		}
	}
}
