package relation

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/adiom-data/weave/internal/storage"
	exprpb "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
)

func TestComputedStaticOutputTypes(t *testing.T) {
	values := []Computed{
		{Type: storage.TypeBool, Expression: `true`},
		{Type: storage.TypeInteger, Expression: `42`},
		{Type: storage.TypeFloat, Expression: `1.5`},
		{Type: storage.TypeDecimal, Expression: `decimal("1.25")`},
		{Type: storage.TypeText, Expression: `"hello"`},
		{Type: storage.TypeBytes, Expression: `b"hello"`},
		{Type: storage.TypeUUID, Expression: `uuid("00000000-0000-0000-0000-000000000001")`},
		{Type: storage.TypeDateTime, Expression: `timestamp("2026-01-01T00:00:00Z")`},
		{Type: storage.TypeList, Expression: `[1, 2]`},
		{Type: storage.TypeObject, Expression: `{"a": 1}`},
	}
	for _, output := range values {
		for _, actual := range values {
			t.Run(fmt.Sprintf("%s/from_%s", output.Type, actual.Type), func(t *testing.T) {
				definition := Computed{Type: output.Type, Expression: actual.Expression}
				e, err := Compile(Definitions{projects: {Computed: map[string]Computed{"v": definition}}}, Limits{})
				if output.Type != actual.Type {
					if err == nil || !strings.Contains(err.Error(), "expected "+string(output.Type)) {
						t.Fatalf("known mismatch was not rejected: %v", err)
					}
					return
				}
				if err != nil {
					t.Fatal(err)
				}
				c := e.Begin().NewContext(Input{Candidate: candidate(projects, "p1", storage.Record{})}, nil, nil, false)
				result, err := c.Compute(t.Context(), "v")
				if err != nil || !result.Complete || result.Value.Type != output.Type {
					t.Fatalf("valid known type failed: %+v %v", result, err)
				}
			})
		}
		for _, source := range []string{`1u`, `duration("1s")`, `type(1)`} {
			if _, err := NewStore(Definitions{projects: {Computed: map[string]Computed{"v": {Type: output.Type, Expression: source}}}}, Limits{}); err == nil {
				t.Fatalf("accepted noncanonical output %s as %s", source, output.Type)
			}
		}
		// Nullability follows the existing runtime contract for every computed
		// type, rather than inheriting visibility's strict boolean requirement.
		e := compile(t, Definitions{projects: {Computed: map[string]Computed{"v": {Type: output.Type, Expression: `null`}}}})
		result, err := e.Begin().NewContext(Input{Candidate: candidate(projects, "p1", storage.Record{})}, nil, nil, false).Compute(t.Context(), "v")
		if err != nil || !result.Complete || result.Value.State != storage.ValueNull {
			t.Fatalf("nullable %s failed: %+v %v", output.Type, result, err)
		}
	}
}

func TestDynamicOutputTypesStillUseRuntimeChecks(t *testing.T) {
	e := compile(t, Definitions{projects: {Computed: map[string]Computed{"v": {Type: storage.TypeInteger, Expression: `item.value`}}}})
	for _, value := range []storage.Value{
		{Type: storage.TypeInteger, State: storage.ValuePresent, Data: []byte("42")},
		text("wrong type"), {State: storage.ValueNull}, {State: storage.ValueUnavailable},
	} {
		c := e.Begin().NewContext(Input{Candidate: candidate(projects, "p1", storage.Record{"value": value})}, nil, nil, false)
		result, err := c.Compute(t.Context(), "v")
		if value.Type == storage.TypeText {
			if err == nil || !strings.Contains(err.Error(), "expected integer") {
				t.Fatalf("dynamic type mismatch was not rejected: %v", err)
			}
		} else if err != nil || result.Complete != (value.State != storage.ValueUnavailable) || !storage.EqualValues(result.Value, value) {
			t.Fatalf("dynamic value changed: %+v %v", result, err)
		}
	}
	for _, definitions := range []Definitions{
		{projects: {Visibility: `null`}},
		{projects: {Computed: map[string]Computed{"v": {Type: storage.TypeBool, Expression: `true`, Propagate: []Propagation{{To: tasks, When: `null`}}}}}, tasks: {}},
	} {
		if _, err := NewStore(definitions, Limits{}); err == nil {
			t.Fatal("null visibility/guard must still fail compilation")
		}
	}
}

func TestExpressionValidationRespectsComprehensionBindings(t *testing.T) {
	for _, source := range []string{
		`[1, 2].exists(inherited, inherited == 2)`,
		`[[1, 2]].exists(inherited, 2 in inherited)`,
		`[{"v":true}].exists(computed, computed.v)`,
		`[{"unknown":true}].exists(computed, has(computed.unknown))`,
		`[{"value":true}].exists(computed, computed[claims.key])`,
		`[true].exists(facts, facts)`,
		`[{"grant":true}].exists(facts, has(facts.grant))`,
		`[[1]].exists(facts, size(facts) > 0)`,
		`[false, true].exists(inherited, [false, true].exists(inherited, inherited))`,
		`[{"v":true}].exists(computed, [computed].exists(facts, facts.v))`,
	} {
		t.Run(source, func(t *testing.T) {
			// A local field named "v" must not create a false self-cycle in
			// this computation or a dependency on a global map/parent export.
			e := compile(t, Definitions{projects: {Computed: map[string]Computed{"v": {Type: storage.TypeBool, Expression: source}}}})
			c := e.Begin().NewContext(Input{Candidate: candidate(projects, "p1", storage.Record{}), Claims: Claims{"key": text("value")}}, nil, nil, false)
			result, err := c.Compute(t.Context(), "v")
			if err != nil || !decision(result).Visible || len(result.Dependencies) != 0 || len(result.Definitions) != 1 {
				t.Fatalf("local binding was treated as a global input: %+v %v", result, err)
			}
		})
	}
}

func TestShadowingDoesNotHideGlobalFactAndInheritedReads(t *testing.T) {
	defs := Definitions{
		projects: {Computed: map[string]Computed{"_role": {Type: storage.TypeInteger, Expression: `2`, Propagate: []Propagation{{To: tasks, When: `true`}}}}},
		tasks: {Facts: []Fact{grantFact()}, Computed: map[string]Computed{"v": {Type: storage.TypeBool, Expression: `facts.grant.exists(facts, facts.item.allowed) && ` +
			`[{"_role":false}].exists(inherited, has(inherited._role) && !inherited._role) && ` +
			`has(inherited._role) && inherited._role == 2`}}},
	}
	calls := 0
	e := compile(t, defs)
	c := e.Begin().NewContext(Input{Candidate: candidate(tasks, "t1", storage.Record{}), Parent: &ParentState{Candidate: candidate(projects, "p1", storage.Record{}), Visible: true}}, resolveFunc(func(context.Context, FactRequest) (FactResult, error) {
		calls++
		return FactResult{Value: list(object(storage.Record{"item": object(storage.Record{"allowed": boolean(true)})})), Complete: true, Dependencies: map[string]struct{}{"grant-selector": {}}}, nil
	}), nil, false)
	result, err := c.Compute(t.Context(), "v")
	if err != nil || !decision(result).Visible || calls != 1 || len(result.Definitions) != 2 {
		t.Fatalf("global read was shadowed beyond its lexical scope: %+v %v calls=%d", result, err, calls)
	}
	if _, found := result.Dependencies["grant-selector"]; !found {
		t.Fatal("global fact lost its dependency")
	}
	for _, source := range []string{
		`[1].exists(inherited, inherited == 1) && size(inherited) > 0`,
		`[1].exists(computed, computed == 1) && computed.unknown`,
		`[1].exists(facts, facts == 1) && has(facts.grant)`,
		`facts.exists(facts, facts == 1)`,
	} {
		if _, err := NewStore(Definitions{projects: {Facts: []Fact{grantFact()}, Visibility: source}}, Limits{}); err == nil {
			t.Fatalf("shadowing bypassed global lazy-map validation: %s", source)
		}
	}
}

func TestExpressionWalkUsesCELComprehensionScopes(t *testing.T) {
	// Exercise every phase, including the optional second iterator and
	// accumulator, independently of the particular macros enabled by CEL.
	var sequence int64
	want := map[int64]bool{}
	ident := func(name string, global bool) *exprpb.Expr {
		sequence++
		want[sequence] = global
		return &exprpb.Expr{Id: sequence, ExprKind: &exprpb.Expr_IdentExpr{IdentExpr: &exprpb.Expr_Ident{Name: name}}}
	}
	refs := func(computed, inherited, facts bool) *exprpb.Expr {
		return &exprpb.Expr{ExprKind: &exprpb.Expr_ListExpr{ListExpr: &exprpb.Expr_CreateList{Elements: []*exprpb.Expr{
			ident("computed", computed), ident("inherited", inherited), ident("facts", facts),
		}}}}
	}
	root := &exprpb.Expr{ExprKind: &exprpb.Expr_ComprehensionExpr{ComprehensionExpr: &exprpb.Expr_Comprehension{
		AccuVar: "computed", IterVar: "inherited", IterVar2: "facts",
		IterRange: refs(true, true, true), AccuInit: refs(true, true, true),
		LoopCondition: refs(false, false, false), LoopStep: refs(false, false, false), Result: refs(false, true, true),
	}}}
	visited := 0
	if err := walkExpr(root, 1, nil, func(e *exprpb.Expr, _ int, scope *expressionScope) error {
		if name := e.GetIdentExpr().GetName(); name != "" {
			visited++
			if got := scope.global(e, name); got != want[e.Id] {
				t.Errorf("identifier %d (%s): global=%t, want %t", e.Id, name, got, want[e.Id])
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if visited != len(want) {
		t.Fatalf("visited %d identifiers, want %d", visited, len(want))
	}
}
