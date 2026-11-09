package relation

import (
	"context"
	policyvalue "github.com/adiom-data/weave/internal/relation/value"
	"github.com/adiom-data/weave/internal/storage"
	"testing"
)

func TestNativeOperationsMatchCEL(t *testing.T) {
	decimal, _ := storage.ParseScalar(storage.TypeDecimal, []byte("10.00"))
	for _, source := range []string{
		`has(item.a)`, `has(item.nested.a)`, `has(item.values[0].a)`,
		`item.a in claims.values`, `item.a in item.values`, `"a" in item.props`,
		`item.a in ["x", "y"]`, `item.a in []`, `"a" in {"a": 1, "b": 2}`, `"a" in {}`,
		`size(item.values) > 0`, `item.values.size() == 1`, `size(item.props) == 1`,
		`size(item.a) >= 0`, `item.a.size() == 3`,
		`item.a.startsWith("x")`, `item.a.endsWith("🙂")`, `item.a.contains("y")`,
		`item.a.startsWith("")`, `item.a.contains("")`, `item.a.endsWith(claims.suffix)`,
		`item.flag ? item.a == "x" : item.a == null`,
		`!(item.flag ? true : false)`,
		`has(item.a) ? item.a == "x" : false`,
		`item.flag ? item.a == claims.b : has(item.a)`,
		`item.flag ? item.nested.missing.child == null : true`,
		`item.values[0] == "x"`, `item.values[1] == "y"`, `item.values[-1] == null`,
		`item.values[0].a == "x"`,
		`item.a in [["x"],{"a":"x"}]`,
	} {
		t.Run(source, func(t *testing.T) {
			plans := map[int]compiledExpression{}
			for _, limit := range []int{1, 2, 3, 4, 5, 6, 7, 8, 10, 11, 12, 15, 20, 30, 31, 32, 40, 1000} {
				plans[limit] = compileExpression(t, source, false, limit)
				if _, native := plans[limit].(*fastExpression); !native {
					t.Fatalf("expected native operation: %s", source)
				}
			}
			for _, record := range []storage.Record{
				{}, {"a": text("x"), "flag": {Type: storage.TypeBool, State: storage.ValuePresent, Data: []byte("true")}, "values": list(text("x"), text("y")), "props": object(storage.Record{"a": text("x")})},
				{"a": text("xy🙂"), "flag": {Type: storage.TypeBool, State: storage.ValuePresent, Data: []byte("false")}, "values": list(), "props": object(storage.Record{})},
				{"a": {State: storage.ValueNull}, "values": {State: storage.ValueNull}},
				{"a": {State: storage.ValueUnavailable}, "flag": {State: storage.ValueUnavailable}, "values": {State: storage.ValueUnavailable}},
				{"a": decimal, "flag": text("not boolean"), "values": list(decimal), "props": text("not object")},
				{"values": list(object(storage.Record{"a": text("x")})), "nested": object(storage.Record{"a": {State: storage.ValueNull}})},
				{"values": object(storage.Record{"0": text("x")}), "a": text("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")},
				{"a": {Type: storage.TypeInteger, State: storage.ValuePresent, Data: []byte("invalid")}},
			} {
				for _, complete := range []bool{false, true} {
					input := Input{Candidate: candidate(projects, "p1", record), Claims: Claims{"values": list(text("x"), text("y")), "suffix": text("🙂")}}
					input.Candidate.Complete = complete
					for limit, plan := range plans {
						differentialExpression(t, plan, input, limit)
					}
				}
			}
		})
	}
}

func TestNativeConstantsRemainLazyAndUseCELFallbackForErrors(t *testing.T) {
	for _, source := range []string{
		`true || item.a == decimal("invalid")`,
		`false ? item.a == decimal("invalid") : true`,
		`item.a == decimal("invalid")`,
	} {
		expression := compileExpression(t, source, false, 1000)
		if _, native := expression.(*fastExpression); native {
			t.Fatal("invalid constructor changed to compile-time failure or native constant")
		}
		differentialExpression(t, expression, Input{Candidate: candidate(projects, "p1", storage.Record{})}, 1000)
	}
}

func TestNativeMembershipDoesNotPublishPendingData(t *testing.T) {
	e := compile(t, Definitions{projects: {Visibility: `has(item.team_id) && item.team_id in claims.team_ids`}})
	input := Input{Candidate: candidate(projects, "p1", storage.Record{"team_id": text("t1")}), Claims: Claims{"team_ids": list(text("t1"))}}
	input.Candidate.Complete = false
	decision, err := e.Evaluate(context.Background(), input, nil)
	if err != nil || decision.Resolved || decision.Visible {
		t.Fatalf("pending entity granted: %+v %v", decision, err)
	}
	input.Candidate.Complete = true
	decision, err = e.Evaluate(context.Background(), input, nil)
	if err != nil || !decision.Resolved || !decision.Visible {
		t.Fatalf("resolved membership failed: %+v %v", decision, err)
	}
}

func TestNativeOperationsDoNotSilentlyDeoptimizeValidInputs(t *testing.T) {
	amount, _ := storage.ParseScalar(storage.TypeDecimal, []byte("12.50"))
	input := Input{Candidate: candidate(projects, "p1", storage.Record{
		"a": text("task-1"), "amount": amount, "values": list(text("task-1")),
		"flag":  {Type: storage.TypeBool, State: storage.ValuePresent, Data: []byte("true")},
		"props": object(storage.Record{"key": text("value")}),
	}), Claims: Claims{"values": list(text("task-1"))}}
	for _, source := range []string{
		`has(item.a)`, `item.a in claims.values`, `item.a in ["task-1","task-2"]`,
		`"key" in item.props`, `"key" in {"key": true}`,
		`size(item.values) == 1`, `size(item.props) == 1`, `item.a.size() == 6`,
		`item.a.startsWith("task-")`, `item.a.endsWith("1")`, `item.a.contains("ask")`,
		`item.values[0] == item.a`,
		`item.flag ? has(item.a) : false`,
	} {
		t.Run(source, func(t *testing.T) {
			plan := compileExpression(t, source, false, 10000).(*fastExpression)
			frame := fastFrame{state: &evaluation{ctx: context.Background(), input: input}, limit: 10000}
			result := plan.root.eval(&frame)
			if frame.deopt || frame.fatal != nil || !policyvalue.IsTrue(result) {
				t.Fatalf("native execution not used: result=%v deopt=%v error=%v", result, frame.deopt, frame.fatal)
			}
		})
	}
}
