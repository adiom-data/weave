package relation

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"cel.dev/cel-go/cel"
	"github.com/adiom-data/weave/internal/storage"
)

func TestCELRecordReadsDoNotAlterStructuralMapSemantics(t *testing.T) {
	null := storage.Value{State: storage.ValueNull}
	for _, test := range []struct {
		expression string
		want       bool
	}{
		{`item == claims`, false},
		{`claims == item`, false},
		{`item != claims`, true},
		{`item == {"b": "non-null"}`, false},
		{`{"b": "non-null"} == item`, false},
		{`item == {"a": null}`, true},
		{`{"a": null} == item`, true},
		{`[item] == [{"b": "non-null"}]`, false},
		{`[{"b": "non-null"}] == [item]`, false},
		{`{"nested": item} == {"nested": {"b": "non-null"}}`, false},
		{`{"nested": {"b": "non-null"}} == {"nested": item}`, false},
		{`item.missing == null`, true},
		{`item[claims.b] == null`, true},
		{`item[claims.b + "-suffix"] == null`, true},
		{`[item][0].missing == null`, true},
		{`[item].exists(row, row.missing == null)`, true},
		{`{"nested": item}.nested.missing == null`, true},
		{`(true ? item : claims).missing == null`, true},
		{`has(item.missing)`, false},
		{`has(item.a)`, true},
		{`"missing" in item`, false},
		{`"a" in item`, true},
		{`size(item) == 1`, true},
	} {
		t.Run(test.expression, func(t *testing.T) {
			e := compile(t, Definitions{projects: {Visibility: test.expression}})
			got, err := e.Evaluate(context.Background(), Input{
				Candidate: candidate(projects, "p1", storage.Record{"a": null}),
				Claims: Claims{
					"b": text("non-null"),
				},
			}, nil)
			if err != nil || !got.Resolved || got.Visible != test.want {
				t.Fatalf("got %+v, %v; want visible=%v", got, err, test.want)
			}
		})
	}
}

func TestCELRecordDefaultsDoNotHideOtherAccessErrors(t *testing.T) {
	for _, expression := range []string{
		`{"a": null}.missing == null`, // ordinary CEL map literals remain strict
		`[item][3].missing == null`,
		`item.a.missing == null`, // null is not a record
	} {
		t.Run(expression, func(t *testing.T) {
			e := compile(t, Definitions{projects: {Visibility: expression}})
			_, err := e.Evaluate(context.Background(), Input{Candidate: candidate(projects, "p1", storage.Record{"a": {State: storage.ValueNull}})}, nil)
			if err == nil {
				t.Fatal("access error became permission or denial")
			}
		})
	}
}

func TestCELDecimalsUseExactCanonicalComparisons(t *testing.T) {
	decimal := func(value string) storage.Value {
		t.Helper()
		parsed, err := storage.ParseScalar(storage.TypeDecimal, []byte(value))
		if err != nil {
			t.Fatal(err)
		}
		return parsed
	}
	for _, test := range []struct {
		left, right, expression string
		want                    bool
	}{
		{"2", "10", `item.left > item.right`, false},
		{"2", "10", `item.left < item.right`, true},
		{"2", "10", `item.left <= item.right`, true},
		{"2", "10", `item.left >= item.right`, false},
		{"10", "2", `item.left > item.right`, true},
		{"1.00", "1e0", `item.left == item.right`, true},
		{"1.00", "1e0", `item.left != item.right`, false},
		{"-10", "-2", `item.left < item.right`, true},
		{"9007199254740993", "9007199254740992", `item.left > item.right`, true},
		{"1.00000000000000000000000000001", "1", `item.left > item.right`, true},
		{"1e1000000", "9e999999", `item.left > item.right`, true},
		{"Infinity", "1e1000000", `item.left > item.right`, true},
		{"-Infinity", "-1e1000000", `item.left < item.right`, true},
		{"NaN", "Infinity", `item.left > item.right`, true}, // canonical total order
		{"NaN", "NaN", `item.left == item.right`, true},
		{"2", "10", `item.left < decimal("10")`, true},
		{"2", "10", `decimal("10") > item.left`, true},
		{"2", "10", `decimal("2") < decimal("10")`, true},
		{"2", "10", `decimal("1.0") == decimal("1")`, true},
		{"2", "10", `item.left == "2"`, false},
		{"2", "10", `"2" == item.left`, false},
		{"2", "10", `item.left == 2`, false},
		{"2", "10", `2 == item.left`, false},
		{"2", "10", `[item.left] == [decimal("2")]`, true},
		{"2", "10", `item.left in [decimal("2")]`, true},
		{"2", "10", `type(item.left) == weave.Decimal`, true},
		{"2", "2.00", `item.left in {item.right: true}`, true},
		{"2", "10", `item.left in {item.right: true}`, false},
	} {
		t.Run(test.left+"/"+test.right+"/"+test.expression, func(t *testing.T) {
			e := compile(t, Definitions{projects: {Visibility: test.expression}})
			got, err := e.Evaluate(context.Background(), Input{Candidate: candidate(projects, "p1", storage.Record{"left": decimal(test.left), "right": decimal(test.right)})}, nil)
			if err != nil || !got.Resolved || got.Visible != test.want {
				t.Fatalf("got %+v %v; want visible=%v", got, err, test.want)
			}
		})
	}
}

func TestRecordAccessPreservesCELCost(t *testing.T) {
	for _, depth := range []int{1, 2, 4, 8, 16, 32} {
		t.Run(fmt.Sprint(depth), func(t *testing.T) {
			record := storage.Record{"x": {Type: storage.TypeInteger, State: storage.ValuePresent, Data: []byte("1")}}
			for n := 1; n < depth; n++ {
				record = storage.Record{"x": object(record)}
			}
			assertRecordAccessCost(t, "item."+strings.Repeat("x.", depth-1)+"x == 1", record)
		})
	}
	for _, expression := range []string{
		`has(item.x)`, `has(item.absent)`, `size(item) == 1`,
		`item["x"] == 1`, `item[item.key] == 1`,
		`(true ? item : item).x == 1`,
		`[item].exists(row, row.x == 1)`,
		`{"nested": item}.nested.x == 1`,
	} {
		t.Run(expression, func(t *testing.T) {
			assertRecordAccessCost(t, expression, storage.Record{
				"x":   {Type: storage.TypeInteger, State: storage.ValuePresent, Data: []byte("1")},
				"key": text("x"),
			})
		})
	}
}

func assertRecordAccessCost(t *testing.T, expression string, record storage.Record) {
	t.Helper()
	env, err := cel.NewEnv(cel.Variable("item", cel.DynType))
	if err != nil {
		t.Fatal(err)
	}
	ast, issues := env.Compile(expression)
	if issues.Err() != nil {
		t.Fatal(issues.Err())
	}
	var nativeCost uint64
	for _, decorated := range []bool{false, true} {
		options := []cel.ProgramOption{cel.CostTracking(nil)}
		if decorated {
			options = append(options, cel.CustomDecoratorV2(recordAccessDecorator()))
		}
		program, err := env.Program(ast, options...)
		if err != nil {
			t.Fatal(err)
		}
		_, details, err := program.ContextEval(context.Background(), map[string]any{"item": recordValue(record)})
		if err != nil {
			t.Fatal(err)
		}
		cost := *details.ActualCost()
		if !decorated {
			nativeCost = cost
		} else if cost != nativeCost {
			t.Fatalf("record access cost=%d, native CEL cost=%d", cost, nativeCost)
		}
	}
}

func TestCELFactRecordsUseSameDecimalAndMapSemantics(t *testing.T) {
	amount, err := storage.ParseScalar(storage.TypeDecimal, []byte("2"))
	if err != nil {
		t.Fatal(err)
	}
	e := compile(t, Definitions{projects: {
		Facts: []Fact{grantFact()}, Visibility: `facts.grant.exists(g,
			g.item.amount < decimal("10") && g.item.optional == null &&
			!has(g.item.optional) && g.item != {"different": null, "amount": decimal("2")})`,
	}})
	decision, err := e.Evaluate(context.Background(), Input{Candidate: candidate(projects, "p1", storage.Record{"id": text("p1")})}, resolveFunc(func(context.Context, FactRequest) (FactResult, error) {
		return FactResult{Value: list(object(storage.Record{"item": object(storage.Record{"amount": amount, "name": {State: storage.ValueNull}})})), Complete: true}, nil
	}))
	if err != nil || !decision.Resolved || !decision.Visible {
		t.Fatalf("fact value semantics diverged: %+v %v", decision, err)
	}
}

func TestCELDecimalsRejectUnsupportedOperations(t *testing.T) {
	value, err := storage.ParseScalar(storage.TypeDecimal, []byte("2"))
	if err != nil {
		t.Fatal(err)
	}
	for _, expression := range []string{
		`item.amount > "1"`, `"1" < item.amount`,
		`item.amount > 1`, `1 < item.amount`,
		`item.amount + item.amount > item.amount`,
		`double(item.amount) > 1.0`,
		`item.amount == decimal("invalid")`,
		`{item.amount: true}[item.amount]`, // CEL qualifiers do not support decimal keys
	} {
		t.Run(expression, func(t *testing.T) {
			e := compile(t, Definitions{projects: {Visibility: expression}})
			_, err := e.Evaluate(context.Background(), Input{Candidate: candidate(projects, "p1", storage.Record{"amount": value})}, nil)
			if err == nil {
				t.Fatal("unsupported decimal operation did not fail")
			}
			if strings.Contains(err.Error(), "internal error:") {
				t.Fatalf("unsupported operation panicked instead of returning a policy error: %v", err)
			}
		})
	}
}
