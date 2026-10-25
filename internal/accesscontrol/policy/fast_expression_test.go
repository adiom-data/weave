package policy

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/adiom-data/weave/internal/accesscontrol"
	"github.com/adiom-data/weave/internal/storage"
)

func referenceCEL(expression compiledExpression) *celExpression {
	if fast, ok := expression.(*fastExpression); ok {
		return fast.fallback
	}
	return expression.(*celExpression)
}
func compileExpression(t *testing.T, source string, parent bool, limit int) compiledExpression {
	t.Helper()
	nodes := 0
	limits := DefaultLimits()
	limits.MaxEvaluationSteps = limit
	expression, err := compileCEL(source, parent, map[string]bool{"grant": true}, limits, &nodes)
	if err != nil {
		t.Fatal(err)
	}
	return expression
}
func TestExpressionStrategySelection(t *testing.T) {
	for _, test := range []struct {
		source string
		fast   bool
	}{
		{`true`, true}, {`!false`, true}, {`item.id == claims.user_id`, true},
		{`item.nested["weird.name"] > 1 && item.deleted_at == null`, true},
		{`item.a < item.b || item.a >= 1`, true},
		{`parent.id == item.parent_id`, true},
		{`has(item.id)`, true}, {`item[claims.key] == "x"`, false},
		{`item.values.exists(x,x == 1)`, false}, {`item.id.startsWith("p")`, true},
		{`item.a == "x" && facts.grant.exists(g,true)`, false},
		{`item.a == decimal("1")`, false}, {`item == claims`, false},
		{`item.array[0] == 1`, true},
		{`item.a in claims.values`, true}, {`item.a in ["a", "b"]`, true},
		{`size(item.values) > 0`, true}, {`item.flag ? item.a == "x" : false`, true},
		{`item.id.matches("^p")`, false}, {`item.a + 1 == 2`, false},
		{`item.a == decimal(claims.input)`, false},
		{`item.a == decimal("invalid")`, false},
		{`item.a in (item.flag ? item.values : claims.values)`, false},
		{`(item.flag ? "x" : "y") == item.a`, false},
		{`item.a == (item.flag ? item.a : item.other)`, false},
		{`item.a.startsWith(item.flag ? "x" : "y")`, false},
	} {
		t.Run(test.source, func(t *testing.T) {
			expression := compileExpression(t, test.source, strings.Contains(test.source, "parent."), 10000)
			_, fast := expression.(*fastExpression)
			if fast != test.fast {
				t.Fatalf("fast=%v, want %v", fast, test.fast)
			}
		})
	}
}

func differentialExpression(t *testing.T, expression compiledExpression, input Input, limit int) {
	t.Helper()
	var decisions [2]truth
	var failures [2]error
	var remaining [2]int
	for i, implementation := range []compiledExpression{expression, referenceCEL(expression)} {
		remaining[i] = limit
		state := evaluation{ctx: context.Background(), input: input, remaining: &remaining[i]}
		decisions[i], failures[i] = state.eval(implementation)
	}
	if (failures[0] == nil) != (failures[1] == nil) || failures[0] == nil && (decisions[0] != decisions[1] || remaining[0] != remaining[1]) {
		t.Fatalf("fast=%v %v remaining=%d; CEL=%v %v remaining=%d; input=%+v", decisions[0], failures[0], remaining[0], decisions[1], failures[1], remaining[1], input)
	}
	if failures[0] != nil && failures[1] != nil {
		// Execution strategies may report budget exhaustion with different
		// wording, but must not disguise a semantic error as a budget error.
		budget := func(err error) bool {
			return strings.Contains(err.Error(), "work limit exceeded") || strings.Contains(err.Error(), "cost limit exceeded")
		}
		if budget(failures[0]) != budget(failures[1]) || !budget(failures[0]) && failures[0].Error() != failures[1].Error() {
			t.Fatalf("error behavior differs: fast=%v; CEL=%v", failures[0], failures[1])
		}
	}
}

func TestExpressionCancellationPreservesContextError(t *testing.T) {
	expression := compileExpression(t, `item.id == claims.id`, false, 10000)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for _, implementation := range []compiledExpression{expression, referenceCEL(expression)} {
		remaining := 10000
		state := evaluation{ctx: ctx, remaining: &remaining}
		decision, err := state.eval(implementation)
		if !errors.Is(err, context.Canceled) || decision != no || remaining != 10000 {
			t.Fatalf("cancellation lost: decision=%v error=%v remaining=%d", decision, err, remaining)
		}
	}
}

func TestFastExpressionsMatchCELScalars(t *testing.T) {
	scalar := func(kind storage.ValueType, value string) storage.Value {
		v, err := storage.ParseScalar(kind, []byte(value))
		if err != nil {
			t.Fatal(err)
		}
		return v
	}
	values := []storage.Value{
		{State: storage.ValueNull}, text(""), text("2"), text("12345678901234567890🙂🙂🙂"),
		scalar(storage.TypeUUID, "00000000-0000-0000-0000-000000000001"),
		scalar(storage.TypeBool, "true"), scalar(storage.TypeBool, "false"),
		scalar(storage.TypeInteger, "-1"), scalar(storage.TypeInteger, "2"), scalar(storage.TypeInteger, "9007199254740993"),
		scalar(storage.TypeFloat, "2"), scalar(storage.TypeFloat, "NaN"), scalar(storage.TypeFloat, "Infinity"),
		scalar(storage.TypeFloat, "-Infinity"), scalar(storage.TypeFloat, fmt.Sprint(math.SmallestNonzeroFloat64)),
		scalar(storage.TypeDecimal, "2.0"), scalar(storage.TypeDecimal, "10"), scalar(storage.TypeDecimal, "NaN"),
		{Type: storage.TypeBytes, State: storage.ValuePresent, Data: []byte("abc")},
		storage.NewDateTime(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)),
	}
	for _, operator := range []string{"==", "!=", "<", "<=", ">", ">="} {
		expression := compileExpression(t, "item.a "+operator+" claims.b", false, 10000)
		for i, left := range values {
			for j, right := range values {
				t.Run(fmt.Sprintf("%s/%d/%d", operator, i, j), func(t *testing.T) {
					differentialExpression(t, expression, Input{Candidate: candidate(projects, "p1", storage.Record{"a": left}), Claims: accesscontrol.Claims{"b": right}}, 10000)
				})
			}
		}
	}
}

func TestFastExpressionPathsUnknownsErrorsAndBudgets(t *testing.T) {
	for _, source := range []string{
		`item.id == claims.id`, `parent.id == item.id`, `item.nested["a.b"] == claims.id`,
		`item.missing == null`, `item.missing.child == null`, `item.a == null`,
		`!false && (item.id == claims.id || item.missing == null)`,
		`item.flag && false`, `item.flag || true`, `!item.flag`,
		`item.flag && true`, `item.flag || false`, `!(item.flag && false)`,
		`item.a == item.b && true`, `item.a != item.b || false`,
		`item.a < item.b || true`, `item.a > item.b && false`,
		`item.a == "` + strings.Repeat("🙂", 50) + `"`,
		`item.a < "` + strings.Repeat("a", 100) + `"`,
		`item.a == b"abc"`, `item.a == 2u`,
	} {
		t.Run(source, func(t *testing.T) {
			expressions := map[int]compiledExpression{}
			for _, limit := range []int{1, 2, 3, 4, 5, 6, 7, 8, 10, 20, 10000} {
				expressions[limit] = compileExpression(t, source, true, limit)
			}
			for _, record := range []storage.Record{
				{}, {"id": text("u1"), "nested": object(storage.Record{"a.b": text("u1")})},
				{"a": object(storage.Record{"x": text("x")}), "b": object(storage.Record{"x": text("x")})},
				{"a": list(text("x")), "b": list(text("x"))},
				{"a": text(strings.Repeat("🙂", 50)), "b": text(strings.Repeat("a", 100))},
				{"a": {Type: storage.TypeInteger, State: storage.ValuePresent, Data: []byte("invalid")}},
				{"flag": text("not-a-bool")}, {"flag": {State: storage.ValueUnavailable}},
				{"flag": {State: storage.ValueNull}},
			} {
				for _, complete := range []bool{true, false} {
					input := Input{Candidate: candidate(projects, "p1", record), Claims: accesscontrol.Claims{"id": text("u1")}}
					input.Candidate.Complete = complete
					for _, parent := range []*ParentState{nil, {Candidate: candidate(projects, "p1", storage.Record{"id": text("u1")}), Visible: true}, {Candidate: accesscontrol.Candidate{Complete: false}}} {
						input.Parent = parent
						for limit, expression := range expressions {
							differentialExpression(t, expression, input, limit)
						}
					}
				}
			}
		})
	}
}

func FuzzFastExpressionAgainstCEL(f *testing.F) {
	sources := []string{
		`item.a == claims.b`, `item.a != claims.b`, `item.a < claims.b`, `item.a >= claims.b`,
		`(item.a == claims.b && item.flag) || item.other`,
		`!(item.a != claims.b || item.flag) && item.other`,
		`item.nested.a == claims.b || (item.flag && item.other)`,
		`item.a < "long comparison string for cost accounting" || item.other`,
		`has(item.a) && item.a in claims.values`,
		`item.a in ["x", "y", "🙂"]`,
		`"a" in item.props`,
		`item.values[0] == item.a`,
		`has(item.values[0].a)`,
		`size(item.values) > 0 && item.other`,
		`item.a.startsWith(claims.b) || item.other`,
		`item.a.contains(claims.b) && item.flag`,
		`item.a.endsWith("")`,
		`item.flag ? item.a == claims.b : has(item.a)`,
		`!(item.flag ? true : false)`,
		`item.a < decimal("10.00")`,
	}
	expressions := make([]compiledExpression, len(sources))
	for i, source := range sources {
		nodes := 0
		var err error
		expressions[i], err = compileCEL(source, false, nil, DefaultLimits(), &nodes)
		if err != nil {
			f.Fatal(err)
		}
	}
	for i := 0; i < 64; i++ {
		f.Add([]byte{byte(i), byte(i * 7), byte(i * 11), byte(i * 13), byte(i * 17), byte(i * 19)})
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) < 6 {
			return
		}
		value := func(b byte) storage.Value {
			switch b % 10 {
			case 0:
				return storage.Value{State: storage.ValueNull}
			case 1:
				return storage.Value{State: storage.ValueUnavailable}
			case 2:
				return storage.Value{Type: storage.TypeBool, State: storage.ValuePresent, Data: []byte("true")}
			case 3:
				return storage.Value{Type: storage.TypeBool, State: storage.ValuePresent, Data: []byte("false")}
			case 4:
				return storage.Value{Type: storage.TypeInteger, State: storage.ValuePresent, Data: []byte(fmt.Sprint(int(b) - 128))}
			case 5:
				return storage.Value{Type: storage.TypeFloat, State: storage.ValuePresent, Data: []byte("NaN")}
			case 6:
				return object(storage.Record{"x": text("x")})
			case 7:
				return list(text("x"))
			default:
				return text(strings.Repeat("🙂", int(b)%64))
			}
		}
		record := storage.Record{"a": value(data[1]), "nested": object(storage.Record{"a": value(data[1])}), "flag": value(data[3]), "other": value(data[4]), "values": list(value(data[1]), value(data[3])), "props": object(storage.Record{"a": value(data[2])})}
		input := Input{Candidate: candidate(projects, "p1", record), Claims: accesscontrol.Claims{"b": value(data[2]), "values": list(value(data[2]), value(data[4]))}}
		input.Candidate.Complete = data[5]&1 == 0
		differentialExpression(t, expressions[int(data[0])%len(expressions)], input, int(data[5])%64+1)
	})
}
