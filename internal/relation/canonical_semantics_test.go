package relation

import (
	"context"
	"github.com/adiom-data/weave/internal/storage"
	"testing"
)

func TestCanonicalPolicySemanticsAcrossExecutionStrategies(t *testing.T) {
	uuid, err := storage.ParseScalar(storage.TypeUUID, []byte("00000000-0000-0000-0000-000000000001"))
	if err != nil {
		t.Fatal(err)
	}
	integer, _ := storage.ParseScalar(storage.TypeInteger, []byte("1"))
	floating, _ := storage.ParseScalar(storage.TypeFloat, []byte("1.0"))
	input := Input{Candidate: candidate(projects, "p", storage.Record{"integer": integer, "floating": floating, "uuid": uuid, "text": text(string(uuid.Data)), "ints": list(integer), "uuids": list(uuid)}), Claims: Claims{}}
	for _, test := range []struct {
		source  string
		want    bool
		failure bool
	}{
		{`item.integer == item.floating`, false, false},
		{`item.integer != item.floating`, true, false},
		{`item.integer < item.floating`, false, true},
		{`item.uuid == item.text`, false, false},
		{`item.text == item.uuid`, false, false},
		{`item.uuid < item.text`, false, true},
		{`item.integer in [1.0]`, false, false},
		{`1.0 in item.ints`, false, false},
		{`item.text in item.uuids`, false, false},
		{`item.ints == [1.0]`, false, false},
		{`item.ints == [1]`, true, false},
		{`item.ints.exists(x, x == 1.0)`, false, false},
		{`item.uuids.exists(x, x == item.text)`, false, false},
		{`item.ints.exists(x, x == 1)`, true, false},
		{`double(item.integer) == item.floating`, true, false},
		{`int(item.floating) == item.integer`, true, false},
		{`string(item.uuid) == item.text`, true, false},
		{`uuid(item.text) == item.uuid`, true, false},
		{`item.integer + 1 == 2`, true, false},
		{`item.integer + item.floating == 2`, false, true},
		{`dyn([1u]) == [1]`, false, false},
		{`dyn([1]) == [1u]`, false, false},
		{`dyn({"x":[1u]}) == {"x":[1]}`, false, false},
		{`dyn({1u:true}) == {1:true}`, false, false},
		{`item.integer in [1u]`, false, false},
		{`item.integer in {1u:true}`, false, false},
		{`1u in dyn({1:true})`, false, false},
		{`item.ints[0] == 1`, true, false},
		{`item.ints[0.0] == 1`, false, true},
		{`item.ints[0u] == 1`, false, true},
		{`dyn([item.integer])[0.0] == 1`, false, true},
		{`dyn({1:true})[1u] == true`, false, true},
		{`dyn({1:true})[1] == true`, true, false},
	} {
		t.Run(test.source, func(t *testing.T) {
			compiled := compileExpression(t, test.source, false, 10000)
			for _, expression := range []compiledExpression{compiled, referenceCEL(compiled)} {
				remaining := 10000
				state := evaluation{ctx: context.Background(), input: input, remaining: &remaining}
				got, err := state.eval(expression)
				if (err != nil) != test.failure || err == nil && (got == yes) != test.want {
					t.Fatalf("%T: result=%v error=%v; want=%v failure=%v", expression, got, err, test.want, test.failure)
				}
			}
		})
	}
}

func TestConversionsAndMathAlwaysSelectCEL(t *testing.T) {
	for _, source := range []string{`decimal("1") == decimal("1")`, `int(item.x) == 1`, `double(item.x) == 1.0`, `string(item.x) == "1"`, `uuid(item.x) == item.id`, `item.x + 1 == 2`, `item.x - 1 == 2`, `item.x * 2 == 4`, `item.x / 2 == 1`} {
		if _, native := compileExpression(t, source, false, 10000).(*fastExpression); native {
			t.Fatalf("conversion or arithmetic entered native execution: %s", source)
		}
	}
}
