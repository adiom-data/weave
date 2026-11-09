package storage

import (
	"cmp"
	"math"
	"strconv"
	"testing"
)

func TestIntegerComparisonUsesTheFullInt64Range(t *testing.T) {
	values := []int64{-9223372036854775808, -9007199254740993, -10, -1, 0, 1, 10, 9007199254740992, 9007199254740993, 9223372036854775807}
	for _, a := range values {
		for _, b := range values {
			left := Value{Type: TypeInteger, State: ValuePresent, Data: []byte(strconv.FormatInt(a, 10))}
			right := Value{Type: TypeInteger, State: ValuePresent, Data: []byte(strconv.FormatInt(b, 10))}
			got, ok := CompareValues(left, right)
			if !ok || got != cmp.Compare(a, b) {
				t.Fatalf("compare %d and %d = %d, %v", a, b, got, ok)
			}
		}
	}
	for _, invalid := range []string{"1/2", "1.0", "NaN", "Infinity", "9223372036854775808", "-9223372036854775809"} {
		left := Value{Type: TypeInteger, State: ValuePresent, Data: []byte(invalid)}
		right := Value{Type: TypeInteger, State: ValuePresent, Data: []byte("0")}
		if _, ok := CompareValues(left, right); ok {
			t.Fatalf("unsupported integer %q was comparable", invalid)
		}
	}
}

func BenchmarkCompareIntegerValues(b *testing.B) {
	left := Value{Type: TypeInteger, State: ValuePresent, Data: []byte("9007199254740992")}
	right := Value{Type: TypeInteger, State: ValuePresent, Data: []byte("9007199254740993")}
	b.ReportAllocs()
	for b.Loop() {
		if compared, ok := CompareValues(left, right); !ok || compared != -1 {
			b.Fatal("integer comparison lost precision")
		}
	}
}

func TestCanonicalEquivalentValuesHaveIdenticalRepresentations(t *testing.T) {
	tests := []struct {
		valueType ValueType
		values    []string
	}{
		{TypeBool, []string{"t", "true"}},
		{TypeInteger, []string{"001", "+1", "1"}},
		{TypeDecimal, []string{"1", "1.0", "01.000", "0.1e1"}},
		{TypeFloat, []string{"0", "-0", "0.0"}},
		{TypeUUID, []string{"10000000-0000-4000-8000-000000000001", "10000000000040008000000000000001"}},
		{TypeDateTime, []string{"2026-10-23T12:00:00Z", "2026-10-23T05:00:00-07:00"}},
	}
	for _, test := range tests {
		want, err := ParseScalar(test.valueType, []byte(test.values[0]))
		if err != nil {
			t.Fatalf("canonicalize %s %q: %v", test.valueType, test.values[0], err)
		}
		for _, input := range test.values[1:] {
			got, err := ParseScalar(test.valueType, []byte(input))
			if err != nil {
				t.Fatalf("canonicalize %s %q: %v", test.valueType, input, err)
			}
			if !EqualValues(got, want) {
				t.Errorf("canonical %s %q = %+v, want %+v", test.valueType, input, got, want)
			}
		}
	}
}

func TestNativeFloatConstructionMatchesScalarParsing(t *testing.T) {
	for _, test := range []struct {
		name  string
		value float64
		want  string
	}{
		{"positive infinity", math.Inf(1), "Infinity"},
		{"negative infinity", math.Inf(-1), "-Infinity"},
		{"NaN", math.NaN(), "NaN"},
		{"negative zero", math.Copysign(0, -1), "0"},
		{"zero", 0, "0"},
		{"fraction", 1.5, "1.5"},
		{"subnormal", math.SmallestNonzeroFloat64, "5e-324"},
		{"maximum", math.MaxFloat64, "1.7976931348623157e+308"},
	} {
		t.Run(test.name, func(t *testing.T) {
			got := NewFloat(test.value)
			if string(got.Data) != test.want {
				t.Fatalf("encoding = %q, want %q", got.Data, test.want)
			}
			if err := got.ValidateComplete(); err != nil {
				t.Fatal(err)
			}
			// The textual path must agree even when Go uses another spelling.
			parsed, err := ParseScalar(TypeFloat, []byte(strconv.FormatFloat(test.value, 'g', -1, 64)))
			if err != nil || !EqualValues(got, parsed) {
				t.Fatalf("native=%+v parsed=%+v error=%v", got, parsed, err)
			}
		})
	}
}

func TestCompareCanonicalValues(t *testing.T) {
	value := func(valueType ValueType, text string) Value {
		canonical, err := ParseScalar(valueType, []byte(text))
		if err != nil {
			t.Fatal(err)
		}
		return canonical
	}
	if compared, ok := CompareValues(value(TypeDecimal, "1.00"), value(TypeDecimal, "1")); !ok || compared != 0 {
		t.Fatalf("decimal comparison = %d, %v", compared, ok)
	}
	if compared, ok := CompareValues(value(TypeDecimal, "10"), value(TypeDecimal, "20")); !ok || compared >= 0 {
		t.Fatalf("decimal exponent comparison = %d, %v", compared, ok)
	}
	if compared, ok := CompareValues(value(TypeDecimal, "1e1000000"), value(TypeDecimal, "9e999999")); !ok || compared <= 0 {
		t.Fatalf("large decimal exponent comparison = %d, %v", compared, ok)
	}
	if compared, ok := CompareValues(value(TypeDateTime, "2026-10-23T05:00:00-07:00"), value(TypeDateTime, "2026-10-23T12:00:01Z")); !ok || compared >= 0 {
		t.Fatalf("timestamp comparison = %d, %v", compared, ok)
	}
}

func TestDateTimeRejectsInvalidCalendarAndClockValues(t *testing.T) {
	invalid := []string{
		"2023-02-29T00:00:00Z",
		"2024-02-30T00:00:00Z",
		"2024-04-31T00:00:00Z",
		"2024-01-01T24:00:00.1Z",
	}
	for _, input := range invalid {
		if _, err := ParseDateTime(input); err == nil {
			t.Errorf("invalid datetime %q was accepted", input)
		}
	}
	valid := []string{
		"2024-02-29T00:00:00Z",
		"2000-02-29T23:59:59.999999999Z",
		"1900-02-28T00:00:00Z",
	}
	for _, input := range valid {
		if _, err := ParseDateTime(input); err != nil {
			t.Errorf("valid datetime %q was rejected: %v", input, err)
		}
	}
}

func TestFieldPathPreservesExactObjectKeys(t *testing.T) {
	path := MustFieldPath("properties", "a.b", "items[0]")
	segments := path.Segments()
	if len(segments) != 3 || segments[1] != "a.b" || segments[2] != "items[0]" {
		t.Fatalf("segments = %#v", segments)
	}
	record := Record{"properties": {Type: TypeObject, State: ValuePresent, Object: map[string]Value{
		"a.b": {Type: TypeObject, State: ValuePresent, Object: map[string]Value{
			"items[0]": {Type: TypeText, State: ValuePresent, Data: []byte("found")},
		}},
	}}}
	value, ok := Lookup(record, path)
	if !ok || string(value.Data) != "found" {
		t.Fatalf("lookup = %+v, %v", value, ok)
	}
}

func TestFieldPathValidationRejectsMalformedInternalEncoding(t *testing.T) {
	if err := FieldPath("\x00not-json").Validate(); err == nil {
		t.Fatal("malformed encoded path was accepted")
	}
	if err := FieldPath("").Validate(); err == nil {
		t.Fatal("empty path was accepted")
	}
}

func TestRecursiveObjectIdentityIgnoresMapIterationOrder(t *testing.T) {
	left := Value{Type: TypeObject, State: ValuePresent, Object: map[string]Value{
		"b": {Type: TypeInteger, State: ValuePresent, Data: []byte("2")},
		"a": {Type: TypeList, State: ValuePresent, List: []Value{{Type: TypeText, State: ValuePresent, Data: []byte("x")}}},
	}}
	right := Value{Type: TypeObject, State: ValuePresent, Object: map[string]Value{
		"a": {Type: TypeList, State: ValuePresent, List: []Value{{Type: TypeText, State: ValuePresent, Data: []byte("x")}}},
		"b": {Type: TypeInteger, State: ValuePresent, Data: []byte("2")},
	}}
	if !EqualValues(left, right) {
		t.Fatalf("equivalent objects differed: %+v != %+v", left, right)
	}
}

func TestCloneValueDeepCopiesStructuredValues(t *testing.T) {
	original := Value{
		Type:  TypeObject,
		State: ValuePresent,
		Object: map[string]Value{
			"items": {
				Type:  TypeList,
				State: ValuePresent,
				List:  []Value{{Type: TypeText, State: ValuePresent, Data: []byte("original")}},
			},
		},
	}

	cloned := CloneValue(original)
	if !EqualValues(original, cloned) {
		t.Fatalf("clone differs from original: %#v", cloned)
	}

	item := cloned.Object["items"]
	item.List[0].Data[0] = 'O'
	item.List = append(item.List, Value{Type: TypeText, State: ValuePresent, Data: []byte("second")})
	cloned.Object["items"] = item
	cloned.Object["new"] = Value{Type: TypeBool, State: ValuePresent, Data: []byte("true")}

	originalItems := original.Object["items"].List
	if got := string(originalItems[0].Data); got != "original" {
		t.Fatalf("clone mutated original data: %q", got)
	}
	if len(originalItems) != 1 {
		t.Fatalf("clone mutated original list length: %d", len(originalItems))
	}
	if _, exists := original.Object["new"]; exists {
		t.Fatal("clone mutated original object map")
	}
}

func TestEvaluatePredicateUsesStrictTypes(t *testing.T) {
	record := Record{"A": {Type: TypeInteger, State: ValuePresent, Data: []byte("123")}}
	comparison := func(operator ComparisonOperator, valueType ValueType) *Predicate {
		return &Predicate{Comparison: &Comparison{Field: "A", Operator: operator, Value: Literal{Type: valueType, Data: []byte("123")}}}
	}
	if matched, err := EvaluatePredicate(comparison(ComparisonEqual, TypeText), record); err != nil || matched {
		t.Fatalf("integer equaled text: matched=%v err=%v", matched, err)
	}
	if matched, err := EvaluatePredicate(comparison(ComparisonNotEqual, TypeText), record); err != nil || !matched {
		t.Fatalf("integer was not unequal to text: matched=%v err=%v", matched, err)
	}
	if matched, err := EvaluatePredicate(comparison(ComparisonEqual, TypeInteger), record); err != nil || !matched {
		t.Fatalf("integer did not equal integer: matched=%v err=%v", matched, err)
	}
}

func TestIntegerIsSigned64Bit(t *testing.T) {
	if _, err := Canonicalize(TypeInteger, []byte("9223372036854775807")); err != nil {
		t.Fatal(err)
	}
	if _, err := Canonicalize(TypeInteger, []byte("9223372036854775808")); err == nil {
		t.Fatal("integer above int64 range was accepted")
	}
}

func TestValueValidationRejectsMalformedLogicalValues(t *testing.T) {
	tests := []Value{
		{Type: TypeInteger, State: ValuePresent, Data: []byte("01")},
		{Type: TypeObject, State: ValuePresent, Data: []byte("payload")},
		{Type: TypeList, State: ValuePresent, List: []Value{{Type: "unknown", State: ValuePresent}}},
		{State: ValueUnavailable, Data: []byte("native")},
	}
	for _, value := range tests {
		if err := value.Validate(); err == nil {
			t.Fatalf("malformed value was accepted: %+v", value)
		}
	}
	valid := Value{Type: TypeObject, State: ValuePresent, Object: map[string]Value{
		"count": {Type: TypeInteger, State: ValuePresent, Data: []byte("1")},
	}}
	if err := valid.Validate(); err != nil {
		t.Fatalf("valid recursive value rejected: %v", err)
	}
	incomplete := Value{Type: TypeObject, State: ValuePresent, Object: map[string]Value{
		"toasted": {State: ValueUnavailable},
	}}
	if err := incomplete.Validate(); err != nil {
		t.Fatalf("well-formed partial value rejected: %v", err)
	}
	if err := incomplete.ValidateComplete(); err == nil {
		t.Fatal("recursive unavailable value was accepted as complete")
	}
}
