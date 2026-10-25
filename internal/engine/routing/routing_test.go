package routing

import (
	"testing"

	"github.com/adiom-data/weave/internal/storage"
)

func TestDecimalEqualityUsesCanonicalValue(t *testing.T) {
	relation := storage.RelationKey{Source: "test", Namespace: "app", Name: "items", OpaqueID: "42"}
	literalCanonical, err := storage.Canonicalize(storage.TypeDecimal, []byte("1"))
	if err != nil {
		t.Fatal(err)
	}
	plan := Compile(relation, storage.QuerySpec{Filter: &storage.Predicate{Comparison: &storage.Comparison{
		Field: "amount", Operator: storage.ComparisonEqual,
		Value: storage.Literal{Type: storage.TypeDecimal, Data: literalCanonical},
	}}})
	rowCanonical, err := storage.Canonicalize(storage.TypeDecimal, []byte("1.00"))
	if err != nil {
		t.Fatal(err)
	}
	tokens, _, complete := Probe(relation, "amount", storage.Value{Type: storage.TypeDecimal, State: storage.ValuePresent, Data: rowCanonical})
	if !complete || len(plan.Exact) != 1 || len(tokens) < 1 || plan.Exact[0].Token != tokens[0] {
		t.Fatalf("equality registration=%+v tokens=%v complete=%v", plan.Exact, tokens, complete)
	}
}

func TestCompileIndexesEveryPredicateLeaf(t *testing.T) {
	relation := storage.RelationKey{Source: "test", Name: "items"}
	literal := func(value string) storage.Literal {
		return storage.Literal{Type: storage.TypeInteger, Data: []byte(value)}
	}
	comparison := func(field storage.FieldPath, operator storage.ComparisonOperator, value string) *storage.Predicate {
		return &storage.Predicate{Comparison: &storage.Comparison{Field: field, Operator: operator, Value: literal(value)}}
	}
	query := storage.QuerySpec{
		Filter: &storage.Predicate{Junction: &storage.Junction{Operator: storage.JunctionAnd, Operands: []*storage.Predicate{
			comparison("A", storage.ComparisonEqual, "1"),
			comparison("B", storage.ComparisonNotEqual, "2"),
			{Junction: &storage.Junction{Operator: storage.JunctionOr, Operands: []*storage.Predicate{
				comparison("C", storage.ComparisonGreater, "10"), comparison("C", storage.ComparisonLess, "20"),
			}}},
		}}},
		Order: []storage.OrderTerm{{Field: "position"}},
	}
	plan := Compile(relation, query)
	if plan.Fallback || len(plan.Exact) != 1 || len(plan.WatchFields) != 1 || len(plan.Ranges) != 2 || len(plan.OrderFields) != 1 {
		t.Fatalf("routing plan = %+v", plan)
	}
}
