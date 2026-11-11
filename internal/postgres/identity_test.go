package postgres

import (
	"testing"
	"time"

	"github.com/adiom-data/weave/internal/storage"
)

func TestIdentityRequiresRepresentableNativeEquality(t *testing.T) {
	for _, test := range []struct {
		name  string
		oid   uint32
		value storage.Value
		want  bool
	}{
		{"smallint_in_range", 21, storage.Value{Type: storage.TypeInteger, State: storage.ValuePresent, Data: []byte("32767")}, true},
		{"smallint_out_of_range", 21, storage.Value{Type: storage.TypeInteger, State: storage.ValuePresent, Data: []byte("32768")}, false},
		{"oid_negative", 26, storage.Value{Type: storage.TypeInteger, State: storage.ValuePresent, Data: []byte("-1")}, false},
		{"date_midnight", 1082, storage.NewDateTime(time.Date(2026, 11, 10, 0, 0, 0, 0, time.UTC)), true},
		{"date_noon", 1082, storage.NewDateTime(time.Date(2026, 11, 10, 12, 0, 0, 0, time.UTC)), false},
	} {
		t.Run(test.name, func(t *testing.T) {
			p := contractPrepared()
			p.plan.comparisons = make(map[*storage.Comparison]postgresComparison)
			p.plan.columns["id"] = column{name: "id", typeOID: test.oid, valueType: test.value.Type, supported: true, primary: true}
			literal, err := storage.LiteralFromValue(test.value)
			if err != nil {
				t.Fatal(err)
			}
			p.plan.query.Filter = &storage.Predicate{Comparison: &storage.Comparison{Field: "id", Operator: storage.ComparisonEqual, Value: literal}}
			if err := p.plan.preparePredicateComparisons(p.plan.query.Filter); err != nil {
				t.Fatal(err)
			}
			_, exact, err := p.PrepareIdentity()
			if err != nil || exact != test.want {
				t.Fatalf("identity=%v, want %v; err=%v", exact, test.want, err)
			}
		})
	}
}

func TestPrepareIdentityUsesOnlyGuaranteedPrimaryEqualities(t *testing.T) {
	eq := func(field, value string) *storage.Predicate {
		return &storage.Predicate{Comparison: &storage.Comparison{Field: storage.FieldPath(field), Operator: storage.ComparisonEqual, Value: storage.Literal{Type: storage.TypeInteger, Data: []byte(value)}}}
	}
	and := func(predicates ...*storage.Predicate) *storage.Predicate {
		return &storage.Predicate{Junction: &storage.Junction{Operator: storage.JunctionAnd, Operands: predicates}}
	}
	or := func(predicates ...*storage.Predicate) *storage.Predicate {
		return &storage.Predicate{Junction: &storage.Junction{Operator: storage.JunctionOr, Operands: predicates}}
	}
	for _, test := range []struct {
		name   string
		filter *storage.Predicate
		want   bool
	}{
		{"composite", and(eq("id", "10"), eq("tenant", "20")), true},
		{"extra_guard", and(eq("guard", "30"), and(eq("id", "10"), eq("tenant", "20"))), true},
		{"or_in_remaining_guard", and(eq("id", "10"), eq("tenant", "20"), or(eq("guard", "30"), eq("guard", "40"))), true},
		{"missing_component", eq("id", "10"), false},
		{"or_is_not_guaranteed", or(eq("id", "10"), eq("tenant", "20")), false},
		{"pk_in_one_or_branch", and(eq("tenant", "20"), or(eq("id", "10"), eq("guard", "30"))), false},
		{"no_predicate", nil, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			p := contractPrepared()
			p.plan.primary = []string{"tenant", "id"}
			p.plan.columns["tenant"] = column{name: "tenant", typeOID: 20, valueType: storage.TypeInteger, supported: true, primary: true}
			p.plan.columns["guard"] = column{name: "guard", typeOID: 20, valueType: storage.TypeInteger, supported: true}
			p.plan.query.Filter = test.filter
			p.plan.comparisons = make(map[*storage.Comparison]postgresComparison)
			if err := p.plan.preparePredicateComparisons(test.filter); err != nil {
				t.Fatal(err)
			}
			identity, matched, err := p.PrepareIdentity()
			if err != nil || matched != test.want {
				t.Fatalf("matched=%v err=%v", matched, err)
			}
			if !matched {
				return
			}
			if identity.Fingerprint() == p.Fingerprint() {
				t.Fatal("entity and window caches share an identity")
			}
			row := contractRecord()
			row["tenant"] = storage.Value{Type: storage.TypeInteger, State: storage.ValuePresent, Data: []byte("20")}
			row["guard"] = storage.Value{Type: storage.TypeInteger, State: storage.ValuePresent, Data: []byte("30")}
			key, err := p.EntityKey(row)
			if err != nil || key != identity.LookupKey() {
				t.Fatalf("identity=%v, expected=%v, err=%v", identity.LookupKey(), key, err)
			}
			if matched, err := identity.Matches(row); err != nil || !matched {
				t.Fatalf("guard: %v %v", matched, err)
			}
			if test.name == "extra_guard" {
				row["guard"] = storage.Value{Type: storage.TypeInteger, State: storage.ValuePresent, Data: []byte("99")}
				if matched, err := identity.Matches(row); err != nil || matched {
					t.Fatalf("discarded additional guard: %v %v", matched, err)
				}
			}
		})
	}
}
