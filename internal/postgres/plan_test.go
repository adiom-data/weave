package postgres

import (
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/adiom-data/weave/internal/entitykey"
	"github.com/adiom-data/weave/internal/storage"
)

func TestResolveQueryOrderDefaultsToPrimaryKey(t *testing.T) {
	query := storage.QuerySpec{Relation: storage.RelationName{Namespace: "app", Name: "items"}}
	order := resolveQueryOrder(&query, []string{"tenant_id", "id"})
	want := []storage.OrderTerm{{Field: "tenant_id"}, {Field: "id"}}
	if !reflect.DeepEqual(order, want) || !reflect.DeepEqual(query.Order, want) {
		t.Fatalf("resolved order = %v, query order = %v; want %v", order, query.Order, want)
	}
	order[0].Descending = true
	if query.Order[0].Descending {
		t.Fatal("resolved order aliases the prepared query")
	}
}

func TestOrderedPrimaryNamesUsesIndexOrder(t *testing.T) {
	names, err := orderedPrimaryNames([]primaryColumn{
		{name: "id", position: 2},
		{name: "tenant_id", position: 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(names, []string{"tenant_id", "id"}) {
		t.Fatalf("primary names = %v", names)
	}
	if _, err := orderedPrimaryNames([]primaryColumn{{name: "id", position: 2}}); err == nil {
		t.Fatal("non-contiguous primary-key positions were accepted")
	}
}

func TestPostgresEntityIdentityIsIndependentOfPrimaryIndexOrder(t *testing.T) {
	columns := map[string]column{
		"id":        {name: "id", typeOID: 20, valueType: storage.TypeInteger, supported: true, primary: true},
		"tenant_id": {name: "tenant_id", typeOID: 20, valueType: storage.TypeInteger, supported: true, primary: true},
	}
	record := storage.Record{
		"id":        {Type: storage.TypeInteger, State: storage.ValuePresent, Data: []byte("20")},
		"tenant_id": {Type: storage.TypeInteger, State: storage.ValuePresent, Data: []byte("10")},
	}
	prepared := &postgresPrepared{plan: &plan{
		schema: "app", table: "items", relationID: 42,
		primary: []string{"tenant_id", "id"}, columns: columns,
	}}
	key, err := prepared.EntityKey(record)
	if err != nil {
		t.Fatal(err)
	}
	physicalOrder := []entitykey.Part{
		{Name: "id", TypeOID: 20, Value: []byte("20")},
		{Name: "tenant_id", TypeOID: 20, Value: []byte("10")},
	}
	if key.Value != entitykey.CanonicalEncode(physicalOrder) {
		t.Fatal("snapshot and canonical WAL identities differ")
	}
	// Refetch also accepts named identity components without assuming their
	// encoded order matches the primary index declaration.
	decoded, err := prepared.plan.decodeIdentity(entitykey.Encode(physicalOrder))
	if err != nil {
		t.Fatal(err)
	}
	if string(decoded["id"].Value) != "20" || string(decoded["tenant_id"].Value) != "10" {
		t.Fatalf("decoded identity = %+v", decoded)
	}
}

func TestResolveQueryOrderPreservesExplicitOrder(t *testing.T) {
	want := []storage.OrderTerm{{Field: "position", Descending: true}}
	query := storage.QuerySpec{Order: want}
	order := resolveQueryOrder(&query, []string{"id"})
	if !reflect.DeepEqual(order, want) || !reflect.DeepEqual(query.Order, want) {
		t.Fatalf("resolved order = %v, query order = %v; want %v", order, query.Order, want)
	}
}

func TestResolvedDefaultOrderSharesExplicitFingerprint(t *testing.T) {
	omitted := storage.QuerySpec{Relation: storage.RelationName{Namespace: "app", Name: "items"}}
	resolveQueryOrder(&omitted, []string{"tenant_id", "id"})
	explicit := storage.QuerySpec{
		Relation: omitted.Relation,
		Order:    []storage.OrderTerm{{Field: "tenant_id"}, {Field: "id"}},
	}
	left, err := queryFingerprint(omitted, 42)
	if err != nil {
		t.Fatal(err)
	}
	right, err := queryFingerprint(explicit, 42)
	if err != nil {
		t.Fatal(err)
	}
	if left != right {
		t.Fatal("omitted and explicitly equivalent primary-key orders did not share a fingerprint")
	}
}

func TestQueryFingerprintIncludesBoundValues(t *testing.T) {
	fingerprint := func(query storage.QuerySpec, relationID uint32) string {
		result, err := queryFingerprint(query, relationID)
		if err != nil {
			t.Fatal(err)
		}
		return result
	}
	base := storage.QuerySpec{
		Relation: storage.RelationName{Namespace: "app", Name: "tasks"},
		Filter:   &storage.Predicate{Comparison: &storage.Comparison{Field: "project_id", Operator: storage.ComparisonEqual, Value: storage.Literal{Data: []byte("a")}}},
		Order:    []storage.OrderTerm{{Field: "position"}, {Field: "id"}},
	}
	otherValue := base
	otherValue.Filter = &storage.Predicate{Comparison: &storage.Comparison{Field: "project_id", Operator: storage.ComparisonEqual, Value: storage.Literal{Data: []byte("b")}}}
	if fingerprint(base, 42) == fingerprint(otherValue, 42) {
		t.Fatal("bound predicate value did not change the query fingerprint")
	}
	if fingerprint(base, 42) == fingerprint(base, 43) {
		t.Fatal("recreated relation did not change the query fingerprint")
	}
}

func TestQueryFingerprintUsesCanonicalPredicateValue(t *testing.T) {
	query := func(text string) storage.QuerySpec {
		canonical, err := storage.Canonicalize(storage.TypeDecimal, []byte(text))
		if err != nil {
			t.Fatal(err)
		}
		return storage.QuerySpec{
			Relation: storage.RelationName{Namespace: "app", Name: "items"},
			Filter: &storage.Predicate{Comparison: &storage.Comparison{Field: "amount", Operator: storage.ComparisonEqual, Value: storage.Literal{
				Type: storage.TypeDecimal, Data: canonical,
			}}},
			Order: []storage.OrderTerm{{Field: "id"}},
		}
	}
	left, err := queryFingerprint(query("1"), 42)
	if err != nil {
		t.Fatal(err)
	}
	right, err := queryFingerprint(query("1.00"), 42)
	if err != nil {
		t.Fatal(err)
	}
	if left != right {
		t.Fatal("semantically equal decimal predicates did not share a fingerprint")
	}
}

func TestQueryFingerprintSupportsExtendedDatetimes(t *testing.T) {
	query := func(year int) storage.QuerySpec {
		return storage.QuerySpec{
			Relation: storage.RelationName{Namespace: "app", Name: "items"},
			Filter: &storage.Predicate{Comparison: &storage.Comparison{
				Field: "created_at", Operator: storage.ComparisonEqual,
				Value: storage.Literal{Type: storage.TypeDateTime, DateTime: time.Date(year, 1, 1, 0, 0, 0, 0, time.UTC)},
			}},
			Order: []storage.OrderTerm{{Field: "id"}},
		}
	}
	left, err := queryFingerprint(query(-100), 42)
	if err != nil {
		t.Fatal(err)
	}
	right, err := queryFingerprint(query(10000), 42)
	if err != nil {
		t.Fatal(err)
	}
	if left == right {
		t.Fatal("distinct extended datetimes shared a query fingerprint")
	}
}

func TestPredicateSQLPreservesTypedMismatchAndStructuredJSONPath(t *testing.T) {
	p := &plan{adapter: newPostgresTypeAdapter(), columns: map[string]column{
		"count":      {name: "count", typeOID: 23, valueType: storage.TypeInteger, supported: true},
		"properties": {name: "properties", typeOID: 3802, valueType: storage.TypeObject, supported: true},
	}}
	args := []any{}
	mismatch := &storage.Predicate{Comparison: &storage.Comparison{Field: "count", Operator: storage.ComparisonEqual, Value: storage.Literal{Type: storage.TypeText, Data: []byte("123")}}}
	preparePredicateForSQL(t, p, mismatch)
	query, err := p.predicateSQL(mismatch, &args)
	if err != nil || query != "FALSE" || len(args) != 0 {
		t.Fatalf("mismatch SQL = %q args=%v err=%v", query, args, err)
	}

	args = nil
	path := storage.MustFieldPath("properties", "a.b", "priority")
	nested := &storage.Predicate{Comparison: &storage.Comparison{Field: path, Operator: storage.ComparisonEqual, Value: storage.Literal{Type: storage.TypeInteger, Data: []byte("2")}}}
	preparePredicateForSQL(t, p, nested)
	query, err = p.predicateSQL(nested, &args)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(query, `"properties" -> $1::text -> $2::text`) || len(args) != 3 {
		t.Fatalf("nested SQL = %q args=%#v", query, args)
	}
	if !strings.Contains(query, "CASE WHEN") {
		t.Fatalf("nested JSON cast is not structurally guarded: %q", query)
	}
	if args[0] != "a.b" || args[1] != "priority" {
		t.Fatalf("path arguments = %#v", args)
	}
}

func TestBoundedIntegerPredicateKeepsIndexedColumnUncast(t *testing.T) {
	p := &plan{adapter: newPostgresTypeAdapter(), columns: map[string]column{
		"count": {name: "count", typeOID: 23, valueType: storage.TypeInteger, supported: true},
	}}
	args := []any{}
	predicate := &storage.Predicate{Comparison: &storage.Comparison{
		Field: "count", Operator: storage.ComparisonGreaterOrEqual,
		Value: storage.Literal{Type: storage.TypeInteger, Data: []byte("123")},
	}}
	preparePredicateForSQL(t, p, predicate)
	query, err := p.predicateSQL(predicate, &args)
	if err != nil || query != `"count" >= $1::integer` || len(args) != 1 {
		t.Fatalf("integer SQL = %q args=%v err=%v", query, args, err)
	}

	args = nil
	predicate.Comparison.Value.Data = []byte("9223372036854775807")
	preparePredicateForSQL(t, p, predicate)
	query, err = p.predicateSQL(predicate, &args)
	if err != nil || query != "FALSE" || len(args) != 0 {
		t.Fatalf("out-of-range integer SQL = %q args=%v err=%v", query, args, err)
	}
}

func TestRealPredicateUsesPreparedNativeLiteralWithoutColumnCast(t *testing.T) {
	p := &plan{adapter: newPostgresTypeAdapter(), columns: map[string]column{
		"score": {name: "score", typeOID: 700, valueType: storage.TypeFloat, supported: true},
	}}
	predicate := &storage.Predicate{Comparison: &storage.Comparison{
		Field: "score", Operator: storage.ComparisonEqual,
		Value: storage.Literal{Type: storage.TypeFloat, Data: []byte("0.1")},
	}}
	preparePredicateForSQL(t, p, predicate)
	args := []any{}
	query, err := p.predicateSQL(predicate, &args)
	if err != nil || query != `"score" = $1::real` || len(args) != 1 {
		t.Fatalf("real equality = %q args=%v err=%v", query, args, err)
	}

	args = nil
	predicate.Comparison.Value.Data = []byte(strconv.FormatFloat(float64(float32(0.1)), 'g', -1, 64))
	preparePredicateForSQL(t, p, predicate)
	query, err = p.predicateSQL(predicate, &args)
	if err != nil || query != `"score" = $1::real` || len(args) != 1 {
		t.Fatalf("representable real equality = %q args=%v err=%v", query, args, err)
	}

	args = nil
	predicate.Comparison.Operator = storage.ComparisonGreater
	predicate.Comparison.Value.Data = []byte("0.1")
	preparePredicateForSQL(t, p, predicate)
	query, err = p.predicateSQL(predicate, &args)
	if err != nil || query != `"score" > $1::real` {
		t.Fatalf("real range = %q args=%v err=%v", query, args, err)
	}

	args = nil
	predicate.Comparison.Operator = storage.ComparisonEqual
	predicate.Comparison.Value.Data = []byte("Infinity")
	preparePredicateForSQL(t, p, predicate)
	query, err = p.predicateSQL(predicate, &args)
	if err != nil || query != `"score" = $1::real` || len(args) != 1 {
		t.Fatalf("infinite real equality = %q args=%v err=%v", query, args, err)
	}
}

func preparePredicateForSQL(t *testing.T, p *plan, predicate *storage.Predicate) {
	t.Helper()
	p.comparisons = make(map[*storage.Comparison]postgresComparison)
	if err := p.canonicalizePredicateValues(predicate); err != nil {
		t.Fatal(err)
	}
	if err := p.preparePredicateComparisons(predicate); err != nil {
		t.Fatal(err)
	}
}

func TestDescendingOrderingDoesNotReverseExplicitNullPlacement(t *testing.T) {
	value := "1"
	for _, test := range []struct {
		name       string
		nullsFirst bool
		want       int
	}{
		{name: "nulls last", nullsFirst: false, want: 1},
		{name: "nulls first", nullsFirst: true, want: -1},
	} {
		t.Run(test.name, func(t *testing.T) {
			p := &plan{adapter: newPostgresTypeAdapter(), order: []storage.OrderTerm{{Field: "position", Descending: true, NullsFirst: test.nullsFirst}}}
			p.columns = map[string]column{"position": {name: "position", typeOID: 23}}
			got, err := p.compareKeys([]*string{nil}, []*string{&value})
			if err != nil {
				t.Fatal(err)
			}
			if got != test.want {
				t.Fatalf("compare null to value = %d, want %d", got, test.want)
			}
		})
	}
}

func TestPostgresTemporalComparisonUsesCanonicalRange(t *testing.T) {
	for _, test := range []struct {
		name  string
		oid   uint32
		left  string
		right string
		want  int
	}{
		{name: "BC dates retain chronology", oid: 1082, left: "0002-01-01 BC", right: "0001-01-01 BC", want: -1},
		{name: "BC precedes AD", oid: 1114, left: "0001-12-31 23:59:59 BC", right: "0001-01-01 00:00:00", want: -1},
		{name: "timezone offsets normalize instants", oid: 1184, left: "2024-01-01 00:00:00+01", right: "2024-01-01 00:00:00+00", want: -1},
		{name: "negative infinity retains ordering", oid: 1184, left: "-infinity", right: "4713-01-01 00:00:00+00 BC", want: -1},
		{name: "positive infinity retains ordering", oid: 1082, left: "infinity", right: "5874897-12-31", want: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			p := &plan{adapter: newPostgresTypeAdapter()}
			got, err := p.compareValue(test.oid, &test.left, &test.right, false)
			if err != nil {
				t.Fatal(err)
			}
			if got != test.want {
				t.Fatalf("compare(%q, %q) = %d, want %d", test.left, test.right, got, test.want)
			}
		})
	}
}

func TestPostgresComparisonRejectsMalformedTypedValues(t *testing.T) {
	p := &plan{adapter: newPostgresTypeAdapter()}
	left, right := "10", "not-a-number"
	if _, err := p.compareValue(23, &left, &right, false); err == nil {
		t.Fatal("expected malformed integer comparison to fail")
	}
	left, right = "2024-01-01", "01/02/2024"
	if _, err := p.compareValue(1082, &left, &right, false); err == nil {
		t.Fatal("expected non-ISO date comparison to fail")
	}
}
