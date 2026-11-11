package postgres

import (
	"context"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/adiom-data/weave/internal/datastore"
	"github.com/adiom-data/weave/internal/storage"
)

func TestReferenceSelectorMatchesOrdinaryPreparationIntegration(t *testing.T) {
	url := os.Getenv("WEAVE_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("set WEAVE_TEST_DATABASE_URL for PostgreSQL integration")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	prepareAdapterFixture(t, ctx, pool)
	source := NewPostgresDataSourceWithOptions(pool, nil, DataSourceOptions{})
	spec := storage.QuerySpec{Relation: storage.RelationName{Namespace: "weave_adapter_test", Name: "tasks"}, Filter: &storage.Predicate{Null: &storage.NullTest{Field: "deleted_at", IsNull: true}}}
	base, err := source.PrepareQuery(ctx, spec)
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := source.PrepareReference(ctx, base, storage.ReferenceFetch{Query: spec, Bindings: []storage.ReferenceBinding{{Source: "id", Target: "id"}}})
	if err != nil {
		t.Fatal(err)
	}
	value, err := storage.ParseScalar(storage.TypeUUID, []byte("10000000-0000-4000-8000-000000000001"))
	if err != nil {
		t.Fatal(err)
	}
	bound, raw, key, matchable, err := prepared.Bind(storage.Record{"id": value})
	if err != nil || !matchable {
		t.Fatalf("bind: %v", err)
	}
	ordinary, err := source.PrepareQuery(ctx, raw)
	if err != nil {
		t.Fatal(err)
	}
	if bound.Fingerprint() != ordinary.Fingerprint() || !reflect.DeepEqual(bound.Query(), ordinary.Query()) {
		t.Fatal("reference selector diverged from ordinary preparation")
	}
	// Mutating a returned raw selector must not change the shared reference
	// plan's static predicate or any later caller's bindings.
	raw.Filter.Junction.Operands[0].Null.Field = "title"
	again, _, _, _, err := prepared.Bind(storage.Record{"id": value})
	if err != nil || again.Fingerprint() != ordinary.Fingerprint() {
		t.Fatal("returned selector leaked mutable reference-plan state")
	}
	ordinaryKey, err := ordinary.EntityKey(storage.Record{"id": value})
	if err != nil || ordinaryKey != key {
		t.Fatal("reference selector changed identity encoding")
	}
	// Explicit metadata invalidation exposes a new schema fingerprint to new
	// selectors without mutating an already prepared reference.
	if _, err := pool.Exec(ctx, "ALTER TABLE weave_adapter_test.tasks ADD COLUMN optional_note text"); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if _, err := pool.Exec(context.WithoutCancel(ctx), "ALTER TABLE weave_adapter_test.tasks DROP COLUMN optional_note"); err != nil {
			t.Error(err)
		}
	}()
	if err := source.InvalidateMetadata(ctx, &spec.Relation); err != nil {
		t.Fatal(err)
	}
	updated, err := source.PrepareQuery(ctx, ordinary.Query())
	if err != nil {
		t.Fatal(err)
	}
	if updated.Fingerprint() == ordinary.Fingerprint() {
		t.Fatal("new preparation reused the old schema fingerprint")
	}
	again, _, _, _, err = prepared.Bind(storage.Record{"id": value})
	if err != nil || again.Fingerprint() != ordinary.Fingerprint() {
		t.Fatal("schema update mutated an existing prepared reference")
	}
}

func referencePlans() (*plan, *plan) {
	makePlan := func(table string, fields []string) *plan {
		p := &plan{schema: "pg_temp", table: table, adapter: newPostgresTypeAdapter(), primary: []string{"id"},
			fingerprint: table, needed: fields, columns: map[string]column{}, order: []storage.OrderTerm{{Field: "id"}}}
		for _, field := range fields {
			p.columns[field] = column{name: field, typeOID: 20, valueType: storage.TypeInteger, supported: true}
		}
		var err error
		p.schemaFingerprint, err = relationSchemaFingerprint(p.columns)
		if err != nil {
			panic(err)
		}
		return p
	}
	return makePlan("weave_reference_links", []string{"id", "task_id", "guard"}), makePlan("weave_reference_tasks", []string{"id", "value"})
}

func referenceRequest() datastore.WindowRequest {
	return datastore.WindowRequest{Limit: 2, Bounds: datastore.Bounds{
		Left: datastore.Boundary{Kind: datastore.BoundaryStart}, Right: datastore.Boundary{Kind: datastore.BoundaryEnd},
	}}
}

func TestReferenceSQLLimitsBaseBeforeLeftJoin(t *testing.T) {
	base, target := referencePlans()
	sql, args, err := referenceWindowSQL(base, target, referenceRequest(), []datastore.ReferenceBinding{{Source: "task_id", Target: "id"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(args) != 0 || !strings.Contains(sql, `LIMIT 2) AS weave_base LEFT JOIN`) ||
		!strings.Contains(sql, `"weave_base"."task_id" = "weave_target"."id"`) ||
		!strings.HasSuffix(sql, `ORDER BY "weave_base"."id" ASC NULLS LAST`) {
		t.Fatalf("unexpected reference SQL: %s (%v)", sql, args)
	}
	if base.fingerprint != base.table || target.fingerprint != target.table || len(base.query.Order) != 0 {
		t.Fatal("building fetch SQL mutated a component plan")
	}
}

func TestReferenceSQLValidatesIdentityBindings(t *testing.T) {
	for _, test := range []struct {
		name     string
		bindings []datastore.ReferenceBinding
		change   func(*plan, *plan)
	}{
		{name: "absent"},
		{name: "non-primary", bindings: []datastore.ReferenceBinding{{Source: "task_id", Target: "value"}}},
		{name: "unknown source", bindings: []datastore.ReferenceBinding{{Source: "unknown", Target: "id"}}},
		{name: "nested source", bindings: []datastore.ReferenceBinding{{Source: storage.MustFieldPath("task", "id"), Target: "id"}}},
		{name: "different native types", bindings: []datastore.ReferenceBinding{{Source: "task_id", Target: "id"}}, change: func(base, _ *plan) {
			base.columns["task_id"] = column{typeOID: 25}
		}},
		{name: "incomplete composite", bindings: []datastore.ReferenceBinding{{Source: "task_id", Target: "id"}}, change: func(_, target *plan) {
			target.primary = []string{"id", "value"}
		}},
		{name: "duplicate composite", bindings: []datastore.ReferenceBinding{{Source: "task_id", Target: "id"}, {Source: "id", Target: "id"}}, change: func(_, target *plan) {
			target.primary = []string{"id", "value"}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			base, target := referencePlans()
			if test.change != nil {
				test.change(base, target)
			}
			if _, _, err := referenceWindowSQL(base, target, referenceRequest(), test.bindings); err == nil {
				t.Fatal("invalid reference accepted")
			}
		})
	}
	base, target := referencePlans()
	target.primary = []string{"id", "value"}
	if _, _, err := referenceWindowSQL(base, target, referenceRequest(), []datastore.ReferenceBinding{
		{Source: "id", Target: "value"}, {Source: "task_id", Target: "id"},
	}); err != nil {
		t.Fatalf("valid composite reference rejected: %v", err)
	}
}

func TestReferenceFetchIntegration(t *testing.T) {
	url := os.Getenv("WEAVE_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("set WEAVE_TEST_DATABASE_URL to run PostgreSQL integration test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(context.Background())
	_, err = conn.Exec(ctx, `
		CREATE TEMP TABLE weave_reference_tasks(id bigint PRIMARY KEY, value bigint);
		CREATE TEMP TABLE weave_reference_links(id bigint PRIMARY KEY, task_id bigint, guard bigint);
		INSERT INTO weave_reference_tasks VALUES (1,100), (2,200);
		INSERT INTO weave_reference_links VALUES (1,1,100), (2,999,100), (3,NULL,100), (4,1,999), (10,2,200);
	`)
	if err != nil {
		t.Fatal(err)
	}
	base, target := referencePlans()
	basePrepared, targetPrepared := &postgresPrepared{plan: base}, &postgresPrepared{plan: target}
	tx, err := conn.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := &postgresSnapshot{source: &PostgresDataSource{}, tx: tx, relation: basePrepared.Relation(), plan: base}
	defer snapshot.Abort(context.Background())
	fetch := datastore.ReferenceFetch{Target: targetPrepared, Bindings: []datastore.ReferenceBinding{{Source: "task_id", Target: "id"}}}
	for _, test := range []struct {
		name    string
		request datastore.WindowRequest
		ids     []string
		targets []string
	}{
		{name: "left window retains dangling", request: referenceRequest(), ids: []string{"1", "2"}, targets: []string{"1", ""}},
		{name: "right window ascending output", request: func() datastore.WindowRequest {
			r := referenceRequest()
			r.Reverse = true
			return r
		}(), ids: []string{"4", "10"}, targets: []string{"1", "2"}},
		{name: "cursor extension retains null", request: func() datastore.WindowRequest {
			r := referenceRequest()
			value := "2"
			r.Bounds.Left = datastore.Boundary{Kind: datastore.BoundaryCursor, Key: postgresSortKey{values: []*string{&value}}}
			return r
		}(), ids: []string{"3", "4"}, targets: []string{"", "1"}},
		{name: "repeated targets", request: func() datastore.WindowRequest {
			r := referenceRequest()
			r.Limit = 5
			return r
		}(), ids: []string{"1", "2", "3", "4", "10"}, targets: []string{"1", "", "", "1", "2"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			rows, err := snapshot.LoadWindowWithReference(ctx, basePrepared, test.request, fetch)
			if err != nil {
				t.Fatal(err)
			}
			plain, err := snapshot.LoadWindow(ctx, basePrepared, test.request)
			if err != nil {
				t.Fatal(err)
			}
			if len(plain) != len(rows) {
				t.Fatalf("joined and ordinary windows differ in length: %d vs %d", len(rows), len(plain))
			}
			var ids, targets []string
			for index, row := range rows {
				if !reflect.DeepEqual(row.Row, plain[index]) {
					t.Fatalf("joined base record differs from ordinary load: %+v vs %+v", row.Row, plain[index])
				}
				if err := row.Row.Validate(); err != nil {
					t.Fatal(err)
				}
				ids = append(ids, string(row.Row.Record["id"].Data))
				targetID := ""
				if row.Reference != nil {
					if err := row.Reference.Validate(); err != nil {
						t.Fatal(err)
					}
					targetID = string(row.Reference.Record["id"].Data)
				}
				targets = append(targets, targetID)
			}
			if !reflect.DeepEqual(ids, test.ids) || !reflect.DeepEqual(targets, test.targets) {
				t.Fatalf("got %v → %v; want %v → %v", ids, targets, test.ids, test.targets)
			}
		})
	}
	guarded := fetch
	guarded.Bindings = append(append([]datastore.ReferenceBinding(nil), fetch.Bindings...), datastore.ReferenceBinding{Source: "guard", Target: "value"})
	guardRequest := referenceRequest()
	guardRequest.Limit = 5
	guardRows, err := snapshot.LoadWindowWithReference(ctx, basePrepared, guardRequest, guarded)
	if err != nil {
		t.Fatal(err)
	}
	if len(guardRows) != 5 || guardRows[0].Reference == nil || guardRows[3].Reference != nil || guardRows[4].Reference == nil {
		t.Fatal("additional equality guard changed base coverage or allowed an unrelated target")
	}
	// Repeated identities with different guards must contribute separate
	// selector predicates; encountering the rejected one first cannot hide a
	// later matching selector for the same primary key.
	guardRecords, err := snapshot.LoadReferences(ctx, guarded, []storage.Record{guardRows[3].Row.Record, guardRows[0].Row.Record})
	if err != nil || len(guardRecords) != 1 || string(guardRecords[0].Record["id"].Data) != "1" {
		t.Fatalf("guarded batch: %v (%v)", guardRecords, err)
	}
	// A target predicate must not turn LEFT JOIN into filtering the base list.
	target.query.Filter = &storage.Predicate{Null: &storage.NullTest{Field: "value", IsNull: true}}
	rows, err := snapshot.LoadWindowWithReference(ctx, basePrepared, referenceRequest(), fetch)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 || rows[0].Reference != nil || rows[1].Reference != nil {
		t.Fatalf("target predicate changed association window: %+v", rows)
	}
	// Target literals share the statement's argument list with base cursors.
	// Overlapping column names must be evaluated in their own query scope.
	target.query.Filter = &storage.Predicate{Comparison: &storage.Comparison{
		Field: "value", Operator: storage.ComparisonEqual, Value: storage.Literal{Type: storage.TypeInteger, Data: []byte("100")},
	}}
	target.comparisons = map[*storage.Comparison]postgresComparison{}
	if err := target.preparePredicateComparisons(target.query.Filter); err != nil {
		t.Fatal(err)
	}
	request := referenceRequest()
	value := "3"
	request.Bounds.Left = datastore.Boundary{Kind: datastore.BoundaryCursor, Key: postgresSortKey{values: []*string{&value}}}
	rows, err = snapshot.LoadWindowWithReference(ctx, basePrepared, request, fetch)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 || rows[0].Reference == nil || rows[1].Reference != nil {
		t.Fatalf("base cursor and target literal were not independently applied: %+v", rows)
	}
}
