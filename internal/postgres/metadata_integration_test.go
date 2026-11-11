package postgres

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/adiom-data/weave/internal/postgres/cdc"
	"github.com/adiom-data/weave/internal/storage"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestPostgresMetadataAndPublicationRemainStableUntilInvalidation(t *testing.T) {
	dsn := os.Getenv("WEAVE_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set WEAVE_TEST_DATABASE_URL for PostgreSQL integration tests")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	name := fmt.Sprintf("weave_metadata_%d", time.Now().UnixNano())
	table := pgx.Identifier{"public", name}.Sanitize()
	publication := pgx.Identifier{name}.Sanitize()
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _ = pool.Exec(cleanup, "DROP PUBLICATION IF EXISTS "+publication)
		_, _ = pool.Exec(cleanup, "DROP TABLE IF EXISTS "+table)
	}()
	if _, err := pool.Exec(ctx, "CREATE TABLE "+table+" (id BIGINT PRIMARY KEY, title TEXT)"); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, "CREATE PUBLICATION "+publication+" FOR TABLE "+table); err != nil {
		t.Fatal(err)
	}
	source := NewPostgresDataSourceWithOptions(pool, cdc.NewBroker(), DataSourceOptions{Publication: name})
	relation := storage.RelationName{Namespace: "public", Name: name}
	spec := storage.QuerySpec{Relation: relation}
	first, err := source.PrepareQuery(ctx, spec)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, "ALTER PUBLICATION "+publication+" DROP TABLE "+table); err != nil {
		t.Fatal(err)
	}
	if _, err := source.PrepareQuery(ctx, spec); err != nil {
		t.Fatal("cached availability was rechecked", err)
	}
	if err := source.InvalidateMetadata(ctx, &relation); err != nil {
		t.Fatal(err)
	}
	if _, err := source.PrepareQuery(ctx, spec); err == nil || !strings.Contains(err.Error(), "not in publication") {
		t.Fatal("publication removal not observed", err)
	}
	if _, err := pool.Exec(ctx, "ALTER PUBLICATION "+publication+" ADD TABLE "+table); err != nil {
		t.Fatal(err)
	}
	if _, err := source.PrepareQuery(ctx, spec); err != nil {
		t.Fatal("negative discovery was cached", err)
	}
	if _, err := pool.Exec(ctx, "ALTER TABLE "+table+" ADD COLUMN extra TEXT COLLATE \"C\""); err != nil {
		t.Fatal(err)
	}
	spec.Filter = &storage.Predicate{Comparison: &storage.Comparison{Field: "extra", Operator: storage.ComparisonEqual, Value: storage.Literal{Type: storage.TypeText, Data: []byte("x")}}}
	if _, err := source.PrepareQuery(ctx, spec); err == nil {
		t.Fatal("cached metadata unexpectedly adopted new column")
	}
	if err := source.InvalidateMetadata(ctx, &relation); err != nil {
		t.Fatal(err)
	}
	updated, err := source.PrepareQuery(ctx, spec)
	if err != nil {
		t.Fatal("fresh metadata missed added column", err)
	}
	if _, ok := first.(*postgresPrepared).plan.columns["extra"]; ok {
		t.Fatal("existing immutable plan was mutated")
	}
	if _, ok := updated.(*postgresPrepared).plan.columns["extra"]; !ok {
		t.Fatal("new plan lacks added column")
	}
}
