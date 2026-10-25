package postgres

import (
	"context"
	"github.com/jackc/pgx/v5/pgxpool"
	"testing"
)

// Core integration tests own their minimal fixture, independent of the demo's
// schema, grants, seeds, or migrations. Use a disposable database with logical
// replication enabled; fixture objects remain for subsequent test runs.
func prepareAdapterFixture(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	_, err := pool.Exec(ctx, `
	CREATE SCHEMA IF NOT EXISTS weave_adapter_test;
	CREATE TABLE IF NOT EXISTS weave_adapter_test.tasks (
		id uuid PRIMARY KEY, organization_id uuid NOT NULL, title text NOT NULL,
		completed boolean NOT NULL DEFAULT false,
		created_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz NOT NULL DEFAULT now(), deleted_at timestamptz
	);
	INSERT INTO weave_adapter_test.tasks(id,organization_id,title) VALUES
	('10000000-0000-4000-8000-000000000001','00000000-0000-4000-8000-000000000001','adapter fixture') ON CONFLICT DO NOTHING;
	DO $fixture$ BEGIN
	IF NOT EXISTS(SELECT 1 FROM pg_publication WHERE pubname='weave_adapter_test_publication') THEN
	CREATE PUBLICATION weave_adapter_test_publication FOR TABLES IN SCHEMA weave_adapter_test;
	END IF;
	END $fixture$;
	`)
	if err != nil {
		t.Fatal(err)
	}
}
