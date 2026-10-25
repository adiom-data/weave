package postgres

import (
	"context"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/adiom-data/weave/internal/storage"
)

func TestWindowSQLOrdersNativeColumns(t *testing.T) {
	p := &plan{schema: "app", table: `odd"table`, order: []storage.OrderTerm{
		{Field: "id"}, {Field: "score", Descending: true, NullsFirst: true},
	}}
	for _, reverse := range []bool{false, true} {
		sql, _, err := p.windowSQLSelecting(boundary{kind: boundaryStart}, boundary{kind: boundaryEnd}, reverse, 2, []string{"id", "score"})
		if err != nil {
			t.Fatal(err)
		}
		want := `ORDER BY "odd""table"."id" ASC NULLS LAST, "odd""table"."score" DESC NULLS FIRST`
		if reverse {
			want = `ORDER BY "odd""table"."id" DESC NULLS FIRST, "odd""table"."score" ASC NULLS LAST`
		}
		if !strings.Contains(sql, want) {
			t.Fatalf("SQL lacks native ordering %s: %s", want, sql)
		}
		if !strings.Contains(sql, `SELECT "id"::text AS "id", "score"::text AS "score"`) {
			t.Fatalf("text decoding selection changed: %s", sql)
		}
	}
}

// A session-local temporary table makes this regression independent of the
// application's schema and never modifies existing data.
func TestWindowSQLNativeNumericOrderingIntegration(t *testing.T) {
	url := os.Getenv("WEAVE_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("set WEAVE_TEST_DATABASE_URL to run PostgreSQL integration test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(context.Background())
	_, err = conn.Exec(ctx, `CREATE TEMP TABLE weave_native_order (id bigint PRIMARY KEY, score numeric);
		INSERT INTO weave_native_order VALUES (10, 10), (2, 2), (1, 1)`)
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range []storage.FieldPath{"id", "score"} {
		for _, reverse := range []bool{false, true} {
			for _, fields := range [][]string{{string(field)}, {"id", "score"}} {
				p := &plan{schema: "pg_temp", table: "weave_native_order", order: []storage.OrderTerm{{Field: field}}}
				sql, args, err := p.windowSQLSelecting(boundary{kind: boundaryStart}, boundary{kind: boundaryEnd}, reverse, 2, fields)
				if err != nil {
					t.Fatal(err)
				}
				rows, err := conn.Query(ctx, sql, args...)
				if err != nil {
					t.Fatal(err)
				}
				got, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (string, error) {
					values, err := row.Values()
					if err != nil {
						return "", err
					}
					return values[0].(string), nil
				})
				if err != nil {
					t.Fatal(err)
				}
				want := []string{"1", "2"}
				if reverse {
					want = []string{"10", "2"}
				}
				if !reflect.DeepEqual(got, want) {
					t.Fatalf("%s: got %v, want %v", sql, got, want)
				}
			}
		}
	}
}
