package postgres

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/adiom-data/weave/internal/datastore"
	"github.com/adiom-data/weave/internal/storage"
)

func TestInvalidStorageConfigurationDoesNotStartCapture(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := NewStorage(ctx, "", Options{}); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	for _, options := range []Options{
		{}, {Publication: "app"}, {Slot: "app"},
		{Publication: "app", Slot: "app", MessageLogInterval: -time.Second},
		{Publication: "app", Slot: "app", QueryLimits: QueryLimits{MaxPredicateDepth: -1}},
	} {
		if _, err := NewStorage(context.Background(), "not a connection URI", options); err == nil {
			t.Fatal("invalid options accepted")
		}
	}
}

// Run with either URI or keyword WEAVE_TEST_DATABASE_URL, including pool-only
// options. Restarting the same slot also exercises its ordinary SQL inspection
// connection, not only the replication startup connection.
func TestStorageConnectionConfigurationIntegration(t *testing.T) {
	dsn := os.Getenv("WEAVE_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set WEAVE_TEST_DATABASE_URL to run PostgreSQL integration test")
	}
	// A valid caller setting must not change the adapter's text wire format.
	if strings.HasPrefix(dsn, "postgres://") || strings.HasPrefix(dsn, "postgresql://") {
		u, err := url.Parse(dsn)
		if err != nil {
			t.Fatal(err)
		}
		params := u.Query()
		params.Set("bytea_output", "escape")
		u.RawQuery = params.Encode()
		dsn = u.String()
	} else {
		dsn += " bytea_output=escape"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	name := fmt.Sprintf("weave_connection_%d", time.Now().UnixNano())
	table := pgx.Identifier{"public", name}.Sanitize()
	publication := pgx.Identifier{name}.Sanitize()
	defer func() {
		cleanup, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		if _, err := pool.Exec(cleanup, `SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = $1`, name); err != nil {
			t.Errorf("clean up replication slot: %v", err)
		}
		for _, statement := range []string{
			`DROP PUBLICATION IF EXISTS ` + publication,
			`DROP TABLE IF EXISTS ` + table,
		} {
			if _, err := pool.Exec(cleanup, statement); err != nil {
				t.Errorf("clean up connection fixture: %v", err)
			}
		}
	}()
	for _, statement := range []string{
		`CREATE TABLE ` + table + ` (id bigint PRIMARY KEY, title text NOT NULL, data bytea)`,
		`INSERT INTO ` + table + ` VALUES (1, 'connection test', decode('616263005cff', 'hex'))`,
		`CREATE PUBLICATION ` + publication + ` FOR TABLE ` + table,
	} {
		if _, err := pool.Exec(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}
	for attempt := 0; attempt < 2; attempt++ {
		func() {
			store, err := NewStorage(ctx, dsn, Options{Publication: name, Slot: name})
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			plan, err := store.PrepareQuery(ctx, storage.QuerySpec{Relation: storage.RelationName{Namespace: "public", Name: name}})
			if err != nil {
				t.Fatal(err)
			}
			snapshot, err := store.BeginSnapshot(ctx, plan)
			if err != nil {
				t.Fatal(err)
			}
			defer snapshot.Abort(ctx)
			rows, err := snapshot.LoadWindow(ctx, plan, datastore.WindowRequest{
				Bounds: datastore.Bounds{Left: datastore.Boundary{Kind: datastore.BoundaryStart}, Right: datastore.Boundary{Kind: datastore.BoundaryEnd}}, Limit: 10,
			})
			if err != nil {
				t.Fatal(err)
			}
			if len(rows) != 1 {
				t.Fatalf("snapshot returned %d rows, want 1", len(rows))
			}
			want := storage.Value{Type: storage.TypeBytes, State: storage.ValuePresent, Data: []byte{'a', 'b', 'c', 0, '\\', 255}}
			if !storage.EqualValues(rows[0].Record["data"], want) {
				t.Fatalf("snapshot bytes = %#v", rows[0].Record["data"])
			}
			feed, err := store.OpenFeed(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer feed.Close()
			marker := fmt.Sprintf("replicated bytes %d", attempt)
			if _, err := pool.Exec(ctx, `UPDATE `+table+` SET title=$1, data=decode('616263005cff', 'hex') WHERE id=1`, marker); err != nil {
				t.Fatal(err)
			}
			for {
				event, err := feed.Receive(ctx)
				if err != nil {
					t.Fatal(err)
				}
				if event.Transaction == nil {
					continue
				}
				for _, change := range event.Transaction.Changes {
					if change.Kind() != datastore.ChangeUpdate {
						continue
					}
					applied, err := change.Apply(nil)
					if err != nil {
						t.Fatal(err)
					}
					if string(applied.Record["title"].Data) != marker {
						continue
					}
					if !storage.EqualValues(applied.Record["data"], want) {
						t.Fatalf("replicated bytes = %#v", applied.Record["data"])
					}
					return
				}
			}
		}()
	}
}
