package postgres

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/adiom-data/weave/internal/datastore"
	"github.com/adiom-data/weave/internal/engine"
	"github.com/adiom-data/weave/internal/engine/changeprocessor"
	"github.com/adiom-data/weave/internal/engine/delivery"
	"github.com/adiom-data/weave/internal/entitykey"
	"github.com/adiom-data/weave/internal/postgres/cdc"
	q "github.com/adiom-data/weave/internal/querysubscription/model"
	"github.com/adiom-data/weave/internal/storage"
)

type coordinatorTestRead struct {
	*engine.OpenSubscription
	queue *delivery.ClientQueue[delivery.GroupMessage]
}

func subscribeCoordinatorForTest(ctx context.Context, c *engine.Coordinator, id string, spec storage.QuerySpec, window engine.InitialWindow) (*coordinatorTestRead, error) {
	queue, err := c.OpenGroup(ctx, id, 16)
	if err != nil {
		return nil, err
	}
	opened, err := c.SubscribeGrouped(ctx, id, id, spec, window)
	if err != nil {
		return nil, err
	}
	return &coordinatorTestRead{opened, queue}, nil
}

func nextCoordinatorBatch(ctx context.Context, read *coordinatorTestRead) (delivery.SubscriptionBatch, error) {
	for {
		message, err := read.queue.Next(ctx)
		if err != nil {
			return delivery.SubscriptionBatch{}, err
		}
		if len(message.Failures) > 0 {
			return delivery.SubscriptionBatch{}, message.Failures[0].Cause
		}
		batch := message.Batches[0]
		if !batch.Clear && len(batch.Events) == 0 && batch.Window == nil {
			continue
		}
		return batch, nil
	}
}

func TestProgressBarrierFlushesWithAsynchronousCommitDefault(t *testing.T) {
	url := os.Getenv("WEAVE_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("set WEAVE_TEST_DATABASE_URL for PostgreSQL integration")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	config, err := pgxpool.ParseConfig(url)
	if err != nil {
		t.Fatal(err)
	}
	config.MaxConns = 1
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if _, err := pool.Exec(ctx, "SET synchronous_commit=off"); err != nil {
		t.Fatal(err)
	}
	var raw string
	if err := pool.QueryRow(ctx, "SELECT pg_current_wal_insert_lsn()::text").Scan(&raw); err != nil {
		t.Fatal(err)
	}
	position, err := pglogrepl.ParseLSN(raw)
	if err != nil {
		t.Fatal(err)
	}
	source := NewPostgresDataSource(pool, cdc.NewBroker(), nil)
	if err := source.RequestProgress(ctx, &q.Position{SourceID: SourceID, Value: uint64(position)}); err != nil {
		t.Fatal(err)
	}
	var flushed bool
	var setting string
	// Other integration tests may write later WAL concurrently. Only require
	// durability beyond our captured fence, not every subsequent cluster write.
	if err := pool.QueryRow(ctx, "SELECT pg_current_wal_flush_lsn() > $1::pg_lsn, current_setting('synchronous_commit')", raw).Scan(&flushed, &setting); err != nil {
		t.Fatal(err)
	}
	if !flushed || setting != "off" {
		t.Fatalf("barrier flushed=%v, session synchronous_commit=%s", flushed, setting)
	}
}

func NewPostgresDataSource(db *pgxpool.Pool, broker *cdc.Broker, allowedTables []string) *PostgresDataSource {
	return NewPostgresDataSourceWithOptions(db, broker, DataSourceOptions{AllowedTables: allowedTables})
}

func TestPostgresCoordinatorSnapshot(t *testing.T) {
	databaseURL := os.Getenv("WEAVE_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("set WEAVE_TEST_DATABASE_URL to run PostgreSQL integration test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	prepareAdapterFixture(t, ctx, pool)
	broker := cdc.NewBroker()
	broker.MarkReady()
	// Start beyond any realistic snapshot fence while leaving one strictly
	// later position for the synthetic CDC transaction below.
	broker.Advance(pglogrepl.LSN(^uint64(0) - 1))
	source := NewPostgresDataSourceWithOptions(pool, broker, DataSourceOptions{
		AllowedTables: []string{"weave_adapter_test.tasks"}, Publication: "weave_adapter_test_publication",
	})
	coordinator, err := engine.StartCoordinator(ctx, source)
	if err != nil {
		t.Fatal(err)
	}
	opened, err := subscribeCoordinatorForTest(ctx, coordinator, "integration", storage.QuerySpec{
		Relation: storage.RelationName{Namespace: "weave_adapter_test", Name: "tasks"},
		Filter:   &storage.Predicate{Comparison: &storage.Comparison{Field: "organization_id", Operator: storage.ComparisonEqual, Value: storage.Literal{Type: storage.TypeUUID, Data: []byte("00000000-0000-4000-8000-000000000001")}}},
		Order:    []storage.OrderTerm{{Field: "id"}},
	}, engine.InitialWindow{FromEnd: true, Count: 100})
	if err != nil {
		t.Fatal(err)
	}
	var expected int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM weave_adapter_test.tasks WHERE organization_id = '00000000-0000-4000-8000-000000000001'`).Scan(&expected); err != nil {
		t.Fatal(err)
	}
	if len(opened.Initial) != expected {
		t.Fatalf("coordinator returned %d rows, expected %d", len(opened.Initial), expected)
	}
	if len(opened.Initial) > 0 {
		relationID, err := strconv.ParseUint(opened.Plan.Relation().OpaqueID, 10, 32)
		if err != nil {
			t.Fatal(err)
		}
		prepared := opened.Plan.(*postgresPrepared)
		row := make(cdc.Row)
		for name, value := range opened.Initial[0].Record {
			oid := prepared.plan.columns[name].typeOID
			cell := cdc.Cell{TypeOID: oid, Null: value.State == storage.ValueNull}
			if !cell.Null {
				native, encodeErr := (postgresTypeAdapter{}).encodeScalar(oid, value)
				if encodeErr != nil {
					t.Fatalf("encode CDC fixture field %s: %v", name, encodeErr)
				}
				switch encoded := native.(type) {
				case string:
					cell.Text = []byte(encoded)
				case []byte:
					cell.Text = []byte(`\x` + fmt.Sprintf("%x", encoded))
				default:
					t.Fatalf("unsupported CDC fixture field %s representation %T", name, native)
				}
			}
			row[name] = cell
		}
		row["id"] = cdc.Cell{TypeOID: 2950, Text: []byte("ffffffff-ffff-4fff-8fff-ffffffffffff")}
		identity := entitykey.Encode([]entitykey.Part{{Name: "id", TypeOID: 2950, Value: []byte("ffffffff-ffff-4fff-8fff-ffffffffffff")}})
		if err := broker.Publish(ctx, cdc.Transaction{CommitLSN: pglogrepl.LSN(^uint64(0)), Changes: []cdc.Change{{Operation: "insert", RelationID: uint32(relationID), Schema: "weave_adapter_test", Table: "tasks", Identity: identity, NewIdentity: identity, New: row}}}); err != nil {
			t.Fatal(err)
		}
		eventCtx, stop := context.WithTimeout(ctx, time.Second)
		defer stop()
		batch, err := nextCoordinatorBatch(eventCtx, opened)
		if err != nil || len(batch.Events) != 1 {
			t.Fatalf("CDC batch = %+v, %v", batch, err)
		}
		if event := batch.Events[0]; event.Kind != changeprocessor.QueryAdded {
			t.Fatalf("CDC event = %+v", event)
		}
	}
}

func TestPostgresCoordinatorLogicalReplication(t *testing.T) {
	databaseURL := os.Getenv("WEAVE_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("set WEAVE_TEST_DATABASE_URL to run PostgreSQL integration test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	prepareAdapterFixture(t, ctx, pool)
	slot := "weave_coordinator_integration"
	_, _ = pool.Exec(ctx, `SELECT pg_drop_replication_slot($1) FROM pg_replication_slots WHERE slot_name = $1 AND NOT active`, slot)
	defer func() {
		cancel()
		deadline, stop := context.WithTimeout(context.Background(), 2*time.Second)
		defer stop()
		for {
			var exists, active bool
			err := pool.QueryRow(deadline, `SELECT count(*) > 0, COALESCE(bool_or(active), false) FROM pg_replication_slots WHERE slot_name = $1`, slot).Scan(&exists, &active)
			if err == nil && !exists {
				break
			}
			if err == nil && !active {
				if _, err := pool.Exec(deadline, `SELECT pg_drop_replication_slot($1)`, slot); err == nil {
					break
				}
			}
			select {
			case <-deadline.Done():
				return
			case <-time.After(10 * time.Millisecond):
			}
		}
	}()
	broker := cdc.NewBroker()
	replicator, err := cdc.NewReplicator(&pool.Config().ConnConfig.Config, "weave_adapter_test_publication", slot, broker, slog.New(slog.NewTextHandler(io.Discard, nil)), cdc.ReplicatorOptions{})
	if err != nil {
		t.Fatal(err)
	}
	go replicator.Run(ctx)
	source := NewPostgresDataSource(pool, broker, []string{"weave_adapter_test.tasks"})
	coordinator, err := engine.StartCoordinator(ctx, source)
	if err != nil {
		t.Fatal(err)
	}
	opened, err := subscribeCoordinatorForTest(ctx, coordinator, "logical", storage.QuerySpec{
		Relation: storage.RelationName{Namespace: "weave_adapter_test", Name: "tasks"},
		Filter:   &storage.Predicate{Comparison: &storage.Comparison{Field: "organization_id", Operator: storage.ComparisonEqual, Value: storage.Literal{Type: storage.TypeUUID, Data: []byte("00000000-0000-4000-8000-000000000001")}}},
		Order:    []storage.OrderTerm{{Field: "id"}},
	}, engine.InitialWindow{FromEnd: true, Count: 100})
	if err != nil {
		t.Fatal(err)
	}
	id := "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee"
	_, _ = pool.Exec(ctx, `DELETE FROM weave_adapter_test.tasks WHERE id = $1`, id)
	defer pool.Exec(context.Background(), `DELETE FROM weave_adapter_test.tasks WHERE id = $1`, id)
	if _, err := pool.Exec(ctx, `INSERT INTO weave_adapter_test.tasks (id, organization_id, title) VALUES ($1, $2, 'logical-integration')`, id, "00000000-0000-4000-8000-000000000001"); err != nil {
		t.Fatal(err)
	}
	eventCtx, stop := context.WithTimeout(ctx, 5*time.Second)
	defer stop()
	batch, err := nextCoordinatorBatch(eventCtx, opened)
	event := batch.Events[0]
	canonicalID, canonicalErr := storage.Canonicalize(storage.TypeUUID, []byte(id))
	if canonicalErr != nil {
		t.Fatal(canonicalErr)
	}
	expectedKey := entitykey.Encode([]entitykey.Part{{Name: "id", TypeOID: 2950, Value: canonicalID}})
	if err != nil || event.Kind != changeprocessor.QueryAdded || event.Key.Value != expectedKey {
		t.Fatalf("logical CDC event = %+v, %v", event, err)
	}
}

func TestPostgresLogicalReplicationSchemaEvolutionAndRelationClear(t *testing.T) {
	databaseURL := os.Getenv("WEAVE_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("set WEAVE_TEST_DATABASE_URL to run PostgreSQL integration test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	prepareAdapterFixture(t, ctx, pool)
	const table = "weave_schema_evolution_test"
	qualified := "weave_adapter_test." + table
	if _, err := pool.Exec(ctx, `DROP TABLE IF EXISTS weave_adapter_test.weave_schema_evolution_test`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `CREATE TABLE weave_adapter_test.weave_schema_evolution_test (id uuid PRIMARY KEY, title text NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	defer pool.Exec(context.Background(), `DROP TABLE IF EXISTS weave_adapter_test.weave_schema_evolution_test`)
	id := "cccccccc-cccc-4ccc-8ccc-cccccccccccc"
	if _, err := pool.Exec(ctx, `INSERT INTO weave_adapter_test.weave_schema_evolution_test (id, title) VALUES ($1, 'before')`, id); err != nil {
		t.Fatal(err)
	}

	slot := "weave_schema_evolution_integration"
	_, _ = pool.Exec(ctx, `SELECT pg_drop_replication_slot($1) FROM pg_replication_slots WHERE slot_name = $1 AND NOT active`, slot)
	defer func() {
		cancel()
		deadline, stop := context.WithTimeout(context.Background(), 3*time.Second)
		defer stop()
		for {
			var exists, active bool
			err := pool.QueryRow(deadline, `SELECT count(*) > 0, COALESCE(bool_or(active), false) FROM pg_replication_slots WHERE slot_name = $1`, slot).Scan(&exists, &active)
			if err == nil && !exists {
				return
			}
			if err == nil && !active {
				if _, err := pool.Exec(deadline, `SELECT pg_drop_replication_slot($1)`, slot); err == nil {
					return
				}
			}
			select {
			case <-deadline.Done():
				return
			case <-time.After(10 * time.Millisecond):
			}
		}
	}()

	broker := cdc.NewBroker()
	replicator, err := cdc.NewReplicator(&pool.Config().ConnConfig.Config, "weave_adapter_test_publication", slot, broker, slog.New(slog.NewTextHandler(io.Discard, nil)), cdc.ReplicatorOptions{})
	if err != nil {
		t.Fatal(err)
	}
	go replicator.Run(ctx)
	source := NewPostgresDataSource(pool, broker, []string{qualified})
	coordinator, err := engine.StartCoordinator(ctx, source)
	if err != nil {
		t.Fatal(err)
	}
	opened, err := subscribeCoordinatorForTest(ctx, coordinator, "schema-evolution", storage.QuerySpec{
		Relation: storage.RelationName{Namespace: "weave_adapter_test", Name: table},
		Order:    []storage.OrderTerm{{Field: "id"}},
	}, engine.InitialWindow{Count: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(opened.Initial) != 1 {
		t.Fatalf("initial rows = %d, want 1", len(opened.Initial))
	}
	if _, err := pool.Exec(ctx, `ALTER TABLE weave_adapter_test.weave_schema_evolution_test ADD COLUMN note text`); err != nil {
		t.Fatal(err)
	}
	// Existing plans tolerate nullable additions. New queries opt into refreshed
	// metadata explicitly, following the adapter's cached-metadata contract.
	name := storage.RelationName{Namespace: "weave_adapter_test", Name: table}
	if err := source.InvalidateMetadata(ctx, &name); err != nil {
		t.Fatal(err)
	}
	nullable, err := subscribeCoordinatorForTest(ctx, coordinator, "schema-evolution-null", storage.QuerySpec{
		Relation: storage.RelationName{Namespace: "weave_adapter_test", Name: table},
		Filter:   &storage.Predicate{Null: &storage.NullTest{Field: "note", IsNull: true}},
		Order:    []storage.OrderTerm{{Field: "id"}},
	}, engine.InitialWindow{Count: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(nullable.Initial) != 1 || nullable.Initial[0].Record["note"].State != storage.ValueNull {
		t.Fatalf("new-shape query reused stale cached row: %+v", nullable.Initial)
	}
	if _, err := pool.Exec(ctx, `UPDATE weave_adapter_test.weave_schema_evolution_test SET title = 'after', note = 'new field' WHERE id = $1`, id); err != nil {
		t.Fatal(err)
	}

	eventCtx, stop := context.WithTimeout(ctx, 8*time.Second)
	defer stop()
	batch, err := nextCoordinatorBatch(eventCtx, opened)
	if err != nil {
		t.Fatalf("schema-evolution update: %v", err)
	}
	if len(batch.Events) != 1 || batch.Events[0].Kind != changeprocessor.QueryUpdated {
		t.Fatalf("schema-evolution batch = %+v", batch)
	}
	if got := string(batch.Events[0].Record["note"].Data); got != "new field" {
		t.Fatalf("new column value = %q, want new field", got)
	}
	nullBatch, err := nextCoordinatorBatch(eventCtx, nullable)
	if err != nil || len(nullBatch.Events) != 1 || nullBatch.Events[0].Kind != changeprocessor.QueryRemoved {
		t.Fatalf("new-column predicate update = %+v, err=%v", nullBatch, err)
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `TRUNCATE weave_adapter_test.weave_schema_evolution_test`); err != nil {
		_ = tx.Rollback(ctx)
		t.Fatal(err)
	}
	newID := "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
	if _, err := tx.Exec(ctx, `INSERT INTO weave_adapter_test.weave_schema_evolution_test (id, title, note) VALUES ($1, 'after clear', 'present')`, newID); err != nil {
		_ = tx.Rollback(ctx)
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	cleared, err := nextCoordinatorBatch(eventCtx, opened)
	if err != nil {
		t.Fatalf("relation clear update: %v", err)
	}
	if !cleared.Clear || cleared.ClearChangeIndex != 1 || len(cleared.Events) != 1 || cleared.Events[0].Kind != changeprocessor.QueryAdded || cleared.Events[0].ChangeIndex != 2 || string(cleared.Events[0].Record["title"].Data) != "after clear" {
		t.Fatalf("relation clear batch = %+v", cleared)
	}
	nullCleared, err := nextCoordinatorBatch(eventCtx, nullable)
	if err != nil || !nullCleared.Clear || nullCleared.ClearChangeIndex != 1 || len(nullCleared.Events) != 0 {
		t.Fatalf("filtered relation clear batch = %+v, err=%v", nullCleared, err)
	}
}

func TestPostgresCoordinatorHydratesUncachedToastedRowEnteringRange(t *testing.T) {
	databaseURL := os.Getenv("WEAVE_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("set WEAVE_TEST_DATABASE_URL to run PostgreSQL integration test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	prepareAdapterFixture(t, ctx, pool)
	slot := "weave_toast_hydration_integration"
	_, _ = pool.Exec(ctx, `SELECT pg_drop_replication_slot($1) FROM pg_replication_slots WHERE slot_name = $1 AND NOT active`, slot)
	defer func() {
		cancel()
		deadline, stop := context.WithTimeout(context.Background(), 3*time.Second)
		defer stop()
		for {
			var exists, active bool
			err := pool.QueryRow(deadline, `SELECT count(*) > 0, COALESCE(bool_or(active), false) FROM pg_replication_slots WHERE slot_name = $1`, slot).Scan(&exists, &active)
			if err == nil && !exists {
				return
			}
			if err == nil && !active {
				if _, err := pool.Exec(deadline, `SELECT pg_drop_replication_slot($1)`, slot); err == nil {
					return
				}
			}
			select {
			case <-deadline.Done():
				return
			case <-time.After(10 * time.Millisecond):
			}
		}
	}()

	broker := cdc.NewBroker()
	replicator, err := cdc.NewReplicator(&pool.Config().ConnConfig.Config, "weave_adapter_test_publication", slot, broker, slog.New(slog.NewTextHandler(io.Discard, nil)), cdc.ReplicatorOptions{})
	if err != nil {
		t.Fatal(err)
	}
	go replicator.Run(ctx)
	source := NewPostgresDataSource(pool, broker, []string{"weave_adapter_test.tasks"})
	coordinator, err := engine.StartCoordinator(ctx, source)
	if err != nil {
		t.Fatal(err)
	}
	opened, err := subscribeCoordinatorForTest(ctx, coordinator, "toast", storage.QuerySpec{
		Relation: storage.RelationName{Namespace: "weave_adapter_test", Name: "tasks"},
		Filter:   &storage.Predicate{Comparison: &storage.Comparison{Field: "completed", Operator: storage.ComparisonEqual, Value: storage.Literal{Type: storage.TypeBool, Data: []byte("false")}}},
		Order:    []storage.OrderTerm{{Field: "id"}},
	}, engine.InitialWindow{FromEnd: true, Count: 100})
	if err != nil {
		t.Fatal(err)
	}

	id := "dddddddd-dddd-4ddd-8ddd-dddddddddddd"
	_, _ = pool.Exec(ctx, `DELETE FROM weave_adapter_test.tasks WHERE id = $1`, id)
	defer pool.Exec(context.Background(), `DELETE FROM weave_adapter_test.tasks WHERE id = $1`, id)
	largeBytes := make([]byte, 120000)
	state := uint64(0x9e3779b97f4a7c15)
	for index := range largeBytes {
		state ^= state << 7
		state ^= state >> 9
		state ^= state << 8
		largeBytes[index] = byte('a' + state%26)
	}
	largeTitle := string(largeBytes)
	if _, err := pool.Exec(ctx, `INSERT INTO weave_adapter_test.tasks (id, organization_id, title, completed) VALUES ($1, $2, $3, true)`, id, "00000000-0000-4000-8000-000000000001", largeTitle); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE weave_adapter_test.tasks SET completed = false, updated_at = now() WHERE id = $1`, id); err != nil {
		t.Fatal(err)
	}

	eventCtx, stop := context.WithTimeout(ctx, 8*time.Second)
	defer stop()
	pending, err := nextCoordinatorBatch(eventCtx, opened)
	if err != nil || len(pending.Events) != 1 || pending.Events[0].Kind != changeprocessor.QueryAdded || !pending.Events[0].Pending {
		t.Fatalf("pending TOAST batch: err=%v events=%d pending=%v", err, len(pending.Events), len(pending.Events) == 1 && pending.Events[0].Pending)
	}
	hydrated, err := nextCoordinatorBatch(eventCtx, opened)
	if err != nil || len(hydrated.Events) != 1 || hydrated.Events[0].Pending || string(hydrated.Events[0].Record["title"].Data) != largeTitle {
		t.Fatalf("hydrated TOAST batch: err=%v events=%d pending=%v", err, len(hydrated.Events), len(hydrated.Events) == 1 && hydrated.Events[0].Pending)
	}
}

func TestPostgresDataSourceSnapshot(t *testing.T) {
	databaseURL := os.Getenv("WEAVE_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("set WEAVE_TEST_DATABASE_URL to run PostgreSQL integration test")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	prepareAdapterFixture(t, ctx, pool)
	source := NewPostgresDataSource(pool, cdc.NewBroker(), []string{"weave_adapter_test.tasks"})
	prepared, err := source.PrepareQuery(ctx, storage.QuerySpec{
		Relation: storage.RelationName{Namespace: "weave_adapter_test", Name: "tasks"},
		Filter:   &storage.Predicate{Comparison: &storage.Comparison{Field: "organization_id", Operator: storage.ComparisonEqual, Value: storage.Literal{Type: storage.TypeUUID, Data: []byte("00000000-0000-4000-8000-000000000001")}}},
		Order:    []storage.OrderTerm{{Field: "id"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	var expected int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM weave_adapter_test.tasks WHERE organization_id = '00000000-0000-4000-8000-000000000001'`).Scan(&expected); err != nil {
		t.Fatal(err)
	}
	snapshot, err := source.BeginSnapshot(ctx, prepared)
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Abort(ctx)
	records, err := snapshot.LoadWindow(ctx, prepared, datastore.WindowRequest{
		Bounds: datastore.Bounds{Left: datastore.Boundary{Kind: datastore.BoundaryStart}, Right: datastore.Boundary{Kind: datastore.BoundaryEnd}},
		Limit:  10,
	})
	if err != nil {
		t.Fatal(err)
	}
	if expected <= 10 && len(records) != expected {
		t.Fatalf("snapshot returned %d records, expected %d", len(records), expected)
	}
	for _, authoritative := range records {
		record := authoritative.Record
		if len(record) != 7 {
			t.Fatalf("snapshot record has %d fields, expected complete 7-field row", len(record))
		}
		key, err := prepared.Key(record)
		if err != nil {
			t.Fatal(err)
		}
		cursor, err := prepared.EncodeKey(key)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := prepared.DecodeKey(cursor); err != nil {
			t.Fatal(err)
		}
	}
	if len(records) > 0 {
		key, err := prepared.EntityKey(records[0].Record)
		if err != nil {
			t.Fatal(err)
		}
		fetched, err := snapshot.FetchEntity(ctx, key)
		if err != nil || string(fetched.Record["id"].Data) != string(records[0].Record["id"].Data) {
			t.Fatalf("primary-key fetch = %+v, err=%v", fetched, err)
		}
	}
	if _, err := snapshot.Finish(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestPostgresPrimaryKeyMetadataExcludesIncludedColumnsAndPreservesIndexOrder(t *testing.T) {
	databaseURL := os.Getenv("WEAVE_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("set WEAVE_TEST_DATABASE_URL to run PostgreSQL integration test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	prepareAdapterFixture(t, ctx, pool)
	const table = "weave_primary_metadata_test"
	if _, err := pool.Exec(ctx, `DROP TABLE IF EXISTS public.weave_primary_metadata_test`); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_, _ = pool.Exec(context.Background(), `DROP TABLE IF EXISTS public.weave_primary_metadata_test`)
	}()
	if _, err := pool.Exec(ctx, `
		CREATE TABLE public.weave_primary_metadata_test (
			id bigint NOT NULL,
			tenant_id bigint NOT NULL,
			updated_at timestamptz NOT NULL DEFAULT now(),
			PRIMARY KEY (tenant_id, id) INCLUDE (updated_at)
		)`); err != nil {
		t.Fatal(err)
	}
	source := NewPostgresDataSource(pool, cdc.NewBroker(), []string{"public." + table})
	prepared, err := source.PrepareQuery(ctx, storage.QuerySpec{
		Relation: storage.RelationName{Namespace: "public", Name: table},
	})
	if err != nil {
		t.Fatal(err)
	}
	postgresPlan := prepared.(*postgresPrepared).plan
	if len(postgresPlan.primary) != 2 || postgresPlan.primary[0] != "tenant_id" || postgresPlan.primary[1] != "id" {
		t.Fatalf("primary columns = %v", postgresPlan.primary)
	}
	order := prepared.Query().Order
	if len(order) != 2 || order[0].Field != "tenant_id" || order[1].Field != "id" {
		t.Fatalf("default order = %v", order)
	}
}

func TestPostgresSnapshotPredicatesMatchDecodedValues(t *testing.T) {
	databaseURL := os.Getenv("WEAVE_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("set WEAVE_TEST_DATABASE_URL to run PostgreSQL integration test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	prepareAdapterFixture(t, ctx, pool)
	conn, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Release()
	if _, err := conn.Exec(ctx, `
		CREATE TEMP TABLE weave_type_contract (
			id integer PRIMARY KEY,
			real_value real,
			small_value smallint,
			numeric_value numeric,
			json_value json,
			jsonb_value jsonb,
			day_value date,
			local_instant timestamp,
			instant_value timestamptz
		);
		INSERT INTO weave_type_contract VALUES
			(1, 0.1, 1, 123, '1e0', '1e0', DATE '2024-01-01', TIMESTAMP '2024-01-01 00:00:00', TIMESTAMPTZ '2024-01-01 00:00:00+00'),
			(2, NULL, NULL, NULL, '"text"', '"text"', NULL, NULL, NULL),
			(3, NULL, NULL, NULL, 'true', 'true', NULL, NULL, NULL)`); err != nil {
		t.Fatal(err)
	}
	var namespace string
	if err := conn.QueryRow(ctx, `
		SELECT n.nspname
		FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE c.oid = 'pg_temp.weave_type_contract'::regclass`).Scan(&namespace); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name      string
		field     string
		oid       uint32
		native    string
		operator  storage.ComparisonOperator
		valueType storage.ValueType
		literal   string
	}{
		{name: "real literal uses native precision", field: "real_value", oid: 700, native: "0.1", operator: storage.ComparisonEqual, valueType: storage.TypeFloat, literal: "0.1"},
		{name: "out of range smallint uses constant comparison", field: "small_value", oid: 21, native: "1", operator: storage.ComparisonLess, valueType: storage.TypeInteger, literal: "40000"},
		{name: "scientific decimal remains exact", field: "numeric_value", oid: 1700, native: "123", operator: storage.ComparisonEqual, valueType: storage.TypeDecimal, literal: "123"},
		{name: "json exponent remains decimal", field: "json_value", oid: 114, native: "1e0", operator: storage.ComparisonEqual, valueType: storage.TypeInteger, literal: "1"},
		{name: "json exponent matches decimal", field: "json_value", oid: 114, native: "1e0", operator: storage.ComparisonEqual, valueType: storage.TypeDecimal, literal: "1"},
		{name: "jsonb normalized exponent is integer", field: "jsonb_value", oid: 3802, native: "1", operator: storage.ComparisonEqual, valueType: storage.TypeInteger, literal: "1"},
		{name: "date compares as UTC midnight", field: "day_value", oid: 1082, native: "2024-01-01", operator: storage.ComparisonLess, valueType: storage.TypeDateTime, literal: "2024-01-01T12:00:00Z"},
		{name: "timestamp without timezone accepts canonical datetime", field: "local_instant", oid: 1114, native: "2024-01-01 00:00:00", operator: storage.ComparisonEqual, valueType: storage.TypeDateTime, literal: "2024-01-01T00:00:00.0000006Z"},
		{name: "timestamp query uses adapter precision", field: "instant_value", oid: 1184, native: "2024-01-01 00:00:00+00", operator: storage.ComparisonEqual, valueType: storage.TypeDateTime, literal: "2024-01-01T00:00:00.0000006Z"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			value, err := storage.ParseScalar(test.valueType, []byte(test.literal))
			if err != nil {
				t.Fatal(err)
			}
			literal, err := storage.LiteralFromValue(value)
			if err != nil {
				t.Fatal(err)
			}
			predicate := &storage.Predicate{Comparison: &storage.Comparison{
				Field: storage.FieldPath(test.field), Operator: test.operator,
				Value: literal,
			}}
			p := &plan{
				adapter: newPostgresTypeAdapter(),
				query:   predicateQuery(predicate), schema: namespace, table: "weave_type_contract",
				columns: map[string]column{
					"id":       {name: "id", typeOID: 23, valueType: storage.TypeInteger, supported: true},
					test.field: {name: test.field, typeOID: test.oid, valueType: test.valueType, supported: true},
				},
				needed: []string{"id"}, order: []storage.OrderTerm{{Field: "id"}},
			}
			if err := p.canonicalizePredicateValues(p.query.Filter); err != nil {
				t.Fatal(err)
			}
			p.comparisons = make(map[*storage.Comparison]postgresComparison)
			if err := p.preparePredicateComparisons(p.query.Filter); err != nil {
				t.Fatal(err)
			}
			rows, err := p.loadWindow(ctx, conn, boundary{kind: boundaryStart}, boundary{kind: boundaryEnd}, false, 2)
			if err != nil {
				t.Fatal(err)
			}
			decoded, supported, err := (postgresTypeAdapter{}).decodeCell(cdc.Cell{TypeOID: test.oid, Text: []byte(test.native)})
			if err != nil || !supported {
				t.Fatalf("decode: supported=%v err=%v", supported, err)
			}
			logicalMatch, err := storage.EvaluatePredicate(predicate, storage.Record{test.field: decoded})
			if err != nil {
				t.Fatal(err)
			}
			if sqlMatch := len(rows) == 1; sqlMatch != logicalMatch {
				t.Fatalf("snapshot match=%v, decoded-value match=%v", sqlMatch, logicalMatch)
			}
		})
	}
}

func predicateQuery(predicate *storage.Predicate) storage.QuerySpec {
	return storage.QuerySpec{Filter: predicate, Order: []storage.OrderTerm{{Field: "id"}}}
}

func TestJSONNumericPredicatesMatchDecoderAtInt64Bounds(t *testing.T) {
	dsn := os.Getenv("WEAVE_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set WEAVE_TEST_DATABASE_URL to run PostgreSQL integration test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	for _, typ := range []struct {
		name string
		oid  uint32
	}{{"json", 114}, {"jsonb", 3802}} {
		for _, native := range []string{
			"-9223372036854775809", "-9223372036854775808", "9223372036854775807", "9223372036854775808",
			"0", "1.0", "1e0", `"text"`, "true", "null", "{}", "[]",
		} {
			// Decode the database's actual representation: jsonb may normalize
			// number spelling differently from json.
			var text string
			if err := pool.QueryRow(ctx, "SELECT ($1::"+typ.name+")::text", native).Scan(&text); err != nil {
				t.Fatal(err)
			}
			decoded, _, err := (postgresTypeAdapter{}).decodeCell(cdc.Cell{TypeOID: typ.oid, Text: []byte(text)})
			if err != nil {
				t.Fatal(err)
			}
			for _, literal := range []storage.Literal{
				{Type: storage.TypeInteger, Data: []byte("0")},
				{Type: storage.TypeInteger, Data: []byte("-9223372036854775808")},
				{Type: storage.TypeInteger, Data: []byte("9223372036854775807")},
				{Type: storage.TypeDecimal, Data: []byte("-9223372036854775809")},
				{Type: storage.TypeDecimal, Data: []byte("9223372036854775808")},
			} {
				for _, op := range []storage.ComparisonOperator{
					storage.ComparisonEqual, storage.ComparisonNotEqual, storage.ComparisonLess,
					storage.ComparisonLessOrEqual, storage.ComparisonGreater, storage.ComparisonGreaterOrEqual,
				} {
					t.Run(fmt.Sprintf("%s/%s/%s/%v/%s", typ.name, native, literal.Type, op, literal.Data), func(t *testing.T) {
						predicate := &storage.Predicate{Comparison: &storage.Comparison{Field: "value", Operator: op, Value: literal}}
						p := &plan{adapter: newPostgresTypeAdapter(), columns: map[string]column{
							"value": {name: "value", typeOID: typ.oid, supported: true},
						}}
						preparePredicateForSQL(t, p, predicate)
						args := []any{native}
						sql, err := p.predicateSQL(predicate, &args)
						if err != nil {
							t.Fatal(err)
						}
						var actual bool
						if err := pool.QueryRow(ctx, "SELECT "+sql+" FROM (SELECT $1::"+typ.name+" AS value) AS input", args...).Scan(&actual); err != nil {
							t.Fatal(err)
						}
						want, err := storage.EvaluatePredicate(predicate, storage.Record{"value": decoded})
						if err != nil || actual != want {
							t.Fatalf("SQL match=%v decoder match=%v err=%v", actual, want, err)
						}
					})
				}
			}
		}
	}
}

func TestJSONPathsTraverseOnlyObjects(t *testing.T) {
	dsn := os.Getenv("WEAVE_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set WEAVE_TEST_DATABASE_URL to run PostgreSQL integration test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	for _, typ := range []struct {
		name string
		oid  uint32
	}{{"json", 114}, {"jsonb", 3802}} {
		for _, native := range []string{
			`["x"]`, `[null]`, `{"0":"x"}`, `{"0":null}`, `{"-1":"x"}`,
			`{"nested":["x"]}`, `{"nested":[null]}`, `{"nested":{"0":"x"}}`, `{"nested":{"0":null}}`,
			`[{"0":"x"}]`, `"x"`, `null`, `{}`,
		} {
			decoded, _, err := (postgresTypeAdapter{}).decodeCell(cdc.Cell{TypeOID: typ.oid, Text: []byte(native)})
			if err != nil {
				t.Fatal(err)
			}
			for _, path := range []storage.FieldPath{
				storage.MustFieldPath("payload", "0"), storage.MustFieldPath("payload", "-1"),
				storage.MustFieldPath("payload", "nested", "0"), storage.MustFieldPath("payload", "0", "0"),
			} {
				for _, predicate := range []*storage.Predicate{
					{Comparison: &storage.Comparison{Field: path, Operator: storage.ComparisonEqual, Value: storage.Literal{Type: storage.TypeText, Data: []byte("x")}}},
					{Comparison: &storage.Comparison{Field: path, Operator: storage.ComparisonNotEqual, Value: storage.Literal{Type: storage.TypeText, Data: []byte("x")}}},
					{Null: &storage.NullTest{Field: path, IsNull: true}},
					{Null: &storage.NullTest{Field: path, IsNull: false}},
				} {
					p := &plan{adapter: newPostgresTypeAdapter(), columns: map[string]column{
						"payload": {name: "payload", typeOID: typ.oid, supported: true},
					}}
					preparePredicateForSQL(t, p, predicate)
					args := []any{native}
					sql, err := p.predicateSQL(predicate, &args)
					if err != nil {
						t.Fatal(err)
					}
					var actual bool
					if err := pool.QueryRow(ctx, "SELECT "+sql+" FROM (SELECT $1::"+typ.name+" AS payload) AS input", args...).Scan(&actual); err != nil {
						t.Fatal(err)
					}
					want, err := storage.EvaluatePredicate(predicate, storage.Record{"payload": decoded})
					if err != nil || actual != want {
						t.Fatalf("type=%s value=%s path=%s predicate=%+v: SQL=%v live=%v err=%v", typ.name, native, path.Display(), predicate, actual, want, err)
					}
				}
			}
		}
	}
}
