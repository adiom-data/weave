package postgres

import (
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"log/slog"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/adiom-data/weave/internal/messagebudget"
	"github.com/adiom-data/weave/internal/postgres/cdc"
)

func TestPostgresTransactionalMessagesIntegration(t *testing.T) {
	databaseURL := os.Getenv("WEAVE_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("set WEAVE_TEST_DATABASE_URL to run PostgreSQL integration test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	prepareAdapterFixture(t, ctx, pool)
	var nonce [8]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		t.Fatal(err)
	}
	slot := fmt.Sprintf("weave_messages_%x", nonce)
	table := pgx.Identifier{"weave_adapter_test", slot}.Sanitize()
	if _, err := pool.Exec(ctx, "CREATE TABLE "+table+" (id bigint PRIMARY KEY)"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanup, done := context.WithTimeout(context.Background(), 5*time.Second)
		defer done()
		if _, err := pool.Exec(cleanup, "DROP TABLE "+table); err != nil {
			t.Errorf("drop isolated test table: %v", err)
		}
	})
	// Register cleanup after table cleanup so the slot is stopped/dropped first.
	// The randomly named slot is created by this test, never a shared slot.
	runCtx, stop := context.WithCancel(ctx)
	broker := cdc.NewBroker()
	replicator, err := cdc.NewReplicator(&pool.Config().ConnConfig.Config, "weave_adapter_test_publication", slot, broker, slog.New(slog.NewTextHandler(io.Discard, nil)), cdc.ReplicatorOptions{MessageLimits: messagebudget.Limits{MaxMessageBytes: 16, MaxTransactionBytes: 256, MaxMessages: 10}, MessageLogInterval: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	replicationDone := make(chan struct{})
	go func() {
		defer close(replicationDone)
		replicator.Run(runCtx)
	}()
	t.Cleanup(func() {
		stop()
		<-replicationDone
		cleanup, done := context.WithTimeout(context.Background(), 5*time.Second)
		defer done()
		if _, err := pool.Exec(cleanup, `SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = $1 AND NOT active`, slot); err != nil {
			t.Errorf("drop isolated replication slot: %v", err)
		}
	})
	source := NewPostgresDataSourceWithOptions(pool, broker, DataSourceOptions{})
	feed, err := source.OpenFeed(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer feed.Close()
	metadata, err := source.OpenMessageFeed(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer metadata.Close()
	// Message-only committed transaction, preserving binary contents and order.
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.Background())
	var messageLSN string
	if err := tx.QueryRow(ctx, `SELECT pg_logical_emit_message(true, 'test.binary', $1::bytea)::text`, []byte{0, 255, 128}).Scan(&messageLSN); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `SELECT pg_logical_emit_message(true, 'test.second', 'two')`); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	// A rolled-back message and nontransactional application message must not
	// appear later, even after the following row+message transaction commits.
	aborted, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer aborted.Rollback(context.Background())
	if _, err := aborted.Exec(ctx, `SELECT pg_logical_emit_message(true, 'test.aborted', 'no')`); err != nil {
		t.Fatal(err)
	}
	if err := aborted.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `SELECT pg_logical_emit_message(false, 'test.nontransactional', 'no')`); err != nil {
		t.Fatal(err)
	}
	withRow, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer withRow.Rollback(context.Background())
	if _, err := withRow.Exec(ctx, "INSERT INTO "+table+" VALUES (1)"); err != nil {
		t.Fatal(err)
	}
	if _, err := withRow.Exec(ctx, `SELECT pg_logical_emit_message(true, 'test.row', 'yes')`); err != nil {
		t.Fatal(err)
	}
	if _, err := withRow.Exec(ctx, `SELECT pg_logical_emit_message(true, 'test.large', $1::bytea)`, make([]byte, 1024)); err != nil {
		t.Fatal(err)
	}
	if _, err := withRow.Exec(ctx, `SELECT pg_logical_emit_message(true, 'test.after', 'yes')`); err != nil {
		t.Fatal(err)
	}
	if err := withRow.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	var barrierLSN string
	if err := pool.QueryRow(ctx, `SELECT pg_logical_emit_message(false, $1, '')::text`, cdc.BarrierPrefix).Scan(&barrierLSN); err != nil {
		t.Fatal(err)
	}
	barrier, err := pglogrepl.ParseLSN(barrierLSN)
	if err != nil {
		t.Fatal(err)
	}
	emittedAt, err := pglogrepl.ParseLSN(messageLSN)
	if err != nil {
		t.Fatal(err)
	}
	var kinds []string
	var previous uint64
	for {
		event, err := feed.Receive(ctx)
		if err != nil {
			t.Fatal(err)
		}
		checkpoint := event.Progress
		if event.Transaction != nil {
			checkpoint = event.Transaction.Checkpoint
			for _, message := range event.Transaction.Messages {
				kinds = append(kinds, message.Kind)
				if message.Kind == "test.binary" {
					if len(event.Transaction.Changes) != 0 || !event.Transaction.Atomic || checkpoint.Order() <= uint64(emittedAt) || string(message.Payload) != string([]byte{0, 255, 128}) {
						t.Fatal("incorrect message-only commit")
					}
				}
				if message.Kind == "test.row" && len(event.Transaction.Changes) != 1 {
					t.Fatal("row and message were separated")
				}
				if message.Kind == "" && (len(message.Payload) != 0 || len(event.Transaction.Changes) != 1) {
					t.Fatal("oversized metadata affected its row transaction")
				}
			}
		}
		if checkpoint.Order() <= previous {
			t.Fatal("source order regressed")
		}
		previous = checkpoint.Order()
		if previous >= uint64(barrier) {
			break
		}
	}
	if !reflect.DeepEqual(kinds, []string{"test.binary", "test.second", "test.row", "", "test.after"}) {
		t.Fatalf("committed message kinds = %v", kinds)
	}
	var metadataKinds []string
	var metadataPosition uint64
	for metadataPosition < uint64(barrier) {
		event, err := metadata.Receive(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if event.Checkpoint.Source() != SourceID || event.Checkpoint.Order() <= metadataPosition {
			t.Fatal("metadata feed progress regressed")
		}
		metadataPosition = event.Checkpoint.Order()
		for _, message := range event.Messages {
			metadataKinds = append(metadataKinds, message.Kind)
		}
	}
	if !reflect.DeepEqual(metadataKinds, kinds) {
		t.Fatalf("metadata-only feed differs from normal feed: %v vs %v", metadataKinds, kinds)
	}
	if broker.Acknowledged() != 0 {
		t.Fatal("reading messages acknowledged WAL")
	}
}
