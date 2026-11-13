package cdc

import (
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// This fixture needs a disposable PostgreSQL cluster and a role with CREATEDB.
// It emits no logical barrier: only the native keepalive can cover the sibling
// database's writes while this database's published transaction remains open.
func TestNativeKeepaliveAdvancesAcrossOtherDatabaseAndResumesOpenTransaction(t *testing.T) {
	dsn := os.Getenv("WEAVE_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set WEAVE_TEST_DATABASE_URL for PostgreSQL integration")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	var nonce [8]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		t.Fatal(err)
	}
	name := fmt.Sprintf("weave_native_%x", nonce)
	quoted := pgx.Identifier{name}.Sanitize()
	if _, err := pool.Exec(ctx, "CREATE TABLE "+quoted+" (id bigint PRIMARY KEY); CREATE PUBLICATION "+quoted+" FOR TABLE "+quoted); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanup, done := context.WithTimeout(context.Background(), 5*time.Second)
		defer done()
		if _, err := pool.Exec(cleanup, "DROP PUBLICATION "+quoted+"; DROP TABLE "+quoted); err != nil {
			t.Error(err)
		}
	})
	if _, err := pool.Exec(ctx, "CREATE DATABASE "+quoted); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanup, done := context.WithTimeout(context.Background(), 5*time.Second)
		defer done()
		if _, err := pool.Exec(cleanup, "DROP DATABASE "+quoted); err != nil {
			t.Error(err)
		}
	})
	otherConfig := pool.Config().Copy()
	otherConfig.ConnConfig.Database = name
	other, err := pgxpool.NewWithConfig(ctx, otherConfig)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(other.Close)
	if _, err := other.Exec(ctx, "CREATE TABLE writes (id bigint PRIMARY KEY)"); err != nil {
		t.Fatal(err)
	}
	config := pool.Config().ConnConfig.Config.Copy()
	config.RuntimeParams["wal_sender_timeout"] = "500ms"
	broker := NewBroker()
	replicator, err := NewReplicator(config, name, name, broker, slog.New(slog.NewTextHandler(io.Discard, nil)), ReplicatorOptions{Replication: ReplicationOptions{StatusInterval: time.Second}})
	if err != nil {
		t.Fatal(err)
	}
	var stop func()
	start := func() {
		run, cancel := context.WithCancel(ctx)
		done := make(chan struct{})
		go func() { defer close(done); replicator.Run(run) }()
		stop = func() { cancel(); <-done }
	}
	t.Cleanup(func() {
		if stop != nil {
			stop()
		}
		cleanup, done := context.WithTimeout(context.Background(), 5*time.Second)
		defer done()
		if _, err := pool.Exec(cleanup, `SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name=$1 AND NOT active`, name); err != nil {
			t.Error(err)
		}
	})
	start()
	if err := broker.WaitReady(ctx); err != nil {
		t.Fatal(err)
	}
	feed, err := broker.Subscribe()
	if err != nil {
		t.Fatal(err)
	}
	defer feed.Close()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.Background())
	if _, err := tx.Exec(ctx, "INSERT INTO "+quoted+" VALUES (1)"); err != nil {
		t.Fatal(err)
	}
	if _, err := other.Exec(ctx, "INSERT INTO writes SELECT generate_series(1, 100)"); err != nil {
		t.Fatal(err)
	}
	var raw string
	if err := other.QueryRow(ctx, "SELECT pg_current_wal_lsn()::text").Scan(&raw); err != nil {
		t.Fatal(err)
	}
	target, err := pglogrepl.ParseLSN(raw)
	if err != nil {
		t.Fatal(err)
	}
	receive := func() StreamEvent {
		t.Helper()
		select {
		case event := <-feed.Events:
			return event
		case err := <-feed.Err:
			t.Fatalf("capture failed: %v", err)
		case <-ctx.Done():
			t.Fatal("native progress or resumed transaction never arrived")
		}
		return StreamEvent{}
	}
	var through pglogrepl.LSN
	for through < target {
		event := receive()
		if event.Transaction != nil {
			t.Fatal("uncommitted transaction escaped")
		}
		through = event.Position
	}
	if broker.Acknowledged() != 0 {
		t.Fatal("native receive bypassed downstream acknowledgment")
	}
	broker.Acknowledge(through)
	// Wait for actual slot confirmation, not merely the in-process Ack value.
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		if err := pool.QueryRow(ctx, `SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name=$1`, name).Scan(&raw); err != nil {
			t.Fatal(err)
		}
		confirmed, err := pglogrepl.ParseLSN(raw)
		if err != nil {
			t.Fatal(err)
		}
		if confirmed >= through {
			break
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			t.Fatal("native progress was not acknowledged to PostgreSQL")
		}
	}
	stop()
	stop = nil
	// The row's WAL precedes the acknowledged marker, but its commit follows
	// it. PostgreSQL must reconstruct it from restart_lsn after reconnect.
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	start()
	for {
		event := receive()
		if event.Transaction == nil {
			continue
		}
		transaction := event.Transaction
		// LSNs are byte boundaries: the commit record may start exactly at
		// the acknowledged end of the preceding record. Its end must follow.
		if transaction.EndLSN <= through || len(transaction.Changes) != 1 || transaction.Changes[0].Table != name || string(transaction.Changes[0].New["id"].Text) != "1" {
			t.Fatalf("wrong transaction after resume from %s: %+v", through, transaction)
		}
		break
	}
}
