package postgres

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"

	"github.com/adiom-data/weave/internal/datastore"
	"github.com/adiom-data/weave/internal/entitykey"
	"github.com/adiom-data/weave/internal/postgres/cdc"
	"github.com/adiom-data/weave/internal/storage"
)

func TestPostgresFeedPreservesCaptureGapCause(t *testing.T) {
	broker := cdc.NewBroker()
	broker.MarkReady(0)
	feed, err := newFeedSource(broker, newPostgresTypeAdapter()).OpenFeed(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer feed.Close()
	// Overflow the broker's input queue, then receive enough times to exercise
	// both buffered-events and closed-events terminal selection paths.
	for index := 1; index <= 1025; index++ {
		broker.Advance(pglogrepl.LSN(index))
	}
	for {
		_, err := feed.Receive(context.Background())
		if err == nil {
			continue
		}
		if !errors.Is(err, storage.ErrResumeRequired) || !errors.Is(err, cdc.ErrSlowConsumer) || errors.Is(err, io.EOF) {
			t.Fatalf("capture gap cause = %v", err)
		}
		break
	}
}

func TestPostgresChangeMergesUnavailableValues(t *testing.T) {
	identity := entitykey.Encode([]entitykey.Part{{Name: "id", TypeOID: 23, Value: []byte("1")}})
	change := postgresChange{adapter: newPostgresTypeAdapter(), change: cdc.Change{
		Operation:   "update",
		RelationID:  42,
		Schema:      "app",
		Table:       "tasks",
		Identity:    identity,
		NewIdentity: identity,
		New: cdc.Row{
			"id":    {TypeOID: 23, Text: []byte("1")},
			"title": {TypeOID: 25, Missing: true},
			"done":  {TypeOID: 16, Text: []byte("t")},
		},
	}}
	previous := storage.Record{
		"id":      {Type: storage.TypeInteger, State: storage.ValuePresent, Data: []byte("1")},
		"title":   {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("kept")},
		"done":    {Type: storage.TypeBool, State: storage.ValuePresent, Data: []byte("f")},
		"dropped": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("stale")},
	}
	result, err := change.Apply(previous)
	if err != nil {
		t.Fatal(err)
	}
	if !result.Complete() || !result.Exists() || string(result.Record["title"].Data) != "kept" || string(result.Record["done"].Data) != "true" {
		t.Fatalf("result = %+v", result)
	}
	if _, exists := result.Record["dropped"]; exists {
		t.Fatalf("field absent from authoritative tuple survived: %+v", result.Record)
	}
}

func TestPostgresChangeIdentityDoesNotDecodePayload(t *testing.T) {
	identity := entitykey.Encode([]entitykey.Part{{Name: "id", TypeOID: 23, Value: []byte("1")}})
	change := newPostgresChange(newPostgresTypeAdapter(), cdc.Change{
		Operation: "update", RelationID: 42, Schema: "app", Table: "tasks", Identity: identity, NewIdentity: identity,
		New: cdc.Row{"payload": {TypeOID: 3802, Text: []byte("invalid JSON")}},
	})
	before, err := change.BeforeKey()
	if err != nil {
		t.Fatal(err)
	}
	after, err := change.AfterKey()
	if err != nil || after != before {
		t.Fatalf("identity depends on row payload: %+v %v", after, err)
	}
	if _, err := change.Apply(nil); err == nil {
		t.Fatal("invalid payload unexpectedly decoded")
	}
}

func TestPostgresFeedPreservesOpaqueCommittedMessages(t *testing.T) {
	broker := cdc.NewBroker()
	broker.MarkReady(0)
	feed, err := newFeedSource(broker, newPostgresTypeAdapter()).OpenFeed(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer feed.Close()
	if err := broker.Publish(context.Background(), cdc.Transaction{CommitLSN: 10, EndLSN: 11, Messages: []cdc.Message{{Prefix: "app.receipt", Content: []byte{0, 255}}}}); err != nil {
		t.Fatal(err)
	}
	event, err := feed.Receive(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if event.Transaction == nil || event.Transaction.Checkpoint.Order() != 11 || !event.Transaction.Atomic || len(event.Transaction.Messages) != 1 || event.Transaction.Messages[0].Kind != "app.receipt" || string(event.Transaction.Messages[0].Payload) != string([]byte{0, 255}) {
		t.Fatalf("event = %+v", event)
	}
	if broker.Acknowledged() != 0 {
		t.Fatal("receiving messages acknowledged WAL")
	}
}

func TestPostgresChangeRequiresHydrationWithoutCachedToast(t *testing.T) {
	identity := entitykey.Encode([]entitykey.Part{{Name: "id", TypeOID: 23, Value: []byte("1")}})
	change := postgresChange{adapter: newPostgresTypeAdapter(), change: cdc.Change{
		Operation: "update", Schema: "app", Table: "tasks", Identity: identity, NewIdentity: identity,
		New: cdc.Row{"document": {TypeOID: 3802, Missing: true}},
	}}
	result, err := change.Apply(nil)
	if err != nil {
		t.Fatal(err)
	}
	if result.Complete() || result.Record["document"].State != storage.ValueUnavailable {
		t.Fatalf("result = %+v", result)
	}
}

func TestPostgresChangeRejectsMissingIdentityAsRelationError(t *testing.T) {
	change := postgresChange{adapter: newPostgresTypeAdapter(), change: cdc.Change{Operation: "update", Schema: "app", Table: "tasks"}}
	if _, err := change.BeforeKey(); err == nil {
		t.Fatal("missing old identity was accepted")
	}
	if _, err := change.Apply(nil); err == nil {
		t.Fatal("missing new identity was accepted")
	}
}

func TestPostgresFeedEmitsCommittedAtomicChanges(t *testing.T) {
	broker := cdc.NewBroker()
	broker.MarkReady(0)
	feedInterface, err := newFeedSource(broker, newPostgresTypeAdapter()).OpenFeed(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	feed := feedInterface.(*changeFeed)
	defer feed.Close()
	identity := entitykey.Encode([]entitykey.Part{{Name: "id", TypeOID: 23, Value: []byte("1")}})
	transaction := cdc.Transaction{ID: 7, CommitLSN: pglogrepl.LSN(100), EndLSN: pglogrepl.LSN(110), Changes: []cdc.Change{{Operation: "insert", Schema: "app", Table: "tasks", Identity: identity, NewIdentity: identity}}}
	if err := broker.Publish(context.Background(), transaction); err != nil {
		t.Fatal(err)
	}
	event, err := feed.Receive(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if event.Transaction == nil || !event.Transaction.Atomic || len(event.Transaction.Changes) != 1 || event.Transaction.Changes[0].Kind() != datastore.ChangeInsert {
		t.Fatalf("event = %+v", event)
	}
	if !feed.Covers(WALPosition{LSN: 110}) {
		t.Fatal("feed should cover the committed position after receiving its queue")
	}
	if broker.Acknowledged() != 0 {
		t.Fatal("broker acknowledged WAL before coordinator Ack")
	}
	if err := feed.Ack(context.Background(), event.Transaction.Checkpoint); err != nil {
		t.Fatal(err)
	}
	if broker.Acknowledged() != 110 {
		t.Fatalf("acknowledged LSN = %v, want 110", broker.Acknowledged())
	}
}

func TestPostgresFeedDefersIdentityConversionToRelationProcessing(t *testing.T) {
	broker := cdc.NewBroker()
	broker.MarkReady(0)
	feedInterface, err := newFeedSource(broker, newPostgresTypeAdapter()).OpenFeed(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	feed := feedInterface.(*changeFeed)
	defer feed.Close()
	identity := entitykey.Encode([]entitykey.Part{{Name: "id", TypeOID: 999999, Value: []byte("native")}})
	transaction := cdc.Transaction{
		CommitLSN: pglogrepl.LSN(200),
		EndLSN:    pglogrepl.LSN(210),
		Changes: []cdc.Change{{
			Operation: "update", Schema: "app", Table: "unsupported_keys",
			Identity: identity, NewIdentity: identity,
		}},
	}
	if err := broker.Publish(context.Background(), transaction); err != nil {
		t.Fatal(err)
	}
	event, err := feed.Receive(context.Background())
	if err != nil {
		t.Fatalf("feed interpreted a relation-scoped identity: %v", err)
	}
	if event.Transaction == nil || len(event.Transaction.Changes) != 1 {
		t.Fatalf("event = %+v, want one change", event)
	}
	if _, err := event.Transaction.Changes[0].BeforeKey(); err == nil {
		t.Fatal("relation processing accepted an unsupported primary-key type")
	}
}

func TestPostgresFeedWaitsForDurableCaptureReadiness(t *testing.T) {
	broker := cdc.NewBroker()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	opened := make(chan error, 1)
	go func() {
		feed, err := newFeedSource(broker, newPostgresTypeAdapter()).OpenFeed(ctx)
		if feed != nil {
			_ = feed.Close()
		}
		opened <- err
	}()
	select {
	case err := <-opened:
		t.Fatalf("feed opened before durable capture was ready: %v", err)
	case <-time.After(10 * time.Millisecond):
	}
	broker.MarkReady(0)
	if err := <-opened; err != nil {
		t.Fatal(err)
	}
}

func TestPostgresFeedCoverageUsesConsumedPositionNotQueueEmptiness(t *testing.T) {
	broker := cdc.NewBroker()
	broker.MarkReady(0)
	feedInterface, err := newFeedSource(broker, newPostgresTypeAdapter()).OpenFeed(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	feed := feedInterface.(*changeFeed)
	defer feed.Close()
	for _, lsn := range []pglogrepl.LSN{110, 120} {
		if err := broker.Publish(context.Background(), cdc.Transaction{CommitLSN: lsn, EndLSN: lsn}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := feed.Receive(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !feed.Covers(WALPosition{LSN: 110}) {
		t.Fatal("feed did not cover a consumed position while a newer event remained queued")
	}
	if feed.Covers(WALPosition{LSN: 120}) {
		t.Fatal("feed covered a position it had not consumed")
	}
}

func TestPostgresFeedEmitsOrderedProgressCheckpoint(t *testing.T) {
	broker := cdc.NewBroker()
	broker.MarkReady(0)
	feedInterface, err := newFeedSource(broker, newPostgresTypeAdapter()).OpenFeed(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	feed := feedInterface.(*changeFeed)
	defer feed.Close()
	broker.Advance(150)
	event, err := feed.Receive(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if event.Progress == nil || event.Transaction != nil || !feed.Covers(WALPosition{LSN: 150}) {
		t.Fatalf("progress event = %+v", event)
	}
	if err := feed.Ack(context.Background(), event.Progress); err != nil {
		t.Fatal(err)
	}
	if broker.Acknowledged() != 150 {
		t.Fatalf("acknowledged LSN = %v, want 150", broker.Acknowledged())
	}
}

func TestPostgresRelationWideChangeIsEngineOwned(t *testing.T) {
	_, err := (postgresChange{adapter: newPostgresTypeAdapter(), change: cdc.Change{Operation: "truncate"}}).Apply(nil)
	if err == nil || errors.Is(err, datastore.ErrInvalidated) {
		t.Fatalf("truncate error = %v", err)
	}
}
