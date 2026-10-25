package cdc

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/adiom-data/weave/internal/entitykey"
	"github.com/adiom-data/weave/internal/messagebudget"
)

func TestCaptureReplacesOversizedMessageBeforeRetainingBytes(t *testing.T) {
	pending := pendingTransaction{messageBudget: messagebudget.Budget{Limits: messagebudget.Limits{MaxMessageBytes: 16, MaxTransactionBytes: 256, MaxMessages: 10}}}
	pending.begin(1)
	pending.changes = []Change{{Operation: "insert"}}
	for _, message := range []*pglogrepl.LogicalDecodingMessage{
		{Transactional: true, Prefix: "good", Content: []byte("before")},
		{Transactional: true, Prefix: "bad", Content: make([]byte, 1024)},
		{Transactional: true, Prefix: "good", Content: []byte("after")},
	} {
		pending.message(message)
	}
	if len(pending.messages) != 3 || pending.messages[1].Prefix != "" || len(pending.messages[1].Content) != 0 || string(pending.messages[2].Content) != "after" {
		t.Fatalf("captured messages = %+v", pending.messages)
	}
	broker := NewBroker()
	subscription, err := broker.Subscribe()
	if err != nil {
		t.Fatal(err)
	}
	defer subscription.Close()
	if err := pending.commit(context.Background(), broker, &pglogrepl.CommitMessage{CommitLSN: 10}); err != nil {
		t.Fatal(err)
	}
	event := <-subscription.Events
	if len(event.Transaction.Changes) != 1 || len(event.Transaction.Messages) != 3 {
		t.Fatal("message limit discarded rows or transaction")
	}
}

func TestCaptureBoundsAnAttackUsingManyEmptyMessages(t *testing.T) {
	pending := pendingTransaction{messageBudget: messagebudget.Budget{Limits: messagebudget.Limits{MaxMessageBytes: 16, MaxTransactionBytes: 256, MaxMessages: 2}}}
	pending.begin(1)
	for index := 0; index < 10000; index++ {
		pending.message(&pglogrepl.LogicalDecodingMessage{Transactional: true})
	}
	if len(pending.messages) != 3 || pending.messageBudget.Bytes > 256 {
		t.Fatalf("unbounded capture = %+v", pending.messageBudget)
	}
	pending.begin(2)
	if pending.messageBudget.Bytes != 0 || pending.messageBudget.Dropped != 0 || pending.messageBudget.Limits.MaxMessages != 2 {
		t.Fatal("budget did not reset between transactions")
	}
}

func TestTransactionalMessagesPublishOnlyAtCommit(t *testing.T) {
	broker := NewBroker()
	feed, err := broker.Subscribe()
	if err != nil {
		t.Fatal(err)
	}
	defer feed.Close()
	var pending pendingTransaction
	pending.begin(1)
	payload := []byte{0, 1, 255}
	pending.message(&pglogrepl.LogicalDecodingMessage{Transactional: true, Prefix: "mutation", Content: payload, LSN: 5})
	payload[0] = 99
	select {
	case <-feed.Events:
		t.Fatal("uncommitted message escaped")
	default:
	}
	if err := pending.commit(context.Background(), broker, &pglogrepl.CommitMessage{CommitLSN: 10, TransactionEndLSN: 11}); err != nil {
		t.Fatal(err)
	}
	event := <-feed.Events
	if event.Position != 11 || event.Transaction == nil || len(event.Transaction.Changes) != 0 || len(event.Transaction.Messages) != 1 || event.Transaction.Messages[0].Content[0] != 0 {
		t.Fatalf("committed message-only transaction = %+v", event)
	}
	// A new transaction cannot overwrite a previously published batch's backing
	// storage, and abandoned/uncommitted messages are discarded on a new Begin.
	pending.begin(2)
	pending.message(&pglogrepl.LogicalDecodingMessage{Transactional: true, Prefix: "abandoned", Content: []byte("old")})
	pending.begin(3)
	pending.message(&pglogrepl.LogicalDecodingMessage{Transactional: false, Prefix: "ignored", Content: []byte("nontransactional")})
	pending.changes = append(pending.changes, Change{Operation: "insert"})
	pending.message(&pglogrepl.LogicalDecodingMessage{Transactional: true, Prefix: "new", Content: []byte("one")})
	pending.message(&pglogrepl.LogicalDecodingMessage{Transactional: true, Prefix: "new", Content: []byte("two")})
	if err := pending.commit(context.Background(), broker, &pglogrepl.CommitMessage{CommitLSN: 20}); err != nil {
		t.Fatal(err)
	}
	next := <-feed.Events
	if next.Position != 20 || len(next.Transaction.Messages) != 2 || len(next.Transaction.Changes) != 1 || string(next.Transaction.Messages[1].Content) != "two" {
		t.Fatalf("next = %+v", next)
	}
	if event.Transaction.Messages[0].Prefix != "mutation" {
		t.Fatal("previous transaction was overwritten")
	}
}

func TestTransactionalBarrierAdvancesOnlyOnCommitWithoutApplicationMetadata(t *testing.T) {
	broker := NewBroker()
	feed, err := broker.Subscribe()
	if err != nil {
		t.Fatal(err)
	}
	defer feed.Close()
	var pending pendingTransaction
	pending.begin(1)
	pending.message(&pglogrepl.LogicalDecodingMessage{Transactional: true, Prefix: BarrierPrefix, LSN: 5})
	select {
	case <-feed.Events:
		t.Fatal("uncommitted barrier escaped")
	default:
	}
	if err := pending.commit(context.Background(), broker, &pglogrepl.CommitMessage{CommitLSN: 10, TransactionEndLSN: 11}); err != nil {
		t.Fatal(err)
	}
	event := <-feed.Events
	if event.Position != 11 || event.Transaction != nil {
		t.Fatalf("barrier = %+v", event)
	}
}

func TestClassifyStartupErrorTreatsMissingPublicationAsFatal(t *testing.T) {
	err := classifyStartupError("start replication", &pgconn.PgError{Code: "42704", Message: "publication does not exist"})
	var fatal fatalReplicationError
	if !errors.As(err, &fatal) {
		t.Fatalf("classification = %v, want fatal replication error", err)
	}
}

func TestClassifyStartupErrorLeavesAvailabilityFailuresRetryable(t *testing.T) {
	want := errors.New("connection refused")
	err := classifyStartupError("connect", want)
	var fatal fatalReplicationError
	if errors.As(err, &fatal) {
		t.Fatalf("classification = %v, want retryable error", err)
	}
	if !errors.Is(err, want) {
		t.Fatalf("classification = %v, want wrapped availability error", err)
	}
}

func TestDecodeTuplePreservesUnchangedToastMarker(t *testing.T) {
	relation := &pglogrepl.RelationMessage{Columns: []*pglogrepl.RelationMessageColumn{
		{Flags: 1, Name: "id", DataType: 2950},
		{Name: "document", DataType: 3802},
		{Name: "deleted_at", DataType: 1184},
	}}
	tuple := &pglogrepl.TupleData{Columns: []*pglogrepl.TupleDataColumn{
		{DataType: pglogrepl.TupleDataTypeText, Data: []byte("abc")},
		{DataType: pglogrepl.TupleDataTypeToast},
		{DataType: pglogrepl.TupleDataTypeNull},
	}}

	row, err := decodeTuple(relation, tuple)
	if err != nil {
		t.Fatal(err)
	}
	parts, err := entitykey.Decode(identity(relation, row))
	if err != nil {
		t.Fatal(err)
	}
	if len(parts) != 1 || parts[0].Name != "id" || parts[0].TypeOID != 2950 || string(parts[0].Value) != "abc" {
		t.Fatalf("identity parts = %#v", parts)
	}
	if !row["document"].Missing {
		t.Fatal("unchanged TOAST value was not marked missing")
	}
	if !row["deleted_at"].Null {
		t.Fatal("null value was not preserved")
	}
}

func TestDecodeTupleRejectsBinaryValues(t *testing.T) {
	relation := &pglogrepl.RelationMessage{Columns: []*pglogrepl.RelationMessageColumn{{Name: "id", DataType: 23}}}
	tuple := &pglogrepl.TupleData{Columns: []*pglogrepl.TupleDataColumn{{DataType: pglogrepl.TupleDataTypeBinary, Data: []byte{1}}}}
	if _, err := decodeTuple(relation, tuple); err == nil {
		t.Fatal("expected binary tuple to be rejected")
	}
}

func TestIdentityRejectsRelationWithoutReplicaKey(t *testing.T) {
	relation := &pglogrepl.RelationMessage{Columns: []*pglogrepl.RelationMessageColumn{{Name: "id", DataType: 23}}}
	row := Row{"id": {TypeOID: 23, Text: []byte("1")}}
	if got := identity(relation, row); got != "" {
		t.Fatalf("identity = %q", got)
	}
}

func TestIdentityCanonicalizesCompositeReplicaKeyOrder(t *testing.T) {
	row := Row{
		"id":        {TypeOID: 20, Text: []byte("20")},
		"tenant_id": {TypeOID: 20, Text: []byte("10")},
	}
	physical := &pglogrepl.RelationMessage{Columns: []*pglogrepl.RelationMessageColumn{
		{Flags: 1, Name: "id", DataType: 20},
		{Flags: 1, Name: "tenant_id", DataType: 20},
	}}
	reversed := &pglogrepl.RelationMessage{Columns: []*pglogrepl.RelationMessageColumn{
		{Flags: 1, Name: "tenant_id", DataType: 20},
		{Flags: 1, Name: "id", DataType: 20},
	}}
	if identity(physical, row) != identity(reversed, row) {
		t.Fatal("equivalent WAL identities depend on relation column order")
	}
}

func TestDecodeTruncateRequiresMetadataForEveryRelation(t *testing.T) {
	relations := map[uint32]*pglogrepl.RelationMessage{
		10: {RelationID: 10, Namespace: "app", RelationName: "tasks"},
	}
	_, err := decodeTruncate(relations, &pglogrepl.TruncateMessage{RelationIDs: []uint32{10, 11}})
	if err == nil || !strings.Contains(err.Error(), "relation metadata missing for relation 11") {
		t.Fatalf("decode truncate error = %v", err)
	}
}

func TestDecodeTruncatePreservesRelationOrder(t *testing.T) {
	relations := map[uint32]*pglogrepl.RelationMessage{
		10: {RelationID: 10, Namespace: "app", RelationName: "tasks"},
		11: {RelationID: 11, Namespace: "app", RelationName: "projects"},
	}
	changes, err := decodeTruncate(relations, &pglogrepl.TruncateMessage{RelationIDs: []uint32{11, 10}})
	if err != nil {
		t.Fatal(err)
	}
	if len(changes) != 2 || changes[0].Table != "projects" || changes[1].Table != "tasks" {
		t.Fatalf("truncate changes = %+v", changes)
	}
}
