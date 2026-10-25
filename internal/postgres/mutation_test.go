package postgres

import (
	"context"
	"encoding/base64"
	"errors"
	"testing"

	"github.com/adiom-data/weave/internal/mutation"
	reactivev1 "github.com/adiom-data/weave/pkg/gen/reactive/v1"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"google.golang.org/protobuf/proto"
)

// Embedding the interface makes any unexpected transaction operation fail the
// test: the helper must only execute SQL, never begin, commit, or roll back.
type receiptTx struct {
	pgx.Tx
	sql     string
	payload []byte
	err     error
}

func (t *receiptTx) Exec(_ context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	t.sql = sql
	t.payload = args[0].([]byte)
	return pgconn.CommandTag{}, t.err
}
func TestEmitMutationReceipt(t *testing.T) {
	c := mutation.Context{ConnectionContext: base64.RawURLEncoding.EncodeToString(make([]byte, 32)), MutationID: "12345678-1234-1234-1234-123456789abc"}
	tx := &receiptTx{}
	if err := EmitMutationReceipt(context.Background(), tx, c); err != nil {
		t.Fatal(err)
	}
	if tx.sql != `SELECT pg_logical_emit_message(true, 'weave.client', $1::bytea)` {
		t.Fatal(tx.sql)
	}
	var envelope reactivev1.RoutedTransactionMessage
	if err := proto.Unmarshal(tx.payload, &envelope); err != nil {
		t.Fatal(err)
	}
	if envelope.ConnectionContext != c.ConnectionContext || envelope.Kind != mutation.ReceiptKind || string(envelope.Payload) != c.MutationID {
		t.Fatal(&envelope)
	}
	failure := errors.New("write failed")
	tx.err = failure
	if err := EmitMutationReceipt(context.Background(), tx, c); !errors.Is(err, failure) {
		t.Fatal(err)
	}
	if err := EmitMutationReceipt(context.Background(), nil, mutation.Context{}); err != nil {
		t.Fatal(err)
	}
	if err := EmitMutationReceipt(context.Background(), nil, c); err == nil {
		t.Fatal("nil transaction accepted")
	}
	if err := EmitMutationReceipt(context.Background(), &receiptTx{}, mutation.Context{MutationID: c.MutationID}); err == nil {
		t.Fatal("invalid correlation accepted")
	}
}
