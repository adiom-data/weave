package postgres

import (
	"context"
	"errors"

	"github.com/adiom-data/weave/internal/mutation"
	"github.com/jackc/pgx/v5"
	"google.golang.org/protobuf/proto"
)

// EmitMutationReceipt must run in the same transaction as application writes.
// It does not commit, roll back, authenticate, or authorize the transaction.
// An empty correlation context is a no-op for nonreactive API callers.
func EmitMutationReceipt(ctx context.Context, tx pgx.Tx, correlation mutation.Context) error {
	message, err := correlation.Receipt()
	if err != nil || message == nil {
		return err
	}
	if tx == nil {
		return errors.New("mutation receipt requires a transaction")
	}
	payload, err := proto.Marshal(message)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `SELECT pg_logical_emit_message(true, 'weave.client', $1::bytea)`, payload)
	return err
}
