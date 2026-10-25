// Package postgres provides optional helpers specific to PostgreSQL storage.
package postgres

import (
	"context"
	impl "github.com/adiom-data/weave/internal/postgres"
	"github.com/adiom-data/weave/pkg/mutation"
	"github.com/jackc/pgx/v5"
)

// EmitMutationReceipt emits committed correlation metadata inside the caller's
// write transaction. It requires no running storage instance and never commits.
func EmitMutationReceipt(ctx context.Context, tx pgx.Tx, correlation mutation.Context) error {
	return impl.EmitMutationReceipt(ctx, tx, correlation)
}
