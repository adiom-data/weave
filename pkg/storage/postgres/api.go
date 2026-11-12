// Package postgres provides optional helpers specific to PostgreSQL storage.
package postgres

import (
	"context"
	impl "github.com/adiom-data/weave/internal/postgres"
	"github.com/adiom-data/weave/pkg/mutation"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// EmitMutationReceipt emits committed correlation metadata inside the caller's
// write transaction. It requires no running storage instance and never commits.
func EmitMutationReceipt(ctx context.Context, tx pgx.Tx, correlation mutation.Context) error {
	return impl.EmitMutationReceipt(ctx, tx, correlation)
}

// CaptureReadFence captures one opaque token after a successful commit. The
// pool must address the primary database consumed by Weave. Applications must
// route tokens back to that database. Return the token however the application
// chooses; projection requests accept it as at_least_fence.
func CaptureReadFence(ctx context.Context, pool *pgxpool.Pool) (string, error) {
	return impl.CaptureReadFence(ctx, pool)
}
