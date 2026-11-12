package postgres

import (
	"context"
	"fmt"

	"github.com/adiom-data/weave/internal/readfence"
	"github.com/adiom-data/weave/internal/storage"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgxpool"
)

// CaptureReadFence must be called on the primary after the mutation commits.
// The application must route the token back to the same database as Weave.
// It deliberately accepts a pool rather than a transaction.
func CaptureReadFence(ctx context.Context, pool *pgxpool.Pool) (string, error) {
	if pool == nil {
		return "", fmt.Errorf("read fence requires a PostgreSQL pool")
	}
	lsn, err := readFencePosition(ctx, pool)
	if err != nil {
		return "", fmt.Errorf("capture PostgreSQL read fence: %w", err)
	}
	return readfence.Encode(uint64(lsn)), nil
}

func (s *PostgresDataSource) DecodeReadFence(ctx context.Context, value string) (storage.Fence, error) {
	position, err := readfence.Decode(value)
	if err != nil {
		return nil, err
	}
	current, err := readFencePosition(ctx, s.db)
	if err != nil {
		return nil, fmt.Errorf("validate PostgreSQL read fence position: %w", err)
	}
	// Untrusted tokens must not add unreachable demand to the shared barrier
	// loop: a barrier failure there also affects legitimate pending reads.
	if position > uint64(current) {
		return nil, fmt.Errorf("read fence is ahead of the database WAL")
	}
	return WALPosition{LSN: pglogrepl.LSN(position)}, nil
}

func readFencePosition(ctx context.Context, pool *pgxpool.Pool) (pglogrepl.LSN, error) {
	var raw string
	err := pool.QueryRow(ctx, `SELECT pg_current_wal_insert_lsn()::text`).Scan(&raw)
	if err != nil {
		return 0, err
	}
	return pglogrepl.ParseLSN(raw)
}
