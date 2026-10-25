package storage

import (
	"context"

	"github.com/adiom-data/weave/internal/postgres"
)

// PostgresOptions configures storage capture and query preparation only.
type PostgresOptions = postgres.Options
type QueryLimits = postgres.QueryLimits
type MessageLimits = postgres.MessageLimits
type ReplicationOptions = postgres.ReplicationOptions

func DefaultPostgresOptions() PostgresOptions { return postgres.DefaultOptions() }

// PostgresStorage implements the storage contract consumed by the query server.
// Its decoder, pool, broker, and lifecycle implementation are private.
type PostgresStorage = postgres.Storage

func NewPostgres(ctx context.Context, databaseURL string, options PostgresOptions) (PostgresStorage, error) {
	return postgres.NewStorage(ctx, databaseURL, options)
}
