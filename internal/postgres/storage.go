package postgres

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/adiom-data/weave/internal/datastore"
	"github.com/adiom-data/weave/internal/messagebudget"
	"github.com/adiom-data/weave/internal/metrics"
	"github.com/adiom-data/weave/internal/postgres/cdc"
)

// MessageLimits bounds committed application metadata captured from WAL.
// An all-zero struct selects defaults.
type MessageLimits struct {
	MaxMessageBytes     int
	MaxTransactionBytes int
	MaxMessages         int
}

// Options configures storage only. The application owns schema/publication
// setup; every concurrently running storage needs a distinct replication slot.
type Options struct {
	Publication        string
	Slot               string
	AllowedTables      []string
	QueryLimits        QueryLimits
	MessageLimits      MessageLimits
	MessageLogInterval time.Duration
	Replication        ReplicationOptions
	CaptureBufferLimit int
	Logger             *slog.Logger
}

// Storage owns the PostgreSQL source consumed by the query server.
type Storage interface {
	datastore.Storage
	datastore.MetadataManager
}

type postgresStorage struct {
	*PostgresDataSource
	cancel context.CancelFunc
	done   chan struct{}
	err    error // immutable once done closes
}

// NewStorage owns the query pool and replication reader. It establishes durable
// capture before returning, but does not start a reactive query coordinator.
func NewStorage(ctx context.Context, databaseURL string, options Options) (Storage, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if strings.TrimSpace(options.Publication) == "" || strings.TrimSpace(options.Slot) == "" {
		return nil, errors.New("publication and replication slot are required")
	}
	if options.MessageLogInterval == 0 {
		options.MessageLogInterval = DefaultOptions().MessageLogInterval
	}
	if options.MessageLogInterval < 0 {
		return nil, errors.New("message log interval must be positive")
	}
	if options.CaptureBufferLimit == 0 {
		options.CaptureBufferLimit = DefaultOptions().CaptureBufferLimit
	}
	if options.CaptureBufferLimit < 1 {
		return nil, errors.New("capture buffer limit must be positive")
	}
	if err := cdc.ReplicationOptions(options.Replication).Validate(); err != nil {
		return nil, err
	}
	logger := options.Logger
	if logger == nil {
		logger = slog.Default()
	}
	messageLimits := messagebudget.Limits(options.MessageLimits)
	if messageLimits == (messagebudget.Limits{}) {
		messageLimits = messagebudget.Defaults()
	}
	if err := messageLimits.Validate(); err != nil {
		return nil, err
	}
	if err := options.QueryLimits.validate(); err != nil {
		return nil, err
	}
	config, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, fmt.Errorf("parse PostgreSQL configuration: %w", err)
	}
	config.ConnConfig.RuntimeParams["DateStyle"] = "ISO, YMD"
	config.ConnConfig.RuntimeParams["TimeZone"] = "UTC"
	config.ConnConfig.RuntimeParams["bytea_output"] = "hex"
	runCtx, cancel := context.WithCancel(ctx)
	pool, err := pgxpool.NewWithConfig(runCtx, config)
	if err != nil {
		cancel()
		return nil, err
	}
	fail := func(cause error) (Storage, error) { cancel(); pool.Close(); return nil, cause }
	if err := pool.Ping(runCtx); err != nil {
		return fail(err)
	}
	var publicationExists bool
	if err := pool.QueryRow(runCtx, `SELECT EXISTS (SELECT 1 FROM pg_publication WHERE pubname=$1)`, options.Publication).Scan(&publicationExists); err != nil {
		return fail(fmt.Errorf("validate PostgreSQL publication: %w", err))
	}
	if !publicationExists {
		return fail(fmt.Errorf("PostgreSQL publication %q does not exist", options.Publication))
	}
	broker, err := cdc.NewBrokerWithOptions(cdc.BrokerOptions{ConsumerBufferLimit: options.CaptureBufferLimit})
	if err != nil {
		return fail(err)
	}
	replicator, err := cdc.NewReplicator(&config.ConnConfig.Config, options.Publication, options.Slot, broker, logger, cdc.ReplicatorOptions{MessageLimits: messageLimits, MessageLogInterval: options.MessageLogInterval, Replication: cdc.ReplicationOptions(options.Replication)})
	if err != nil {
		return fail(err)
	}
	store := &postgresStorage{PostgresDataSource: NewPostgresDataSourceWithOptions(pool, broker, DataSourceOptions{
		AllowedTables: append([]string(nil), options.AllowedTables...), Publication: options.Publication, QueryLimits: options.QueryLimits,
	}), cancel: cancel, done: make(chan struct{})}
	store.metadata.ready = metrics.NewGauge(metrics.CachedMetadata)
	store.metadata.loading = metrics.NewGauge(metrics.LoadingMetadata)
	go func() {
		defer close(store.done)
		defer store.metadata.ready.Close()
		defer store.metadata.loading.Close()
		defer pool.Close()
		replicator.Run(runCtx)
		store.err = runCtx.Err()
		if store.err == nil {
			store.err = broker.WaitReady(runCtx)
			if store.err == nil {
				store.err = errors.New("PostgreSQL replication stopped unexpectedly")
			}
		}
		broker.Fail(store.err)
	}()
	if err := broker.WaitReady(runCtx); err != nil {
		_ = store.Close()
		return nil, err
	}
	return store, nil
}

func (s *postgresStorage) Close() error { s.cancel(); <-s.done; return nil }
func (s *postgresStorage) Wait(ctx context.Context) error {
	select {
	case <-s.done:
		return s.err
	case <-ctx.Done():
		return ctx.Err()
	}
}
