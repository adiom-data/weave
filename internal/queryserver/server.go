package queryserver

import (
	"context"
	"errors"
	"time"

	"google.golang.org/grpc"

	"github.com/adiom-data/weave/internal/datastore"
	"github.com/adiom-data/weave/internal/engine"
	"github.com/adiom-data/weave/internal/metrics"
	querygrpc "github.com/adiom-data/weave/internal/querysubscription/grpc"
	querylocal "github.com/adiom-data/weave/internal/querysubscription/local"
	"github.com/adiom-data/weave/internal/querysubscription/model"
	"github.com/adiom-data/weave/internal/transactionmessage"
	messagegrpc "github.com/adiom-data/weave/internal/transactionmessage/grpc"
)

// Limits configures reactive computation. Zero fields select defaults, except
// ProgressBarrierDelay: zero requests sparse-stream progress immediately.
type Limits struct {
	EntityIdleTTL             time.Duration
	EvictionPeriod            time.Duration
	EvictionBatchSize         int
	QueryIdleTTL              time.Duration
	QueryMaintenancePeriod    time.Duration
	MaxResyncTransactions     int
	MaxResyncChanges          int
	MaxHydrations             int
	ResyncResultBuffer        int
	HydrationResultBuffer     int
	MaxWindowRows             int
	SnapshotPageRows          int
	SubscriptionBatchSize     int
	MaxReferenceInvalidations int
	ProgressBarrierDelay      time.Duration
}

type Options struct {
	Limits     Limits
	Messages   transactionmessage.Options
	Transport  querygrpc.TransportOptions
	References ReferenceOptions
	// Metrics optionally exports runtime counts. Nil disables them.
	Metrics metrics.Provider
}

type ReferenceOptions = querylocal.ReferenceOptions

// DefaultOptions returns independently customizable query and message defaults.
func DefaultOptions() Options {
	return Options{Limits: Limits(engine.DefaultCoordinatorLimits()), Messages: transactionmessage.DefaultOptions(), Transport: querygrpc.DefaultTransportOptions(), References: querylocal.DefaultReferenceOptions()}
}

type backend struct {
	queries   querysubscription.Client
	messages  transactionmessage.Service
	cancel    context.CancelFunc
	done      chan struct{}
	err       error
	transport querygrpc.TransportOptions
}

// New borrows storage. Closing this server never closes or takes ownership of
// that storage; the application closes dependent components before their source.
func New(ctx context.Context, source datastore.Storage, options Options) (Backend, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if source == nil {
		return nil, errors.New("storage is required")
	}
	runCtx, cancel := context.WithCancel(ctx)
	coordinator, err := engine.StartCoordinatorWithOptions(runCtx, source, engine.CoordinatorOptions{Limits: engine.CoordinatorLimits(options.Limits), Metrics: options.Metrics})
	if err != nil {
		cancel()
		return nil, err
	}
	messages, err := transactionmessage.Start(runCtx, source, options.Messages)
	if err != nil {
		cancel()
		_ = coordinator.Wait(context.Background())
		return nil, err
	}
	b := &backend{queries: querylocal.NewWithOptions(coordinator, options.References), messages: messages, cancel: cancel, done: make(chan struct{}), transport: options.Transport}
	go func() {
		defer close(b.done)
		// Source failure stops its consumers, but canceling this server only ends
		// its wait on the independently owned source.
		b.err = supervise(runCtx, cancel, coordinator.Wait, messages.Wait, func(context.Context) error { return source.Wait(runCtx) })
		_ = messages.Close()
	}()
	return b, nil
}

func supervise(ctx context.Context, cancel context.CancelFunc, waits ...func(context.Context) error) error {
	results := make(chan error, len(waits))
	for _, wait := range waits {
		go func(wait func(context.Context) error) { results <- wait(context.Background()) }(wait)
	}
	var err error
	select {
	case err = <-results:
		if err == nil {
			err = errors.New("query backend component stopped unexpectedly")
		}
		cancel()
		for remaining := len(waits) - 1; remaining > 0; remaining-- {
			<-results
		}
	case <-ctx.Done():
		err = ctx.Err()
		cancel()
		for range waits {
			<-results
		}
	}
	return err
}

func (b *backend) OpenFeed(ctx context.Context, options querysubscription.FeedOptions) (querysubscription.Feed, error) {
	select {
	case <-b.done:
		return nil, querysubscription.ErrClosed
	default:
	}
	return b.queries.OpenFeed(ctx, options)
}
func (b *backend) Messages() transactionmessage.Client {
	return transactionmessage.NewLocalClient(b.messages)
}
func (b *backend) Register(registrar grpc.ServiceRegistrar) {
	querygrpc.NewServerWithOptions(b, b.transport).Register(registrar)
	messagegrpc.NewServer(b.messages).Register(registrar)
}
func (b *backend) Close() error { b.cancel(); <-b.done; return nil }
func (b *backend) Wait(ctx context.Context) error {
	select {
	case <-b.done:
		return b.err
	case <-ctx.Done():
		return ctx.Err()
	}
}
