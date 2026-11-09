// This isolated composition root serves either query subscriptions or an
// allow-all projection service with an embedded or remote query backend.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/adiom-data/weave/benchmarks/query/internal/diagnostics"

	"github.com/adiom-data/weave/internal/engine"
	"github.com/adiom-data/weave/internal/postgres"
	"github.com/adiom-data/weave/internal/postgres/cdc"
	querylocal "github.com/adiom-data/weave/internal/querysubscription/local"
	"github.com/adiom-data/weave/pkg/projectionserver"
	"github.com/adiom-data/weave/pkg/querysubscription"
	querygrpc "github.com/adiom-data/weave/pkg/querysubscription/grpc"
)

func main() {
	if err := serve(); err != nil {
		slog.Error("query benchmark server", "error", err)
		os.Exit(1)
	}
}

func serve() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	stopDiagnostics, err := diagnostics.StartFromEnv(ctx, "server")
	if err != nil {
		return err
	}
	defer stopDiagnostics()
	mode := os.Getenv("BENCH_SERVICE")
	if mode == "" {
		mode = "query"
	}
	if mode != "query" && mode != "projection" {
		return fmt.Errorf("unknown benchmark service %q", mode)
	}
	var queries querysubscription.Client
	if address := os.Getenv("QUERY_GRPC_ADDRESS"); address != "" {
		if mode != "projection" {
			return errors.New("remote queries require projection mode")
		}
		connection, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return err
		}
		defer connection.Close()
		queries = querygrpc.NewClient(connection)
	} else {
		local, closeLocal, err := openLocalQueries(ctx)
		if err != nil {
			return err
		}
		defer closeLocal()
		queries = local
	}
	listener, err := net.Listen("tcp", ":9090")
	if err != nil {
		return err
	}
	defer listener.Close()
	server := grpc.NewServer()
	defer server.Stop()
	if mode == "projection" {
		service, err := projectionserver.New(ctx, queries, projectionserver.DefaultOptions())
		if err != nil {
			return err
		}
		defer service.Close()
		service.Register(server)
	} else {
		querygrpc.NewServer(diagnostics.WrapClient(queries)).Register(server)
	}
	health := &http.Server{Addr: ":8081", ReadHeaderTimeout: 5 * time.Second, Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/ready" {
			http.NotFound(w, r)
			return
		}
		w.WriteHeader(http.StatusOK)
	})}
	defer health.Close()
	failure := make(chan error, 2)
	go func() { failure <- server.Serve(listener) }()
	go func() { failure <- health.ListenAndServe() }()
	select {
	case <-ctx.Done():
		return nil
	case err := <-failure:
		if errors.Is(err, context.Canceled) {
			return nil
		}
		return err
	}
}

func openLocalQueries(parent context.Context) (querysubscription.Client, func(), error) {
	ctx, cancel := context.WithCancel(parent)
	var pool *pgxpool.Pool
	cleanup := func() {
		cancel()
		if pool != nil {
			pool.Close()
		}
	}
	opened := false
	defer func() {
		if !opened {
			cleanup()
		}
	}()
	config, err := pgxpool.ParseConfig(os.Getenv("DATABASE_URL"))
	if err != nil {
		return nil, nil, err
	}
	config.MaxConns = 20 // Electric's default query pool size; recorded in README.
	config.ConnConfig.RuntimeParams["TimeZone"] = "UTC"
	config.ConnConfig.RuntimeParams["DateStyle"] = "ISO, YMD"
	pool, err = pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, nil, err
	}
	var database string
	if err := pool.QueryRow(ctx, "SELECT current_database()").Scan(&database); err != nil {
		return nil, nil, err
	}
	if database != "weave_query_benchmark" {
		return nil, nil, fmt.Errorf("refusing non-benchmark database %q", database)
	}
	broker := cdc.NewBroker()
	replicator, err := cdc.NewReplicator(&config.ConnConfig.Config, "bench_publication", "bench_weave", broker, slog.Default(), cdc.ReplicatorOptions{})
	if err != nil {
		return nil, nil, err
	}
	go replicator.Run(ctx)
	source := postgres.NewPostgresDataSourceWithOptions(pool, broker, postgres.DataSourceOptions{
		AllowedTables: []string{"public.tasks", "public.links"}, Publication: "bench_publication",
	})
	coordinator, err := engine.StartCoordinatorWithOptions(ctx, source, engine.CoordinatorOptions{
		ProgressBarrierDelay: 50 * time.Millisecond, MaxWindowRows: 1000, SnapshotPageRows: 1000,
	})
	if err != nil {
		return nil, nil, err
	}
	opened = true
	return querylocal.New(coordinator), cleanup, nil
}
