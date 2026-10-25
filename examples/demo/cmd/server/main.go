// Command server adds the demo's mutation endpoints to the public reactive host.
package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"os"

	"github.com/adiom-data/framework/httpapp"
	"github.com/adiom-data/weave/examples/demo/internal/demomutations"
	"github.com/adiom-data/weave/pkg/server"
	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	if err := execute(logger, os.Args[1:]); err != nil {
		logger.Error("demo server stopped", "error", err)
		os.Exit(1)
	}
}

func execute(logger *slog.Logger, args []string) error {
	flags := flag.NewFlagSet("demo-server", flag.ContinueOnError)
	defaultPath := os.Getenv("WEAVE_CONFIG")
	if defaultPath == "" {
		defaultPath = "server.yaml"
	}
	path := flags.String("config", defaultPath, "reactive host YAML configuration")
	check := flags.Bool("check-config", false, "validate config and policy without contacting PostgreSQL or OIDC")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("unexpected positional arguments")
	}
	cfg, err := server.LoadConfig(*path, os.LookupEnv)
	if err != nil {
		return err
	}
	if *check {
		if err := server.ValidateConfig(cfg); err != nil {
			return err
		}
		logger.Info("demo configuration valid (external services not checked)")
		return nil
	}
	ctx, cancel := httpapp.SignalContext(context.Background())
	defer cancel()
	return run(ctx, cfg, logger)
}

func run(ctx context.Context, cfg server.Config, logger *slog.Logger) error {
	// The mutation pool is application-owned and closes after the reactive host.
	pool, err := pgxpool.New(ctx, cfg.Storage.Postgres.URL)
	if err != nil {
		return errors.New("configure demo mutation database failed")
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		return errors.New("connect to demo mutation database failed")
	}
	host, err := server.New(ctx, cfg, server.Options{
		Logger:                logger,
		AuthenticatedServices: []httpapp.ConnectService{demomutations.Endpoint(pool, cfg.WebOrigin, logger)},
	})
	if err != nil {
		return err
	}
	defer host.Close()
	return host.Run(ctx)
}
