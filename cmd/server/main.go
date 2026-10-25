// Command server hosts the reusable Weave libraries without application Go code.
package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"os"

	"github.com/adiom-data/framework/httpapp"

	"github.com/adiom-data/weave/pkg/server"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	if err := execute(logger); err != nil {
		// Database/network errors can contain connection strings; do not print
		// credentials. The adapters report operational diagnostics separately.
		logger.Error("server stopped", "error", err)
		os.Exit(1)
	}
}

func execute(logger *slog.Logger) error {
	path := flag.String("config", os.Getenv("WEAVE_CONFIG"), "YAML configuration path (optional with environment configuration)")
	check := flag.Bool("check-config", false, "validate config and compile policies without connecting to PostgreSQL or OIDC")
	flag.Parse()
	if flag.NArg() != 0 {
		return errors.New("unexpected positional arguments")
	}
	c, err := server.LoadConfig(*path, os.LookupEnv)
	if err != nil {
		return err
	}
	if *check {
		if err := server.ValidateConfig(c); err != nil {
			return err
		}
		logger.Info("configuration valid (external services not checked)")
		return nil
	}
	ctx, cancel := httpapp.SignalContext(context.Background())
	defer cancel()
	host, err := server.New(ctx, c, server.Options{Logger: logger})
	if err != nil {
		return err
	}
	defer host.Close()
	return host.Run(ctx)
}
