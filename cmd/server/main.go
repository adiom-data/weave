// Command server hosts the reusable Weave libraries without application Go code.
package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"os"

	"github.com/adiom-data/framework/httpapp"
	"github.com/adiom-data/framework/telemetry"

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

func execute(logger *slog.Logger) (runErr error) {
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
	if err := c.ValidateListener(); err != nil {
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
	shutdownTelemetry, err := telemetry.Setup(ctx, telemetry.DefaultConfig("weave"))
	if err != nil {
		return err
	}
	defer func() {
		shutdownCtx, stop := context.WithTimeout(context.Background(), telemetry.DefaultShutdownTimeout)
		defer stop()
		runErr = errors.Join(runErr, shutdownTelemetry(shutdownCtx))
	}()
	host, err := server.New(ctx, c, server.Options{Logger: logger})
	if err != nil {
		return err
	}
	defer func() { runErr = errors.Join(runErr, host.Close()) }()
	logger.Info("listener uses plaintext HTTP/1 and HTTP/2; terminate TLS upstream before exposure")
	app := httpapp.NewService(
		httpapp.WithServiceAddr(c.Address), httpapp.WithServiceLogger(logger),
		httpapp.WithServiceTimeouts(c.ReadHeaderTimeout, c.ShutdownTimeout),
		httpapp.WithServiceIdleTimeout(c.IdleTimeout),
		httpapp.WithServices(host.Services()...), httpapp.WithServiceRoutes(host.Routes()...),
	)
	return serve(ctx, app, host)
}

func serve(ctx context.Context, app httpapp.Service, reactive server.Server) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		err := reactive.Wait(ctx)
		if ctx.Err() != nil && errors.Is(err, ctx.Err()) {
			err = nil
		}
		done <- err
		cancel()
	}()
	httpErr := app.Run(ctx)
	cancel()
	reactiveErr := <-done
	return errors.Join(httpErr, reactiveErr)
}
