package server_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"connectrpc.com/connect"
	"github.com/adiom-data/framework/httpapp"
	"github.com/adiom-data/weave/pkg/accesscontrol"
	metricsotel "github.com/adiom-data/weave/pkg/metrics/otel"
	"github.com/adiom-data/weave/pkg/server"
	"go.opentelemetry.io/otel"
	"google.golang.org/protobuf/types/known/emptypb"
)

func ExampleValidateConfig() {
	cfg := server.DefaultConfig()
	cfg.Storage.Postgres.URL = "postgres://localhost/app"
	cfg.Storage.Postgres.Publication = "app_publication"
	cfg.Storage.Postgres.Slot = "app_server"
	cfg.Auth.Mode = "fixed"
	cfg.Visibility.Mode = "allow_all"
	fmt.Println(server.ValidateConfig(cfg) == nil)
	// Output: true
}

func ExampleNew() {
	cfg, err := server.LoadConfig("server.yaml", func(string) (string, bool) { return "", false })
	if err != nil {
		panic(err)
	}
	if err := cfg.ValidateListener(); err != nil {
		panic(err)
	}
	custom := httpapp.Connect("app.v1.Commands", func(options ...connect.HandlerOption) (string, http.Handler) {
		return "/app.v1.Commands/", connect.NewUnaryHandler("/app.v1.Commands/Execute",
			func(ctx context.Context, request *connect.Request[emptypb.Empty]) (*connect.Response[emptypb.Empty], error) {
				claims, err := accesscontrol.ClaimsFromContext(ctx)
				if err != nil {
					return nil, connect.NewError(connect.CodeUnauthenticated, err)
				}
				// Authorize and execute application-specific mutations using these claims.
				_ = claims
				return connect.NewResponse(&emptypb.Empty{}), nil
			}, options...)
	})
	runtime, err := httpapp.Init(context.Background())
	if err != nil {
		panic(err)
	}
	defer runtime.Shutdown(context.Background())
	ctx, cancel := context.WithCancel(runtime.Context())
	defer cancel()
	host, err := server.New(ctx, cfg, server.Options{AuthenticatedServices: []httpapp.ConnectService{custom}, Metrics: metricsotel.New(otel.GetMeterProvider())})
	if err != nil {
		panic(err)
	}
	defer host.Close()
	app := runtime.NewService(
		httpapp.WithServiceAddr(cfg.Address), httpapp.WithServiceTimeouts(cfg.ReadHeaderTimeout, cfg.ShutdownTimeout),
		httpapp.WithServiceIdleTimeout(cfg.IdleTimeout),
		httpapp.WithServices(host.Services()...), httpapp.WithServiceRoutes(host.Routes()...),
	)
	done := make(chan error, 1)
	go func() {
		err := host.Wait(ctx)
		if ctx.Err() != nil && errors.Is(err, ctx.Err()) {
			err = nil
		}
		done <- err
		cancel()
	}()
	httpErr := app.Run(ctx)
	cancel()
	reactiveErr := <-done
	if err := errors.Join(httpErr, reactiveErr); err != nil {
		panic(err)
	}
}
