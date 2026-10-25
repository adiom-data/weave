package server_test

import (
	"context"
	"fmt"
	"net/http"

	"connectrpc.com/connect"
	"github.com/adiom-data/framework/httpapp"
	"github.com/adiom-data/weave/pkg/accesscontrol"
	"github.com/adiom-data/weave/pkg/server"
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
	ctx := context.Background()
	host, err := server.New(ctx, cfg, server.Options{AuthenticatedServices: []httpapp.ConnectService{custom}})
	if err != nil {
		panic(err)
	}
	defer host.Close()
	if err := host.Run(ctx); err != nil {
		panic(err)
	}
}
