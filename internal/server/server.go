package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"connectrpc.com/connect"
	"github.com/adiom-data/framework/httpapp"
	"github.com/adiom-data/weave/internal/server/identity"
	"github.com/adiom-data/weave/pkg/gen/management/v1/managementv1connect"
	"github.com/adiom-data/weave/pkg/gen/reactive/v1/reactivev1connect"
	"github.com/adiom-data/weave/pkg/management"
	"github.com/adiom-data/weave/pkg/projectionserver"
	projectionws "github.com/adiom-data/weave/pkg/projectionserver/websocket"
	"github.com/adiom-data/weave/pkg/queryserver"
	"github.com/adiom-data/weave/pkg/storage"
)

// Options configures application integration with the reactive services.
// AuthenticatedServices use the configured authentication and trusted claims.
type Options struct {
	Logger                *slog.Logger
	AuthenticatedServices []httpapp.ConnectService
	// Optional application-owned WebSocket authentication/renewal. When set,
	// its verified claims replace configured admission for WebSockets only.
	// Mutually exclusive with configured websocket.renewal.
	WebSocketAuthentication *projectionws.AuthenticationOptions
}

// Server owns reactive resources and supplies endpoints for a caller-owned HTTP
// server. The caller owns HTTP serving and telemetry. Stop serving before Close.
type Server interface {
	// Services and Routes return endpoint descriptors to mount on the caller's
	// HTTP server. Returned slices can be changed without mutating this server.
	Services() []httpapp.ConnectService
	Routes() []httpapp.Route
	// Wait reports terminal component failure or context cancellation. Canceling
	// a wait does not close the server. Close releases all reactive resources.
	Wait(context.Context) error
	Close() error
}

// New validates configuration, loads fixed policies, and initializes reactive
// resources. It does not start listening. The caller must Close a successful server.
func New(parent context.Context, c Config, options Options) (Server, error) {
	projectionConfig, err := prepareConfig(c)
	if err != nil {
		return nil, err
	}
	if err := validateEndpoints(options); err != nil {
		return nil, err
	}
	if c.Socket.Renewal.Enabled && options.WebSocketAuthentication != nil {
		return nil, errors.New("configured websocket.renewal and WebSocketAuthentication are mutually exclusive")
	}
	logger := options.Logger
	if logger == nil {
		logger = slog.Default()
	}
	authenticate, err := identity.New(c.Auth)
	if err != nil {
		return nil, err
	}
	authentication, err := socketAuthenticationOptions(c, authenticate, options.WebSocketAuthentication)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(parent)
	h := &server{ctx: ctx, cancel: cancel}
	success := false
	defer func() {
		if !success {
			_ = h.Close()
		}
	}()
	if c.Auth.Mode != "oidc" {
		logger.Warn("claims are not authenticated by this server; restrict access to trusted callers", "mode", c.Auth.Mode)
	}
	pg := postgresOptions(c)
	pg.Logger = logger
	source, err := storage.NewPostgres(ctx, c.Storage.Postgres.URL, pg)
	if err != nil {
		return nil, errors.New("initialize PostgreSQL storage failed (check connectivity, publication, slot, and privileges)")
	}
	h.source = source
	h.queries, err = queryserver.New(ctx, source, queryOptions(c))
	if err != nil {
		return nil, fmt.Errorf("initialize query backend: %w", err)
	}
	projectionConfig.TransactionMessages = h.queries.Messages()
	h.projections, err = projectionserver.New(ctx, h.queries, projectionConfig)
	if err != nil {
		return nil, fmt.Errorf("initialize projections: %w", err)
	}
	services := []httpapp.ConnectService{
		httpapp.Connect(reactivev1connect.ReactiveQueriesName, h.projections.Handler,
			httpapp.WithInterceptors(authenticate),
			httpapp.WithConnectOptions(connect.WithReadMaxBytes(c.MaxMessageBytes), connect.WithSendMaxBytes(c.MaxMessageBytes))),
	}
	if c.Management.Enabled {
		admin, err := management.NewServer(source)
		if err != nil {
			return nil, err
		}
		services = append(services, httpapp.Connect(managementv1connect.ManagementName, admin.Handler,
			httpapp.WithInterceptors(managementAuthentication(c.Management.BearerToken)),
			httpapp.WithConnectOptions(connect.WithReadMaxBytes(c.MaxMessageBytes), connect.WithSendMaxBytes(c.MaxMessageBytes))))
	}
	for _, service := range options.AuthenticatedServices {
		service.Interceptors = append([]connect.Interceptor{authenticate}, service.Interceptors...)
		services = append(services, service)
	}
	var routes []httpapp.Route
	if c.Socket.Enabled {
		routes = append(routes, httpapp.Handle("/api/reactive", newSocketHandler(c, logger, authenticate, h.projections, authentication)))
	}
	h.services, h.routes = cloneServices(services), routes
	h.done = make(chan struct{})
	go h.supervise()
	success = true
	return h, nil
}

func validateEndpoints(options Options) error {
	seen := map[string]bool{reactivev1connect.ReactiveQueriesName: true, managementv1connect.ManagementName: true, "grpc.health.v1.Health": true}
	for _, service := range options.AuthenticatedServices {
		if service.Name == "" || service.NewHandler == nil || seen[service.Name] {
			return errors.New("additional services require unique, nonreserved names and handler factories")
		}
		seen[service.Name] = true
	}
	return nil
}
