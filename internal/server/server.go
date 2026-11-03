package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"connectrpc.com/connect"
	"github.com/adiom-data/framework/httpapp"
	"github.com/adiom-data/weave/internal/server/config"
	"github.com/adiom-data/weave/internal/server/identity"
	"github.com/adiom-data/weave/pkg/accesscontrol"
	"github.com/adiom-data/weave/pkg/accesscontrol/policy"
	"github.com/adiom-data/weave/pkg/gen/reactive/v1/reactivev1connect"
	"github.com/adiom-data/weave/pkg/projectionserver"
	projectionws "github.com/adiom-data/weave/pkg/projectionserver/websocket"
	"github.com/adiom-data/weave/pkg/queryserver"
	"github.com/adiom-data/weave/pkg/storage"
)

// Options adds application-owned endpoints to the configured reactive host.
// Services and Routes keep their own admission rules. AuthenticatedServices
// explicitly use the host's configured authentication and trusted claims.
type Options struct {
	Logger                *slog.Logger
	Services              []httpapp.ConnectService
	AuthenticatedServices []httpapp.ConnectService
	Routes                []httpapp.Route
	// Optional application-owned WebSocket authentication/renewal. When set,
	// its verified claims replace configured admission for WebSockets only.
	WebSocketAuthentication *projectionws.AuthenticationOptions
}

// Host owns reactive resources and one HTTP listener, not application resources.
// Run may be called once. Close cancels an active Run, waits for it, and closes
// the projection service, query backend, and storage in dependency order.
type Host interface {
	Run(context.Context) error
	Close() error
}

// New validates configuration, loads fixed policies, and initializes reactive
// resources. It does not start listening. The caller must Close a successful host.
func New(parent context.Context, c Config, options Options) (Host, error) {
	projectionConfig, err := prepareConfig(c)
	if err != nil {
		return nil, err
	}
	if err := validateEndpoints(options); err != nil {
		return nil, err
	}
	logger := options.Logger
	if logger == nil {
		logger = slog.Default()
	}
	authenticate, err := identity.New(c.Auth)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(parent)
	h := &host{ctx: ctx, cancel: cancel, runDone: make(chan struct{})}
	success := false
	defer func() {
		if !success {
			_ = h.Close()
		}
	}()
	if c.Auth.Mode != "oidc" {
		logger.Warn("claims are not authenticated by this server; restrict access to trusted callers", "mode", c.Auth.Mode)
	}
	logger.Info("listener uses plaintext HTTP/1 and HTTP/2; terminate TLS upstream before exposure")
	pg := storage.DefaultPostgresOptions()
	pg.Publication, pg.Slot = c.Storage.Postgres.Publication, c.Storage.Postgres.Slot
	pg.AllowedTables, pg.Logger = c.Storage.Postgres.AllowedTables, logger
	source, err := storage.NewPostgres(ctx, c.Storage.Postgres.URL, pg)
	if err != nil {
		return nil, errors.New("initialize PostgreSQL storage failed (check connectivity, publication, slot, and privileges)")
	}
	h.source = source
	queryOptions := queryserver.DefaultOptions()
	if c.Limits.SubscriptionBatchSize > 0 {
		queryOptions.Limits.SubscriptionBatchSize = c.Limits.SubscriptionBatchSize
	}
	if c.Limits.ReferenceLoadConcurrency > 0 {
		queryOptions.References.LoadConcurrency = c.Limits.ReferenceLoadConcurrency
	}
	if c.Limits.ReferenceLoadQueueLimit > 0 {
		queryOptions.References.LoadQueueLimit = c.Limits.ReferenceLoadQueueLimit
	}
	if c.Limits.MaxReferenceInvalidations > 0 {
		queryOptions.Limits.MaxReferenceInvalidations = c.Limits.MaxReferenceInvalidations
	}
	if c.Limits.MaxWindowRows > 0 {
		queryOptions.Limits.MaxWindowRows = c.Limits.MaxWindowRows
	}
	h.queries, err = queryserver.New(ctx, source, queryOptions)
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
	services = append(services, options.Services...)
	for _, service := range options.AuthenticatedServices {
		service.Interceptors = append([]connect.Interceptor{authenticate}, service.Interceptors...)
		services = append(services, service)
	}
	routes := append([]httpapp.Route(nil), options.Routes...)
	if c.Socket.Enabled {
		routes = append(routes, httpapp.Handle("/api/reactive", newSocketHandler(c, logger, authenticate, h.projections, options.WebSocketAuthentication)))
	}
	h.app = httpapp.NewService(
		httpapp.WithServiceAddr(c.Address), httpapp.WithServiceLogger(logger),
		httpapp.WithServiceTimeouts(c.ReadHeaderTimeout, c.ShutdownTimeout),
		httpapp.WithServiceIdleTimeout(c.IdleTimeout),
		httpapp.WithServices(services...), httpapp.WithServiceRoutes(routes...),
	)
	success = true
	return h, nil
}

func validateEndpoints(options Options) error {
	seen := map[string]bool{reactivev1connect.ReactiveQueriesName: true, "grpc.health.v1.Health": true}
	for _, group := range [][]httpapp.ConnectService{options.Services, options.AuthenticatedServices} {
		for _, service := range group {
			if service.Name == "" || service.NewHandler == nil || seen[service.Name] {
				return errors.New("additional services require unique, nonreserved names and handler factories")
			}
			seen[service.Name] = true
		}
	}
	for _, route := range options.Routes {
		if route.Pattern == "" || route.Handler == nil {
			return errors.New("additional routes require a pattern and handler")
		}
	}
	return nil
}

func projectionOptions(c config.Config, provider policy.Provider) (projectionserver.Options, error) {
	o := projectionserver.DefaultOptions()
	o.ClaimsProvider = accesscontrol.ClaimsFromContext
	if c.Limits.MaxSubscriptionsPerConnection > 0 {
		o.MaxSubscriptionsPerConnection = c.Limits.MaxSubscriptionsPerConnection
	}
	if c.Visibility.Mode == "policy" {
		if provider == nil {
			return o, errors.New("policy mode requires a compiled provider")
		}
		p := projectionserver.DefaultPolicyOptions()
		p.Provider = provider
		if c.Limits.MaxFactRows > 0 {
			p.MaxFactRows = c.Limits.MaxFactRows
		}
		if c.Limits.MaxFactQueries > 0 {
			p.MaxFactQueries = c.Limits.MaxFactQueries
		}
		maxWindow := queryserver.DefaultOptions().Limits.MaxWindowRows
		if c.Limits.MaxWindowRows > 0 {
			maxWindow = c.Limits.MaxWindowRows
		}
		if p.MaxFactRows >= maxWindow {
			return o, errors.New("max_fact_rows + 1 must fit within max_window_rows")
		}
		o.Policies = &p
	}
	return o, nil
}
