package projectionserver

import (
	"connectrpc.com/connect"
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/adiom-data/weave/internal/accesscontrol"
	proj "github.com/adiom-data/weave/internal/projection"
	"github.com/adiom-data/weave/internal/projection/dependency"
	"github.com/adiom-data/weave/internal/projection/transactionmessages"
	"github.com/adiom-data/weave/internal/querysubscription/model"
	relationdef "github.com/adiom-data/weave/internal/relation"
	"github.com/adiom-data/weave/internal/storage"
	"github.com/adiom-data/weave/internal/transactionmessage"
	reactivev1 "github.com/adiom-data/weave/pkg/gen/reactive/v1"
)

var streamSequence atomic.Uint64

// ProjectionLimits exposes only the runtime's configuration, not the expansion
// implementation or its private lifecycle/publication machinery.
type ProjectionLimits struct {
	MaxDefinitionDepth      int
	MaxDefinitionNodes      int
	MaxActiveInstances      int
	MaxConcurrentExpansions int
	MaxPendingExpansions    int
	RecoveryInitialDelay    time.Duration
	RecoveryMaxDelay        time.Duration
	RecoveryMaxAttempts     int
}

// MessageOptions bounds the optional connection-message routing owned by this server.
type MessageOptions struct {
	MaxQueuedTransactions int
	MaxQueuedBytes        int
	ReconnectDelay        time.Duration
}

// Server is the embedding handle. RPC handlers and connection machinery are
// private; callers mount the service and manage only its lifetime.
type Server interface {
	Register(grpc.ServiceRegistrar)
	Handler(...connect.HandlerOption) (string, http.Handler)
	ServeStream(Stream) error
	Close() error
	Wait(context.Context) error
}

type Options struct {
	TransactionMessages               transactionmessage.Client
	MessageOptions                    MessageOptions
	MessageProgressInterval           time.Duration
	MaxRequestsPerConnection          int
	MaxSubscriptionsPerConnection     int
	SubscriptionQueueLimit            int
	OutboundQueueLimit                int
	MaxSelectedFields                 int
	MaxPredicateNodes                 int
	MaxPredicateDepth                 int
	ProjectionLimits                  ProjectionLimits
	MaxConcurrentProjectionExpansions int
	// AccessController owns shared authorization state. ClaimsProvider extracts
	// trusted policy inputs from the host. No identity claim is required;
	// authentication and token verification deliberately remain edge concerns.
	AccessController accesscontrol.Controller
	Relations        *RelationOptions
	ClaimsProvider   func(context.Context) (accesscontrol.Claims, error)
	Dependencies     DependencyOptions
}

// RelationOptions configures library-owned computed values and visibility.
// It is mutually exclusive with a custom AccessController. Zero resource limits
// select defaults; applications may override them for their workload.
type RelationOptions struct {
	// AllowAll bypasses visibility decisions, not selected computations.
	AllowAll bool
	// Definitions and Store are alternatives. Fixed definitions create a private
	// store during construction. A supplied Store supports atomic live updates.
	// Limits configure a private store; a supplied Store owns its own limits.
	Definitions         relationdef.Definitions
	Store               *relationdef.Store
	Limits              relationdef.Limits
	MaxFactQueries      int
	MaxFactRows         int
	FactLoadConcurrency int
}

// DependencyOptions configures projection-side sharing, independently of the
// query server's caches. Zero values select defaults.
type DependencyOptions struct {
	UpstreamBufferLimit int
	MaxSubscriptions    int
	IdleTTL             time.Duration
	SweepInterval       time.Duration
	EvictionBatchSize   int
	CleanupBatchSize    int
}

type server struct {
	reactivev1.UnimplementedReactiveQueriesServer
	queries          querysubscription.Client
	options          Options
	access           accesscontrol.Controller
	expansionLimiter *proj.ExpansionLimiter
	router           *transactionmessages.Router
	lifetime         context.Context
	cancel           context.CancelFunc
	dependencies     *dependency.Cache
	evaluator        *relationdef.Evaluator
	relationAccess   *relationAccess // populated only on a connection-local service copy
}

// New constructs the projection service, shared dependencies, and optional message routing. The
// caller owns the query/message clients, policy, listeners, and gRPC server.
func New(ctx context.Context, queries querysubscription.Client, options Options) (Server, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if queries == nil {
		return nil, errors.New("query subscription client is required")
	}
	options = options.withDefaults()
	if options.ProjectionLimits.RecoveryMaxDelay < options.ProjectionLimits.RecoveryInitialDelay {
		return nil, errors.New("projection recovery max delay must be at least initial delay")
	}
	s := newServer(queries, options)
	s.lifetime, s.cancel = context.WithCancel(ctx)
	if options.Relations != nil {
		if options.AccessController != nil {
			s.cancel()
			return nil, errors.New("relation definitions and a custom access controller are mutually exclusive")
		}
		configuration := options.Relations
		if configuration.MaxFactQueries < 1 || configuration.MaxFactRows < 1 || configuration.MaxFactRows == int(^uint(0)>>1) || configuration.FactLoadConcurrency < 1 {
			s.cancel()
			return nil, errors.New("positive relation dependency limits are required")
		}
		if configuration.Store != nil && configuration.Definitions != nil {
			s.cancel()
			return nil, errors.New("relation store and fixed definitions are mutually exclusive")
		}
		store := configuration.Store
		if store == nil {
			var err error
			store, err = relationdef.NewStore(configuration.Definitions, configuration.Limits)
			if err != nil {
				s.cancel()
				return nil, fmt.Errorf("compile relation definitions: %w", err)
			}
		}
		compiled, err := relationdef.NewEvaluator(store)
		if err != nil {
			s.cancel()
			return nil, fmt.Errorf("compile relation definitions: %w", err)
		}
		s.evaluator = compiled
		copy := *configuration
		copy.Definitions = nil
		s.options.Relations = &copy
	}
	dependencies := options.Dependencies
	var err error
	s.dependencies, err = dependency.New(s.lifetime, queries, dependency.Options(dependencies))
	if err != nil {
		s.cancel()
		return nil, err
	}
	s.queries = s.dependencies
	if options.TransactionMessages != nil {
		messageOptions := options.MessageOptions
		var err error
		s.router, err = transactionmessages.Start(s.lifetime, options.TransactionMessages, transactionmessages.Options{
			MaxQueuedTransactions: messageOptions.MaxQueuedTransactions, MaxQueuedBytes: messageOptions.MaxQueuedBytes, ReconnectDelay: messageOptions.ReconnectDelay,
		})
		if err != nil {
			s.cancel()
			_ = s.dependencies.Close()
			return nil, err
		}
	}
	return s, nil
}

func (s *server) Close() error {
	s.cancel()
	var dependencyErr error
	if s.dependencies != nil {
		dependencyErr = s.dependencies.Close()
	}
	if s.router != nil {
		return errors.Join(dependencyErr, s.router.Close())
	}
	return dependencyErr
}

func (s *server) Wait(ctx context.Context) error {
	if s.router != nil {
		return s.router.Wait(ctx)
	}
	select {
	case <-s.lifetime.Done():
		return s.lifetime.Err()
	case <-ctx.Done():
		return ctx.Err()
	}
}

func newServer(queries querysubscription.Client, options Options) *server {
	options = options.withDefaults()
	controller := options.AccessController
	if controller == nil {
		controller = accesscontrol.NewAllowAll()
	}
	return &server{
		queries: queries, options: options, access: controller,
		expansionLimiter: proj.NewExpansionLimiter(options.MaxConcurrentProjectionExpansions),
		lifetime:         context.Background(), cancel: func() {},
	}
}

// Register mounts the service on an application-owned gRPC server.
func (s *server) Register(registrar grpc.ServiceRegistrar) {
	reactivev1.RegisterReactiveQueriesServer(registrar, s)
}

func (s *server) Open(stream grpc.BidiStreamingServer[reactivev1.ClientMessage, reactivev1.ServerMessage]) error {
	return s.ServeStream(stream)
}

// Stream is the only transport capability required by orchestration. Callers
// may send concurrently with a single receiver. Recv must unblock when the
// transport closes; the host owns closing the transport when ServeStream returns.
type Stream interface {
	Context() context.Context
	Send(*reactivev1.ServerMessage) error
	Recv() (*reactivev1.ClientMessage, error)
}

func (s *server) ServeStream(stream Stream) error {
	ctx, cancel := context.WithCancel(stream.Context())
	stop := context.AfterFunc(s.lifetime, cancel)
	defer stop()
	defer cancel()
	session := &connectionSession{service: s, stream: stream, ctx: ctx}
	if s.router == nil {
		return session.run()
	}
	route, err := s.router.Open()
	if err != nil {
		return status.Error(codes.Unavailable, err.Error())
	}
	defer route.Close()
	session.ctx, session.route = ctx, route
	// The handler owns the session's writer. Never return while session.run
	// can still call Send: HTTP transports finalize their response on return.
	// Route failure cancels session work instead of racing the handler's return.
	watchDone := make(chan struct{})
	go func() {
		defer close(watchDone)
		select {
		case <-route.Done():
			cancel()
		case <-ctx.Done():
		}
	}()
	defer func() { cancel(); <-watchDone }()
	err = session.run()
	if cause := route.Err(); cause != nil {
		code := codes.Unavailable
		if errors.Is(cause, transactionmessages.ErrBackpressure) {
			code = codes.ResourceExhausted
		}
		return status.Error(code, cause.Error())
	}
	return err
}

func (s *server) resolveClaims(ctx context.Context) (accesscontrol.Claims, error) {
	if s.options.ClaimsProvider == nil {
		return accesscontrol.Claims{}, nil
	}
	claims, err := s.options.ClaimsProvider(ctx)
	if err != nil {
		return accesscontrol.Claims{}, err
	}
	return accesscontrol.Claims(storage.CloneRecord(storage.Record(claims))), nil
}

func forwardQueryFeed(ctx context.Context, session querysubscription.Feed, outbound chan<- outboundMessage) {
	for {
		event, err := session.Next(ctx)
		select {
		case outbound <- queryEventMessage{event: event, err: err}:
		case <-ctx.Done():
			return
		}
		if err != nil {
			return
		}
	}
}

func handleQueryEvent(
	event querysubscription.Event,
	emit func([]feedDelivery) error,
	closeDirect func(*sharedSubscription, error) error,
	closeProjection func(*sharedProjection, error) error,
	subscriptions map[string]*sharedSubscription,
	projections map[string]*sharedProjection,
	pending map[string]*pendingSharedSubscription,
) error {
	if err := event.Validate(); err != nil {
		return fmt.Errorf("invalid query feed event: %w", err)
	}
	if event.Failure != nil {
		failure := event.Failure
		for _, state := range subscriptions {
			if state.internalID == failure.SubscriptionID {
				return closeDirect(state, failure.Cause)
			}
		}
		for _, state := range projections {
			batch, changed, err := state.projection.ApplyEvent(event)
			if err != nil {
				return closeProjection(state, err)
			}
			if changed {
				// Failures have no source transaction. Publish their branch state
				// as hydration, without inventing an earlier commit position.
				if err := emit([]feedDelivery{{metadata: batch.Delivery, parts: []deliveryPayload{projectionBatchMessage{externalID: state.externalID, generation: state.generation, batch: batch}}}}); err != nil {
					return err
				}
			}
		}
		for _, state := range pending {
			if state.projection != nil {
				if !state.projection.OwnsSubscription(failure.SubscriptionID) {
					continue
				}
				if _, _, err := state.projection.ApplyEvent(event); err != nil {
					state.cancel()
				}
				// A partial failure changes the private initial image even though
				// ApplyEvent has no public batch until admission. Reconsider it
				// without waiting for unrelated fact IO or another source position.
				state.retry = true
				continue
			}
			if state.owns != nil && state.owns(failure.SubscriptionID) {
				state.cancel()
				state.retry = true
				return nil
			}
		}
		return nil
	}
	if event.Progress != nil {
		return errors.New("query progress must be handled by the feed sequencer")
	}
	if event.Delivery == nil {
		return errors.New("query feed returned an invalid event")
	}
	envelope := *event.Delivery
	if err := envelope.Validate(); err != nil {
		return fmt.Errorf("invalid query delivery: %w", err)
	}
	parts := make([]deliveryPayload, 0, len(envelope.Effects))
	for effect := range envelope.SubscriptionEffects() {
		for _, state := range subscriptions {
			if state.internalID == effect.SubscriptionID {
				parts = append(parts, subscriptionBatchMessage{externalID: state.externalID, generation: state.internalID, delivery: envelope.Delivery, batch: effect.Batch})
				break
			}
		}
	}
	for _, state := range projections {
		batch, ok, err := state.projection.ApplyDelivery(envelope)
		if err != nil {
			if closeErr := closeProjection(state, err); closeErr != nil {
				return closeErr
			}
			continue
		}
		if ok {
			parts = append(parts, projectionBatchMessage{externalID: state.externalID, generation: state.generation, batch: batch})
		}
	}
	for _, state := range pending {
		if state.projection == nil {
			continue
		}
		if _, _, err := state.projection.ApplyDelivery(envelope); err != nil {
			state.cancel()
		}
	}
	return emit([]feedDelivery{{
		metadata: envelope.Delivery, parts: parts,
	}})
}
