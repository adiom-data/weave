package projectionserver

import (
	"connectrpc.com/connect"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/adiom-data/weave/internal/accesscontrol"
	"github.com/adiom-data/weave/internal/accesscontrol/policy"
	proj "github.com/adiom-data/weave/internal/projection"
	"github.com/adiom-data/weave/internal/projection/dependency"
	"github.com/adiom-data/weave/internal/projection/transactionmessages"
	"github.com/adiom-data/weave/internal/querysubscription/model"
	"github.com/adiom-data/weave/internal/storage"
	"github.com/adiom-data/weave/internal/transactionmessage"
	reactivev1 "github.com/adiom-data/weave/pkg/gen/reactive/v1"
)

var streamSequence atomic.Uint64

var errRequestHistoryFull = errors.New("connection request history is full")

// ProjectionLimits exposes only the runtime's configuration, not the expansion
// implementation or its private lifecycle/publication machinery.
type ProjectionLimits struct {
	MaxDefinitionDepth      int
	MaxDefinitionNodes      int
	MaxActiveInstances      int
	MaxConcurrentExpansions int
	MaxPendingExpansions    int
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
	Policies         *PolicyOptions
	ClaimsProvider   func(context.Context) (accesscontrol.Claims, error)
	Dependencies     DependencyOptions
}

// PolicyOptions configures library-owned declarative visibility evaluation.
// It is mutually exclusive with a custom AccessController. Zero resource limits
// select defaults; applications may override them for their workload.
type PolicyOptions struct {
	// Definitions and Provider are alternatives. Fixed definitions are checked
	// during construction; provider definitions are checked when first needed.
	Definitions policy.Map
	Provider    policy.Provider
	Cache       policy.CacheOptions
	// Optional broadcast hints. Providers need not track what the engine read.
	Invalidations       <-chan []policy.Key
	Limits              policy.Limits
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
	policy           *policy.Evaluator
	policyAccess     *policyAccess // populated only on a connection-local service copy
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
	s := newServer(queries, options)
	s.lifetime, s.cancel = context.WithCancel(ctx)
	if options.Policies != nil {
		if options.AccessController != nil {
			s.cancel()
			return nil, errors.New("policies and a custom access controller are mutually exclusive")
		}
		configuration := options.Policies
		if configuration.MaxFactQueries < 1 || configuration.MaxFactRows < 1 || configuration.MaxFactRows == int(^uint(0)>>1) || configuration.FactLoadConcurrency < 1 {
			s.cancel()
			return nil, errors.New("positive authorization dependency limits are required")
		}
		if configuration.Provider != nil && configuration.Definitions != nil {
			s.cancel()
			return nil, errors.New("policy provider and fixed definitions are mutually exclusive")
		}
		provider := configuration.Provider
		if provider == nil {
			var err error
			provider, err = policy.NewStaticProvider(configuration.Definitions, configuration.Limits)
			if err != nil {
				s.cancel()
				return nil, fmt.Errorf("compile visibility policies: %w", err)
			}
		}
		compiled, err := policy.NewEvaluator(s.lifetime, provider, configuration.Limits, configuration.Cache, configuration.Invalidations)
		if err != nil {
			s.cancel()
			return nil, fmt.Errorf("compile visibility policies: %w", err)
		}
		s.policy = compiled
		copy := *configuration
		copy.Definitions = nil
		s.options.Policies = &copy
	}
	dependencies := options.Dependencies
	var err error
	s.dependencies, err = dependency.New(s.lifetime, queries, dependency.Options(dependencies))
	if err != nil {
		s.cancel()
		if s.policy != nil {
			_ = s.policy.Close()
		}
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
			if s.policy != nil {
				_ = s.policy.Close()
			}
			return nil, err
		}
	}
	return s, nil
}

func (s *server) Close() error {
	s.cancel()
	if s.policy != nil {
		_ = s.policy.Close()
	}
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

type requestLedger struct {
	limit int
	seen  map[string][sha256.Size]byte
}

func newRequestLedger(limit int) *requestLedger {
	return &requestLedger{limit: limit, seen: make(map[string][sha256.Size]byte)}
}

// Accept records one immutable request identity for the lifetime of a
// connection. An identical retry is a no-op: on one ordered gRPC stream the
// original response either remains in flight or the connection fails, in
// which case the next connection intentionally starts a new ledger.
func (l *requestLedger) Accept(message *reactivev1.ClientMessage) (bool, error) {
	if message == nil || message.RequestId == "" {
		return true, nil
	}
	encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(message)
	if err != nil {
		return false, fmt.Errorf("encode request identity: %w", err)
	}
	digest := sha256.Sum256(encoded)
	if previous, exists := l.seen[message.RequestId]; exists {
		if previous != digest {
			return false, errors.New("request_id was reused for a different request")
		}
		return false, nil
	}
	if len(l.seen) >= l.limit {
		return false, fmt.Errorf("%w: limit of %d reached", errRequestHistoryFull, l.limit)
	}
	l.seen[message.RequestId] = digest
	return true, nil
}

type sharedSubscription struct {
	publication  publicationHold
	cancel       context.CancelFunc
	internalID   string
	queryID      string
	externalID   string
	subscription querysubscription.QuerySubscription
	visibility   *subscriptionVisibility
	sequence     uint64
}

type sharedProjection struct {
	policyOwnership projectionPolicyOwnership
	publication     publicationHold
	externalID      string
	generation      string
	projection      *proj.Projection
	cancel          context.CancelFunc
	visibility      *projectionVisibility
	selected        map[string]*fieldSelection
	sequence        uint64
}

func (p *sharedProjection) close() {
	p.publication.close()
	p.projection.Close()
	if p.cancel != nil {
		p.cancel()
	}
}

type pendingSharedSubscription struct {
	context    context.Context
	cancel     context.CancelFunc
	owns       func(string) bool
	projection *proj.Projection
	generation string
	admit      func() (proj.AdmissionWait, error)
	retry      bool
	wait       <-chan struct{}
	policyOwns func(string) bool
	direct     *initialQueryImage
	requestID  string
}

type feedDelivery struct {
	// replay restricts historical catch-up to the newly installed projection.
	// It must not rewind other scopes or advance the connection's live clock.
	replay   *sharedProjection
	metadata querysubscription.DeliveryMetadata
	parts    []deliveryPayload
	messages []*reactivev1.TransactionMessage
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

// connectionSession owns all mutable state for one bidirectional stream. The
// shared service and engine remain free of per-client entity/query state.
type connectionSession struct {
	service *server
	stream  Stream
	ctx     context.Context
	route   *transactionmessages.Connection
}

func (session *connectionSession) run() error {
	s := session.service
	if s.queries == nil {
		return errors.New("query subscription client is required")
	}
	stream := session.stream
	parent := session.ctx
	if parent == nil {
		parent = stream.Context()
	}
	ctx, cancel := context.WithCancel(parent)
	prefix := strconv.FormatUint(streamSequence.Add(1), 10) + ":"
	claims, err := s.resolveClaims(ctx)
	if err != nil {
		cancel()
		return fmt.Errorf("resolve access-control claims: %w", err)
	}
	preparedID := accesscontrol.PreparedID(prefix)
	queries, err := s.queries.OpenFeed(ctx, querysubscription.FeedOptions{BufferLimit: s.options.SubscriptionQueueLimit})
	if err != nil {
		cancel()
		return fmt.Errorf("open query feed: %w", err)
	}
	s, prepared, err := s.prepareAccess(ctx, queries, preparedID, claims)
	if err != nil {
		_ = queries.Close(context.WithoutCancel(ctx))
		cancel()
		return fmt.Errorf("prepare access control: %w", err)
	}
	if prepared == nil {
		_ = queries.Close(context.WithoutCancel(ctx))
		cancel()
		return errors.New("prepare access control: controller returned nil state")
	}
	if prepared.ID() != preparedID {
		_ = prepared.Close()
		_ = queries.Close(context.WithoutCancel(ctx))
		cancel()
		return errors.New("prepare access control: controller returned a different prepared ID")
	}
	if session.route != nil {
		if err := stream.Send(&reactivev1.ServerMessage{Event: &reactivev1.ServerMessage_ConnectionOpened{ConnectionOpened: &reactivev1.ConnectionOpened{ConnectionContext: session.route.Context}}}); err != nil {
			_ = queries.Close(context.WithoutCancel(ctx))
			_ = prepared.Close()
			cancel()
			return err
		}
	}
	messages := make(chan *reactivev1.ClientMessage)
	receiveErr := make(chan error, 1)
	go func() {
		defer close(messages)
		for {
			message, err := stream.Recv()
			if err != nil {
				receiveErr <- err
				return
			}
			select {
			case messages <- message:
			case <-ctx.Done():
				return
			}
		}
	}()
	outbound := make(chan outboundMessage, s.options.OutboundQueueLimit)
	if s.policyAccess != nil {
		go func() {
			if err := s.policyAccess.watch(ctx); err != nil && ctx.Err() == nil {
				select {
				case outbound <- queryEventMessage{err: err}:
				case <-ctx.Done():
				}
			}
		}()
	}
	subscriptions := make(map[string]*sharedSubscription)
	projections := make(map[string]*sharedProjection)
	pending := make(map[string]*pendingSharedSubscription)
	var loaders sync.WaitGroup
	model := newConnectionModel()
	projectionOutputs := &proj.OutputScheduler[*sharedProjection]{}
	requests := newRequestLedger(s.options.MaxRequestsPerConnection)
	var connectionSequence uint64
	if session.route != nil {
		go forwardJoinedFeed(ctx, queries, session.route, outbound, s.options.MessageProgressInterval)
	} else {
		go forwardQueryFeed(ctx, queries, outbound)
	}
	defer func() {
		cancel()
		loaders.Wait()
		for {
			select {
			case item := <-outbound:
				if completion, ok := item.(completionMessage); ok && completion.abandon != nil {
					completion.abandon()
				}
			default:
				goto drained
			}
		}
	drained:
		for _, state := range subscriptions {
			_ = state.subscription.Close(context.WithoutCancel(ctx))
			if state.cancel != nil {
				state.cancel()
			}
		}
		for _, state := range projections {
			state.close()
		}
		_ = queries.Close(context.WithoutCancel(ctx))
		_ = prepared.Close()
	}()
	sendCleanup := func(subscriptionID string, sequence *uint64) error {
		batch, err := model.closeScope(subscriptionID)
		if err != nil {
			return err
		}
		if len(batch.Entities) == 0 && len(batch.Queries) == 0 && len(batch.Projections) == 0 {
			return nil
		}
		*sequence = *sequence + 1
		return stream.Send(&reactivev1.ServerMessage{SubscriptionId: subscriptionID, Sequence: *sequence, Event: &reactivev1.ServerMessage_ModelBatch{ModelBatch: batch}})
	}
	cleanupFailedScope := func(subscriptionID string) error {
		batch, err := model.closeScope(subscriptionID)
		if err != nil {
			return err
		}
		// The failed initial scope was never attached publicly. Its removal may
		// nevertheless render deferred entity changes for surviving references;
		// those must be sent rather than silently marked as already published.
		if len(batch.Entities) == 0 {
			return nil
		}
		connectionSequence++
		return stream.Send(&reactivev1.ServerMessage{ConnectionSequence: connectionSequence, Event: &reactivev1.ServerMessage_ModelBatch{ModelBatch: &reactivev1.ModelBatch{Kind: reactivev1.ModelBatch_KIND_WINDOW, Entities: batch.Entities}}})
	}
	closeDirect := func(state *sharedSubscription, cause error) error {
		state.publication.close()
		delete(subscriptions, state.externalID)
		_ = state.subscription.Close(context.WithoutCancel(ctx))
		if state.cancel != nil {
			state.cancel()
		}
		_ = prepared.Release(context.WithoutCancel(ctx), state.visibility.id, nil)
		if err := sendCleanup(state.externalID, &state.sequence); err != nil {
			return err
		}
		if !errors.Is(cause, querysubscription.ErrClosed) {
			state.sequence++
			if err := stream.Send(&reactivev1.ServerMessage{SubscriptionId: state.externalID, Sequence: state.sequence, Event: &reactivev1.ServerMessage_Error{Error: &reactivev1.SubscriptionError{Code: "RESYNC_REQUIRED", Message: cause.Error()}}}); err != nil {
				return err
			}
		}
		return nil
	}
	closeProjection := func(state *sharedProjection, cause error) error {
		delete(projections, state.externalID)
		projectionOutputs.Drop(state.projection)
		state.close()
		_ = state.releaseVisibility(context.WithoutCancel(ctx), prepared)
		if err := sendCleanup(state.externalID, &state.sequence); err != nil {
			return err
		}
		if !errors.Is(cause, querysubscription.ErrClosed) {
			state.sequence++
			if err := stream.Send(&reactivev1.ServerMessage{SubscriptionId: state.externalID, Sequence: state.sequence, Event: &reactivev1.ServerMessage_Error{Error: &reactivev1.SubscriptionError{Code: "RESYNC_REQUIRED", Message: cause.Error()}}}); err != nil {
				return err
			}
		}
		return nil
	}
	var sendProjectionImmediate func(projectionBatchMessage, bool) error
	// Projection owns publication readiness and image completion; transport
	// owns visibility, connection assembly, and serialization of ready output.
	consumeProjectionOutput := func(output proj.Output[*sharedProjection]) error {
		state := output.Owner
		if projections[state.externalID] != state {
			return nil
		}
		if output.Err != nil {
			return closeProjection(state, fmt.Errorf("complete projection publication: %w", output.Err))
		}
		return sendProjectionImmediate(projectionBatchMessage{
			externalID: state.externalID, generation: state.generation, batch: output.Batch,
		}, true)
	}
	var sendSubscriptionImmediate func(subscriptionBatchMessage, bool) error
	var publications *policyPublications
	emitDeliveries := func(deliveries []feedDelivery) error {
		failed := make(map[string]visibilityFailure)
		failureKey := func(failure visibilityFailure) string {
			if failure.projection {
				return "projection:" + failure.externalID
			}
			return "subscription:" + failure.externalID
		}
		addFailure := func(failure visibilityFailure) {
			if failure.cause != nil {
				failed[failureKey(failure)] = failure
			}
		}
		isFailed := func(externalID string, projection bool) bool {
			return failed[failureKey(visibilityFailure{externalID: externalID, projection: projection})].cause != nil
		}
		for _, delivery := range deliveries {
			// Empty commits still synchronize auxiliary state: an authorization-only
			// transaction can produce visibility changes without a data payload.
			var authorizationView accesscontrol.View
			var invalidation accesscontrol.Invalidation
			var err error
			if delivery.metadata.Checkpoint != nil {
				authorizationView, invalidation, err = synchronizeVisibility(ctx, s.access, prepared, delivery.metadata.Checkpoint)
				if err != nil {
					return fmt.Errorf("synchronize access control: %w", err)
				}
			} else {
				authorizationView, err = acquireVisibilityAt(ctx, s.access, prepared, delivery.metadata.Checkpoint, delivery.metadata.Fence)
				if err != nil {
					return fmt.Errorf("synchronize materialized access control: %w", err)
				}
			}
			parts := append([]deliveryPayload(nil), delivery.parts...)
			if (delivery.metadata.Kind == querysubscription.DeliveryCommit || delivery.metadata.Kind == querysubscription.DeliveryHydration) && (invalidation.All || len(invalidation.Candidates) > 0 || len(invalidation.Targets) > 0) {
				// Every scope receives one frame joining raw effects and policy
				// invalidations. Catch-up changes only the target set, not the path.
				seen := make(map[string]bool)
				seenSubscriptions := make(map[string]bool)
				for index, raw := range parts {
					if part, ok := raw.(subscriptionBatchMessage); ok && delivery.replay == nil {
						part.invalidation = invalidation
						parts[index] = part
						seenSubscriptions[part.externalID] = true
					}
					if part, ok := raw.(projectionBatchMessage); ok {
						part.invalidation = invalidation
						parts[index] = part
						seen[part.externalID] = true
					}
				}
				appendFrame := func(state *sharedProjection) {
					if !seen[state.externalID] {
						parts = append(parts, projectionBatchMessage{externalID: state.externalID, generation: state.generation,
							batch: proj.ProjectionBatch{Delivery: delivery.metadata}, invalidation: invalidation})
					}
				}
				if delivery.replay != nil {
					appendFrame(delivery.replay)
				} else {
					for _, state := range subscriptions {
						if !seenSubscriptions[state.externalID] {
							parts = append(parts, subscriptionBatchMessage{externalID: state.externalID, generation: state.internalID, delivery: delivery.metadata, invalidation: invalidation})
						}
					}
					for _, state := range projections {
						appendFrame(state)
					}
				}
			}
			filteredParts := make([]deliveryPayload, 0, len(parts))
			for _, rawPart := range parts {
				var externalID string
				var projectionPart bool
				switch part := rawPart.(type) {
				case projectionBatchMessage:
					externalID, projectionPart = part.externalID, true
				case subscriptionBatchMessage:
					externalID = part.externalID
				}
				if isFailed(externalID, projectionPart) {
					continue
				}
				filtered, failure, filterErr := filterVisibilityPart(ctx, prepared, authorizationView, rawPart, subscriptions, projections)
				if filterErr != nil {
					_ = authorizationView.Close()
					return filterErr
				}
				if failure != nil {
					addFailure(*failure)
					continue
				}
				if filtered != nil {
					if publications != nil && delivery.metadata.Kind != querysubscription.DeliveryWindow && publications.withhold(filtered) {
						continue
					}
					filteredParts = append(filteredParts, filtered)
				}
			}
			if err := authorizationView.Close(); err != nil {
				return fmt.Errorf("close access-control view: %w", err)
			}
			if delivery.metadata.Kind == querysubscription.DeliveryWindow {
				for _, rawPart := range filteredParts {
					switch part := rawPart.(type) {
					case projectionBatchMessage:
						if isFailed(part.externalID, true) {
							continue
						}
						if err := sendProjectionImmediate(part, false); err != nil {
							return err
						}
					case subscriptionBatchMessage:
						if isFailed(part.externalID, false) {
							continue
						}
						if err := sendSubscriptionImmediate(part, false); err != nil {
							return err
						}
					}
				}
				continue
			}
			batch := &reactivev1.ModelBatch{Checkpoint: checkpointString(delivery.metadata.Checkpoint)}
			if delivery.replay == nil && delivery.metadata.Kind == querysubscription.DeliveryCommit {
				batch.Messages = delivery.messages
			}
			if err := model.assembleBatch(batch, func() error {
				for _, rawPart := range filteredParts {
					switch part := rawPart.(type) {
					case projectionBatchMessage:
						if isFailed(part.externalID, true) {
							continue
						}
						projection := projections[part.externalID]
						if projection == nil || (part.generation != "" && part.generation != projection.generation) {
							continue
						}
						modelBatch, err := model.applyProjection(projection.externalID, part.batch, projection.selected)
						if err != nil {
							return err
						}
						appendModelBatch(batch, modelBatch)
					case subscriptionBatchMessage:
						if isFailed(part.externalID, false) {
							continue
						}
						state := subscriptions[part.externalID]
						if state == nil || (part.generation != "" && part.generation != state.internalID) {
							continue
						}
						modelBatch, err := model.applySubscriptionBatch(part.versions, state.queryID, part.delivery, part.batch)
						if err != nil {
							return err
						}
						appendModelBatch(batch, modelBatch)
					default:
						return errors.New("unknown feed delivery payload")
					}
				}
				return nil
			}); err != nil {
				return err
			}
			if delivery.metadata.Kind == querysubscription.DeliveryHydration {
				batch.Kind = reactivev1.ModelBatch_KIND_HYDRATION
			} else {
				batch.Kind = reactivev1.ModelBatch_KIND_COMMIT
			}
			batch.Atomic = delivery.metadata.Atomic
			coalesceEntityEvents(batch)
			if len(batch.Entities) > 0 || len(batch.Queries) > 0 || len(batch.Projections) > 0 || len(batch.Messages) > 0 {
				connectionSequence++
				if err := stream.Send(&reactivev1.ServerMessage{ConnectionSequence: connectionSequence, Event: &reactivev1.ServerMessage_ModelBatch{ModelBatch: batch}}); err != nil {
					return err
				}
			}
		}
		for _, failure := range failed {
			if failure.projection {
				if state := projections[failure.externalID]; state != nil {
					if err := closeProjection(state, failure.cause); err != nil {
						return err
					}
				}
				continue
			}
			if state := subscriptions[failure.externalID]; state != nil {
				if err := closeDirect(state, failure.cause); err != nil {
					return err
				}
			}
		}
		for _, delivery := range deliveries {
			if delivery.replay != nil {
				continue
			}
			if err := projectionOutputs.Advance(delivery.metadata.Revision, consumeProjectionOutput); err != nil {
				return err
			}
		}
		return nil
	}
	sendProjectionImmediate = func(item projectionBatchMessage, evaluate bool) error {
		projection := projections[item.externalID]
		if projection == nil {
			return nil
		}
		if item.generation != "" && item.generation != projection.generation {
			return nil
		}
		if failure := item.batch.WindowFailure; failure != nil {
			projection.sequence++
			return stream.Send(&reactivev1.ServerMessage{
				SubscriptionId: projection.externalID, RequestId: failure.Token, Sequence: projection.sequence,
				Event: &reactivev1.ServerMessage_Error{Error: &reactivev1.SubscriptionError{Code: "WINDOW_FAILED", Message: failure.Cause.Error()}},
			})
		}
		var err error
		if evaluate && item.batch.Window != nil {
			authorizationView, accessErr := acquireVisibilityAt(ctx, s.access, prepared, item.batch.Window.Checkpoint, item.batch.Window.Fence)
			if accessErr != nil {
				return closeProjection(projection, accessErr)
			}
			item.batch, err = projection.visibility.filterBatch(ctx, prepared, authorizationView, item.batch)
			closeErr := authorizationView.Close()
			if err == nil {
				err = closeErr
			}
		} else if evaluate && len(item.batch.Events) > 0 {
			item.batch.Events, err = projection.visibility.filterInitial(ctx, s.access, prepared, item.batch.Events)
		}
		if err != nil {
			return closeProjection(projection, err)
		}
		if publications != nil && publications.withhold(item) {
			if window := item.batch.Window; window != nil {
				projection.publication.acks = append(projection.publication.acks, func() error {
					return publications.acknowledgeWindow(projection.externalID, window.Token, window.InstanceID, &projection.sequence)
				})
			}
			return nil
		}
		batch, err := model.applyProjection(projection.externalID, item.batch, projection.selected)
		if err != nil {
			return closeProjection(projection, err)
		}
		requestID := ""
		if window := item.batch.Window; window != nil {
			windowBatch, windowErr := model.applyProjectionWindow(window.InstanceID, window.Bounds)
			if windowErr != nil {
				return closeProjection(projection, windowErr)
			}
			appendModelBatch(batch, windowBatch)
			requestID = window.Token
		}
		batch.Kind = reactivev1.ModelBatch_KIND_WINDOW
		coalesceEntityEvents(batch)
		if len(batch.Entities) == 0 && len(batch.Queries) == 0 && len(batch.Projections) == 0 && requestID == "" {
			return nil
		}
		projection.sequence++
		return stream.Send(&reactivev1.ServerMessage{SubscriptionId: projection.externalID, RequestId: requestID, Sequence: projection.sequence, Event: &reactivev1.ServerMessage_ModelBatch{ModelBatch: batch}})
	}
	sendSubscriptionImmediate = func(item subscriptionBatchMessage, evaluate bool) error {
		state := subscriptions[item.externalID]
		if state == nil || (item.generation != "" && item.generation != state.internalID) {
			return nil
		}
		if err := item.delivery.Validate(); err != nil {
			return closeDirect(state, err)
		}
		if item.delivery.Kind == 0 {
			return closeDirect(state, errors.New("direct subscription output requires delivery metadata"))
		}
		var batch *reactivev1.ModelBatch
		var err error
		requestID := ""
		if window := item.batch.Window; window != nil {
			if evaluate {
				authorizationView, accessErr := acquireVisibilityAt(ctx, s.access, prepared, item.delivery.Checkpoint, item.delivery.Fence)
				if accessErr != nil {
					return closeDirect(state, accessErr)
				}
				item, err = state.visibility.filterTransaction(ctx, prepared, authorizationView, item)
				closeErr := authorizationView.Close()
				if err == nil {
					err = closeErr
				}
				if err != nil {
					return closeDirect(state, err)
				}
			}
			requestID = window.Token
		}
		if publications != nil && publications.withhold(item) {
			if window := item.batch.Window; window != nil {
				state.publication.acks = append(state.publication.acks, func() error {
					return publications.acknowledgeWindow(state.externalID, window.Token, state.queryID, &state.sequence)
				})
			}
			return nil
		}
		batch, err = model.applySubscriptionBatch(item.versions, state.queryID, item.delivery, item.batch)
		if err != nil {
			return closeDirect(state, err)
		}
		batch.Kind = reactivev1.ModelBatch_KIND_WINDOW
		state.sequence++
		return stream.Send(&reactivev1.ServerMessage{SubscriptionId: state.externalID, RequestId: requestID, Sequence: state.sequence, Event: &reactivev1.ServerMessage_ModelBatch{ModelBatch: batch}})
	}

	sendOutbound := func(raw outboundMessage) error {
		switch item := raw.(type) {
		case transactionMessage:
			connectionSequence++
			batch := &reactivev1.ModelBatch{Kind: reactivev1.ModelBatch_KIND_COMMIT, Atomic: true, Checkpoint: checkpointString(item.transaction.Position), Messages: item.transaction.Messages}
			return stream.Send(&reactivev1.ServerMessage{ConnectionSequence: connectionSequence, Event: &reactivev1.ServerMessage_ModelBatch{ModelBatch: batch}})
		case queryEventMessage:
			if item.err != nil {
				return item.err
			}
			if s.policyAccess != nil {
				publications.observe(item.event)
				affected, err := s.policyAccess.observe(item.event)
				if err != nil {
					return err
				}
				for _, loading := range pending {
					if item.event.Progress != nil {
						loading.retry = true
						if loading.direct != nil {
							loading.direct.observe(item.event.Progress.Position, item.event.Progress.Revision)
						}
					}
					if item.event.Delivery != nil {
						if loading.direct != nil {
							if err := loading.direct.apply(*item.event.Delivery); err != nil {
								loading.cancel()
								loading.retry = true
							}
						}
						for effect := range item.event.Delivery.SubscriptionEffects() {
							if loading.owns != nil && loading.owns(effect.SubscriptionID) {
								loading.retry = true
							}
						}
					}
					for _, target := range affected {
						if loading.policyOwns != nil && loading.policyOwns(target.Scope) {
							loading.retry = true
						}
					}
				}
			}
			if item.event.Progress != nil {
				for _, state := range projections {
					if _, _, err := state.projection.ApplyEvent(item.event); err != nil {
						return closeProjection(state, err)
					}
				}
				for _, state := range pending {
					if state.projection != nil {
						if _, _, err := state.projection.ApplyEvent(item.event); err != nil {
							state.cancel()
						}
					}
				}
				if err := projectionOutputs.Advance(item.event.Progress.Revision, consumeProjectionOutput); err != nil {
					return err
				}
				if s.policyAccess != nil {
					progress := item.event.Progress
					return emitDeliveries([]feedDelivery{{metadata: querysubscription.DeliveryMetadata{Kind: querysubscription.DeliveryHydration, Checkpoint: progress.Position, Revision: progress.Revision, PreviousRevision: progress.PreviousRevision}}})
				}
				return nil
			}
			return handleQueryEvent(item.event, func(deliveries []feedDelivery) error {
				for index := range deliveries {
					deliveries[index].messages = item.messages
				}
				return emitDeliveries(deliveries)
			}, closeDirect, closeProjection, subscriptions, projections, pending)
		case completionMessage:
			err := item.run()
			if item.finished != nil {
				item.finished <- err
			}
			return err
		case projectionClosedMessage:
			projection := projections[item.externalID]
			if projection == nil || (item.generation != "" && item.generation != projection.generation) {
				if loading := pending[item.externalID]; loading != nil && loading.generation == item.generation {
					loading.cancel()
					loading.retry = true
				}
				return nil
			}
			return closeProjection(projection, item.cause)
		case windowFailedMessage:
			if projection := projections[item.externalID]; projection != nil {
				if item.generation != "" && item.generation != projection.generation {
					return nil
				}
				projection.sequence++
				return stream.Send(&reactivev1.ServerMessage{SubscriptionId: projection.externalID, RequestId: item.requestID, Sequence: projection.sequence, Event: &reactivev1.ServerMessage_Error{Error: &reactivev1.SubscriptionError{Code: "WINDOW_FAILED", Message: item.cause.Error()}}})
			}
			state := subscriptions[item.externalID]
			if state == nil || (item.generation != "" && item.generation != state.internalID) {
				return nil
			}
			state.sequence++
			return stream.Send(&reactivev1.ServerMessage{SubscriptionId: state.externalID, RequestId: item.requestID, Sequence: state.sequence, Event: &reactivev1.ServerMessage_Error{Error: &reactivev1.SubscriptionError{Code: "WINDOW_FAILED", Message: item.cause.Error()}}})
		case projectionBatchMessage:
			projection := projections[item.externalID]
			if projection == nil || (item.generation != "" && item.generation != projection.generation) {
				return nil
			}
			if output := projectionOutputs.Submit(projection.projection, projection, item.batch); output != nil {
				return consumeProjectionOutput(*output)
			}
			return nil
		case subscriptionBatchMessage:
			return sendSubscriptionImmediate(item, true)
		default:
			return errors.New("unknown outbound message")
		}
	}
	// Initial output drains in this consumer before another query event can
	// apply a newer transaction. It must not round-trip through outbound.
	releaseProjection := func(state *sharedProjection) error {
		err := projectionOutputs.Release(state.projection, state, func(output proj.Output[*sharedProjection]) error {
			if output.Err != nil {
				return consumeProjectionOutput(output)
			}
			batch := output.Batch
			item := projectionBatchMessage{externalID: state.externalID, generation: state.generation, batch: batch}
			if batch.Delivery.Kind != 0 {
				return emitDeliveries([]feedDelivery{{
					replay: state, metadata: batch.Delivery, parts: []deliveryPayload{item},
				}})
			}
			return consumeProjectionOutput(output)
		})
		if err != nil && projections[state.externalID] == state {
			return closeProjection(state, err)
		}
		if errors.Is(err, querysubscription.ErrClosed) && projections[state.externalID] != state {
			return nil
		}
		return err
	}
	admissions := &policyAdmissions{ctx: ctx, service: s, stream: stream, queries: queries, prepared: prepared, subscriptions: subscriptions, projections: projections, pending: pending, loaders: &loaders, model: model, outbound: outbound, releaseProjection: releaseProjection, cleanupFailedScope: cleanupFailedScope}
	if s.policyAccess != nil {
		publications = &policyPublications{ctx: ctx, service: s, prepared: prepared, model: model, subscriptions: subscriptions, projections: projections, stream: stream, outbound: outbound, loaders: &loaders, closeDirect: closeDirect, closeProjection: closeProjection}
	}
	drain := func() error {
		for {
			select {
			case item := <-outbound:
				if err := sendOutbound(item); err != nil {
					return err
				}
				if s.policyAccess != nil {
					if err := admissions.retry(); err != nil {
						return err
					}
					if err := publications.retry(); err != nil {
						return err
					}
				}
			default:
				return nil
			}
		}
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-receiveErr:
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		case item := <-outbound:
			if err := sendOutbound(item); err != nil {
				return err
			}
			if s.policyAccess != nil {
				if err := admissions.retry(); err != nil {
					return err
				}
				if err := publications.retry(); err != nil {
					return err
				}
			}
		case message, ok := <-messages:
			if !ok {
				return nil
			}
			if err := drain(); err != nil {
				return err
			}
			accepted, requestErr := requests.Accept(message)
			if requestErr != nil {
				code := "INVALID_ARGUMENT"
				if errors.Is(requestErr, errRequestHistoryFull) {
					code = "RESOURCE_EXHAUSTED"
				}
				if err := sendSharedError(stream, message.GetSubscriptionId(), message.GetRequestId(), code, requestErr.Error()); err != nil {
					return err
				}
				continue
			}
			if !accepted {
				continue
			}
			if err := s.handleSharedMessage(ctx, stream, prefix, queries, prepared, subscriptions, projections, pending, &loaders, model, outbound, projectionOutputs, releaseProjection, cleanupFailedScope, message); err != nil {
				return err
			}
			if err := drain(); err != nil {
				return err
			}
		}
	}
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
			if state.projection.OwnsSubscription(failure.SubscriptionID) {
				return closeProjection(state, failure.Cause)
			}
		}
		for _, state := range pending {
			if state.owns != nil && state.owns(failure.SubscriptionID) {
				if state.projection != nil {
					state.projection.Close()
				}
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
