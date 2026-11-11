package engine

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/adiom-data/weave/internal/datastore"
	"github.com/adiom-data/weave/internal/engine/changeprocessor"
	"github.com/adiom-data/weave/internal/engine/delivery"
	"github.com/adiom-data/weave/internal/engine/entitycache"
	"github.com/adiom-data/weave/internal/engine/prefetchcache"
	"github.com/adiom-data/weave/internal/engine/queryregistry"
	"github.com/adiom-data/weave/internal/engine/recovery"
	"github.com/adiom-data/weave/internal/engine/selectorindex"
	"github.com/adiom-data/weave/internal/engine/subscriptions"
	"github.com/adiom-data/weave/internal/engine/windowloader"
	"github.com/adiom-data/weave/internal/metrics"
	"github.com/adiom-data/weave/internal/storage"
)

var ErrSubscriptionNotFound = errors.New("subscription not found")

type ReferenceSource struct {
	SubscriptionID string
	ParentKey      storage.EntityKey
}

type FetchOptions struct {
	Reference *storage.ReferenceFetch
	From      *ReferenceSource
	Entity    bool
}

type CoordinatorOptions struct {
	Limits  CoordinatorLimits
	Metrics metrics.Provider
}

type CoordinatorLimits struct {
	EntityIdleTTL          time.Duration
	EvictionPeriod         time.Duration
	EvictionBatchSize      int
	QueryIdleTTL           time.Duration
	QueryMaintenancePeriod time.Duration
	MaxResyncTransactions  int
	MaxResyncChanges       int
	MaxHydrations          int
	ResyncResultBuffer     int
	HydrationResultBuffer  int
	// MaxWindowRows limits client-requested initial and boundary-change counts.
	// It does not cap membership after rows enter a stable cursor range.
	MaxWindowRows int
	// SnapshotPageRows bounds each datastore read while materializing a range.
	// All pages for one load still share a single consistent snapshot and fence.
	SnapshotPageRows int
	// SubscriptionBatchSize bounds in-memory registration work per actor turn.
	SubscriptionBatchSize int
	// MaxReferenceInvalidations bounds speculative-reference dirty-key tracking.
	// Overflow disables reuse of that read, never CDC processing.
	MaxReferenceInvalidations int
	// ProgressBarrierDelay is the grace period for ordinary CDC to cover a
	// snapshot fence. Zero requests source progress immediately.
	ProgressBarrierDelay time.Duration
}

type InitialWindow struct {
	FromEnd bool
	Count   int
	Bounds  *storage.CursorBounds
}

type OpenSubscription struct {
	Subscription      *delivery.Subscription
	Plan              datastore.PreparedQuery
	ReferenceAccepted bool
	Bounds            datastore.Bounds
	// Initial owns detached records, transferred to the successful caller.
	// The cache and delivery hub do not retain these mutable records.
	Initial []entitycache.EntitySnapshot
	// Checkpoint is the latest source position incorporated into Initial. Fence
	// remains the snapshot read's lower reconciliation bound and is used only
	// when a source has not supplied a checkpoint yet.
	Checkpoint storage.Checkpoint
	Fence      storage.Fence
	Revision   uint64
}

type coordinatorCommand struct{ run func() }

type feedResult struct {
	event datastore.ChangeEvent
	err   error
}

type progressResult struct {
	fence storage.Fence
	err   error
}

type resyncReadResult struct {
	relation   storage.RelationKey
	generation uint64
	windows    []*windowloader.PendingWindow
	failures   []changeprocessor.QueryFailure
	err        error
}

type hydrationReadResult struct {
	key     storage.EntityKey
	load    entitycache.Load
	fence   storage.Fence
	record  storage.Record
	exists  bool
	err     error
	covered bool
}

type initialActivationResult struct{ err error }

type pendingInitialActivation struct {
	operation    initialWindowOperation
	materialized *materializedWindow
	result       chan initialActivationResult
}

type windowActivationResult struct {
	rows []entitycache.EntitySnapshot
	err  error
}

type pendingWindowActivation struct {
	operation    windowUpdateOperation
	materialized *materializedWindow
	result       chan windowActivationResult
}

type activeSubscription = subscriptions.State

type BoundarySide uint8

const (
	BoundaryLeft BoundarySide = iota + 1
	BoundaryRight
)

type BoundaryOperation uint8

const (
	BoundaryMove BoundaryOperation = iota + 1
	BoundaryExtend
	BoundaryContract
)

type BoundaryChange struct {
	Side      BoundarySide
	Operation BoundaryOperation
	Count     int
	Key       datastore.SortKey
}

// Coordinator is the single-writer owner of shared query membership and the
// source feed. Snapshot IO is performed outside the actor and reconciled back
// through commands, so slow database reads never pause CDC consumption.
type Coordinator struct {
	metrics   coordinatorMetrics
	source    datastore.DataSource
	feed      datastore.ChangeFeed
	entities  entityState
	queries   queryState
	processor changeEvaluator
	loader    windowReader
	hub       subscriptionDelivery

	commands         chan coordinatorCommand
	done             chan struct{}
	mu               sync.RWMutex
	err              error
	subscriptions    *subscriptions.Registry
	nextOperation    uint64
	deliveryRevision uint64
	// lastCheckpoint is actor-applied progress, never adapter receive progress.
	lastCheckpoint   storage.Checkpoint
	recovery         *recovery.Manager
	resyncResults    chan resyncReadResult
	hydrationResults chan hydrationReadResult
	options          CoordinatorLimits
	progressFence    storage.Fence
	progressStarting bool
	progressResults  chan progressResult
	pendingInitial   map[string]*pendingInitialActivation
	pendingWindows   map[string]*pendingWindowActivation
	referenceReads   map[storage.RelationKey]map[*referenceRead]struct{}
	windowReads      map[storage.RelationKey]map[*windowRead]struct{}
	prefetch         *prefetchcache.Cache
}

func StartCoordinator(ctx context.Context, source datastore.DataSource) (*Coordinator, error) {
	return StartCoordinatorWithOptions(ctx, source, CoordinatorOptions{})
}

func StartCoordinatorWithOptions(ctx context.Context, source datastore.DataSource, options CoordinatorOptions) (*Coordinator, error) {
	limits := options.Limits.withDefaults()
	if limits.EntityIdleTTL < 0 || limits.EvictionPeriod < 0 || limits.EvictionBatchSize < 1 || limits.QueryIdleTTL < 0 || limits.QueryMaintenancePeriod <= 0 || limits.ProgressBarrierDelay < 0 || limits.MaxResyncTransactions < 1 || limits.MaxResyncChanges < 1 || limits.MaxHydrations < 1 || limits.ResyncResultBuffer < 1 || limits.HydrationResultBuffer < 1 || limits.MaxWindowRows < 1 || limits.SnapshotPageRows < 1 || limits.SubscriptionBatchSize < 1 || limits.MaxReferenceInvalidations < 1 {
		return nil, errors.New("coordinator limits and durations must be positive")
	}
	feed, err := source.OpenFeed(ctx)
	if err != nil {
		return nil, err
	}
	entities := entitycache.NewEntityCache()
	queries := selectorindex.New(entities, limits.QueryIdleTTL)
	coordinator := &Coordinator{
		source: source, feed: feed, entities: entities, queries: queries,
		processor: changeprocessor.NewProcessor(source.ID(), entities, queries),
		loader:    windowloader.NewWindowLoader(source, entities, queries), hub: delivery.NewSubscriptionHub(queries),
		commands: make(chan coordinatorCommand), done: make(chan struct{}), subscriptions: subscriptions.NewRegistry(), recovery: recovery.NewManager(limits.MaxHydrations, limits.MaxResyncTransactions, limits.MaxResyncChanges), resyncResults: make(chan resyncReadResult, limits.ResyncResultBuffer), hydrationResults: make(chan hydrationReadResult, limits.HydrationResultBuffer), options: limits, progressResults: make(chan progressResult, 1), pendingInitial: make(map[string]*pendingInitialActivation), pendingWindows: make(map[string]*pendingWindowActivation),
	}
	coordinator.prefetch = prefetchcache.New(entities, limits.QueryIdleTTL)
	coordinator.metrics = newCoordinatorMetrics(options.Metrics)
	runCtx, cancelRun := context.WithCancel(ctx)
	go coordinator.run(runCtx, cancelRun)
	return coordinator, nil
}

func (c *Coordinator) SubscribeGrouped(ctx context.Context, id, group string, spec storage.QuerySpec, initial InitialWindow) (*OpenSubscription, error) {
	return c.SubscribeGroupedWithFetch(ctx, id, group, spec, initial, FetchOptions{})
}

func (c *Coordinator) OpenGroup(ctx context.Context, group string, limit int) (*delivery.ClientQueue[delivery.GroupMessage], error) {
	if group == "" || limit < 1 {
		return nil, errors.New("delivery group and positive queue limit are required")
	}
	result := make(chan *delivery.ClientQueue[delivery.GroupMessage], 1)
	select {
	case c.commands <- coordinatorCommand{run: func() { result <- c.hub.OpenGroup(group, limit) }}:
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-c.done:
		return nil, c.terminalError()
	}
	select {
	case queue := <-result:
		if err := ctx.Err(); err != nil {
			_ = c.ReleaseGroup(context.WithoutCancel(ctx), group)
			return nil, err
		}
		return queue, nil
	case <-c.done:
		return nil, c.terminalError()
	}
}

func (c *Coordinator) SubscribeGroupedWithFetch(ctx context.Context, id, group string, spec storage.QuerySpec, initial InitialWindow, fetch FetchOptions) (*OpenSubscription, error) {
	if initial.Bounds == nil && (initial.Count < 1 || initial.Count > c.options.MaxWindowRows) {
		return nil, fmt.Errorf("initial count must be between 1 and %d", c.options.MaxWindowRows)
	}
	plan, err := c.prepareFromReference(ctx, spec, fetch.From)
	if err != nil {
		return nil, err
	}
	if plan == nil {
		plan, err = c.source.PrepareQuery(ctx, spec)
	}
	if err != nil {
		return nil, err
	}
	if fetch.Entity {
		if preparer, ok := plan.(datastore.IdentityPreparer); ok {
			identity, matched, prepareErr := preparer.PrepareIdentity()
			if prepareErr != nil {
				return nil, prepareErr
			}
			if matched {
				plan = identity
			}
		}
	}
	operation := initialWindowOperation{subscriptionID: id, group: group, plan: plan}
	if initial.Bounds != nil {
		if initial.Count != 0 || initial.FromEnd {
			return nil, errors.New("initial bounds cannot be combined with a count")
		}
		bounds, err := decodeInitialBounds(plan, *initial.Bounds)
		if err != nil {
			return nil, err
		}
		operation.bounds = &bounds
	}
	if hint := fetch.Reference; hint != nil {
		if err := hint.Validate(); err != nil {
			return nil, err
		}
		if preparer, ok := c.source.(datastore.ReferencePreparer); ok {
			operation.reference, err = preparer.PrepareReference(ctx, plan, *hint)
			if err != nil {
				return nil, err
			}
		}
	}
	start, err := c.startSubscribe(ctx, operation, initial)
	if err != nil {
		return nil, err
	}
	return c.completeSubscribe(ctx, operation, initial, start)
}

func (c *Coordinator) completeSubscribe(ctx context.Context, operation initialWindowOperation, initial InitialWindow, start initialStart) (*OpenSubscription, error) {
	plan := operation.plan
	if start.opened != nil {
		return start.opened, nil
	}
	materialization := start.materialization
	if !start.staged {
		resolved, err := c.resolveInitialWindow(ctx, plan, initial, start.cached)
		if err != nil {
			c.cancelInitializing(operation)
			return nil, err
		}
		materialization, err = c.installInitializingWindow(ctx, operation, resolved)
		if err != nil {
			c.cancelInitializing(operation)
			return nil, err
		}
	}
	if materialization != nil {
		materialization.reference = operation.reference
		materialized, readErr := c.readMaterialization(ctx, materialization)
		if readErr != nil {
			c.cancelInitializing(operation)
			return nil, readErr
		}
		if err := c.activateInitializingWindow(ctx, operation, materialized); err != nil {
			c.cancelInitializing(operation)
			return nil, err
		}
	}
	if err := c.prefetchCurrentWindow(ctx, operation.subscriptionID, operation.reference); err != nil {
		c.cancelInitializing(operation)
		return nil, err
	}
	type response struct {
		opened *OpenSubscription
		err    error
	}
	finished := make(chan response, 1)
	command := coordinatorCommand{run: func() {
		opened, finishErr := c.finishSubscribeActor(operation)
		finished <- response{opened: opened, err: finishErr}
	}}
	select {
	case c.commands <- command:
	case <-c.done:
		return nil, c.terminalError()
	case <-ctx.Done():
		c.cancelInitializing(operation)
		return nil, ctx.Err()
	}
	select {
	case response := <-finished:
		return response.opened, response.err
	case <-c.done:
		return nil, c.terminalError()
	case <-ctx.Done():
		go c.cancelSubscription(operation)
		return nil, ctx.Err()
	}
}

// ReportProgress captures the latest consumed checkpoint and an ordering marker
// for one feed. Once accepted by the actor, the report must be returned and
// installed even if ctx is canceled, so later deliveries cannot lose their
// predecessor. No datastore IO is performed. A nil report means no source
// checkpoint has been observed yet; no delivery revision is allocated.
func (c *Coordinator) ReportProgress(ctx context.Context, group string) (*delivery.ProgressReport, error) {
	if group == "" {
		return nil, errors.New("progress group is required")
	}
	result := make(chan *delivery.ProgressReport, 1)
	command := coordinatorCommand{run: func() {
		if c.lastCheckpoint == nil {
			result <- nil
			return
		}
		c.deliveryRevision++
		report := c.hub.CaptureGroupProgress(group, c.lastCheckpoint, c.deliveryRevision)
		result <- &report
	}}
	select {
	case c.commands <- command:
	case <-c.done:
		return nil, c.terminalError()
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	select {
	case report := <-result:
		return report, nil
	case <-c.done:
		return nil, c.terminalError()
	}
}

type windowResolutionSource uint8

const (
	windowResolvedFromCache windowResolutionSource = iota + 1
	windowResolvedFromDiscovery
)

// resolvedWindow carries cursor provenance across asynchronous phases.
// Discovery is deliberately unfenced, so a discovery-derived resolution may
// not be published unless a later materialization supplies a source fence.
type resolvedWindow struct {
	bounds datastore.Bounds
	source windowResolutionSource
}

func (r resolvedWindow) validate() error {
	if r.source != windowResolvedFromCache && r.source != windowResolvedFromDiscovery {
		return errors.New("window resolution has no provenance")
	}
	return nil
}

// initialWindowOperation carries one subscription initialization's immutable
// identity across its cache-planning, discovery, materialization, and commit
// phases. Relation-epoch validity remains actor-owned by subscriptions.Registry.
type initialWindowOperation struct {
	subscriptionID string
	group          string
	plan           datastore.PreparedQuery
	reference      datastore.PreparedReference
	bounds         *datastore.Bounds
}

// resolveInitialWindow finds stable final bounds. A partial cache hit is
// extended only across the adjacent uncovered interval.
func (c *Coordinator) resolveInitialWindow(ctx context.Context, plan datastore.PreparedQuery, initial InitialWindow, cached queryregistry.InitialWindowPlan) (resolvedWindow, error) {
	full := datastore.Bounds{Left: datastore.Boundary{Kind: datastore.BoundaryStart}, Right: datastore.Boundary{Kind: datastore.BoundaryEnd}}
	if resolved, ok := cachedInitialWindow(plan, cached); ok {
		return resolved, nil
	}
	if !cached.Available {
		discoveries, err := c.loader.DiscoverWindows(ctx, plan, []datastore.WindowRequest{{Bounds: full, Limit: initial.Count + 1, Reverse: initial.FromEnd}})
		if err != nil {
			return resolvedWindow{}, err
		}
		discovered := discoveries[0]
		bounds, err := resolvedInitialBounds(plan, discovered, initial)
		return resolvedWindow{bounds: bounds, source: windowResolvedFromDiscovery}, err
	}

	remaining := initial.Count - cached.CachedRows
	if remaining < 1 {
		return resolvedWindow{}, errors.New("partial initial coverage has no missing rows")
	}
	extension := full
	if initial.FromEnd {
		extension.Right = cached.Bounds.Left
	} else {
		extension.Left = cached.Bounds.Right
	}
	discoveries, err := c.loader.DiscoverWindows(ctx, plan, []datastore.WindowRequest{{Bounds: extension, Limit: remaining + 1, Reverse: initial.FromEnd}})
	if err != nil {
		return resolvedWindow{}, err
	}
	discovered := discoveries[0]
	if err := sortWindowDiscoveries(plan, discovered); err != nil {
		return resolvedWindow{}, err
	}
	final := full
	if initial.FromEnd {
		if len(discovered) > remaining {
			final.Left = datastore.Boundary{Kind: datastore.BoundaryCursor, Key: discovered[len(discovered)-remaining-1].Key}
		}
		return resolvedWindow{bounds: final, source: windowResolvedFromDiscovery}, nil
	}
	if len(discovered) > remaining {
		final.Right = datastore.Boundary{Kind: datastore.BoundaryCursor, Key: discovered[remaining-1].Key}
	}
	return resolvedWindow{bounds: final, source: windowResolvedFromDiscovery}, nil
}

func (c *Coordinator) Unsubscribe(ctx context.Context, id string) (uint64, error) {
	type response struct {
		revision uint64
		err      error
	}
	result := make(chan response, 1)
	command := coordinatorCommand{run: func() {
		revision, err := c.retireSubscriptionActor(id)
		result <- response{revision, err}
	}}
	select {
	case c.commands <- command:
	case <-c.done:
		return 0, c.terminalError()
	case <-ctx.Done():
		return 0, ctx.Err()
	}
	// Once accepted, collect the bounded actor result even if the caller
	// cancels. Otherwise a successful unsubscribe could lose its retirement
	// floor and leave the local routing handle retained indefinitely.
	select {
	case response := <-result:
		return response.revision, response.err
	case <-c.done:
		return 0, c.terminalError()
	}
}

// ReleaseGroup discards delivery-order state after a connection has removed
// all of its subscriptions.
func (c *Coordinator) ReleaseGroup(ctx context.Context, group string) error {
	result := make(chan error, 1)
	command := coordinatorCommand{run: func() {
		if !c.hub.ReleaseGroup(group) {
			result <- errors.New("delivery group still has active subscriptions")
			return
		}
		result <- nil
	}}
	return c.waitCommand(ctx, command, result)
}

// QueueWindowUpdate installs the window and enqueues its transition in the
// subscription's CDC queue. Token is returned with that transition so a
// transport can acknowledge the originating request in stream order.
