package local

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/adiom-data/weave/internal/datastore"
	"github.com/adiom-data/weave/internal/engine"
	"github.com/adiom-data/weave/internal/engine/changeprocessor"
	"github.com/adiom-data/weave/internal/engine/delivery"
	"github.com/adiom-data/weave/internal/engine/entitycache"
	"github.com/adiom-data/weave/internal/querysubscription/feedbuffer"
	"github.com/adiom-data/weave/internal/querysubscription/model"
	sourceposition "github.com/adiom-data/weave/internal/querysubscription/position"
	"github.com/adiom-data/weave/internal/querysubscription/referenceview"
	"github.com/adiom-data/weave/internal/storage"
)

type Coordinator interface {
	OpenGroup(context.Context, string, int) (*delivery.ClientQueue[delivery.GroupMessage], error)
	SubscribeEntities(context.Context, string, []engine.EntitySubscribe, func(int, engine.SubscribeResult))
	SubscribeGroupedWithFetch(context.Context, string, string, storage.QuerySpec, engine.InitialWindow, engine.FetchOptions) (*engine.OpenSubscription, error)
	QueueWindowUpdate(context.Context, string, []engine.BoundaryChange, string) (datastore.Bounds, error)
	Unsubscribe(context.Context, string) (uint64, error)
	ReleaseGroup(context.Context, string) error
	ReportProgress(context.Context, string) (*delivery.ProgressReport, error)
}

func (s *localFeed) RequestProgress(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return querysubscription.ErrClosed
	}
	s.beginPendingLocked()
	s.mu.Unlock()
	defer s.finishPending()

	requestCtx, cancel := context.WithCancel(ctx)
	stopFeedCancel := context.AfterFunc(s.ctx, cancel)
	defer func() {
		stopFeedCancel()
		cancel()
	}()
	report, err := s.coordinator.ReportProgress(requestCtx, s.group)
	if err != nil {
		return err
	}
	if report == nil {
		return nil
	}
	if err := s.acceptProgress(*report); err != nil {
		s.fail(err)
		return err
	}
	return nil
}

// Client adapts the coordinator's native, in-process API to the consumer
// boundary. Prepared queries and delivery queues remain private here.
type Client struct {
	coordinator Coordinator
	references  ReferenceOptions
}

var feedSequence atomic.Uint64

func New(coordinator Coordinator) *Client {
	return NewWithOptions(coordinator, DefaultReferenceOptions())
}

func NewWithOptions(coordinator Coordinator, options ReferenceOptions) *Client {
	return &Client{coordinator: coordinator, references: options.withDefaults()}
}

func (c *Client) OpenFeed(ctx context.Context, options querysubscription.FeedOptions) (querysubscription.Feed, error) {
	base, err := c.openFeed(ctx, options)
	if err != nil {
		return nil, err
	}
	return referenceview.WrapFeed(ctx, base, options), nil
}

func (c *Client) openFeed(ctx context.Context, options querysubscription.FeedOptions) (*localFeed, error) {
	if options.BufferLimit <= 0 {
		return nil, errors.New("query feed buffer limit must be positive")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	feedCtx, cancel := context.WithCancel(ctx)
	pendingDone := make(chan struct{})
	close(pendingDone)
	group := fmt.Sprintf("query-feed-%d", feedSequence.Add(1))
	queue, err := c.coordinator.OpenGroup(feedCtx, group, options.BufferLimit)
	if err != nil {
		cancel()
		return nil, err
	}
	feed := &localFeed{
		coordinator:   c.coordinator,
		group:         group,
		bufferLimit:   options.BufferLimit,
		ctx:           feedCtx,
		cancel:        cancel,
		subscriptions: make(map[string]*localSubscription),
		pendingDone:   pendingDone,
		events:        feedbuffer.New(options.BufferLimit),
		deliveries:    make(map[uint64]*pendingDelivery),
		referenceWork: newReferenceWork(feedCtx, c.references, options.BufferLimit),
		groupQueue:    queue,
		registered:    make(chan struct{}, 1),
	}
	go feed.watchGroup()
	return feed, nil
}

type localFeed struct {
	referenceWork *referenceWork
	coordinator   Coordinator
	group         string
	bufferLimit   int
	ctx           context.Context
	cancel        context.CancelFunc
	groupQueue    *delivery.ClientQueue[delivery.GroupMessage]
	registered    chan struct{}

	mu              sync.Mutex
	closed          bool
	released        bool
	subscriptions   map[string]*localSubscription
	pending         int
	pendingDone     chan struct{}
	closeMu         sync.Mutex
	receiveMu       sync.Mutex
	feedMu          sync.Mutex
	failOnce        sync.Once
	events          *feedbuffer.Buffer
	deliveries      map[uint64]*pendingDelivery
	retirements     map[uint64][]string // guarded by feedMu; accepted delivery floors
	flushedRevision uint64
}

func (s *localFeed) nativeID(id string) string { return s.group + "\x00" + id }

func (s *localFeed) publicID(id string) (string, error) {
	prefix := s.group + "\x00"
	if !strings.HasPrefix(id, prefix) {
		return "", errors.New("query engine returned a subscription from another feed")
	}
	return strings.TrimPrefix(id, prefix), nil
}

func (s *localFeed) SubscribeQuery(ctx context.Context, request querysubscription.QuerySubscribeRequest, install querysubscription.QuerySnapshotInstaller) error {
	if err := querysubscription.ValidateQuerySubscribeRequest(request); err != nil {
		return err
	}
	if install == nil {
		return errors.New("snapshot installer is required")
	}
	return s.subscribe(ctx, request, false, nil, func(subscription *localSubscription, snapshot querysubscription.QuerySnapshot) error {
		return install(subscription, snapshot)
	}, nil)
}

func (s *localFeed) SubscribeEntity(ctx context.Context, request querysubscription.EntitySubscribeRequest, install querysubscription.EntitySnapshotInstaller) error {
	if err := querysubscription.ValidateEntitySubscribeRequest(request); err != nil {
		return err
	}
	if install == nil {
		return errors.New("entity snapshot installer is required")
	}
	queryRequest := querysubscription.QuerySubscribeRequest{ID: request.ID, Query: request.Query, Window: querysubscription.InitialWindow{Count: 1}}
	return s.subscribe(ctx, queryRequest, true, request.FetchFrom, func(subscription *localSubscription, snapshot querysubscription.QuerySnapshot) error {
		entitySnapshot := querysubscription.EntitySnapshot{Checkpoint: snapshot.Checkpoint, Fence: snapshot.Fence, Revision: snapshot.Revision}
		if len(snapshot.Rows) > 0 {
			row := snapshot.Rows[0].Row
			row.Cursor = nil
			entitySnapshot.Row = &row
		}
		return install(&localEntitySubscription{subscription: subscription}, entitySnapshot)
	}, nil)
}

func (s *localFeed) subscribe(ctx context.Context, request querysubscription.QuerySubscribeRequest, entity bool, from *querysubscription.ReferenceSource, install func(*localSubscription, querysubscription.QuerySnapshot) error, referenceChild *ownedReferenceChild) error {
	if install == nil {
		return errors.New("snapshot installer is required")
	}
	if err := s.reserveSubscription(request.ID); err != nil {
		return err
	}
	defer s.finishPending()

	subscribeCtx, cancel := context.WithCancel(ctx)
	stopFeedCancel := context.AfterFunc(s.ctx, cancel)
	defer func() {
		stopFeedCancel()
		cancel()
	}()

	nativeID := s.nativeID(request.ID)
	fetch := engine.FetchOptions{Reference: request.Reference, Entity: entity}
	if from != nil {
		fetch.From = &engine.ReferenceSource{SubscriptionID: s.nativeID(from.SubscriptionID), ParentKey: from.ParentKey}
	}
	opened, err := s.coordinator.SubscribeGroupedWithFetch(subscribeCtx, nativeID, s.group, request.Query, engine.InitialWindow{
		FromEnd: request.Window.FromEnd,
		Count:   request.Window.Count,
		Bounds:  request.Window.Bounds,
	}, fetch)
	if err != nil {
		s.removeReservation(request.ID)
		return err
	}
	return s.installOpened(subscribeCtx, request, entity, referenceChild, opened, install)
}

func (s *localFeed) reserveSubscription(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return querysubscription.ErrClosed
	}
	if _, exists := s.subscriptions[id]; exists {
		return errors.New("subscription ID already exists in feed")
	}
	// A nil entry reserves the connection-scoped ID while the snapshot loads.
	if err := s.events.RegisterSubscription(id); err != nil {
		return err
	}
	s.subscriptions[id] = nil
	s.beginPendingLocked()
	return nil
}

func (s *localFeed) installOpened(ctx context.Context, request querysubscription.QuerySubscribeRequest, entity bool, referenceChild *ownedReferenceChild, opened *engine.OpenSubscription, install func(*localSubscription, querysubscription.QuerySnapshot) error) error {
	nativeID := s.nativeID(request.ID)
	var err error
	subscription := &localSubscription{
		owner:          s,
		coordinator:    s.coordinator,
		id:             request.ID,
		nativeID:       nativeID,
		plan:           opened.Plan,
		referenceChild: referenceChild,
	}
	if entity {
		subscription.selector = newEntitySelection(opened.Plan, opened.Initial)
	}
	if opened.ReferenceAccepted {
		initialRows := make([]querysubscription.Row, 0, len(opened.Initial))
		for _, initial := range opened.Initial {
			initialRows = append(initialRows, querysubscription.Row{Key: initial.Key, Record: initial.Record, Pending: initial.State != entitycache.EntityCurrent})
		}
		subscription.references, err = newOwnedReferences(subscription, *request.Reference, initialRows)
		if err != nil {
			s.removeReservation(request.ID)
			_, closeErr := s.coordinator.Unsubscribe(context.WithoutCancel(ctx), nativeID)
			return errors.Join(err, closeErr)
		}
	}
	s.mu.Lock()
	if s.closed {
		delete(s.subscriptions, request.ID)
		s.signalRegistered()
		s.mu.Unlock()
		_, closeErr := s.coordinator.Unsubscribe(context.WithoutCancel(ctx), nativeID)
		return errors.Join(querysubscription.ErrClosed, closeErr)
	}
	// Stage output before publishing the handle to the shared consumer. CDC may
	// already have queued a frame between native registration and this point.
	s.beginInstallation()
	s.subscriptions[request.ID] = subscription
	s.signalRegistered()
	s.mu.Unlock()

	// Drain the engine queue as soon as it exists. Snapshot conversion is bounded
	// by the requested window but can still be non-trivial; leaving the native
	// queue idle here would create an avoidable backpressure window. Feed output
	// remains staged until the installer has accepted the complete snapshot.
	installed := false
	defer func() {
		if installed {
			return
		}
		s.discardStagedSubscription(request.ID)
		s.finishInstallation()
	}()
	cleanup := func() error {
		subscription.markDiscarding()
		if closeErr := subscription.Close(context.WithoutCancel(ctx)); closeErr != nil {
			s.fail(closeErr)
			return closeErr
		}
		return nil
	}
	rows := make([]querysubscription.QueryRow, 0, len(opened.Initial))
	for _, initial := range opened.Initial {
		var cursor []byte
		if !entity {
			key, keyErr := opened.Plan.Key(initial.Record)
			if keyErr != nil {
				return errors.Join(keyErr, cleanup())
			}
			var cursorErr error
			cursor, cursorErr = opened.Plan.EncodeKey(key)
			if cursorErr != nil {
				return errors.Join(cursorErr, cleanup())
			}
		}
		// The coordinator transferred these detached records to this operation.
		// Ambiguous selectors keep their own candidates for future promotion;
		// exact identities retain no payload. In both cases the installer owns
		// this snapshot and may freely mutate it.
		rows = append(rows, querysubscription.QueryRow{Row: querysubscription.Row{Key: initial.Key, Record: initial.Record, Pending: initial.State != entitycache.EntityCurrent, Cursor: append([]byte(nil), cursor...)}})
	}
	var bounds querysubscription.Bounds
	if !entity {
		var err error
		bounds, err = encodeBounds(opened.Plan, opened.Bounds)
		if err != nil {
			return errors.Join(err, cleanup())
		}
	}
	snapshot := querysubscription.QuerySnapshot{Bounds: bounds, Rows: rows, Checkpoint: sourceposition.FromSource(opened.Checkpoint), Fence: sourceposition.FromSource(opened.Fence), Revision: opened.Revision}
	if subscription.references != nil {
		err = subscription.references.attachInitialSnapshots(ctx, snapshot.Rows)
		if err != nil {
			return errors.Join(err, cleanup())
		}
		snapshot.OwnedReferences = true
	}
	if err := install(subscription, snapshot); err != nil {
		return errors.Join(err, cleanup())
	}
	subscription.markInstalled()
	installed = true
	s.finishInstallation()
	return nil
}

// beginPendingLocked registers operations that can create coordinator group
// state. Close waits for all of them before releasing the group.
func (s *localFeed) beginPendingLocked() {
	if s.pending == 0 {
		s.pendingDone = make(chan struct{})
	}
	s.pending++
}

func (s *localFeed) finishPending() {
	s.mu.Lock()
	s.pending--
	if s.pending == 0 {
		close(s.pendingDone)
	}
	s.mu.Unlock()
}

func (s *localFeed) removeReservation(id string) {
	s.mu.Lock()
	delete(s.subscriptions, id)
	s.events.RetireSubscription(id)
	s.signalRegistered()
	s.mu.Unlock()
}

func (s *localFeed) signalRegistered() {
	select {
	case s.registered <- struct{}{}:
	default:
	}
}

func (s *localFeed) Next(ctx context.Context) (querysubscription.Event, error) {
	s.receiveMu.Lock()
	defer s.receiveMu.Unlock()
	return s.events.Next(ctx)
}

func (s *localFeed) offer(event querysubscription.Event) error {
	return s.events.Offer(event)
}

func (s *localFeed) beginInstallation() { s.events.BeginInstallation() }
func (s *localFeed) finishInstallation() {
	if err := s.events.FinishInstallation(); err != nil {
		s.fail(err)
	}
}
func (s *localFeed) discardStagedSubscription(id string) { s.events.DiscardSubscription(id) }

func (s *localFeed) fail(reason error) {
	s.failOnce.Do(func() {
		s.events.Close(reason)
		s.cancel()
		go func() { _ = s.Close(context.Background()) }()
	})
}

func (s *localFeed) Close(ctx context.Context) error {
	s.closeMu.Lock()
	defer s.closeMu.Unlock()
	s.mu.Lock()
	if s.released {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	s.cancel()
	s.events.Close(querysubscription.ErrClosed)
	pendingDone := s.pendingDone
	subs := make([]*localSubscription, 0, len(s.subscriptions))
	for _, subscription := range s.subscriptions {
		if subscription != nil {
			subs = append(subs, subscription)
		}
	}
	s.mu.Unlock()
	var result error
	for _, subscription := range subs {
		result = errors.Join(result, subscription.Close(ctx))
	}
	// Subscription and progress requests are canceled above. Waiting prevents
	// either operation from recreating group state after release, while the
	// caller's close context still bounds how long Close itself may wait.
	select {
	case <-pendingDone:
	case <-ctx.Done():
		return errors.Join(result, ctx.Err())
	}
	if result != nil {
		return result
	}
	if err := s.coordinator.ReleaseGroup(ctx, s.group); err != nil {
		return err
	}
	s.mu.Lock()
	s.released = true
	// The consumer is cancelled during shutdown. Do not retain closed interests
	// through a held feed handle.
	s.subscriptions = nil
	s.mu.Unlock()
	s.feedMu.Lock()
	s.retirements = nil
	s.deliveries = nil
	s.feedMu.Unlock()
	return nil
}

type localSubscription struct {
	owner          *localFeed
	coordinator    Coordinator
	id             string
	nativeID       string
	plan           datastore.PreparedQuery
	selector       entitySelection
	references     *ownedReferences
	referenceChild *ownedReferenceChild

	lifecycle sync.RWMutex
	closed    bool
	// Failure stops effects immediately; routing survives until its accepted
	// revision floor has drained. Normal Close still preserves queued effects.
	// This monotonic flag is independent of lifecycle: a reference
	// failure must not wait behind an UpdateWindow holding a read lock during IO.
	failed    atomic.Bool
	installed bool
	discard   bool
}

func (s *localSubscription) ID() string { return s.id }

func (s *localSubscription) markInstalled() {
	s.lifecycle.Lock()
	s.installed = true
	s.lifecycle.Unlock()
}

func (s *localSubscription) markDiscarding() {
	s.lifecycle.Lock()
	s.discard = true
	s.lifecycle.Unlock()
}

// markFailed transitions once into terminal, metadata-only draining.
func (s *localSubscription) markFailed() bool {
	return s.failed.CompareAndSwap(false, true)
}

func (s *localSubscription) discardsQueuedEffects() bool {
	s.lifecycle.RLock()
	defer s.lifecycle.RUnlock()
	return s.failed.Load() || s.discard || (s.closed && !s.installed)
}

func (s *localSubscription) convertBatch(native delivery.SubscriptionBatch) (querysubscription.Batch, error) {
	if s.selector != nil {
		return s.selector.apply(native)
	}
	changes := make([]querysubscription.Change, 0, len(native.Events))
	for _, event := range native.Events {
		kind, kindErr := changeKind(event.Kind)
		if kindErr != nil {
			return querysubscription.Batch{}, kindErr
		}
		change := querysubscription.Change{Kind: kind, Row: querysubscription.Row{Key: event.Key, Record: storage.CloneRecord(event.Record), Pending: event.Pending, ChangeIndex: event.ChangeIndex}}
		if event.Kind != changeprocessor.QueryRemoved {
			key, keyErr := s.plan.Key(event.Record)
			if keyErr != nil {
				return querysubscription.Batch{}, keyErr
			}
			cursor, cursorErr := s.plan.EncodeKey(key)
			if cursorErr != nil {
				return querysubscription.Batch{}, cursorErr
			}
			change.Cursor = append([]byte(nil), cursor...)
		}
		changes = append(changes, change)
	}
	membership := querysubscription.MembershipDelta{Removed: append([]storage.EntityKey(nil), native.Membership.Removed...)}
	for _, insertion := range native.Membership.Insertions {
		membership.Insertions = append(membership.Insertions, querysubscription.Insertion{Key: insertion.Key, FinalIndex: insertion.FinalIndex})
	}
	var window *querysubscription.WindowTransition
	if native.Window != nil {
		bounds, err := encodeBounds(s.plan, native.Window.Bounds)
		if err != nil {
			return querysubscription.Batch{}, err
		}
		window = &querysubscription.WindowTransition{Token: native.Window.Token, Bounds: bounds, Fence: sourceposition.FromSource(native.Window.Fence), Checkpoint: sourceposition.FromSource(native.Window.Checkpoint)}
	}
	return querysubscription.Batch{
		Clear: native.Clear, ClearChangeIndex: native.ClearChangeIndex,
		Changes: changes, Membership: membership, Window: window,
	}, nil
}

func (s *localSubscription) UpdateWindow(ctx context.Context, changes []querysubscription.BoundaryChange, token string) error {
	s.lifecycle.RLock()
	defer s.lifecycle.RUnlock()
	if s.closed || s.failed.Load() {
		return querysubscription.ErrClosed
	}
	native := make([]engine.BoundaryChange, len(changes))
	for index, change := range changes {
		converted := engine.BoundaryChange{Count: change.Count}
		switch change.Side {
		case querysubscription.BoundaryLeft:
			converted.Side = engine.BoundaryLeft
		case querysubscription.BoundaryRight:
			converted.Side = engine.BoundaryRight
		default:
			return errors.New("boundary side is required")
		}
		switch change.Operation {
		case querysubscription.BoundaryMove:
			converted.Operation = engine.BoundaryMove
		case querysubscription.BoundaryExtend:
			converted.Operation = engine.BoundaryExtend
		case querysubscription.BoundaryContract:
			converted.Operation = engine.BoundaryContract
		default:
			return errors.New("boundary operation is required")
		}
		if len(change.Cursor) > 0 {
			key, err := s.plan.DecodeKey(change.Cursor)
			if err != nil {
				return err
			}
			converted.Key = key
		}
		native[index] = converted
	}
	_, err := s.coordinator.QueueWindowUpdate(ctx, s.nativeID, native, token)
	return err
}

func (s *localSubscription) Close(ctx context.Context) error {
	if s.references != nil {
		if err := s.references.close(ctx); err != nil {
			return err
		}
	}
	s.lifecycle.Lock()
	if s.closed {
		s.lifecycle.Unlock()
		return nil
	}
	// Cancellation, backpressure, or an isolated query failure may already
	// have retired the native subscription. Closing its handle is idempotent.
	revision, err := s.coordinator.Unsubscribe(ctx, s.nativeID)
	if err != nil && !errors.Is(err, engine.ErrSubscriptionNotFound) {
		s.lifecycle.Unlock()
		return err
	}
	s.closed = true
	s.lifecycle.Unlock()
	// Query failures have their own ordered terminal event. Normal retirement
	// uses an applied revision floor without adding anything to the data queue.
	if err == nil {
		s.owner.retireAfter(s.nativeID, revision)
	}
	return nil
}

// Owned interests fail as a unit: reporting a parent failure must also stop
// its parent and siblings, rather than merely relabeling a child failure.
func (s *localSubscription) failSubscription(err error) {
	if s.referenceChild != nil {
		s.referenceChild.owner.fail(err)
		return
	}
	if s.references != nil {
		s.references.fail(err)
		return
	}
	if !s.markFailed() {
		return
	}
	if offerErr := s.owner.offer(querysubscription.Event{Failure: &querysubscription.SubscriptionFailure{SubscriptionID: s.id, Cause: err}}); offerErr != nil {
		s.owner.fail(offerErr)
		return
	}
	if closeErr := s.Close(s.owner.ctx); closeErr != nil {
		s.owner.fail(errors.Join(err, closeErr))
		return
	}
}

type localEntitySubscription struct {
	subscription *localSubscription
}

func (s *localEntitySubscription) ID() string                      { return s.subscription.ID() }
func (s *localEntitySubscription) Close(ctx context.Context) error { return s.subscription.Close(ctx) }

func encodeBounds(plan datastore.PreparedQuery, bounds datastore.Bounds) (querysubscription.Bounds, error) {
	left, err := encodeBoundary(plan, bounds.Left)
	if err != nil {
		return querysubscription.Bounds{}, err
	}
	right, err := encodeBoundary(plan, bounds.Right)
	if err != nil {
		return querysubscription.Bounds{}, err
	}
	return querysubscription.Bounds{Left: left, Right: right}, nil
}

func encodeBoundary(plan datastore.PreparedQuery, boundary datastore.Boundary) (querysubscription.Boundary, error) {
	switch boundary.Kind {
	case datastore.BoundaryStart:
		return querysubscription.Boundary{Kind: querysubscription.BoundaryStart}, nil
	case datastore.BoundaryCursor:
		cursor, err := plan.EncodeKey(boundary.Key)
		return querysubscription.Boundary{Kind: querysubscription.BoundaryCursor, Cursor: append([]byte(nil), cursor...)}, err
	case datastore.BoundaryEnd:
		return querysubscription.Boundary{Kind: querysubscription.BoundaryEnd}, nil
	default:
		return querysubscription.Boundary{}, errors.New("query engine returned an unknown boundary kind")
	}
}

func changeKind(kind changeprocessor.QueryChangeKind) (querysubscription.ChangeKind, error) {
	switch kind {
	case changeprocessor.QueryAdded:
		return querysubscription.Added, nil
	case changeprocessor.QueryUpdated:
		return querysubscription.Updated, nil
	case changeprocessor.QueryRemoved:
		return querysubscription.Removed, nil
	default:
		return 0, errors.New("query engine returned an unknown row change kind")
	}
}

func deliveryKind(kind delivery.DeliveryKind) (querysubscription.DeliveryKind, error) {
	switch kind {
	case 0:
		return 0, nil
	case delivery.DeliveryCommit:
		return querysubscription.DeliveryCommit, nil
	case delivery.DeliveryHydration:
		return querysubscription.DeliveryHydration, nil
	case delivery.DeliveryWindow:
		return querysubscription.DeliveryWindow, nil
	case delivery.DeliveryProgress:
		return 0, nil
	default:
		return 0, errors.New("query engine returned an unknown delivery kind")
	}
}
