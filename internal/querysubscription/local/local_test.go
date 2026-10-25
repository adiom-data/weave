package local

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/adiom-data/weave/internal/datastore"
	"github.com/adiom-data/weave/internal/engine"
	"github.com/adiom-data/weave/internal/engine/changeprocessor"
	"github.com/adiom-data/weave/internal/engine/delivery"
	"github.com/adiom-data/weave/internal/engine/deliverytest"
	"github.com/adiom-data/weave/internal/engine/entitycache"
	"github.com/adiom-data/weave/internal/querysubscription/feedbuffer"
	"github.com/adiom-data/weave/internal/querysubscription/model"
	"github.com/adiom-data/weave/internal/storage"
)

type coordinatorStub struct {
	subscribe   func(context.Context, string, string, storage.QuerySpec, engine.InitialWindow, int) (*engine.OpenSubscription, error)
	unsubscribe func(context.Context, string) error
	release     func(context.Context, string) error
	progress    func(context.Context, string) (*delivery.ProgressReport, error)
}

// Old per-subscription fixture producers are adapted into the native group
// stream here, never in production. Real-backend integration tests exercise
// the hub's single shared queue directly.
type stubGroup struct {
	queue   *delivery.ClientQueue[delivery.GroupMessage]
	ctx     context.Context
	natives sync.Map
	limit   int
}

var stubGroups sync.Map

func (coordinatorStub) OpenGroup(ctx context.Context, group string, limit int) (*delivery.ClientQueue[delivery.GroupMessage], error) {
	entry := &stubGroup{queue: delivery.NewClientQueue[delivery.GroupMessage](limit), ctx: ctx, limit: limit}
	stubGroups.Store(group, entry)
	return entry.queue, nil
}

func fixtureInterest(id, group string) *deliverytest.Interest {
	entry, _ := stubGroups.Load(group)
	g := entry.(*stubGroup)
	native := deliverytest.New(&delivery.Subscription{ID: id, Group: group}, g.queue)
	g.natives.Store(id, native)
	return native
}

func (c coordinatorStub) ReportProgress(ctx context.Context, group string) (*delivery.ProgressReport, error) {
	if c.progress != nil {
		return c.progress(ctx, group)
	}
	return nil, errors.New("unexpected progress request")
}

func (c coordinatorStub) SubscribeGroupedWithFetch(ctx context.Context, id, group string, spec storage.QuerySpec, window engine.InitialWindow, _ engine.FetchOptions) (*engine.OpenSubscription, error) {
	if c.subscribe == nil {
		panic("unexpected SubscribeGrouped")
	}
	entry, _ := stubGroups.Load(group)
	opened, err := c.subscribe(ctx, id, group, spec, window, entry.(*stubGroup).limit)
	if err != nil {
		return opened, err
	}
	return opened, nil
}

func (c coordinatorStub) SubscribeEntities(ctx context.Context, group string, requests []engine.EntitySubscribe, install func(int, engine.SubscribeResult)) {
	for i, request := range requests {
		opened, err := c.SubscribeGroupedWithFetch(ctx, request.ID, group, request.Query, engine.InitialWindow{Count: 1}, engine.FetchOptions{Entity: true, From: request.From})
		install(i, engine.SubscribeResult{Opened: opened, Err: err})
	}
}

func (coordinatorStub) QueueWindowUpdate(context.Context, string, []engine.BoundaryChange, string) (datastore.Bounds, error) {
	panic("unexpected QueueWindowUpdate")
}

func (c coordinatorStub) Unsubscribe(ctx context.Context, id string) (uint64, error) {
	if err := c.unsubscribe(ctx, id); err != nil {
		return 0, err
	}
	var revision uint64
	stubGroups.Range(func(_, value any) bool {
		if native, ok := value.(*stubGroup).natives.Load(id); ok {
			revision = native.(*deliverytest.Interest).Queue.RetirementRevision()
		}
		return true
	})
	return revision, nil
}

func (c coordinatorStub) ReleaseGroup(ctx context.Context, group string) error {
	if c.release != nil {
		if err := c.release(ctx, group); err != nil {
			return err
		}
	}
	if entry, ok := stubGroups.LoadAndDelete(group); ok {
		entry.(*stubGroup).queue.Close(delivery.ErrClientClosed)
	}
	return nil
}

func subscribeLocalTest(ctx context.Context, feed querysubscription.Feed, request querysubscription.QuerySubscribeRequest) (querysubscription.QuerySubscription, querysubscription.QuerySnapshot, error) {
	var subscription querysubscription.QuerySubscription
	var snapshot querysubscription.QuerySnapshot
	err := feed.SubscribeQuery(ctx, request, func(opened querysubscription.QuerySubscription, initial querysubscription.QuerySnapshot) error {
		subscription, snapshot = opened, initial
		return nil
	})
	return subscription, snapshot, err
}

func TestFeedForwardsRequestedProgress(t *testing.T) {
	called := false
	feed, err := New(coordinatorStub{progress: func(_ context.Context, group string) (*delivery.ProgressReport, error) {
		called = group != ""
		return &delivery.ProgressReport{Checkpoint: &querysubscription.Position{SourceID: "test", Value: 42}, Revision: 1}, nil
	}}).OpenFeed(context.Background(), querysubscription.FeedOptions{BufferLimit: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer feed.Close(context.Background())
	if err := feed.RequestProgress(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !called {
		t.Fatal("coordinator did not receive progress request")
	}
	event, err := feed.Next(context.Background())
	if err != nil || event.Progress == nil || event.Progress.Position.Value != 42 {
		t.Fatalf("empty feed progress = %+v, %v", event, err)
	}
}

func TestUnknownProgressDoesNotEscapeLocalAdapter(t *testing.T) {
	calls := 0
	feed, err := New(coordinatorStub{progress: func(context.Context, string) (*delivery.ProgressReport, error) {
		calls++
		if calls == 1 {
			return nil, nil
		}
		return &delivery.ProgressReport{Checkpoint: &querysubscription.Position{SourceID: "test", Value: 42}, Revision: 1}, nil
	}}).OpenFeed(context.Background(), querysubscription.FeedOptions{BufferLimit: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer feed.Close(context.Background())
	if err := feed.RequestProgress(context.Background()); err != nil {
		t.Fatal(err)
	}
	wait, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if event, err := feed.Next(wait); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("fabricated progress: %+v, %v", event, err)
	}
	if err := feed.RequestProgress(context.Background()); err != nil {
		t.Fatal(err)
	}
	ctx, stop := context.WithTimeout(context.Background(), time.Second)
	defer stop()
	if event, err := feed.Next(ctx); err != nil || event.Progress == nil || event.Progress.Position.Value != 42 || event.Progress.Revision != 1 {
		t.Fatalf("later progress = %+v, %v", event, err)
	}
}

func TestFeedCloseWaitsForProgressBeforeReleasingGroup(t *testing.T) {
	for _, accepted := range []bool{false, true} {
		name := "unaccepted"
		if accepted {
			name = "accepted"
		}
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			started := make(chan struct{})
			canceled := make(chan struct{})
			released := make(chan struct{})
			finish := make(chan struct{})
			var finishOnce sync.Once
			unblock := func() { finishOnce.Do(func() { close(finish) }) }
			defer unblock()
			feed, err := New(coordinatorStub{
				progress: func(requestCtx context.Context, _ string) (*delivery.ProgressReport, error) {
					close(started)
					select {
					case <-requestCtx.Done():
					case <-ctx.Done():
						return nil, ctx.Err()
					}
					close(canceled)
					select {
					case <-finish:
					case <-ctx.Done():
						return nil, ctx.Err()
					}
					if !accepted {
						return nil, requestCtx.Err()
					}
					return &delivery.ProgressReport{Checkpoint: &querysubscription.Position{SourceID: "test", Value: 42}, Revision: 1}, nil
				},
				release: func(context.Context, string) error {
					close(released)
					return nil
				},
			}).OpenFeed(context.Background(), querysubscription.FeedOptions{BufferLimit: 4})
			if err != nil {
				t.Fatal(err)
			}
			defer feed.Close(ctx)
			progressDone := make(chan error, 1)
			go func() { progressDone <- feed.RequestProgress(ctx) }()
			<-started
			closeDone := make(chan error, 1)
			go func() { closeDone <- feed.Close(ctx) }()
			select {
			case <-canceled:
			case <-ctx.Done():
				t.Fatal("Close did not cancel the in-flight progress request")
			}
			select {
			case <-released:
				t.Fatal("group released while a progress request could still recreate it")
			default:
			}
			unblock()
			if err := <-closeDone; err != nil {
				t.Fatal(err)
			}
			if err := <-progressDone; !errors.Is(err, context.Canceled) && !errors.Is(err, querysubscription.ErrClosed) {
				t.Fatalf("progress after close = %v", err)
			}
			select {
			case <-released:
			default:
				t.Fatal("group was not released after progress completed")
			}
		})
	}
}

func TestEntitySubscriptionUsesCountOneQueryBehindNarrowInterface(t *testing.T) {
	relation := storage.RelationKey{Source: "test", Name: "items"}
	plan := entityNoCursorPlan{nonComparingPlan{relation: relation}}
	record := storage.Record{"id": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("one")}}
	key := storage.EntityKey{Relation: relation, Value: "one"}
	coordinator := coordinatorStub{
		subscribe: func(_ context.Context, id, group string, _ storage.QuerySpec, window engine.InitialWindow, limit int) (*engine.OpenSubscription, error) {
			if window.FromEnd || window.Count != 1 {
				t.Fatalf("entity window = %+v", window)
			}
			return &engine.OpenSubscription{
				Plan: plan, Subscription: fixtureInterest(id, group).Subscription,
				Bounds:  datastore.Bounds{Left: datastore.Boundary{Kind: datastore.BoundaryStart}, Right: datastore.Boundary{Kind: datastore.BoundaryCursor, Key: datastore.EncodedSortKey("one")}},
				Initial: []entitycache.EntitySnapshot{{Key: key, Record: record, State: entitycache.EntityCurrent}},
			}, nil
		},
		unsubscribe: func(context.Context, string) error { return nil },
	}
	feed, err := New(coordinator).OpenFeed(context.Background(), querysubscription.FeedOptions{BufferLimit: 4})
	if err != nil {
		t.Fatal(err)
	}
	defer feed.Close(context.Background())
	var subscription querysubscription.EntitySubscription
	var snapshot querysubscription.EntitySnapshot
	err = feed.SubscribeEntity(context.Background(), querysubscription.EntitySubscribeRequest{ID: "one"}, func(opened querysubscription.EntitySubscription, initial querysubscription.EntitySnapshot) error {
		subscription, snapshot = opened, initial
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, mutable := subscription.(querysubscription.QuerySubscription); mutable {
		t.Fatal("entity subscription exposed window controls")
	}
	if snapshot.Row == nil || snapshot.Row.Key != key || len(snapshot.Row.Cursor) != 0 {
		t.Fatalf("entity snapshot = %+v", snapshot)
	}
}

func TestEntitySubscriptionRetainsTiedInitialCandidates(t *testing.T) {
	relation := storage.RelationKey{Source: "test", Name: "items"}
	plan := tiedPlan{nonComparingPlan{relation: relation}}
	firstKey := storage.EntityKey{Relation: relation, Value: "first"}
	secondKey := storage.EntityKey{Relation: relation, Value: "second"}
	first := storage.Record{"id": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("first")}}
	second := storage.Record{"id": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("second")}}
	coordinator := coordinatorStub{
		subscribe: func(_ context.Context, id, group string, _ storage.QuerySpec, _ engine.InitialWindow, limit int) (*engine.OpenSubscription, error) {
			return &engine.OpenSubscription{
				Plan: plan, Subscription: fixtureInterest(id, group).Subscription,
				Bounds: datastore.Bounds{Left: datastore.Boundary{Kind: datastore.BoundaryStart}, Right: datastore.Boundary{Kind: datastore.BoundaryEnd}},
				Initial: []entitycache.EntitySnapshot{
					{Key: firstKey, Record: first, State: entitycache.EntityCurrent},
					{Key: secondKey, Record: second, State: entitycache.EntityCurrent},
				},
			}, nil
		},
		unsubscribe: func(context.Context, string) error { return nil },
	}
	feed, err := New(coordinator).OpenFeed(context.Background(), querysubscription.FeedOptions{BufferLimit: 4})
	if err != nil {
		t.Fatal(err)
	}
	defer feed.Close(context.Background())
	var opened querysubscription.EntitySubscription
	var snapshot querysubscription.EntitySnapshot
	if err := feed.SubscribeEntity(context.Background(), querysubscription.EntitySubscribeRequest{ID: "ambiguous"}, func(subscription querysubscription.EntitySubscription, initial querysubscription.EntitySnapshot) error {
		opened, snapshot = subscription, initial
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if snapshot.Row == nil || snapshot.Row.Key != firstKey {
		t.Fatalf("selected initial row = %+v", snapshot.Row)
	}
	local, ok := opened.(*localEntitySubscription)
	if !ok {
		t.Fatalf("entity subscription type = %T", opened)
	}
	selector, ok := local.subscription.selector.(*entitySelector)
	if !ok || len(selector.order) != 2 || len(selector.rows) != 2 {
		t.Fatalf("ambiguous selection did not retain its candidates: %+v", local.subscription.selector)
	}
	// The installer owns its row. Mutating it must not change the private
	// candidate state used when future events select/promote an entity.
	before := storage.CloneRecord(selector.rows[firstKey].Record)
	for _, value := range snapshot.Row.Record {
		if len(value.Data) > 0 {
			value.Data[0] = 'X'
		}
	}
	delete(snapshot.Row.Record, "id")
	for field, value := range before {
		if !storage.EqualValues(selector.rows[firstKey].Record[field], value) {
			t.Fatal("installer snapshot aliases retained entity candidates")
		}
	}
}

func TestCloseCanRetryFailedUnsubscribe(t *testing.T) {
	attempts := 0
	subscription := &localSubscription{
		id: "items", nativeID: "\x00items", owner: &localFeed{subscriptions: make(map[string]*localSubscription), registered: make(chan struct{}, 1)},
		coordinator: coordinatorStub{unsubscribe: func(context.Context, string) error {
			attempts++
			if attempts == 1 {
				return context.Canceled
			}
			return nil
		}},
	}
	if err := subscription.Close(context.Background()); !errors.Is(err, context.Canceled) {
		t.Fatalf("first Close error = %v", err)
	}
	if err := subscription.Close(context.Background()); err != nil {
		t.Fatalf("retry Close: %v", err)
	}
	if attempts != 2 || !subscription.closed {
		t.Fatalf("attempts = %d, closed = %v", attempts, subscription.closed)
	}
	if err := subscription.Close(context.Background()); err != nil || attempts != 2 {
		t.Fatalf("idempotent Close error = %v, attempts = %d", err, attempts)
	}
}

func TestCloseSucceedsAfterNativeCancellationRetiredSubscription(t *testing.T) {
	attempts := 0
	subscription := &localSubscription{
		id: "retired", nativeID: "retired", owner: &localFeed{subscriptions: make(map[string]*localSubscription)},
		coordinator: coordinatorStub{unsubscribe: func(context.Context, string) error {
			attempts++
			return engine.ErrSubscriptionNotFound
		}},
	}
	for range 2 {
		if err := subscription.Close(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if !subscription.closed || attempts != 1 {
		t.Fatal("already-retired handle did not close idempotently")
	}
}

func TestNativeEnumConversionRejectsUnknownValues(t *testing.T) {
	if _, err := changeKind(255); err == nil {
		t.Fatal("unknown change kind was accepted")
	}
	if _, err := deliveryKind(delivery.DeliveryKind(255)); err == nil {
		t.Fatal("unknown delivery kind was accepted")
	}
	if _, err := encodeBoundary(nil, datastore.Boundary{Kind: 255}); err == nil {
		t.Fatal("unknown boundary kind was accepted")
	}
}

type nonComparingPlan struct{ relation storage.RelationKey }

type entityNoCursorPlan struct{ nonComparingPlan }

func (entityNoCursorPlan) EncodeKey(datastore.SortKey) ([]byte, error) {
	return nil, errors.New("entity subscriptions must not encode cursors")
}

func (p nonComparingPlan) Fingerprint() datastore.QueryFingerprint { return "items" }
func (p nonComparingPlan) Relation() storage.RelationKey           { return p.relation }
func (p nonComparingPlan) EntityKey(record storage.Record) (storage.EntityKey, error) {
	return storage.EntityKey{Relation: p.relation, Value: string(record["id"].Data)}, nil
}
func (nonComparingPlan) Matches(storage.Record) (bool, error) { return true, nil }
func (nonComparingPlan) Key(record storage.Record) (datastore.SortKey, error) {
	return datastore.EncodedSortKey(record["id"].Data), nil
}
func (nonComparingPlan) Compare(datastore.SortKey, datastore.SortKey) (int, error) {
	panic("local query adapter must not reconstruct ordering")
}
func (nonComparingPlan) EncodeKey(key datastore.SortKey) ([]byte, error) {
	return append([]byte(nil), key.(datastore.EncodedSortKey)...), nil
}
func (nonComparingPlan) DecodeKey(encoded []byte) (datastore.SortKey, error) {
	return datastore.EncodedSortKey(append([]byte(nil), encoded...)), nil
}
func (nonComparingPlan) Query() storage.QuerySpec { return storage.QuerySpec{} }

type tiedPlan struct{ nonComparingPlan }

func (tiedPlan) Key(storage.Record) (datastore.SortKey, error) {
	return datastore.EncodedSortKey("same"), nil
}

type failingConversionPlan struct{ nonComparingPlan }

func (failingConversionPlan) Key(storage.Record) (datastore.SortKey, error) {
	return nil, errors.New("cannot encode cursor")
}

type blockingSnapshotPlan struct {
	nonComparingPlan
	started chan struct{}
	release chan struct{}
	once    *sync.Once
}

func (p blockingSnapshotPlan) Key(record storage.Record) (datastore.SortKey, error) {
	p.once.Do(func() { close(p.started) })
	<-p.release
	return p.nonComparingPlan.Key(record)
}

func TestConvertBatchPassesThroughEngineMembershipDeltaWithoutReordering(t *testing.T) {
	relation := storage.RelationKey{Source: "test", Name: "items"}
	key := storage.EntityKey{Relation: relation, Value: "b"}
	record := storage.Record{"id": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("b")}}
	native := delivery.SubscriptionBatch{
		Events:     []delivery.SubscriptionEvent{{Kind: changeprocessor.QueryAdded, Key: key, Record: record}},
		Membership: delivery.MembershipDelta{Insertions: []delivery.MembershipInsertion{{Key: key, FinalIndex: 1}}},
	}
	subscription := &localSubscription{plan: nonComparingPlan{relation: relation}}
	batch, err := subscription.convertBatch(native)
	if err != nil {
		t.Fatal(err)
	}
	want := querysubscription.Insertion{Key: key, FinalIndex: 1}
	if len(batch.Membership.Insertions) != 1 || batch.Membership.Insertions[0] != want {
		t.Fatalf("membership = %+v, want %+v", batch.Membership, want)
	}
}

func TestNativeBackpressureFailsSessionFeed(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	queue := delivery.NewClientQueue[delivery.GroupMessage](1)
	queue.Close(delivery.ErrClientQueueFull)
	owner := &localFeed{
		ctx: ctx, cancel: cancel, subscriptions: make(map[string]*localSubscription),
		groupQueue: queue,
		events:     feedbuffer.New(1),
		deliveries: make(map[uint64]*pendingDelivery),
	}
	go owner.watchGroup()
	if _, err := owner.Next(context.Background()); !errors.Is(err, querysubscription.ErrBackpressure) {
		t.Fatalf("Next error = %v, want %v", err, querysubscription.ErrBackpressure)
	}
}

func TestBatchConversionFailureIsIsolatedToSubscription(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	relation := storage.RelationKey{Source: "test", Name: "items"}
	queue := delivery.NewClientQueue[delivery.GroupMessage](4)
	owner := &localFeed{
		group: "feed", ctx: ctx, cancel: cancel, subscriptions: make(map[string]*localSubscription),
		groupQueue:  queue,
		bufferLimit: 4,
		events:      feedbuffer.New(4),
		deliveries:  make(map[uint64]*pendingDelivery),
	}
	subscription := &localSubscription{
		id: "items", nativeID: owner.nativeID("items"), owner: owner,
		coordinator: coordinatorStub{unsubscribe: func(context.Context, string) error { return nil }},
		plan:        failingConversionPlan{nonComparingPlan{relation: relation}},
	}
	owner.subscriptions[subscription.id] = subscription
	record := storage.Record{"id": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("1")}}
	if err := queue.Offer(delivery.GroupMessage{Batches: []delivery.SubscriptionBatch{{SubscriptionID: subscription.nativeID,
		Delivery: delivery.DeliveryCommit, Checkpoint: &querysubscription.Position{SourceID: "test", Value: 1}, Revision: 1, Events: []delivery.SubscriptionEvent{{Kind: changeprocessor.QueryAdded, Key: storage.EntityKey{Relation: relation, Value: "1"}, Record: record}},
	}}}); err != nil {
		t.Fatal(err)
	}
	go owner.watchGroup()
	progress, err := owner.Next(context.Background())
	if err != nil || progress.Delivery == nil || progress.Delivery.Delivery.Revision != 1 || len(progress.Delivery.Effects) != 0 {
		t.Fatalf("progress delivery = %+v, err=%v", progress, err)
	}
	failure, err := owner.Next(context.Background())
	if err != nil || failure.Failure == nil || failure.Failure.SubscriptionID != "items" || failure.Failure.Cause.Error() != "cannot encode cursor" {
		t.Fatalf("isolated failure = %+v, err=%v", failure, err)
	}
	waitCtx, stop := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer stop()
	if _, err := owner.Next(waitCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("session feed terminated after isolated failure: %v", err)
	}
}

func TestSessionSubscriptionIDsAreUniqueForSessionLifetime(t *testing.T) {
	relation := storage.RelationKey{Source: "test", Name: "items"}
	plan := nonComparingPlan{relation: relation}
	subscribeCalls := 0
	coordinator := coordinatorStub{
		subscribe: func(_ context.Context, id, group string, _ storage.QuerySpec, _ engine.InitialWindow, limit int) (*engine.OpenSubscription, error) {
			subscribeCalls++
			return &engine.OpenSubscription{
				Plan:         plan,
				Subscription: fixtureInterest(id, group).Subscription,
				Bounds:       datastore.Bounds{Left: datastore.Boundary{Kind: datastore.BoundaryStart}, Right: datastore.Boundary{Kind: datastore.BoundaryEnd}},
			}, nil
		},
		unsubscribe: func(context.Context, string) error { return nil },
	}
	session, err := New(coordinator).OpenFeed(context.Background(), querysubscription.FeedOptions{BufferLimit: 4})
	if err != nil {
		t.Fatal(err)
	}
	subscription, _, err := subscribeLocalTest(context.Background(), session, querysubscription.QuerySubscribeRequest{ID: "items", Window: querysubscription.InitialWindow{Count: 1}})
	if err != nil {
		t.Fatal(err)
	}
	if err := subscription.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, _, err := subscribeLocalTest(context.Background(), session, querysubscription.QuerySubscribeRequest{ID: "items", Window: querysubscription.InitialWindow{Count: 1}}); err == nil {
		t.Fatal("session reused a retired subscription ID")
	}
	if subscribeCalls != 1 {
		t.Fatalf("SubscribeGrouped calls = %d, want 1", subscribeCalls)
	}
	if err := session.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestFailedSubscribeConsumesID(t *testing.T) {
	subscribeCalls := 0
	coordinator := coordinatorStub{
		subscribe: func(context.Context, string, string, storage.QuerySpec, engine.InitialWindow, int) (*engine.OpenSubscription, error) {
			subscribeCalls++
			return nil, errors.New("rejected")
		},
		unsubscribe: func(context.Context, string) error { return nil },
	}
	session, err := New(coordinator).OpenFeed(context.Background(), querysubscription.FeedOptions{BufferLimit: 4})
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close(context.Background())
	request := querysubscription.QuerySubscribeRequest{ID: "failed", Window: querysubscription.InitialWindow{Count: 1}}
	if _, _, err := subscribeLocalTest(context.Background(), session, request); err == nil {
		t.Fatal("first Subscribe succeeded")
	}
	if _, _, err := subscribeLocalTest(context.Background(), session, request); err == nil {
		t.Fatal("failed subscription ID was reused")
	}
	if subscribeCalls != 1 {
		t.Fatalf("SubscribeGrouped calls = %d, want 1", subscribeCalls)
	}
}

func TestSessionCloseCancelsInFlightSubscribe(t *testing.T) {
	started := make(chan struct{})
	coordinator := coordinatorStub{
		subscribe: func(ctx context.Context, _ string, _ string, _ storage.QuerySpec, _ engine.InitialWindow, _ int) (*engine.OpenSubscription, error) {
			close(started)
			<-ctx.Done()
			return nil, ctx.Err()
		},
		unsubscribe: func(context.Context, string) error { return nil },
	}
	session, err := New(coordinator).OpenFeed(context.Background(), querysubscription.FeedOptions{BufferLimit: 4})
	if err != nil {
		t.Fatal(err)
	}
	subscribeDone := make(chan error, 1)
	go func() {
		_, _, err := subscribeLocalTest(context.Background(), session, querysubscription.QuerySubscribeRequest{ID: "items", Window: querysubscription.InitialWindow{Count: 1}})
		subscribeDone <- err
	}()
	<-started
	closeCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := session.Close(closeCtx); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := <-subscribeDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("Subscribe error = %v, want context canceled", err)
	}
}

func TestSubscribeStagesLiveDeliveryUntilSnapshotInstalled(t *testing.T) {
	relation := storage.RelationKey{Source: "test", Name: "items"}
	native := make(chan *deliverytest.Interest, 1)
	coordinator := coordinatorStub{
		subscribe: func(_ context.Context, id, group string, _ storage.QuerySpec, _ engine.InitialWindow, limit int) (*engine.OpenSubscription, error) {
			subscription := fixtureInterest(id, group)
			native <- subscription
			return &engine.OpenSubscription{
				Plan: nonComparingPlan{relation: relation}, Subscription: subscription.Subscription,
				Bounds: datastore.Bounds{Left: datastore.Boundary{Kind: datastore.BoundaryStart}, Right: datastore.Boundary{Kind: datastore.BoundaryEnd}},
			}, nil
		},
		unsubscribe: func(context.Context, string) error { return nil },
	}
	feed, err := New(coordinator).OpenFeed(context.Background(), querysubscription.FeedOptions{BufferLimit: 4})
	if err != nil {
		t.Fatal(err)
	}
	defer feed.Close(context.Background())
	installing := make(chan struct{})
	release := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- feed.SubscribeQuery(context.Background(), querysubscription.QuerySubscribeRequest{ID: "items", Window: querysubscription.InitialWindow{Count: 1}}, func(querysubscription.QuerySubscription, querysubscription.QuerySnapshot) error {
			close(installing)
			<-release
			return nil
		})
	}()
	underlying := <-native
	<-installing
	if err := underlying.Queue.Offer(delivery.SubscriptionBatch{
		Delivery: delivery.DeliveryCommit, Checkpoint: &querysubscription.Position{SourceID: "test", Value: 1}, Revision: 1,
	}); err != nil {
		t.Fatal(err)
	}
	blocked, stopBlocked := context.WithTimeout(context.Background(), 20*time.Millisecond)
	if _, err := feed.Next(blocked); !errors.Is(err, context.DeadlineExceeded) {
		stopBlocked()
		t.Fatalf("delivery escaped during snapshot installation: %v", err)
	}
	stopBlocked()
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	event, err := feed.Next(context.Background())
	if err != nil || event.Delivery == nil || event.Delivery.Delivery.Revision != 1 {
		t.Fatalf("staged delivery = %+v, err=%v", event, err)
	}
}

func TestSubscribeDrainsNativeDeliveryWhilePreparingSnapshot(t *testing.T) {
	relation := storage.RelationKey{Source: "test", Name: "items"}
	started := make(chan struct{})
	release := make(chan struct{})
	native := make(chan *deliverytest.Interest, 1)
	plan := blockingSnapshotPlan{nonComparingPlan: nonComparingPlan{relation: relation}, started: started, release: release, once: &sync.Once{}}
	record := storage.Record{"id": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("1")}}
	coordinator := coordinatorStub{
		subscribe: func(_ context.Context, id, group string, _ storage.QuerySpec, _ engine.InitialWindow, limit int) (*engine.OpenSubscription, error) {
			subscription := fixtureInterest(id, group)
			native <- subscription
			return &engine.OpenSubscription{
				Plan: plan, Subscription: subscription.Subscription,
				Initial: []entitycache.EntitySnapshot{{Key: storage.EntityKey{Relation: relation, Value: "1"}, Record: record}},
				Bounds:  datastore.Bounds{Left: datastore.Boundary{Kind: datastore.BoundaryStart}, Right: datastore.Boundary{Kind: datastore.BoundaryEnd}},
			}, nil
		},
		unsubscribe: func(context.Context, string) error { return nil },
	}
	feed, err := New(coordinator).openFeed(context.Background(), querysubscription.FeedOptions{BufferLimit: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer feed.Close(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- feed.SubscribeQuery(context.Background(), querysubscription.QuerySubscribeRequest{ID: "items", Window: querysubscription.InitialWindow{Count: 1}}, func(querysubscription.QuerySubscription, querysubscription.QuerySnapshot) error { return nil })
	}()
	underlying := <-native
	<-started
	if err := underlying.Queue.Offer(delivery.SubscriptionBatch{
		Delivery: delivery.DeliveryProgress, Checkpoint: &querysubscription.Position{SourceID: "test", Value: 1}, Revision: 1,
	}); err != nil {
		t.Fatal(err)
	}
	applied := func() bool { feed.feedMu.Lock(); defer feed.feedMu.Unlock(); return feed.flushedRevision == 1 }
	deadline := time.Now().Add(time.Second)
	for !applied() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if !applied() {
		t.Fatal("native delivery remained queued while snapshot conversion was blocked")
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if event, err := feed.Next(context.Background()); err != nil || event.Progress == nil || event.Progress.Position.Value != 1 {
		t.Fatalf("staged progress = %+v, err=%v", event, err)
	}
}

func TestFailedSnapshotInstallationDoesNotExposeSubscriptionEffects(t *testing.T) {
	relation := storage.RelationKey{Source: "test", Name: "items"}
	native := make(chan *deliverytest.Interest, 1)
	coordinator := coordinatorStub{
		subscribe: func(_ context.Context, id, group string, _ storage.QuerySpec, _ engine.InitialWindow, limit int) (*engine.OpenSubscription, error) {
			subscription := fixtureInterest(id, group)
			native <- subscription
			return &engine.OpenSubscription{
				Plan: nonComparingPlan{relation: relation}, Subscription: subscription.Subscription,
				Bounds: datastore.Bounds{Left: datastore.Boundary{Kind: datastore.BoundaryStart}, Right: datastore.Boundary{Kind: datastore.BoundaryEnd}},
			}, nil
		},
		unsubscribe: func(context.Context, string) error { return nil },
	}
	feed, err := New(coordinator).OpenFeed(context.Background(), querysubscription.FeedOptions{BufferLimit: 4})
	if err != nil {
		t.Fatal(err)
	}
	defer feed.Close(context.Background())
	installing := make(chan struct{})
	release := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- feed.SubscribeQuery(context.Background(), querysubscription.QuerySubscribeRequest{ID: "items", Window: querysubscription.InitialWindow{Count: 1}}, func(querysubscription.QuerySubscription, querysubscription.QuerySnapshot) error {
			close(installing)
			<-release
			return errors.New("installation failed")
		})
	}()
	underlying := <-native
	<-installing
	if err := underlying.Queue.Offer(delivery.SubscriptionBatch{
		Delivery: delivery.DeliveryCommit, Checkpoint: &querysubscription.Position{SourceID: "test", Value: 1}, Revision: 1,
		Events: []delivery.SubscriptionEvent{{Kind: changeprocessor.QueryAdded, Key: storage.EntityKey{Relation: relation, Value: "1"}, Record: storage.Record{"id": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("1")}}}},
	}); err != nil {
		t.Fatal(err)
	}
	close(release)
	if err := <-done; err == nil || err.Error() != "installation failed" {
		t.Fatalf("Subscribe error = %v", err)
	}
	event, err := feed.Next(context.Background())
	if err != nil || event.Delivery == nil || event.Delivery.Delivery.Revision != 1 || len(event.Delivery.Effects) != 0 {
		t.Fatalf("failed installation delivery = %#v, err=%v", event, err)
	}
}
