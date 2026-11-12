package dependency

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	q "github.com/adiom-data/weave/internal/querysubscription/model"
	"github.com/adiom-data/weave/internal/storage"
	"github.com/adiom-data/weave/internal/streamqueue"
)

type testClient struct {
	mu    sync.Mutex
	feeds []*testFeed
	block <-chan struct{}
}

func (f *testFeed) WaitForFence(ctx context.Context, token string) (*q.Position, error) {
	if f.waitFence != nil {
		return f.waitFence(ctx, token)
	}
	return nil, errors.New("unexpected fence wait")
}

func (c *testClient) OpenFeed(ctx context.Context, _ q.FeedOptions) (q.Feed, error) {
	f := &testFeed{ctx: ctx, events: streamqueue.New[q.Event](1024, q.ErrBackpressure, q.ErrClosed), subscriptions: make(map[string]*testSubscription), block: c.block, closedSubscriptions: make(chan string, 1024)}
	c.mu.Lock()
	c.feeds = append(c.feeds, f)
	c.mu.Unlock()
	return f, nil
}
func (c *testClient) latest() *testFeed {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.feeds[len(c.feeds)-1]
}
func (c *testClient) opens() int { c.mu.Lock(); defer c.mu.Unlock(); return len(c.feeds) }

type testFeed struct {
	mu                  sync.Mutex
	ctx                 context.Context
	events              *streamqueue.Queue[q.Event]
	subscriptions       map[string]*testSubscription
	block               <-chan struct{}
	closedSubscriptions chan string
	loads               atomic.Int32
	previous            uint64
	waitFence           func(context.Context, string) (*q.Position, error)
}
type testSubscription struct {
	id      string
	feed    *testFeed
	closed  atomic.Bool
	updates atomic.Int32
}

func (s *testSubscription) ID() string { return s.id }
func (s *testSubscription) Close(context.Context) error {
	if s.closed.CompareAndSwap(false, true) {
		s.feed.closedSubscriptions <- s.id
	}
	return nil
}
func (s *testSubscription) UpdateWindow(context.Context, []q.BoundaryChange, string) error {
	s.updates.Add(1)
	return nil
}
func (f *testFeed) prepare(ctx context.Context, id string) (*testSubscription, error) {
	f.loads.Add(1)
	if f.block != nil {
		select {
		case <-f.block:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	s := &testSubscription{id: id, feed: f}
	f.mu.Lock()
	f.subscriptions[id] = s
	f.mu.Unlock()
	return s, nil
}
func (f *testFeed) SubscribeEntity(ctx context.Context, r q.EntitySubscribeRequest, install q.EntitySnapshotInstaller) error {
	s, err := f.prepare(ctx, r.ID)
	if err != nil {
		return err
	}
	row := testRow("1", "initial")
	return install(s, q.EntitySnapshot{Row: &row, Checkpoint: position(1), Fence: position(1), Revision: 1})
}
func (f *testFeed) SubscribeQuery(ctx context.Context, r q.QuerySubscribeRequest, install q.QuerySnapshotInstaller) error {
	s, err := f.prepare(ctx, r.ID)
	if err != nil {
		return err
	}
	return install(s, q.QuerySnapshot{Rows: []q.QueryRow{{Row: testRow("1", "initial")}}, Checkpoint: position(1), Fence: position(1), Revision: 1, Bounds: q.Bounds{Left: q.Boundary{Kind: q.BoundaryStart}, Right: q.Boundary{Kind: q.BoundaryEnd}}})
}
func (f *testFeed) Next(ctx context.Context) (q.Event, error) { return f.events.Next(ctx) }
func (f *testFeed) RequestProgress(context.Context) error     { f.emit(nil, true); return nil }
func (f *testFeed) Close(context.Context) error               { f.events.Close(q.ErrClosed); return nil }
func (f *testFeed) emit(effects []q.SubscriptionEffect, progress bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	revision := f.previous + 1
	if revision == 1 {
		revision = 2
	}
	var event q.Event
	if progress {
		event.Progress = &q.Progress{Position: position(revision), Revision: revision, PreviousRevision: f.previous}
	} else {
		event.Delivery = &q.DeliveryEnvelope{Delivery: q.DeliveryMetadata{Kind: q.DeliveryCommit, Checkpoint: position(revision), Revision: revision, PreviousRevision: f.previous, Atomic: true}, Effects: effects}
	}
	f.previous = revision
	if err := f.events.Offer(event); err != nil {
		panic(err)
	}
}
func (f *testFeed) ids() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	var ids []string
	for id := range f.subscriptions {
		ids = append(ids, id)
	}
	return ids
}
func position(revision uint64) *q.Position { return &q.Position{SourceID: "test", Value: revision} }
func testRow(id, title string) q.Row {
	return q.Row{Key: storage.EntityKey{Relation: storage.RelationKey{Source: "test", Namespace: "app", Name: "tasks", OpaqueID: "1"}, Value: id}, Record: storage.Record{"title": {State: storage.ValuePresent, Type: storage.TypeText, Data: []byte(title)}}}
}
func query(name string) storage.QuerySpec {
	return storage.QuerySpec{Relation: storage.RelationName{Namespace: "app", Name: name}}
}
func newCache(t *testing.T, source *testClient) *Cache {
	t.Helper()
	c, err := New(context.Background(), source, Options{UpstreamBufferLimit: 1024, MaxSubscriptions: 128, IdleTTL: 20 * time.Millisecond, SweepInterval: 2 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}
func open(t *testing.T, c *Cache, limit int) q.Feed {
	t.Helper()
	f, err := c.OpenFeed(context.Background(), q.FeedOptions{BufferLimit: limit})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { f.Close(context.Background()) })
	return f
}
func next(t *testing.T, f q.Feed) q.Event {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	event, err := f.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return event
}
func subscribe(t *testing.T, f q.Feed, id, name string) (q.EntitySubscription, q.EntitySnapshot) {
	t.Helper()
	var handle q.EntitySubscription
	var snapshot q.EntitySnapshot
	err := f.SubscribeEntity(context.Background(), q.EntitySubscribeRequest{ID: id, Query: query(name)}, func(s q.EntitySubscription, v q.EntitySnapshot) error { handle, snapshot = s, v; return nil })
	if err != nil {
		t.Fatal(err)
	}
	return handle, snapshot
}

func TestCoalescesLoadsAcrossConsumersAndKeepsSnapshotsCallerOwned(t *testing.T) {
	block := make(chan struct{})
	source := &testClient{block: block}
	c := newCache(t, source)
	first, second := open(t, c, 128), open(t, c, 128)
	var workers sync.WaitGroup
	failures := make(chan error, 100)
	for i := 0; i < 100; i++ {
		workers.Add(1)
		go func(i int) {
			defer workers.Done()
			f := first
			if i%2 == 1 {
				f = second
			}
			err := f.SubscribeEntity(context.Background(), q.EntitySubscribeRequest{ID: fmt.Sprint(i), Query: query("projects")}, func(_ q.EntitySubscription, v q.EntitySnapshot) error {
				v.Row.Record["title"].Data[0] = '!'
				return nil
			})
			if err != nil {
				failures <- err
			}
		}(i)
	}
	deadline := time.Now().Add(time.Second)
	for source.latest().loads.Load() != 1 {
		if time.Now().After(deadline) {
			t.Fatal("load did not start")
		}
		time.Sleep(time.Millisecond)
	}
	close(block)
	workers.Wait()
	close(failures)
	for err := range failures {
		t.Fatal(err)
	}
	_, snapshot := subscribe(t, first, "warm", "projects")
	if string(snapshot.Row.Record["title"].Data) != "initial" {
		t.Fatal("snapshot callback mutated shared state")
	}
	if source.opens() != 1 || source.latest().loads.Load() != 1 {
		t.Fatal("duplicate upstream feeds or loads")
	}
}

func TestOneTransactionFansOutAtomicallyWithIndependentOwnedPayloads(t *testing.T) {
	source := &testClient{}
	c := newCache(t, source)
	a, b := open(t, c, 8), open(t, c, 8)
	subscribe(t, a, "project", "projects")
	subscribe(t, a, "task", "tasks")
	subscribe(t, b, "other-project", "projects")
	up := source.latest()
	up.mu.Lock()
	var effects []q.SubscriptionEffect
	for id := range up.subscriptions {
		effects = append(effects, q.SubscriptionEffect{SubscriptionID: id, Batch: q.Batch{Changes: []q.Change{{Kind: q.Updated, Row: testRow("1", "new")}}}})
	}
	up.mu.Unlock()
	up.emit(effects, false)
	left, right := next(t, a), next(t, b)
	if len(left.Delivery.Entities) != 2 || len(right.Delivery.Entities) != 1 {
		t.Fatal("transaction split or fanout lost")
	}
	if !q.SamePosition(left.Delivery.Delivery.Checkpoint, right.Delivery.Delivery.Checkpoint) {
		t.Fatal("transaction checkpoint changed")
	}
	left.Delivery.Entities[0].Delta.Changes[0].Record["title"].Data[0] = '!'
	if string(right.Delivery.Entities[0].Delta.Changes[0].Record["title"].Data) != "new" {
		t.Fatal("consumer payload aliases another consumer")
	}
	_, snapshot := subscribe(t, a, "warm", "projects")
	if string(snapshot.Row.Record["title"].Data) != "new" {
		t.Fatal("warm snapshot not updated")
	}
}

func TestSlowConsumerDoesNotBlockOtherConsumers(t *testing.T) {
	source := &testClient{}
	c := newCache(t, source)
	slow, fast := open(t, c, 1), open(t, c, 8)
	subscribe(t, slow, "slow", "projects")
	subscribe(t, fast, "fast", "projects")
	for i := 0; i < 5; i++ {
		source.latest().emit(nil, false)
		next(t, fast)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := slow.Next(ctx); !errors.Is(err, q.ErrBackpressure) {
		t.Fatalf("slow consumer error=%v", err)
	}
	_, snapshot := subscribe(t, fast, "still-alive", "projects")
	if snapshot.Row == nil {
		t.Fatal("fast consumer failed")
	}
}

func TestBlockedSnapshotInstallerIsConsumerLocal(t *testing.T) {
	source := &testClient{}
	c := newCache(t, source)
	slow, fast := open(t, c, 1), open(t, c, 16)
	subscribe(t, fast, "fast", "projects")
	entered, release, finished := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	go func() {
		finished <- slow.SubscribeEntity(context.Background(), q.EntitySubscribeRequest{ID: "slow", Query: query("projects")}, func(q.EntitySubscription, q.EntitySnapshot) error { close(entered); <-release; return nil })
	}()
	<-entered
	for i := 0; i < 4; i++ {
		source.latest().emit(nil, false)
		next(t, fast)
	}
	close(release)
	if err := <-finished; !errors.Is(err, q.ErrBackpressure) {
		t.Fatalf("blocked consumer error=%v", err)
	}
}

func TestLastReferenceRetainsWarmDependencyThenEvicts(t *testing.T) {
	source := &testClient{}
	c := newCache(t, source)
	f := open(t, c, 8)
	first, _ := subscribe(t, f, "first", "projects")
	second, _ := subscribe(t, f, "second", "projects")
	first.Close(context.Background())
	time.Sleep(25 * time.Millisecond)
	if source.latest().loads.Load() != 1 {
		t.Fatal("unexpected reload")
	}
	select {
	case <-source.latest().closedSubscriptions:
		t.Fatal("shared dependency closed with live reference")
	default:
	}
	second.Close(context.Background())
	third, _ := subscribe(t, f, "third", "projects")
	if source.latest().loads.Load() != 1 {
		t.Fatal("idle dependency was not reused")
	}
	third.Close(context.Background())
	select {
	case <-source.latest().closedSubscriptions:
	case <-time.After(time.Second):
		t.Fatal("idle dependency was not evicted")
	}
}

func TestFixedFactQueriesShareButMutableWindowsRemainIndependent(t *testing.T) {
	source := &testClient{}
	c := newCache(t, source)
	facts, err := c.OpenFactFeed(context.Background(), q.FeedOptions{BufferLimit: 8})
	if err != nil {
		t.Fatal(err)
	}
	defer facts.Close(context.Background())
	f := open(t, c, 8)
	var handles []q.QuerySubscription
	for _, target := range []q.Feed{facts, facts, f, f} {
		id := fmt.Sprint(len(handles))
		err := target.SubscribeQuery(context.Background(), q.QuerySubscribeRequest{ID: id, Query: query("grants"), Window: q.InitialWindow{Count: 10}}, func(s q.QuerySubscription, _ q.QuerySnapshot) error { handles = append(handles, s); return nil })
		if err != nil {
			t.Fatal(err)
		}
	}
	if source.latest().loads.Load() != 3 {
		t.Fatalf("loads=%d want one shared fact and two independent windows", source.latest().loads.Load())
	}
	if err := handles[0].UpdateWindow(context.Background(), nil, "no"); err == nil {
		t.Fatal("fixed fact query allowed window mutation")
	}
	if err := handles[2].UpdateWindow(context.Background(), nil, "yes"); err != nil {
		t.Fatal(err)
	}
	up := source.latest()
	up.mu.Lock()
	updates := int32(0)
	for _, s := range up.subscriptions {
		updates += s.updates.Load()
	}
	up.mu.Unlock()
	if updates != 1 {
		t.Fatal("window update affected multiple subscriptions")
	}
}

func TestFailureReleasesGenerationAndAllowsFreshReconnect(t *testing.T) {
	source := &testClient{}
	c := newCache(t, source)
	f := open(t, c, 8)
	subscribe(t, f, "first", "projects")
	c.mu.Lock()
	generation := c.current
	c.mu.Unlock()
	source.latest().events.Close(errors.New("upstream disconnected"))
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := f.Next(ctx); err == nil {
		t.Fatal("upstream disconnect was ignored")
	}
	select {
	case <-generation.done:
	case <-ctx.Done():
		t.Fatal("failed upstream did not close")
	}
	fresh := open(t, c, 8)
	_, snapshot := subscribe(t, fresh, "first", "projects")
	if source.opens() != 2 || snapshot.Row == nil {
		t.Fatal("reconnect did not use a new feed and snapshot")
	}
}

type failingLoadClient struct {
	testClient
	cause error
}
type failingLoadFeed struct {
	q.Feed
	cause error
}

func (c *failingLoadClient) OpenFeed(ctx context.Context, options q.FeedOptions) (q.Feed, error) {
	feed, err := c.testClient.OpenFeed(ctx, options)
	return &failingLoadFeed{Feed: feed, cause: c.cause}, err
}
func (f *failingLoadFeed) SubscribeQuery(context.Context, q.QuerySubscribeRequest, q.QuerySnapshotInstaller) error {
	return f.cause
}
func (f *failingLoadFeed) SubscribeEntity(context.Context, q.EntitySubscribeRequest, q.EntitySnapshotInstaller) error {
	return f.cause
}

func TestFailedLoadPreservesCauseAfterEntryRetirement(t *testing.T) {
	cause := errors.New("adapter rejected predicate")
	c, err := New(t.Context(), &failingLoadClient{cause: cause}, DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	f := open(t, c, 8)
	for _, entity := range []bool{false, true} {
		var err error
		if entity {
			err = f.SubscribeEntity(t.Context(), q.EntitySubscribeRequest{ID: "entity", Query: query("projects")}, func(q.EntitySubscription, q.EntitySnapshot) error {
				t.Fatal("failed load installed an image")
				return nil
			})
		} else {
			err = f.SubscribeQuery(t.Context(), q.QuerySubscribeRequest{ID: "list", Query: query("projects"), Window: q.InitialWindow{Count: 1}}, func(q.QuerySubscription, q.QuerySnapshot) error {
				t.Fatal("failed load installed an image")
				return nil
			})
		}
		if !errors.Is(err, cause) || errors.Is(err, q.ErrClosed) {
			t.Fatalf("load cause was replaced: %v", err)
		}
	}
}

func TestCancelingOneWaiterDoesNotCancelSharedLoad(t *testing.T) {
	block := make(chan struct{})
	source := &testClient{block: block}
	c := newCache(t, source)
	f := open(t, c, 8)
	ctx, cancel := context.WithCancel(context.Background())
	first, second := make(chan error, 1), make(chan error, 1)
	request := func(ctx context.Context, id string, out chan<- error) {
		out <- f.SubscribeEntity(ctx, q.EntitySubscribeRequest{ID: id, Query: query("projects")}, func(q.EntitySubscription, q.EntitySnapshot) error { return nil })
	}
	go request(ctx, "canceled", first)
	go request(context.Background(), "survivor", second)
	deadline := time.Now().Add(time.Second)
	for {
		c.mu.Lock()
		waiters := 0
		for _, e := range c.current.entries {
			waiters += e.waiters
		}
		c.mu.Unlock()
		if waiters == 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("shared waiters did not attach")
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	if err := <-first; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	close(block)
	if err := <-second; err != nil {
		t.Fatal(err)
	}
	if source.latest().loads.Load() != 1 {
		t.Fatal("canceling one waiter caused a duplicate load")
	}
	if err := f.SubscribeEntity(context.Background(), q.EntitySubscribeRequest{ID: "fresh-attempt", Query: query("projects")}, func(q.EntitySubscription, q.EntitySnapshot) error { return nil }); err != nil {
		t.Fatal("fresh attempt failed after cancellation", err)
	}
}

func TestShutdownCancelsPendingLoads(t *testing.T) {
	block := make(chan struct{})
	source := &testClient{block: block}
	c := newCache(t, source)
	f := open(t, c, 8)
	result := make(chan error, 1)
	go func() {
		result <- f.SubscribeEntity(context.Background(), q.EntitySubscribeRequest{ID: "pending", Query: query("projects")}, func(q.EntitySubscription, q.EntitySnapshot) error { return nil })
	}()
	deadline := time.Now().Add(time.Second)
	for source.latest().loads.Load() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("load did not start")
		}
		time.Sleep(time.Millisecond)
	}
	closed := make(chan error, 1)
	go func() { closed <- c.Close() }()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("cache shutdown blocked on a pending load")
	}
	select {
	case err := <-result:
		if err == nil {
			t.Fatal("pending load succeeded after shutdown")
		}
	case <-time.After(time.Second):
		t.Fatal("pending subscriber did not terminate")
	}
}
