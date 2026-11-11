package querygrpc

import (
	"context"
	"errors"
	"fmt"
	"net"
	"reflect"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/adiom-data/weave/internal/projection"
	"github.com/adiom-data/weave/internal/querysubscription/model"
	"github.com/adiom-data/weave/internal/storage"
	"github.com/adiom-data/weave/internal/streamqueue"
)

type testClient struct {
	session *testFeed
}

func (c *testClient) OpenFeed(context.Context, querysubscription.FeedOptions) (querysubscription.Feed, error) {
	return c.session, nil
}

type testFeed struct {
	mu              sync.Mutex
	subscriptions   map[string]*testSubscription
	beforeSubscribe func(context.Context, querysubscription.QuerySubscribeRequest) error
	beforeClose     func(context.Context) error
	snapshots       func(querysubscription.QuerySubscribeRequest) (querysubscription.QuerySnapshot, error)
	events          *streamqueue.Queue[querysubscription.Event]
	closed          bool
	progress        func(context.Context) error
}

func (s *testFeed) RequestProgress(ctx context.Context) error {
	if s.progress != nil {
		return s.progress(ctx)
	}
	return nil
}

func newTestSession(snapshot func(querysubscription.QuerySubscribeRequest) (querysubscription.QuerySnapshot, error)) *testFeed {
	return &testFeed{subscriptions: make(map[string]*testSubscription), snapshots: snapshot, events: streamqueue.New[querysubscription.Event](1024, querysubscription.ErrBackpressure, querysubscription.ErrClosed)}
}

func (s *testFeed) SubscribeQuery(ctx context.Context, request querysubscription.QuerySubscribeRequest, install querysubscription.QuerySnapshotInstaller) error {
	if s.beforeSubscribe != nil {
		if err := s.beforeSubscribe(ctx, request); err != nil {
			return err
		}
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return querysubscription.ErrClosed
	}
	if _, exists := s.subscriptions[request.ID]; exists {
		s.mu.Unlock()
		return errors.New("duplicate subscription")
	}
	snapshot, err := s.snapshots(request)
	if err != nil {
		s.mu.Unlock()
		return err
	}
	subscription := &testSubscription{id: request.ID, request: request}
	s.subscriptions[request.ID] = subscription
	s.mu.Unlock()
	return install(subscription, snapshot)
}

func (s *testFeed) SubscribeEntity(ctx context.Context, request querysubscription.EntitySubscribeRequest, install querysubscription.EntitySnapshotInstaller) error {
	return s.SubscribeQuery(ctx, querysubscription.QuerySubscribeRequest{ID: request.ID, Query: request.Query, Window: querysubscription.InitialWindow{Count: 1}}, func(subscription querysubscription.QuerySubscription, snapshot querysubscription.QuerySnapshot) error {
		entity := querysubscription.EntitySnapshot{Checkpoint: snapshot.Checkpoint, Fence: snapshot.Fence, Revision: snapshot.Revision}
		if len(snapshot.Rows) > 0 {
			row := snapshot.Rows[0].Row
			row.Cursor = nil
			entity.Row = &row
		}
		return install(testEntitySubscription{Subscription: subscription}, entity)
	})
}

type testEntitySubscription struct{ querysubscription.Subscription }

func (s *testFeed) Next(ctx context.Context) (querysubscription.Event, error) {
	return s.events.Next(ctx)
}

func (s *testFeed) offer(envelope querysubscription.DeliveryEnvelope) error {
	return s.events.Offer(querysubscription.Event{Delivery: &envelope})
}

func (s *testFeed) Close(ctx context.Context) error {
	if s.beforeClose != nil {
		if err := s.beforeClose(ctx); err != nil {
			return err
		}
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	s.mu.Unlock()
	s.events.Close(querysubscription.ErrClosed)
	return nil
}

type testSubscription struct {
	id      string
	request querysubscription.QuerySubscribeRequest

	mu      sync.Mutex
	updates [][]querysubscription.BoundaryChange
	closed  bool
}

func (s *testSubscription) ID() string { return s.id }
func (s *testSubscription) UpdateWindow(_ context.Context, changes []querysubscription.BoundaryChange, _ string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.updates = append(s.updates, append([]querysubscription.BoundaryChange(nil), changes...))
	return nil
}
func (s *testSubscription) Close(context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.closed {
		s.closed = true
	}
	return nil
}

func remoteTestClient(t *testing.T, backend querysubscription.Client) querysubscription.Client {
	return remoteTestClientWithOptions(t, backend, TransportOptions{})
}

func subscribeForTest(ctx context.Context, feed querysubscription.Feed, request querysubscription.QuerySubscribeRequest) (querysubscription.QuerySubscription, querysubscription.QuerySnapshot, error) {
	var subscription querysubscription.QuerySubscription
	var snapshot querysubscription.QuerySnapshot
	err := feed.SubscribeQuery(ctx, request, func(opened querysubscription.QuerySubscription, initial querysubscription.QuerySnapshot) error {
		subscription, snapshot = opened, initial
		return nil
	})
	return subscription, snapshot, err
}

func remoteTestClientWithOptions(t *testing.T, backend querysubscription.Client, options TransportOptions) querysubscription.Client {
	t.Helper()
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	NewServerWithOptions(backend, options).Register(server)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	connection, err := grpc.NewClient("passthrough:///query-subscriptions", grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
		return listener.Dial()
	}), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	return NewClientWithOptions(connection, options)
}

func TestRemoteSessionPreservesContractValuesAndDelivery(t *testing.T) {
	instant := time.Date(12000, time.March, 4, 5, 6, 7, 8, time.FixedZone("offset", 3600))
	key := storage.EntityKey{Relation: storage.RelationKey{Source: "source", Namespace: "tenant", Name: "items", OpaqueID: "relation-1"}, Value: "item-1"}
	row := querysubscription.Row{Key: key, Record: storage.Record{
		"id":     {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("item-1")},
		"when":   storage.NewDateTime(instant),
		"detail": {Type: storage.TypeObject, State: storage.ValuePresent, Object: map[string]storage.Value{"hidden": {State: storage.ValueUnavailable}}},
	}, Cursor: []byte("cursor")}
	var received querysubscription.QuerySubscribeRequest
	backend := newTestSession(func(request querysubscription.QuerySubscribeRequest) (querysubscription.QuerySnapshot, error) {
		received = request
		return querysubscription.QuerySnapshot{Bounds: allBounds(), Rows: []querysubscription.QueryRow{{Row: row}}, Checkpoint: &querysubscription.Position{SourceID: "source", Value: 10}, Fence: &querysubscription.Position{SourceID: "source", Value: 11}, Revision: 3}, nil
	})
	client := remoteTestClientWithOptions(t, &testClient{session: backend}, TransportOptions{ChunkBytes: 32})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	session, err := client.OpenFeed(ctx, querysubscription.FeedOptions{BufferLimit: 8})
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close(context.Background())
	field := storage.MustFieldPath("score")
	literal, _ := storage.LiteralFromValue(storage.Value{Type: storage.TypeInteger, State: storage.ValuePresent, Data: []byte("10")})
	request := querysubscription.QuerySubscribeRequest{ID: "items", Query: storage.QuerySpec{
		Relation: storage.RelationName{Namespace: "tenant", Name: "items"},
		Filter:   &storage.Predicate{Comparison: &storage.Comparison{Field: field, Operator: storage.ComparisonGreater, Value: literal}},
		Order:    []storage.OrderTerm{{Field: field, Descending: true, NullsFirst: true}},
	}, Window: querysubscription.InitialWindow{Count: 10}}
	subscription, snapshot, err := subscribeForTest(ctx, session, request)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(received, request) {
		t.Fatalf("backend request mismatch\n got: %#v\nwant: %#v", received, request)
	}
	if !reflect.DeepEqual(snapshot.Rows, []querysubscription.QueryRow{{Row: row}}) || !querysubscription.SamePosition(snapshot.Fence, &querysubscription.Position{SourceID: "source", Value: 11}) {
		t.Fatalf("snapshot did not round trip: %#v", snapshot)
	}
	backend.mu.Lock()
	native := backend.subscriptions["items"]
	backend.mu.Unlock()
	batch := querysubscription.Batch{Changes: []querysubscription.Change{{Kind: querysubscription.Updated, Row: row}}, Membership: querysubscription.MembershipDelta{Removed: []storage.EntityKey{key}, Insertions: []querysubscription.Insertion{{Key: key, FinalIndex: 0}}}}
	metadata := querysubscription.DeliveryMetadata{Kind: querysubscription.DeliveryCommit, Checkpoint: &querysubscription.Position{SourceID: "source", Value: 12}, Atomic: true, Revision: 4, PreviousRevision: 3}
	if err := backend.offer(querysubscription.DeliveryEnvelope{Delivery: metadata, Effects: []querysubscription.SubscriptionEffect{{SubscriptionID: native.id, Batch: batch}}}); err != nil {
		t.Fatal(err)
	}
	receivedEvent, err := session.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if receivedEvent.Delivery == nil || !reflect.DeepEqual(*receivedEvent.Delivery, (querysubscription.DeliveryEnvelope{Delivery: metadata, Effects: []querysubscription.SubscriptionEffect{{SubscriptionID: native.id, Batch: batch}}})) {
		t.Fatalf("delivery did not round trip: %#v", receivedEvent)
	}
	changes := []querysubscription.BoundaryChange{{Side: querysubscription.BoundaryRight, Operation: querysubscription.BoundaryExtend, Count: 5}}
	if err := subscription.UpdateWindow(ctx, changes, "extend"); err != nil {
		t.Fatal(err)
	}
	native.mu.Lock()
	updates := append([][]querysubscription.BoundaryChange(nil), native.updates...)
	native.mu.Unlock()
	if !reflect.DeepEqual(updates, [][]querysubscription.BoundaryChange{changes}) {
		t.Fatalf("updates = %#v", updates)
	}
	progress := querysubscription.Event{Progress: &querysubscription.Progress{Position: &querysubscription.Position{SourceID: "source", Value: 13}, Revision: 5, PreviousRevision: 4}}
	backend.progress = func(_ context.Context) error {
		return backend.events.Offer(progress)
	}
	if err := session.RequestProgress(ctx); err != nil {
		t.Fatal(err)
	}
	progressEvent, err := session.Next(ctx)
	if err != nil || !reflect.DeepEqual(progressEvent, progress) {
		t.Fatalf("progress event = %#v, %v", progressEvent, err)
	}
}

func TestRemoteInitialWindowPreservesExplicitBounds(t *testing.T) {
	bounds := querysubscription.Bounds{Left: querysubscription.Boundary{Kind: querysubscription.BoundaryCursor, Cursor: []byte("left")}, Right: querysubscription.Boundary{Kind: querysubscription.BoundaryCursor, Cursor: []byte("right")}}
	var received querysubscription.QuerySubscribeRequest
	backend := newTestSession(func(request querysubscription.QuerySubscribeRequest) (querysubscription.QuerySnapshot, error) {
		received = request
		return querysubscription.QuerySnapshot{Bounds: *request.Window.Bounds, Checkpoint: &querysubscription.Position{SourceID: "db", Value: 20}}, nil
	})
	client := remoteTestClientWithOptions(t, &testClient{session: backend}, TransportOptions{ChunkBytes: 32})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	feed, err := client.OpenFeed(ctx, querysubscription.FeedOptions{BufferLimit: 8})
	if err != nil {
		t.Fatal(err)
	}
	defer feed.Close(context.Background())
	request := querysubscription.QuerySubscribeRequest{ID: "restored", Query: storage.QuerySpec{Relation: storage.RelationName{Name: "items"}}, Window: querysubscription.InitialWindow{Bounds: &bounds}}
	_, snapshot, err := subscribeForTest(ctx, feed, request)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(received, request) || !reflect.DeepEqual(snapshot.Bounds, bounds) {
		t.Fatalf("explicit bounds changed across transport: %+v, %+v", received, snapshot)
	}
	request.ID, request.Window.Count = "conflicting", 1
	if _, _, err := subscribeForTest(ctx, feed, request); err == nil {
		t.Fatal("accepted count and explicit bounds together")
	}
}

func TestRemoteEntitySubscriptionRoundTripsWithoutWindowControl(t *testing.T) {
	key := storage.EntityKey{Relation: storage.RelationKey{Source: "source", Name: "items"}, Value: "one"}
	row := querysubscription.Row{Key: key, Record: storage.Record{"id": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("one")}}, Cursor: []byte("private-cursor")}
	backend := newTestSession(func(querysubscription.QuerySubscribeRequest) (querysubscription.QuerySnapshot, error) {
		return querysubscription.QuerySnapshot{Bounds: allBounds(), Rows: []querysubscription.QueryRow{{Row: row}}, Checkpoint: &querysubscription.Position{SourceID: "source", Value: 1}, Revision: 1}, nil
	})
	client := remoteTestClientWithOptions(t, &testClient{session: backend}, TransportOptions{ChunkBytes: 8})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	feed, err := client.OpenFeed(ctx, querysubscription.FeedOptions{BufferLimit: 8})
	if err != nil {
		t.Fatal(err)
	}
	defer feed.Close(context.Background())
	var subscription querysubscription.EntitySubscription
	var snapshot querysubscription.EntitySnapshot
	err = feed.SubscribeEntity(ctx, querysubscription.EntitySubscribeRequest{ID: "one", Query: storage.QuerySpec{Relation: storage.RelationName{Name: "items"}, Order: []storage.OrderTerm{{Field: "id"}}}}, func(opened querysubscription.EntitySubscription, initial querysubscription.EntitySnapshot) error {
		subscription, snapshot = opened, initial
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, mutable := subscription.(querysubscription.QuerySubscription); mutable {
		t.Fatal("remote entity subscription exposed window controls")
	}
	if snapshot.Row == nil || snapshot.Row.Key != key || len(snapshot.Row.Cursor) != 0 {
		t.Fatalf("remote entity snapshot = %+v", snapshot)
	}
}

func TestProgressWithoutKnownPositionIsAcknowledgedWithoutEvent(t *testing.T) {
	backend := newTestSession(nil)
	requests := 0
	backend.progress = func(context.Context) error {
		requests++
		if requests == 1 {
			return nil
		}
		return backend.events.Offer(querysubscription.Event{Progress: &querysubscription.Progress{Position: &querysubscription.Position{SourceID: "test", Value: 10}, Revision: 1}})
	}
	client := remoteTestClient(t, &testClient{session: backend})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	feed, err := client.OpenFeed(ctx, querysubscription.FeedOptions{BufferLimit: 8})
	if err != nil {
		t.Fatal(err)
	}
	defer feed.Close(ctx)
	if err := feed.RequestProgress(ctx); err != nil {
		t.Fatal(err)
	}
	wait, stop := context.WithTimeout(ctx, 10*time.Millisecond)
	defer stop()
	if event, err := feed.Next(wait); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("request fabricated progress: %+v, %v", event, err)
	}
	if err := feed.RequestProgress(ctx); err != nil {
		t.Fatal(err)
	}
	if event, err := feed.Next(ctx); err != nil || event.Progress == nil || event.Progress.Position.Value != 10 || event.Progress.Revision != 1 {
		t.Fatalf("later progress did not recover: %+v, %v", event, err)
	}
}

func TestRemoteTransportPreservesNilAndEmptyRecords(t *testing.T) {
	key := storage.EntityKey{Relation: storage.RelationKey{Source: "test", Name: "items"}, Value: "item"}
	backend := newTestSession(func(querysubscription.QuerySubscribeRequest) (querysubscription.QuerySnapshot, error) {
		return querysubscription.QuerySnapshot{Bounds: allBounds(), Rows: []querysubscription.QueryRow{{Row: querysubscription.Row{Key: key, Record: nil}}, {Row: querysubscription.Row{Key: key, Record: storage.Record{}}}}}, nil
	})
	client := remoteTestClientWithOptions(t, &testClient{session: backend}, TransportOptions{ChunkBytes: 1})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	session, err := client.OpenFeed(ctx, querysubscription.FeedOptions{BufferLimit: 4})
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close(context.Background())
	_, snapshot, err := subscribeForTest(ctx, session, querysubscription.QuerySubscribeRequest{ID: "records", Query: storage.QuerySpec{Relation: storage.RelationName{Name: "items"}}, Window: querysubscription.InitialWindow{Count: 2}})
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Rows[0].Record != nil {
		t.Fatalf("nil record became %#v", snapshot.Rows[0].Record)
	}
	if snapshot.Rows[1].Record == nil || len(snapshot.Rows[1].Record) != 0 {
		t.Fatalf("empty record became %#v", snapshot.Rows[1].Record)
	}
}

func TestRemoteSessionCloseWaitsForBackendAcknowledgement(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	var closeOnce sync.Once
	backend := newTestSession(func(querysubscription.QuerySubscribeRequest) (querysubscription.QuerySnapshot, error) {
		return querysubscription.QuerySnapshot{Bounds: allBounds()}, nil
	})
	backend.beforeClose = func(ctx context.Context) error {
		var err error
		closeOnce.Do(func() {
			close(started)
			select {
			case <-release:
			case <-ctx.Done():
				err = ctx.Err()
			}
		})
		return err
	}
	client := remoteTestClient(t, &testClient{session: backend})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	session, err := client.OpenFeed(ctx, querysubscription.FeedOptions{BufferLimit: 4})
	if err != nil {
		t.Fatal(err)
	}
	closed := make(chan error, 1)
	go func() { closed <- session.Close(ctx) }()
	<-started
	select {
	case err := <-closed:
		t.Fatalf("Close returned before backend cleanup: %v", err)
	default:
	}
	close(release)
	if err := <-closed; err != nil {
		t.Fatal(err)
	}
}

func TestFeedOutputClosureCannotCancelItsOwnCleanup(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	backend := newTestSession(nil)
	closing, outputReturned := make(chan struct{}), make(chan struct{})
	backend.beforeClose = func(cleanupCtx context.Context) error {
		// Model a real feed: output ends before subscription cleanup finishes.
		backend.events.Close(querysubscription.ErrClosed)
		close(closing)
		select {
		case <-outputReturned:
			return cleanupCtx.Err()
		case <-cleanupCtx.Done():
			return cleanupCtx.Err()
		}
	}
	state := &serverFeed{ctx: ctx, feed: backend}
	closed := make(chan error, 1)
	go func() { closed <- state.closeFeed(ctx) }()
	select {
	case <-closing:
	case <-ctx.Done():
		t.Fatal("cleanup did not start")
	}
	if err := state.forwardFeed(); err != nil {
		t.Fatalf("graceful output closure: %v", err)
	}
	close(outputReturned)
	select {
	case err := <-closed:
		if err != nil {
			t.Fatalf("ending feed output canceled cleanup: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("cleanup did not complete")
	}
	if ctx.Err() != nil || !state.released {
		t.Fatal("graceful cleanup lost its live context or release acknowledgment state")
	}
}

func TestUnexpectedFeedClosureTerminatesIdleRPC(t *testing.T) {
	for _, cause := range []error{querysubscription.ErrClosed, querysubscription.ErrBackpressure, errors.New("source failed")} {
		t.Run(cause.Error(), func(t *testing.T) {
			backend := newTestSession(nil)
			client := remoteTestClient(t, &testClient{session: backend})
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			feed, err := client.OpenFeed(ctx, querysubscription.FeedOptions{BufferLimit: 16})
			if err != nil {
				t.Fatal(err)
			}
			backend.events.Close(cause)
			// No further request is sent: terminal feed output must itself end
			// the RPC, rather than leaving the handler waiting in Recv.
			_, err = feed.Next(ctx)
			if err == nil || ctx.Err() != nil {
				t.Fatalf("feed termination did not reach client before deadline: %v", err)
			}
			if errors.Is(cause, querysubscription.ErrBackpressure) && !errors.Is(err, querysubscription.ErrBackpressure) {
				t.Fatalf("transport lost the backpressure classification: %v", err)
			}
		})
	}
}

func TestRemoteFailedSubscribeReleasesAttemptRegistration(t *testing.T) {
	calls := 0
	backend := newTestSession(func(querysubscription.QuerySubscribeRequest) (querysubscription.QuerySnapshot, error) {
		calls++
		return querysubscription.QuerySnapshot{}, errors.New("rejected")
	})
	remote := remoteTestClient(t, &testClient{session: backend})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	session, err := remote.(*client).openFeed(ctx, querysubscription.FeedOptions{BufferLimit: 4})
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close(context.Background())
	request := querysubscription.QuerySubscribeRequest{ID: "failed", Query: storage.QuerySpec{Relation: storage.RelationName{Name: "items"}}, Window: querysubscription.InitialWindow{Count: 1}}
	if _, _, err := subscribeForTest(ctx, session, request); err == nil {
		t.Fatal("first Subscribe succeeded")
	}
	for i := 0; i < 100; i++ {
		request.ID = fmt.Sprintf("retry-%d", i)
		if _, _, err := subscribeForTest(ctx, session, request); err == nil {
			t.Fatal("Subscribe succeeded")
		}
	}
	if calls != 101 || len(session.(*remoteFeed).subscriptions) != 0 {
		t.Fatalf("calls=%d active=%d", calls, len(session.(*remoteFeed).subscriptions))
	}
}

func TestOversizedSnapshotFailsOnlyItsSubscription(t *testing.T) {
	backend := newTestSession(func(request querysubscription.QuerySubscribeRequest) (querysubscription.QuerySnapshot, error) {
		snapshot := querysubscription.QuerySnapshot{Bounds: allBounds()}
		if request.ID == "large" {
			snapshot.Rows = []querysubscription.QueryRow{{Row: querysubscription.Row{
				Key:    storage.EntityKey{Relation: storage.RelationKey{Source: "test", Name: "items"}, Value: "large"},
				Record: storage.Record{"body": {Type: storage.TypeText, State: storage.ValuePresent, Data: make([]byte, 1024)}},
			}}}
		}
		return snapshot, nil
	})
	client := remoteTestClientWithOptions(t, &testClient{session: backend}, TransportOptions{ChunkBytes: 32, MaxPayloadBytes: 256})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	session, err := client.OpenFeed(ctx, querysubscription.FeedOptions{BufferLimit: 4})
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close(context.Background())
	request := func(id string) querysubscription.QuerySubscribeRequest {
		return querysubscription.QuerySubscribeRequest{ID: id, Query: storage.QuerySpec{Relation: storage.RelationName{Name: "items"}}, Window: querysubscription.InitialWindow{Count: 1}}
	}
	if _, _, err := subscribeForTest(ctx, session, request("large")); err == nil {
		t.Fatal("oversized snapshot succeeded")
	}
	healthy, _, err := subscribeForTest(ctx, session, request("small"))
	if err != nil {
		t.Fatalf("session did not survive oversized snapshot: %v", err)
	}
	if err := healthy.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestProjectionRunsAcrossRemoteQuerySession(t *testing.T) {
	projectKey := storage.EntityKey{Relation: storage.RelationKey{Source: "test", Name: "projects"}, Value: "p1"}
	taskKey := storage.EntityKey{Relation: storage.RelationKey{Source: "test", Name: "tasks"}, Value: "t1"}
	backend := newTestSession(func(request querysubscription.QuerySubscribeRequest) (querysubscription.QuerySnapshot, error) {
		snapshot := querysubscription.QuerySnapshot{Bounds: allBounds(), Checkpoint: &querysubscription.Position{SourceID: "test", Value: 1}, Fence: &querysubscription.Position{SourceID: "test", Value: 2}, Revision: 1}
		switch request.Query.Relation.Name {
		case "projects":
			snapshot.Rows = []querysubscription.QueryRow{{Row: querysubscription.Row{Key: projectKey, Record: storage.Record{"id": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("p1")}}, Cursor: []byte("project")}}}
		case "tasks":
			snapshot.Rows = []querysubscription.QueryRow{{Row: querysubscription.Row{Key: taskKey, Record: storage.Record{"id": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("t1")}}, Cursor: []byte("task")}}}
		default:
			return querysubscription.QuerySnapshot{}, errors.New("unexpected relation")
		}
		return snapshot, nil
	})
	client := remoteTestClient(t, &testClient{session: backend})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	session, err := client.OpenFeed(ctx, querysubscription.FeedOptions{BufferLimit: 16})
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close(context.Background())
	root := &projection.ProjectionNode{Name: "projects", BuildQuery: func(storage.Record) (storage.QuerySpec, bool, error) {
		return storage.QuerySpec{Relation: storage.RelationName{Name: "projects"}}, true, nil
	}, Children: []*projection.ProjectionNode{{Name: "tasks", BuildQuery: func(parent storage.Record) (storage.QuerySpec, bool, error) {
		if string(parent["id"].Data) != "p1" {
			return storage.QuerySpec{}, false, errors.New("parent record was not preserved")
		}
		return storage.QuerySpec{Relation: storage.RelationName{Name: "tasks"}}, true, nil
	}}}}
	result, err := projection.BeginProjection(ctx, session, root, projection.StartOptions{Prefix: "remote", QueueLimit: 16})
	if err != nil {
		t.Fatal(err)
	}
	defer result.Close()
	delivered := make(chan projection.ProjectionBatch, 1)
	drainDone := make(chan error, 1)
	go func() {
		for {
			event, nextErr := session.Next(ctx)
			if nextErr != nil {
				drainDone <- nextErr
				return
			}
			batch, ok, applyErr := result.ApplyEvent(event)
			if applyErr != nil {
				drainDone <- applyErr
				return
			}
			if ok {
				delivered <- batch
			}
		}
	}()
	initial, _, err := result.TakeInitial(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var output projection.OutputScheduler[struct{}]
	if err := output.Release(result, struct{}{}, func(item projection.Output[struct{}]) error {
		delivered <- item.Batch
		return item.Err
	}); err != nil {
		t.Fatal(err)
	}
	rows := make(map[string]bool)
	for _, event := range initial {
		if event.Type == projection.ProjectionRowChanged {
			rows[event.Key.Value] = true
		}
	}
	if !rows["p1"] || !rows["t1"] {
		t.Fatalf("remote projection rows = %#v", rows)
	}
	backend.mu.Lock()
	var taskSubscription *testSubscription
	for _, subscription := range backend.subscriptions {
		if subscription.request.Query.Relation.Name == "tasks" {
			taskSubscription = subscription
		}
	}
	backend.mu.Unlock()
	if taskSubscription == nil {
		t.Fatal("task subscription was not opened remotely")
	}
	updated := querysubscription.Row{Key: taskKey, Record: storage.Record{
		"id":    {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("t1")},
		"title": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("updated")},
	}, Cursor: []byte("task")}
	if err := backend.offer(querysubscription.DeliveryEnvelope{Delivery: querysubscription.DeliveryMetadata{Kind: querysubscription.DeliveryCommit, Checkpoint: &querysubscription.Position{SourceID: "test", Value: 8}, Revision: 8}, Effects: []querysubscription.SubscriptionEffect{{SubscriptionID: taskSubscription.id, Batch: querysubscription.Batch{Changes: []querysubscription.Change{{Kind: querysubscription.Updated, Row: updated}}}}}}); err != nil {
		t.Fatal(err)
	}
	select {
	case batch := <-delivered:
		if batch.Delivery.Revision != 8 || len(batch.Events) != 1 || string(batch.Events[0].Record["title"].Data) != "updated" {
			t.Fatalf("remote live batch = %#v", batch)
		}
	case err := <-drainDone:
		t.Fatalf("remote projection ended before live delivery: %v", err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	cancel()
	<-drainDone
}

func TestCanceledSubscribeDoesNotLeakOrCloseRemoteSession(t *testing.T) {
	started := make(chan struct{})
	stopped := make(chan struct{})
	backend := newTestSession(func(querysubscription.QuerySubscribeRequest) (querysubscription.QuerySnapshot, error) {
		return querysubscription.QuerySnapshot{Bounds: allBounds()}, nil
	})
	backend.beforeSubscribe = func(ctx context.Context, request querysubscription.QuerySubscribeRequest) error {
		if request.ID != "blocked" {
			return nil
		}
		close(started)
		<-ctx.Done()
		close(stopped)
		return ctx.Err()
	}
	client := remoteTestClient(t, &testClient{session: backend})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	session, err := client.OpenFeed(ctx, querysubscription.FeedOptions{BufferLimit: 8})
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close(context.Background())
	subscribeCtx, stopSubscribe := context.WithCancel(ctx)
	result := make(chan error, 1)
	go func() {
		_, _, subscribeErr := subscribeForTest(subscribeCtx, session, querysubscription.QuerySubscribeRequest{ID: "blocked", Query: storage.QuerySpec{Relation: storage.RelationName{Name: "items"}}, Window: querysubscription.InitialWindow{Count: 1}})
		result <- subscribeErr
	}()
	<-started
	stopSubscribe()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("subscribe error = %v", err)
	}
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("server did not propagate request cancellation")
	}
	backend.mu.Lock()
	_, leaked := backend.subscriptions["blocked"]
	backend.mu.Unlock()
	if leaked {
		t.Fatal("canceled subscription leaked on server")
	}
	active, _, err := subscribeForTest(ctx, session, querysubscription.QuerySubscribeRequest{ID: "active", Query: storage.QuerySpec{Relation: storage.RelationName{Name: "items"}}, Window: querysubscription.InitialWindow{Count: 1}})
	if err != nil {
		t.Fatalf("session did not survive canceled request: %v", err)
	}
	if err := active.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestRemoteSubscribeStagesDeliveryUntilSnapshotInstalled(t *testing.T) {
	backend := newTestSession(func(querysubscription.QuerySubscribeRequest) (querysubscription.QuerySnapshot, error) {
		return querysubscription.QuerySnapshot{Bounds: allBounds()}, nil
	})
	client := remoteTestClient(t, &testClient{session: backend})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	feed, err := client.OpenFeed(ctx, querysubscription.FeedOptions{BufferLimit: 8})
	if err != nil {
		t.Fatal(err)
	}
	defer feed.Close(context.Background())
	installing := make(chan struct{})
	release := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- feed.SubscribeQuery(ctx, querysubscription.QuerySubscribeRequest{
			ID: "items", Query: storage.QuerySpec{Relation: storage.RelationName{Name: "items"}}, Window: querysubscription.InitialWindow{Count: 1},
		}, func(querysubscription.QuerySubscription, querysubscription.QuerySnapshot) error {
			close(installing)
			<-release
			return nil
		})
	}()
	<-installing
	if err := backend.offer(querysubscription.DeliveryEnvelope{
		Delivery: querysubscription.DeliveryMetadata{Kind: querysubscription.DeliveryCommit, Checkpoint: &querysubscription.Position{SourceID: "test", Value: 1}, Revision: 1},
		Effects:  []querysubscription.SubscriptionEffect{{SubscriptionID: "items"}},
	}); err != nil {
		t.Fatal(err)
	}
	blocked, stopBlocked := context.WithTimeout(ctx, 20*time.Millisecond)
	if _, err := feed.Next(blocked); !errors.Is(err, context.DeadlineExceeded) {
		stopBlocked()
		t.Fatalf("delivery escaped during snapshot installation: %v", err)
	}
	stopBlocked()
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	event, err := feed.Next(ctx)
	if err != nil || event.Delivery == nil || event.Delivery.Delivery.Revision != 1 {
		t.Fatalf("staged delivery = %+v, err=%v", event, err)
	}
}

func TestRemoteFailedSnapshotInstallationDropsLaterSubscriptionEffects(t *testing.T) {
	backend := newTestSession(func(querysubscription.QuerySubscribeRequest) (querysubscription.QuerySnapshot, error) {
		return querysubscription.QuerySnapshot{Bounds: allBounds()}, nil
	})
	client := remoteTestClient(t, &testClient{session: backend})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	feed, err := client.OpenFeed(ctx, querysubscription.FeedOptions{BufferLimit: 8})
	if err != nil {
		t.Fatal(err)
	}
	defer feed.Close(context.Background())
	installErr := errors.New("installation failed")
	err = feed.SubscribeQuery(ctx, querysubscription.QuerySubscribeRequest{
		ID: "items", Query: storage.QuerySpec{Relation: storage.RelationName{Name: "items"}}, Window: querysubscription.InitialWindow{Count: 1},
	}, func(querysubscription.QuerySubscription, querysubscription.QuerySnapshot) error { return installErr })
	if !errors.Is(err, installErr) {
		t.Fatalf("Subscribe error = %v", err)
	}
	if err := backend.offer(querysubscription.DeliveryEnvelope{
		Delivery: querysubscription.DeliveryMetadata{Kind: querysubscription.DeliveryCommit, Checkpoint: &querysubscription.Position{SourceID: "test", Value: 1}, Revision: 1},
		Effects:  []querysubscription.SubscriptionEffect{{SubscriptionID: "items"}},
	}); err != nil {
		t.Fatal(err)
	}
	event, err := feed.Next(ctx)
	if err != nil || event.Delivery == nil || len(event.Delivery.Effects) != 0 {
		t.Fatalf("delivery after rejected snapshot = %+v, err=%v", event, err)
	}
}

func TestRemoteFeedBackpressureClosesTheSession(t *testing.T) {
	backend := newTestSession(func(querysubscription.QuerySubscribeRequest) (querysubscription.QuerySnapshot, error) {
		return querysubscription.QuerySnapshot{Bounds: allBounds()}, nil
	})
	client := remoteTestClient(t, &testClient{session: backend})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	session, err := client.OpenFeed(ctx, querysubscription.FeedOptions{BufferLimit: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close(context.Background())
	request := func(id string) querysubscription.QuerySubscribeRequest {
		return querysubscription.QuerySubscribeRequest{ID: id, Query: storage.QuerySpec{Relation: storage.RelationName{Name: "items"}}, Window: querysubscription.InitialWindow{Count: 1}}
	}
	_, _, err = subscribeForTest(ctx, session, request("slow"))
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = subscribeForTest(ctx, session, request("healthy"))
	if err != nil {
		t.Fatal(err)
	}
	for revision := uint64(1); revision <= 100; revision++ {
		if err := backend.offer(querysubscription.DeliveryEnvelope{Delivery: querysubscription.DeliveryMetadata{Kind: querysubscription.DeliveryCommit, Checkpoint: &querysubscription.Position{SourceID: "test", Value: revision}, Revision: revision, PreviousRevision: revision - 1}}); err != nil {
			t.Fatal(err)
		}
	}
	deadline := time.Now().Add(time.Second)
	for {
		if _, err := session.Next(ctx); errors.Is(err, querysubscription.ErrBackpressure) {
			break
		} else if err != nil {
			t.Fatalf("feed error = %v", err)
		}
		if time.Now().After(deadline) {
			t.Fatal("remote feed did not report backpressure")
		}
	}
}

func allBounds() querysubscription.Bounds {
	return querysubscription.Bounds{Left: querysubscription.Boundary{Kind: querysubscription.BoundaryStart}, Right: querysubscription.Boundary{Kind: querysubscription.BoundaryEnd}}
}
