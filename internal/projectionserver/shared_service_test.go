package projectionserver

import (
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/adiom-data/weave/internal/accesscontrol"
	"github.com/adiom-data/weave/internal/datastore"
	"github.com/adiom-data/weave/internal/engine"
	"github.com/adiom-data/weave/internal/engine/delivery"
	"github.com/adiom-data/weave/internal/engine/deliverytest"
	"github.com/adiom-data/weave/internal/engine/entitycache"
	querylocal "github.com/adiom-data/weave/internal/querysubscription/local"
	"github.com/adiom-data/weave/internal/querysubscription/model"
	sourceposition "github.com/adiom-data/weave/internal/querysubscription/position"
	"github.com/adiom-data/weave/internal/storage"
	reactivev1 "github.com/adiom-data/weave/pkg/gen/reactive/v1"
)

func TestStoragePredicatePushesNegationToLeaves(t *testing.T) {
	comparison := func(column string, operator reactivev1.Comparison_Operator, value string) *reactivev1.Predicate {
		return &reactivev1.Predicate{Expression: &reactivev1.Predicate_Comparison{Comparison: &reactivev1.Comparison{Column: &reactivev1.FieldPath{Segments: []string{column}}, Operator: operator, Value: &reactivev1.Value{Kind: &reactivev1.Value_TextValue{TextValue: value}}}}}
	}
	predicate := &reactivev1.Predicate{Expression: &reactivev1.Predicate_Negation{Negation: &reactivev1.Negation{Operand: &reactivev1.Predicate{
		Expression: &reactivev1.Predicate_Junction{Junction: &reactivev1.Junction{Operator: reactivev1.Junction_AND, Operands: []*reactivev1.Predicate{
			comparison("A", reactivev1.Comparison_EQUAL, "1"),
			&reactivev1.Predicate{Expression: &reactivev1.Predicate_NullTest{NullTest: &reactivev1.NullTest{Column: &reactivev1.FieldPath{Segments: []string{"B"}}, IsNull: true}}},
		}}},
	}}}}
	defaults := DefaultOptions()
	canonical, err := storagePredicateWithLimits(predicate, defaults.MaxPredicateDepth, defaults.MaxPredicateNodes)
	if err != nil {
		t.Fatal(err)
	}
	if canonical.Junction == nil || canonical.Junction.Operator != storage.JunctionOr || len(canonical.Junction.Operands) != 2 {
		t.Fatalf("canonical predicate = %+v", canonical)
	}
	if got := canonical.Junction.Operands[0].Comparison; got == nil || got.Operator != storage.ComparisonNotEqual {
		t.Fatalf("negated comparison = %+v", got)
	}
	if got := canonical.Junction.Operands[1].Null; got == nil || got.IsNull {
		t.Fatalf("negated null test = %+v", got)
	}
}

func TestStoragePredicateUsesConfiguredComplexityLimits(t *testing.T) {
	leaf := &reactivev1.Predicate{Expression: &reactivev1.Predicate_NullTest{NullTest: &reactivev1.NullTest{
		Column: &reactivev1.FieldPath{Segments: []string{"value"}}, IsNull: true,
	}}}
	nested := &reactivev1.Predicate{Expression: &reactivev1.Predicate_Negation{Negation: &reactivev1.Negation{Operand: leaf}}}
	if _, err := storagePredicateWithLimits(nested, 1, 10); err == nil {
		t.Fatal("configured predicate depth limit was ignored")
	}
	if _, err := storagePredicateWithLimits(nested, 10, 1); err == nil {
		t.Fatal("configured predicate node limit was ignored")
	}
}

func TestStorageQueryAppliesSelectedFieldLimitAtReactiveBoundary(t *testing.T) {
	query := &reactivev1.Query{
		Schema:  "tenant",
		Table:   "items",
		Columns: []*reactivev1.FieldPath{{Segments: []string{"id"}}, {Segments: []string{"title"}}},
		Order:   []*reactivev1.OrderTerm{{Column: &reactivev1.FieldPath{Segments: []string{"id"}}}},
	}
	if _, _, err := storageQuery(query, 1, DefaultOptions().MaxPredicateDepth, DefaultOptions().MaxPredicateNodes); err == nil {
		t.Fatal("configured selected-field limit was ignored")
	}
}

func TestRecursiveValueWireRoundTrip(t *testing.T) {
	original := storage.Value{Type: storage.TypeObject, State: storage.ValuePresent, Object: map[string]storage.Value{
		"name": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("task")},
		"at":   storage.NewDateTime(time.Date(2026, 10, 23, 12, 34, 56, 123456789, time.UTC)),
		"items": {Type: storage.TypeList, State: storage.ValuePresent, List: []storage.Value{
			{Type: storage.TypeInteger, State: storage.ValuePresent, Data: []byte("1")},
			{State: storage.ValueNull},
		}},
	}}
	wire, err := protoValue(original)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := storageValue(wire)
	if err != nil {
		t.Fatal(err)
	}
	if !storage.EqualValues(original, decoded) {
		t.Fatalf("round trip = %+v", decoded)
	}
}

func TestDatetimeWireRejectsOutOfRangeTimestamp(t *testing.T) {
	_, err := storageValue(&reactivev1.Value{Kind: &reactivev1.Value_DatetimeValue{
		DatetimeValue: &timestamppb.Timestamp{Seconds: -62135596801},
	}})
	if err == nil {
		t.Fatal("out-of-range protobuf timestamp was accepted")
	}
}

func TestDatetimeWireClampsInternalValuesToProtobufRange(t *testing.T) {
	for _, test := range []struct {
		name  string
		value time.Time
		want  time.Time
	}{
		{name: "minimum", value: time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC), want: minimumProtobufDateTime},
		{name: "maximum", value: time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC), want: maximumProtobufDateTime},
	} {
		t.Run(test.name, func(t *testing.T) {
			wire, err := protoValue(storage.NewDateTime(test.value))
			if err != nil {
				t.Fatal(err)
			}
			if got := wire.GetDatetimeValue().AsTime(); !got.Equal(test.want) {
				t.Fatalf("wire datetime = %s, want %s", got, test.want)
			}
		})
	}
}

func TestStructuredPathDoesNotParseSpecialCharacters(t *testing.T) {
	path, err := storageFieldPath(&reactivev1.FieldPath{Segments: []string{"properties", "a.b", "items[0]"}})
	if err != nil {
		t.Fatal(err)
	}
	segments := path.Segments()
	if len(segments) != 3 || segments[1] != "a.b" || segments[2] != "items[0]" {
		t.Fatalf("segments = %#v", segments)
	}
}

func TestRequestLedgerMakesIdenticalRetriesNoOps(t *testing.T) {
	ledger := newRequestLedger(2)
	request := &reactivev1.ClientMessage{SubscriptionId: "tasks", RequestId: "request-1"}
	accepted, err := ledger.Accept(request)
	if err != nil || !accepted {
		t.Fatalf("first request accepted=%v err=%v", accepted, err)
	}
	accepted, err = ledger.Accept(request)
	if err != nil || accepted {
		t.Fatalf("retry accepted=%v err=%v", accepted, err)
	}
}

func TestRequestLedgerRejectsRequestIDReuseForDifferentPayload(t *testing.T) {
	ledger := newRequestLedger(2)
	_, _ = ledger.Accept(&reactivev1.ClientMessage{SubscriptionId: "tasks", RequestId: "request-1"})
	if _, err := ledger.Accept(&reactivev1.ClientMessage{SubscriptionId: "projects", RequestId: "request-1"}); err == nil {
		t.Fatal("expected request-ID reuse to fail")
	}
}

func TestRequestLedgerIsBounded(t *testing.T) {
	ledger := newRequestLedger(1)
	_, _ = ledger.Accept(&reactivev1.ClientMessage{SubscriptionId: "tasks", RequestId: "request-1"})
	if _, err := ledger.Accept(&reactivev1.ClientMessage{SubscriptionId: "tasks", RequestId: "request-2"}); !errors.Is(err, errRequestHistoryFull) {
		t.Fatalf("history error = %v", err)
	}
}

func TestAuthorizationOnlyCommitProducesClientRemoval(t *testing.T) {
	relation := storage.RelationKey{Source: "test", Namespace: "app", Name: "projects", OpaqueID: "projects"}
	record := modelRecord("project-1", "1", "Project")
	key := modelKey(relation, "project-1")
	queryEngine := &authorizationCommitEngine{
		plan:    modelTestPlan{relation: relation},
		initial: []entitycache.EntitySnapshot{{Key: key, Record: record}},
		opened:  make(chan *deliverytest.Interest, 1),
	}
	controller := &authorizationCommitController{candidate: accesscontrol.Candidate{Key: key, Record: record, Complete: true}}
	service := newServer(querylocal.New(queryEngine), Options{AccessController: controller})
	ctx, cancel := context.WithCancel(context.Background())
	stream := &authorizationCommitStream{
		ctx: ctx, received: make(chan *reactivev1.ClientMessage, 1), sent: make(chan *reactivev1.ServerMessage, 8),
	}
	done := make(chan error, 1)
	go func() { done <- service.Open(stream) }()
	defer func() {
		cancel()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("service did not stop")
		}
	}()

	stream.received <- &reactivev1.ClientMessage{
		SubscriptionId: "projects", RequestId: "subscribe",
		Operation: &reactivev1.ClientMessage_Subscribe{Subscribe: &reactivev1.Subscribe{
			Query: &reactivev1.Query{
				Schema: "app", Table: "projects",
				Columns: []*reactivev1.FieldPath{{Segments: []string{"id"}}, {Segments: []string{"position"}}, {Segments: []string{"title"}}},
				Order:   []*reactivev1.OrderTerm{{Column: &reactivev1.FieldPath{Segments: []string{"id"}}}},
			},
			Window: &reactivev1.InitialWindow{From: reactivev1.InitialWindow_START, Count: 10},
		}},
	}
	snapshot := receiveAuthorizationMessage(t, stream.sent)
	if batch := snapshot.GetModelBatch(); batch == nil || batch.Kind != reactivev1.ModelBatch_SNAPSHOT || len(batch.Entities) != 1 {
		t.Fatalf("snapshot = %#v", snapshot)
	}
	ready := receiveAuthorizationMessage(t, stream.sent)
	if ready.GetReady() == nil {
		t.Fatalf("ready = %#v", ready)
	}

	subscription := <-queryEngine.opened
	if err := subscription.Queue.Offer(delivery.SubscriptionBatch{
		Checkpoint: testCheckpoint("2"), Atomic: true, Revision: 1, Delivery: delivery.DeliveryCommit,
	}); err != nil {
		t.Fatal(err)
	}
	commit := receiveAuthorizationMessage(t, stream.sent)
	batch := commit.GetModelBatch()
	if batch == nil || batch.Kind != reactivev1.ModelBatch_COMMIT || !batch.Atomic || len(batch.Queries) != 1 {
		t.Fatalf("commit = %#v", commit)
	}
	delta := batch.Queries[0].GetDelta()
	if delta == nil || len(delta.RemovedEntityIds) != 1 {
		t.Fatalf("query delta = %#v", batch.Queries[0])
	}
}

func TestProjectionRemainsLiveAfterReady(t *testing.T) {
	relation := storage.RelationKey{Source: "test", Namespace: "app", Name: "projects", OpaqueID: "projects"}
	key := modelKey(relation, "project-1")
	initial := modelRecord("project-1", "1", "Initial")
	queries := newProjectionLifecycleClient(querysubscription.QuerySnapshot{
		Bounds:     querysubscription.Bounds{Left: querysubscription.Boundary{Kind: querysubscription.BoundaryStart}, Right: querysubscription.Boundary{Kind: querysubscription.BoundaryEnd}},
		Rows:       []querysubscription.QueryRow{{Row: querysubscription.Row{Key: key, Record: initial, Cursor: []byte("project-1")}}},
		Checkpoint: sourceposition.FromSource(testCheckpoint("1")),
		Fence:      sourceposition.FromSource(visibilityTestFence{}),
	})
	service := newWithDefaults(queries)
	ctx, cancel := context.WithCancel(context.Background())
	stream := &authorizationCommitStream{ctx: ctx, received: make(chan *reactivev1.ClientMessage, 1), sent: make(chan *reactivev1.ServerMessage, 8)}
	done := make(chan error, 1)
	go func() { done <- service.Open(stream) }()
	defer func() {
		cancel()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("service did not stop")
		}
	}()

	stream.received <- &reactivev1.ClientMessage{
		SubscriptionId: "projection", RequestId: "subscribe",
		Operation: &reactivev1.ClientMessage_SubscribeProjection{SubscribeProjection: &reactivev1.SubscribeProjection{
			Root: &reactivev1.ProjectionNode{NodeId: "projects", Selection: &reactivev1.ProjectionNode_List{List: &reactivev1.ProjectionList{
				Query: &reactivev1.Query{
					Schema: "app", Table: "projects",
					Columns: []*reactivev1.FieldPath{{Segments: []string{"id"}}, {Segments: []string{"title"}}},
					Order:   []*reactivev1.OrderTerm{{Column: &reactivev1.FieldPath{Segments: []string{"id"}}}},
				},
				Window: &reactivev1.InitialWindow{From: reactivev1.InitialWindow_START, Count: 10},
			}}},
		}},
	}
	snapshotMessage := receiveAuthorizationMessage(t, stream.sent)
	if snapshot := snapshotMessage.GetModelBatch(); snapshot == nil || snapshot.Kind != reactivev1.ModelBatch_SNAPSHOT {
		t.Fatalf("projection snapshot message = %v", snapshotMessage)
	}
	if ready := receiveAuthorizationMessage(t, stream.sent).GetReady(); ready == nil {
		t.Fatal("projection did not become ready")
	}

	subscription := <-queries.opened
	updated := modelRecord("project-1", "1", "Updated")
	if err := subscription.offer(querysubscription.DeliveryEnvelope{
		Delivery: querysubscription.DeliveryMetadata{Kind: querysubscription.DeliveryCommit, Checkpoint: sourceposition.FromSource(testCheckpoint("2")), Atomic: true, Revision: 1},
		Effects: []querysubscription.SubscriptionEffect{{SubscriptionID: subscription.id, Batch: querysubscription.Batch{
			Changes: []querysubscription.Change{{Kind: querysubscription.Updated, Row: querysubscription.Row{Key: key, Record: updated, Cursor: []byte("project-1")}}},
		}}},
	}); err != nil {
		t.Fatal(err)
	}
	commit := receiveAuthorizationMessage(t, stream.sent).GetModelBatch()
	if commit == nil || commit.Kind != reactivev1.ModelBatch_COMMIT || len(commit.Entities) != 1 {
		t.Fatalf("projection update = %#v", commit)
	}
}

type projectionLifecycleClient struct {
	snapshot    querysubscription.QuerySnapshot
	snapshotFor func(querysubscription.QuerySubscribeRequest) querysubscription.QuerySnapshot
	opened      chan *projectionLifecycleSubscription
	progress    chan struct{}
}

func newProjectionLifecycleClient(snapshot querysubscription.QuerySnapshot) *projectionLifecycleClient {
	return &projectionLifecycleClient{snapshot: snapshot, opened: make(chan *projectionLifecycleSubscription, 8), progress: make(chan struct{}, 8)}
}

func (c *projectionLifecycleClient) OpenFeed(context.Context, querysubscription.FeedOptions) (querysubscription.Feed, error) {
	return &projectionLifecycleSession{snapshot: c.snapshot, snapshotFor: c.snapshotFor, opened: c.opened, requestedProgress: c.progress, subscriptions: make(map[string]*projectionLifecycleSubscription), events: make(chan querysubscription.Event, 8), closed: make(chan struct{})}, nil
}

type projectionLifecycleSession struct {
	mu                sync.Mutex
	snapshot          querysubscription.QuerySnapshot
	snapshotFor       func(querysubscription.QuerySubscribeRequest) querysubscription.QuerySnapshot
	opened            chan<- *projectionLifecycleSubscription
	requestedProgress chan<- struct{}
	subscriptions     map[string]*projectionLifecycleSubscription
	events            chan querysubscription.Event
	closed            chan struct{}
	closeOnce         sync.Once
}

func (s *projectionLifecycleSession) RequestProgress(context.Context) error {
	s.requestedProgress <- struct{}{}
	return nil
}

func (s *projectionLifecycleSession) SubscribeQuery(_ context.Context, request querysubscription.QuerySubscribeRequest, install querysubscription.QuerySnapshotInstaller) error {
	subscription := &projectionLifecycleSubscription{id: request.ID, owner: s, closed: make(chan struct{})}
	s.mu.Lock()
	s.subscriptions[request.ID] = subscription
	s.mu.Unlock()
	s.opened <- subscription
	snapshot := s.snapshot
	if s.snapshotFor != nil {
		snapshot = s.snapshotFor(request)
	}
	return install(subscription, snapshot)
}

func (s *projectionLifecycleSession) SubscribeEntity(ctx context.Context, request querysubscription.EntitySubscribeRequest, install querysubscription.EntitySnapshotInstaller) error {
	return s.SubscribeQuery(ctx, querysubscription.QuerySubscribeRequest{ID: request.ID, Query: request.Query, Window: querysubscription.InitialWindow{Count: 1}}, func(subscription querysubscription.QuerySubscription, snapshot querysubscription.QuerySnapshot) error {
		entity := querysubscription.EntitySnapshot{Checkpoint: snapshot.Checkpoint, Fence: snapshot.Fence, Revision: snapshot.Revision}
		if len(snapshot.Rows) > 0 {
			row := snapshot.Rows[0].Row
			row.Cursor = nil
			entity.Row = &row
		}
		return install(projectionLifecycleEntitySubscription{Subscription: subscription}, entity)
	})
}

type projectionLifecycleEntitySubscription struct{ querysubscription.Subscription }

func (s *projectionLifecycleSession) Next(ctx context.Context) (querysubscription.Event, error) {
	select {
	case event := <-s.events:
		return event, nil
	case <-s.closed:
		return querysubscription.Event{}, querysubscription.ErrClosed
	case <-ctx.Done():
		return querysubscription.Event{}, ctx.Err()
	}
}

func (s *projectionLifecycleSession) Close(context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, subscription := range s.subscriptions {
		_ = subscription.Close(context.Background())
	}
	s.closeOnce.Do(func() { close(s.closed) })
	return nil
}

type projectionLifecycleSubscription struct {
	id        string
	owner     *projectionLifecycleSession
	closed    chan struct{}
	closeOnce sync.Once
}

func (s *projectionLifecycleSubscription) ID() string { return s.id }

func (*projectionLifecycleSubscription) UpdateWindow(context.Context, []querysubscription.BoundaryChange, string) error {
	return nil
}

func (s *projectionLifecycleSubscription) Close(context.Context) error {
	s.closeOnce.Do(func() { close(s.closed) })
	return nil
}

func (s *projectionLifecycleSubscription) offer(envelope querysubscription.DeliveryEnvelope) error {
	select {
	case s.owner.events <- querysubscription.Event{Delivery: &envelope}:
		return nil
	case <-s.closed:
		return querysubscription.ErrClosed
	}
}

func receiveAuthorizationMessage(t *testing.T, messages <-chan *reactivev1.ServerMessage) *reactivev1.ServerMessage {
	t.Helper()
	select {
	case message := <-messages:
		return message
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for server message")
		return nil
	}
}

type authorizationCommitStream struct {
	ctx      context.Context
	received chan *reactivev1.ClientMessage
	sent     chan *reactivev1.ServerMessage
}

func (s *authorizationCommitStream) Send(message *reactivev1.ServerMessage) error {
	select {
	case s.sent <- message:
		return nil
	case <-s.ctx.Done():
		return s.ctx.Err()
	}
}

func (s *authorizationCommitStream) Recv() (*reactivev1.ClientMessage, error) {
	select {
	case message, ok := <-s.received:
		if !ok {
			return nil, io.EOF
		}
		return message, nil
	case <-s.ctx.Done():
		return nil, s.ctx.Err()
	}
}

func (*authorizationCommitStream) SetHeader(metadata.MD) error  { return nil }
func (*authorizationCommitStream) SendHeader(metadata.MD) error { return nil }
func (*authorizationCommitStream) SetTrailer(metadata.MD)       {}
func (s *authorizationCommitStream) Context() context.Context   { return s.ctx }
func (s *authorizationCommitStream) SendMsg(message any) error {
	return s.Send(message.(*reactivev1.ServerMessage))
}
func (s *authorizationCommitStream) RecvMsg(message any) error {
	received, err := s.Recv()
	if err != nil {
		return err
	}
	target := message.(*reactivev1.ClientMessage)
	proto.Reset(target)
	proto.Merge(target, received)
	return nil
}

type authorizationCommitEngine struct {
	plan       datastore.PreparedQuery
	initial    []entitycache.EntitySnapshot
	opened     chan *deliverytest.Interest
	groupQueue *delivery.ClientQueue[delivery.GroupMessage]
}

func (e *authorizationCommitEngine) OpenGroup(ctx context.Context, _ string, limit int) (*delivery.ClientQueue[delivery.GroupMessage], error) {
	e.groupQueue = delivery.NewClientQueue[delivery.GroupMessage](limit)
	return e.groupQueue, nil
}

func (e *authorizationCommitEngine) SubscribeEntities(ctx context.Context, group string, requests []engine.EntitySubscribe, install func(int, engine.SubscribeResult)) {
	for i, request := range requests {
		opened, err := e.SubscribeGroupedWithFetch(ctx, request.ID, group, request.Query, engine.InitialWindow{Count: 1}, engine.FetchOptions{Entity: true, From: request.From})
		install(i, engine.SubscribeResult{Opened: opened, Err: err})
	}
}

func (*authorizationCommitEngine) ReportProgress(context.Context, string) (*delivery.ProgressReport, error) {
	return nil, nil
}

func testStorageBounds() datastore.Bounds {
	return datastore.Bounds{Left: datastore.Boundary{Kind: datastore.BoundaryStart}, Right: datastore.Boundary{Kind: datastore.BoundaryEnd}}
}

func (e *authorizationCommitEngine) SubscribeGroupedWithFetch(ctx context.Context, id, group string, _ storage.QuerySpec, _ engine.InitialWindow, _ engine.FetchOptions) (*engine.OpenSubscription, error) {
	subscription := deliverytest.New(&delivery.Subscription{ID: id, Group: group, Query: e.plan.Fingerprint()}, e.groupQueue)
	e.opened <- subscription
	return &engine.OpenSubscription{
		Subscription: subscription.Subscription, Plan: e.plan, Bounds: testStorageBounds(), Initial: e.initial,
		Checkpoint: testCheckpoint("1"),
	}, nil
}

func (*authorizationCommitEngine) QueueWindowUpdate(context.Context, string, []engine.BoundaryChange, string) (datastore.Bounds, error) {
	return datastore.Bounds{}, errors.New("unexpected window update")
}
func (*authorizationCommitEngine) Unsubscribe(context.Context, string) (uint64, error) { return 0, nil }
func (*authorizationCommitEngine) ReleaseGroup(context.Context, string) error          { return nil }

type authorizationCommitController struct{ candidate accesscontrol.Candidate }

func (c *authorizationCommitController) Prepare(_ context.Context, request accesscontrol.PrepareRequest) (accesscontrol.Prepared, error) {
	return &authorizationCommitPrepared{id: request.ID}, nil
}
func (c *authorizationCommitController) Synchronize(_ context.Context, _ accesscontrol.PreparedID, position storage.Checkpoint) (accesscontrol.Checkpoint, error) {
	allowed := position.StableID() != "2"
	checkpoint := accesscontrol.Checkpoint{Position: position, View: authorizationCommitView{allowed: allowed}}
	if !allowed {
		checkpoint.Invalidation.Candidates = []accesscontrol.Candidate{c.candidate}
	}
	return checkpoint, nil
}
func (*authorizationCommitController) AcquireSnapshot(_ context.Context, fence storage.Fence) (accesscontrol.SnapshotCheckpoint, error) {
	return accesscontrol.SnapshotCheckpoint{Fence: fence, View: authorizationCommitView{allowed: true}}, nil
}

type authorizationCommitView struct{ allowed bool }

func (authorizationCommitView) Close() error { return nil }

type authorizationCommitPrepared struct{ id accesscontrol.PreparedID }

func (p *authorizationCommitPrepared) ID() accesscontrol.PreparedID { return p.id }
func (*authorizationCommitPrepared) Evaluate(_ context.Context, view accesscontrol.View, candidates []accesscontrol.Candidate) ([]bool, error) {
	allowed := view.(authorizationCommitView).allowed
	result := make([]bool, len(candidates))
	for index := range result {
		result[index] = allowed
	}
	return result, nil
}
func (*authorizationCommitPrepared) Close() error { return nil }
func (*authorizationCommitPrepared) Release(context.Context, string, []storage.EntityKey) error {
	return nil
}
