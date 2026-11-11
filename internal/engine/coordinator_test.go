package engine

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/adiom-data/weave/internal/datastore"
	"github.com/adiom-data/weave/internal/engine/changeprocessor"
	"github.com/adiom-data/weave/internal/engine/delivery"
	"github.com/adiom-data/weave/internal/engine/entitycache"
	"github.com/adiom-data/weave/internal/engine/routing"
	"github.com/adiom-data/weave/internal/storage"
)

type testPosition int

// Subscribe is test shorthand for the production grouped subscription path.
func (c *Coordinator) Subscribe(ctx context.Context, id string, spec storage.QuerySpec, initial InitialWindow, queueLimit int) (*OpenSubscription, error) {
	queue, err := c.OpenGroup(ctx, "test:"+id, queueLimit)
	if err != nil {
		return nil, err
	}
	opened, err := c.SubscribeGrouped(ctx, id, "test:"+id, spec, initial)
	if err == nil {
		testSubscriptionQueues.Store(opened.Subscription, queue)
	}
	return opened, err
}

func (c *Coordinator) queryEntities(fingerprint datastore.QueryFingerprint) []entitycache.EntitySnapshot {
	keys := c.queries.Members(fingerprint)
	result := make([]entitycache.EntitySnapshot, 0, len(keys))
	for _, key := range keys {
		if entity, ok := c.entities.Get(key); ok && entity.State == entitycache.EntityCurrent {
			result = append(result, entity)
		}
	}
	return result
}

func (testPosition) StorageFence()            {}
func (testPosition) StorageCheckpoint()       {}
func (p testPosition) StableID() string       { return fmt.Sprint(int(p)) }
func (testPosition) Source() storage.SourceID { return "test" }
func (p testPosition) Order() uint64          { return uint64(p) }

type foreignPosition int

func (foreignPosition) StorageFence()            {}
func (foreignPosition) StorageCheckpoint()       {}
func (p foreignPosition) StableID() string       { return fmt.Sprint(int(p)) }
func (foreignPosition) Source() storage.SourceID { return "foreign" }
func (p foreignPosition) Order() uint64          { return uint64(p) }

type coordinatorFeed struct {
	events chan datastore.CommittedChanges
	mu     sync.RWMutex
	pos    testPosition
	acked  testPosition
}

func newCoordinatorFeed() *coordinatorFeed {
	feed := &coordinatorFeed{events: make(chan datastore.CommittedChanges, 16)}
	// Snapshot fences at zero need an explicit consumed baseline too. Merely
	// initializing the adapter's receive counter to zero is not applied progress.
	feed.events <- datastore.CommittedChanges{Checkpoint: testPosition(0)}
	return feed
}

func (f *coordinatorFeed) Receive(ctx context.Context) (datastore.ChangeEvent, error) {
	select {
	case batch, ok := <-f.events:
		if !ok {
			return datastore.ChangeEvent{}, io.EOF
		}
		f.mu.Lock()
		if position, ok := batch.Checkpoint.(testPosition); ok {
			f.pos = position
		}
		f.mu.Unlock()
		return datastore.TransactionEvent(batch), nil
	case <-ctx.Done():
		return datastore.ChangeEvent{}, ctx.Err()
	}
}

func (f *coordinatorFeed) Covers(fence storage.Fence) bool {
	target := fence.(testPosition)
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.pos >= target
}
func (f *coordinatorFeed) Ack(_ context.Context, checkpoint storage.Checkpoint) error {
	position, ok := checkpoint.(testPosition)
	if !ok {
		return errors.New("unexpected checkpoint type")
	}
	f.mu.Lock()
	f.acked = position
	f.mu.Unlock()
	return nil
}
func (f *coordinatorFeed) acknowledged() testPosition {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.acked
}
func (*coordinatorFeed) Close() error { return nil }

func TestCoordinatorStopsOnFatalSourceErrorWhileActivationWaits(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	relation := storage.RelationKey{Source: "test", Name: "tasks"}
	feed := newCoordinatorFeed()
	source := &coordinatorSource{
		plan: fakePlan{fingerprint: "all", relation: relation, routing: routing.RoutingPlan{Fallback: true}},
		records: []storage.Record{
			{"id": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("1")}},
		},
		feed: feed, fence: testPosition(1),
	}
	coordinator, err := StartCoordinator(ctx, source)
	if err != nil {
		t.Fatal(err)
	}
	opened := make(chan error, 1)
	go func() {
		_, err := coordinator.Subscribe(ctx, "viewer", storage.QuerySpec{}, InitialWindow{Count: 1}, 4)
		opened <- err
	}()
	feed.events <- datastore.CommittedChanges{Checkpoint: foreignPosition(1)}
	if err := <-opened; !errors.Is(err, changeprocessor.ErrFatalSource) {
		t.Fatalf("subscription error = %v, want fatal source error", err)
	}
	if !coordinator.hasTerminalError() {
		t.Fatal("fatal feed error did not stop coordinator")
	}
}

func TestCoordinatorStopsOnMalformedBatchWhileActivationWaits(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	relation := storage.RelationKey{Source: "test", Name: "tasks"}
	feed := newCoordinatorFeed()
	source := &coordinatorSource{
		plan:    fakePlan{fingerprint: "all", relation: relation, routing: routing.RoutingPlan{Fallback: true}},
		records: []storage.Record{{"id": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("1")}}},
		feed:    feed,
		fence:   testPosition(1),
	}
	coordinator, err := StartCoordinator(ctx, source)
	if err != nil {
		t.Fatal(err)
	}
	opened := make(chan error, 1)
	go func() {
		_, err := coordinator.Subscribe(ctx, "viewer", storage.QuerySpec{}, InitialWindow{Count: 1}, 4)
		opened <- err
	}()
	feed.events <- datastore.CommittedChanges{Checkpoint: testPosition(1), Changes: []datastore.Change{nil}}
	if err := <-opened; !errors.Is(err, changeprocessor.ErrFatalSource) {
		t.Fatalf("subscription error = %v, want fatal source error", err)
	}
	if !coordinator.hasTerminalError() {
		t.Fatal("malformed feed batch did not stop coordinator")
	}
}

type coordinatorSource struct {
	plan             fakePlan
	records          []storage.Record
	feed             *coordinatorFeed
	fence            testPosition
	loadStarted      chan struct{}
	loadRelease      <-chan struct{}
	loadCanceled     chan<- struct{}
	snapshotErr      error
	queryErrors      map[datastore.QueryFingerprint]error
	windowReads      chan<- datastore.WindowRequest
	windowKeysLoaded func()
	snapshotBegins   *int
}

type unkeyedChange struct{ relation storage.RelationKey }

func (c unkeyedChange) Kind() datastore.ChangeKind    { return datastore.ChangeInsert }
func (c unkeyedChange) Relation() storage.RelationKey { return c.relation }
func (c unkeyedChange) BeforeKey() (storage.EntityKey, error) {
	return storage.EntityKey{Relation: c.relation}, nil
}
func (c unkeyedChange) Apply(storage.Record) (datastore.AppliedChange, error) {
	return datastore.AppliedChange{Kind: datastore.AppliedComplete, Key: storage.EntityKey{Relation: c.relation}, Record: storage.Record{}}, nil
}

type progressCoordinatorSource struct {
	*coordinatorSource
	mu        sync.Mutex
	requests  int
	requestFn func(context.Context, storage.Fence) error
}

func (s *progressCoordinatorSource) RequestProgress(ctx context.Context, fence storage.Fence) error {
	s.mu.Lock()
	s.requests++
	s.mu.Unlock()
	if s.requestFn != nil {
		return s.requestFn(ctx, fence)
	}
	return nil
}

func (s *progressCoordinatorSource) requestCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.requests
}

func (s *coordinatorSource) ID() storage.SourceID { return "test" }
func (s *coordinatorSource) PrepareQuery(context.Context, storage.QuerySpec) (datastore.PreparedQuery, error) {
	return s.plan, nil
}
func (s *coordinatorSource) BeginSnapshot(_ context.Context, query datastore.PreparedQuery) (datastore.SnapshotSession, error) {
	if s.snapshotErr != nil {
		return nil, s.snapshotErr
	}
	if s.snapshotBegins != nil {
		*s.snapshotBegins++
	}
	return &coordinatorSnapshot{relation: query.Relation(), records: s.records, fence: s.fence, loadStarted: s.loadStarted, loadRelease: s.loadRelease, loadCanceled: s.loadCanceled, queryErrors: s.queryErrors, windowReads: s.windowReads, windowKeysLoaded: s.windowKeysLoaded}, nil
}
func (s *coordinatorSource) OpenFeed(context.Context) (datastore.ChangeFeed, error) {
	return s.feed, nil
}

func TestCoordinatorIgnoresChangesForRelationsWithoutLiveQueries(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	liveRelation := storage.RelationKey{Source: "test", Name: "tasks"}
	feed := newCoordinatorFeed()
	source := &coordinatorSource{
		plan:    fakePlan{fingerprint: "all", relation: liveRelation, routing: routing.RoutingPlan{Fallback: true}},
		records: []storage.Record{{"id": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("1")}}},
		feed:    feed, fence: testPosition(1),
	}
	coordinator, err := StartCoordinator(ctx, source)
	if err != nil {
		t.Fatal(err)
	}
	feed.events <- datastore.CommittedChanges{
		Checkpoint: testPosition(1), Atomic: true,
		Changes: []datastore.Change{unkeyedChange{relation: storage.RelationKey{Source: "test", Name: "unrelated_without_key"}}},
	}
	if _, err := coordinator.Subscribe(ctx, "after-unrelated", storage.QuerySpec{}, InitialWindow{Count: 1}, 4); err != nil {
		t.Fatalf("irrelevant unkeyed relation stopped coordinator: %v", err)
	}
}

func TestCoordinatorUsesNaturalProgressBeforeRequestingBarrier(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	relation := storage.RelationKey{Source: "test", Name: "tasks"}
	plan := fakePlan{fingerprint: "all", relation: relation, routing: routing.RoutingPlan{Fallback: true}}
	feed := newCoordinatorFeed()
	started := make(chan struct{}, 1)
	base := &coordinatorSource{
		plan:    plan,
		records: []storage.Record{{"id": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("1")}}},
		feed:    feed, fence: testPosition(1), loadStarted: started,
	}
	source := &progressCoordinatorSource{coordinatorSource: base}
	coordinator, err := StartCoordinatorWithOptions(ctx, source, CoordinatorOptions{Limits: CoordinatorLimits{ProgressBarrierDelay: 100 * time.Millisecond}})
	if err != nil {
		t.Fatal(err)
	}
	opened := make(chan error, 1)
	go func() {
		_, err := coordinator.Subscribe(ctx, "natural", storage.QuerySpec{}, InitialWindow{Count: 1}, 4)
		opened <- err
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("snapshot did not start")
	}
	feed.events <- datastore.CommittedChanges{Checkpoint: testPosition(1)}
	if err := <-opened; err != nil {
		t.Fatal(err)
	}
	if count := source.requestCount(); count != 0 {
		t.Fatalf("requested %d barriers despite natural progress", count)
	}
}

func TestCoordinatorReportsKnownProgressWithoutRequestingBarrier(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	relation := storage.RelationKey{Source: "test", Name: "tasks"}
	feed := newCoordinatorFeed()
	started := make(chan struct{}, 1)
	base := &coordinatorSource{
		plan:    fakePlan{fingerprint: "all", relation: relation, routing: routing.RoutingPlan{Fallback: true}},
		records: []storage.Record{{"id": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("1")}}},
		feed:    feed, fence: testPosition(1), loadStarted: started,
	}
	source := &progressCoordinatorSource{coordinatorSource: base}
	coordinator, err := StartCoordinatorWithOptions(ctx, source, CoordinatorOptions{Limits: CoordinatorLimits{ProgressBarrierDelay: time.Second}})
	if err != nil {
		t.Fatal(err)
	}
	openedResult := make(chan *OpenSubscription, 1)
	openedError := make(chan error, 1)
	go func() {
		queue, _ := coordinator.OpenGroup(ctx, "connection", 4)
		opened, openErr := coordinator.SubscribeGrouped(ctx, "viewer", "connection", storage.QuerySpec{}, InitialWindow{Count: 1})
		if opened != nil {
			testSubscriptionQueues.Store(opened.Subscription, queue)
		}
		openedResult <- opened
		openedError <- openErr
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("snapshot did not start")
	}
	feed.events <- datastore.CommittedChanges{Checkpoint: testPosition(1)}
	opened := <-openedResult
	if err := <-openedError; err != nil {
		t.Fatal(err)
	}
	marker, err := coordinator.ReportProgress(ctx, "connection")
	if err != nil {
		t.Fatal(err)
	}
	if marker.Checkpoint != opened.Checkpoint || marker.Revision <= opened.Revision {
		t.Fatalf("progress report = %+v", marker)
	}
	if count := source.requestCount(); count != 0 {
		t.Fatalf("progress report requested %d datastore barriers", count)
	}
}

func TestCoordinatorReturnsAcceptedProgressDespiteCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	coordinator := &Coordinator{
		commands: make(chan coordinatorCommand), done: make(chan struct{}),
		hub: delivery.NewSubscriptionHub(nil), lastCheckpoint: testPosition(42),
	}
	go func() {
		command := <-coordinator.commands
		cancel()
		command.run()
	}()
	report, err := coordinator.ReportProgress(ctx, "connection")
	if err != nil || report.Checkpoint != testPosition(42) || report.Revision != 1 {
		t.Fatalf("accepted progress lost after cancellation: %+v, %v", report, err)
	}
}

func TestCoordinatorWithoutPositionDoesNotAllocateProgress(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	coordinator := &Coordinator{commands: make(chan coordinatorCommand), done: make(chan struct{}), hub: delivery.NewSubscriptionHub(nil)}
	go func() {
		for index := range 2 {
			select {
			case command := <-coordinator.commands:
				if index == 1 {
					coordinator.lastCheckpoint = testPosition(42)
				}
				command.run()
			case <-ctx.Done():
				return
			}
		}
	}()
	if report, err := coordinator.ReportProgress(ctx, "connection"); err != nil || report != nil {
		t.Fatalf("unknown progress = %+v, %v", report, err)
	}
	report, err := coordinator.ReportProgress(ctx, "connection")
	if err != nil || report == nil || report.Checkpoint != testPosition(42) || report.Revision != 1 || report.PreviousGroupRevision != 0 {
		t.Fatalf("first real progress = %+v, %v", report, err)
	}
}

func TestCoordinatorRequestsOneBarrierForQuietFence(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	relation := storage.RelationKey{Source: "test", Name: "tasks"}
	plan := fakePlan{fingerprint: "all", relation: relation, routing: routing.RoutingPlan{Fallback: true}}
	feed := newCoordinatorFeed()
	base := &coordinatorSource{
		plan:    plan,
		records: []storage.Record{{"id": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("1")}}},
		feed:    feed, fence: testPosition(1),
	}
	source := &progressCoordinatorSource{coordinatorSource: base}
	source.requestFn = func(_ context.Context, fence storage.Fence) error {
		feed.events <- datastore.CommittedChanges{Checkpoint: fence.(testPosition)}
		return nil
	}
	coordinator, err := StartCoordinatorWithOptions(ctx, source, CoordinatorOptions{Limits: CoordinatorLimits{ProgressBarrierDelay: time.Millisecond}})
	if err != nil {
		t.Fatal(err)
	}
	opened, err := coordinator.Subscribe(ctx, "quiet", storage.QuerySpec{}, InitialWindow{Count: 1}, 4)
	if err != nil {
		t.Fatal(err)
	}
	if opened.Checkpoint != testPosition(1) {
		t.Fatalf("materialization checkpoint = %v, want 1", opened.Checkpoint)
	}
	if count := source.requestCount(); count != 1 {
		t.Fatalf("requested %d barriers, want 1", count)
	}
}

func TestCoordinatorProcessesCommandsAndCDCWhileActivationWaitsForProgress(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	relation := storage.RelationKey{Source: "test", Name: "tasks"}
	record := func(title string) storage.Record {
		return storage.Record{
			"id":    {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("1")},
			"title": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte(title)},
		}
	}
	feed := newCoordinatorFeed()
	base := &coordinatorSource{plan: fakePlan{fingerprint: "first", relation: relation, routing: routing.RoutingPlan{Fallback: true}}, records: []storage.Record{record("old")}, feed: feed}
	requested := make(chan struct{})
	source := &progressCoordinatorSource{coordinatorSource: base}
	source.requestFn = func(ctx context.Context, _ storage.Fence) error {
		close(requested)
		<-ctx.Done()
		return ctx.Err()
	}
	coordinator, err := StartCoordinatorWithOptions(ctx, source, CoordinatorOptions{Limits: CoordinatorLimits{ProgressBarrierDelay: time.Millisecond}})
	if err != nil {
		t.Fatal(err)
	}
	_, err = coordinator.Subscribe(ctx, "first", storage.QuerySpec{}, InitialWindow{Count: 1}, 8)
	if err != nil {
		t.Fatal(err)
	}
	base.plan = fakePlan{fingerprint: "second", relation: relation, routing: routing.RoutingPlan{Fallback: true}}
	base.fence = testPosition(2)
	opened := make(chan error, 1)
	go func() {
		_, err := coordinator.Subscribe(ctx, "second", storage.QuerySpec{}, InitialWindow{Count: 1}, 8)
		opened <- err
	}()
	select {
	case <-requested:
	case <-time.After(time.Second):
		t.Fatal("activation did not request progress")
	}
	commandCtx, stopCommand := context.WithTimeout(ctx, time.Second)
	defer stopCommand()
	if _, err := coordinator.Unsubscribe(commandCtx, "first"); err != nil {
		t.Fatalf("unrelated command was blocked by activation: %v", err)
	}
	key := storage.EntityKey{Relation: relation, Value: "1"}
	feed.events <- datastore.CommittedChanges{Checkpoint: testPosition(2), Changes: []datastore.Change{
		fakeChange{kind: datastore.ChangeUpdate, relation: relation, before: key, after: key, record: record("new"), complete: true},
	}}
	select {
	case err := <-opened:
		if err != nil {
			t.Fatalf("subscription did not finish from natural CDC progress: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("CDC processing was blocked by the pending activation")
	}
}

func TestCoordinatorBarrierFailureDoesNotStopCoordinator(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	relation := storage.RelationKey{Source: "test", Name: "tasks"}
	plan := fakePlan{fingerprint: "all", relation: relation, routing: routing.RoutingPlan{Fallback: true}}
	feed := newCoordinatorFeed()
	base := &coordinatorSource{
		plan:    plan,
		records: []storage.Record{{"id": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("1")}}},
		feed:    feed, fence: testPosition(1),
	}
	barrierErr := errors.New("barrier unavailable")
	source := &progressCoordinatorSource{coordinatorSource: base, requestFn: func(context.Context, storage.Fence) error { return barrierErr }}
	coordinator, err := StartCoordinatorWithOptions(ctx, source, CoordinatorOptions{Limits: CoordinatorLimits{ProgressBarrierDelay: time.Millisecond}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := coordinator.Subscribe(ctx, "fails", storage.QuerySpec{}, InitialWindow{Count: 1}, 4); !errors.Is(err, barrierErr) {
		t.Fatalf("subscription error = %v, want %v", err, barrierErr)
	}
	base.fence = 0
	if _, err := coordinator.Subscribe(ctx, "still-running", storage.QuerySpec{}, InitialWindow{Count: 1}, 4); err != nil {
		t.Fatalf("coordinator stopped after isolated barrier failure: %v", err)
	}
}

func TestCoordinatorBarrierTimerSurvivesUnrelatedCommands(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	relation := storage.RelationKey{Source: "test", Name: "tasks"}
	plan := fakePlan{fingerprint: "all", relation: relation, routing: routing.RoutingPlan{Fallback: true}}
	record := func(title string) storage.Record {
		return storage.Record{
			"id":    {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("1")},
			"title": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte(title)},
		}
	}
	feed := newCoordinatorFeed()
	base := &coordinatorSource{plan: plan, records: []storage.Record{record("old")}, feed: feed}
	requested := make(chan struct{}, 1)
	source := &progressCoordinatorSource{coordinatorSource: base}
	source.requestFn = func(_ context.Context, fence storage.Fence) error {
		select {
		case requested <- struct{}{}:
		default:
		}
		feed.events <- datastore.CommittedChanges{Checkpoint: fence.(testPosition)}
		return nil
	}
	coordinator, err := StartCoordinatorWithOptions(ctx, source, CoordinatorOptions{Limits: CoordinatorLimits{ProgressBarrierDelay: 20 * time.Millisecond}})
	if err != nil {
		t.Fatal(err)
	}
	opened, err := coordinator.Subscribe(ctx, "viewer", storage.QuerySpec{}, InitialWindow{Count: 1}, 8)
	if err != nil {
		t.Fatal(err)
	}
	base.records = []storage.Record{record("new")}
	base.fence = testPosition(2)
	key := storage.EntityKey{Relation: relation, Value: "1"}
	feed.events <- datastore.CommittedChanges{Checkpoint: testPosition(1), Changes: []datastore.Change{
		fakeChange{kind: datastore.ChangeUpdate, relation: relation, before: key, after: key, complete: false},
	}}
	eventCtx, stop := context.WithTimeout(ctx, time.Second)
	defer stop()
	if batch, err := nextTestBatch(eventCtx, opened.Subscription); err != nil || len(batch.Events) != 1 || !batch.Events[0].Pending {
		t.Fatalf("pending hydration batch = %+v, err=%v", batch, err)
	}
	spamDone := make(chan struct{})
	go func() {
		defer close(spamDone)
		deadline := time.Now().Add(150 * time.Millisecond)
		for time.Now().Before(deadline) {
			_, _ = coordinator.Unsubscribe(ctx, "missing")
			time.Sleep(time.Millisecond)
		}
	}()
	select {
	case <-requested:
	case <-time.After(120 * time.Millisecond):
		t.Fatal("unrelated coordinator commands starved the progress barrier timer")
	}
	<-spamDone
	if count := source.requestCount(); count != 1 {
		t.Fatalf("requested %d barriers, want 1", count)
	}
}

type coordinatorSnapshot struct {
	relation         storage.RelationKey
	records          []storage.Record
	fence            testPosition
	loadStarted      chan struct{}
	loadRelease      <-chan struct{}
	loadCanceled     chan<- struct{}
	queryErrors      map[datastore.QueryFingerprint]error
	windowReads      chan<- datastore.WindowRequest
	windowKeysLoaded func()
}

func (s *coordinatorSnapshot) Relation() storage.RelationKey { return s.relation }
func (s *coordinatorSnapshot) FetchEntity(ctx context.Context, key storage.EntityKey) (datastore.AuthoritativeRecord, error) {
	if s.loadStarted != nil {
		select {
		case s.loadStarted <- struct{}{}:
		default:
		}
	}
	if s.loadRelease != nil {
		select {
		case <-s.loadRelease:
		case <-ctx.Done():
			if s.loadCanceled != nil {
				select {
				case s.loadCanceled <- struct{}{}:
				default:
				}
			}
			return datastore.AuthoritativeRecord{}, ctx.Err()
		}
	}
	for _, record := range s.records {
		if string(record["id"].Data) == key.Value {
			return datastore.AuthoritativeRecord{Record: record}, nil
		}
	}
	return datastore.AuthoritativeRecord{}, datastore.ErrNotFound
}
func (s *coordinatorSnapshot) LoadWindow(_ context.Context, query datastore.PreparedQuery, request datastore.WindowRequest) ([]datastore.AuthoritativeRecord, error) {
	if s.windowReads != nil {
		s.windowReads <- request
	}
	if err := s.queryErrors[query.Fingerprint()]; err != nil {
		return nil, err
	}
	if s.loadStarted != nil {
		select {
		case s.loadStarted <- struct{}{}:
		default:
		}
	}
	if s.loadRelease != nil {
		<-s.loadRelease
	}
	var records []storage.Record
	for _, record := range s.records {
		key, _ := query.Key(record)
		if inCoverage(query, key, []datastore.Bounds{request.Bounds}) {
			records = append(records, record)
		}
	}
	sortRecords(query, records)
	if request.Reverse && len(records) > request.Limit {
		records = records[len(records)-request.Limit:]
	}
	if !request.Reverse && len(records) > request.Limit {
		records = records[:request.Limit]
	}
	result := make([]datastore.AuthoritativeRecord, 0, len(records))
	for _, record := range records {
		result = append(result, datastore.AuthoritativeRecord{Record: record})
	}
	return result, nil
}

func (s *coordinatorSnapshot) LoadWindowKeys(ctx context.Context, query datastore.PreparedQuery, request datastore.WindowRequest) ([]datastore.SortKey, error) {
	rows, err := s.LoadWindow(ctx, query, request)
	if err != nil {
		return nil, err
	}
	keys := make([]datastore.SortKey, 0, len(rows))
	for _, row := range rows {
		key, err := query.Key(row.Record)
		if err != nil {
			return nil, err
		}
		keys = append(keys, key)
	}
	if s.windowKeysLoaded != nil {
		s.windowKeysLoaded()
	}
	return keys, nil
}

func TestCoordinatorConsumesCDCWhileAnotherSnapshotIsBlocked(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	relation := storage.RelationKey{Source: "test", Name: "tasks"}
	plan := fakePlan{fingerprint: "all", relation: relation, routing: routing.RoutingPlan{Fallback: true}}
	record := func(title string) storage.Record {
		return storage.Record{"id": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("1")}, "title": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte(title)}}
	}
	feed := newCoordinatorFeed()
	source := &coordinatorSource{plan: plan, records: []storage.Record{record("old")}, feed: feed}
	coordinator, err := StartCoordinator(ctx, source)
	if err != nil {
		t.Fatal(err)
	}
	existing, err := coordinator.Subscribe(ctx, "existing", storage.QuerySpec{}, InitialWindow{Count: 1}, 4)
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	source.loadStarted = started
	source.loadRelease = release
	// Use a new query so ready coverage from "existing" cannot bypass IO.
	loadingPlan := plan
	loadingPlan.fingerprint = "loading"
	source.plan = loadingPlan
	type openResult struct {
		opened *OpenSubscription
		err    error
	}
	opened := make(chan openResult, 1)
	go func() {
		result, err := coordinator.Subscribe(ctx, "loading", storage.QuerySpec{}, InitialWindow{Count: 1}, 4)
		opened <- openResult{opened: result, err: err}
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("snapshot did not start")
	}
	key := storage.EntityKey{Relation: relation, Value: "1"}
	feed.events <- datastore.CommittedChanges{Checkpoint: testPosition(1), Changes: []datastore.Change{
		fakeChange{kind: datastore.ChangeUpdate, relation: relation, before: key, after: key, record: record("new"), complete: true},
	}}
	eventCtx, stop := context.WithTimeout(ctx, time.Second)
	batch, err := nextTestBatch(eventCtx, existing.Subscription)
	stop()
	if err != nil || len(batch.Events) != 1 || string(batch.Events[0].Record["title"].Data) != "new" {
		t.Fatalf("CDC was blocked by snapshot: batch=%+v err=%v", batch, err)
	}
	close(release)
	result := <-opened
	if result.err != nil {
		t.Fatal(result.err)
	}
	if result.opened.Checkpoint != testPosition(1) {
		t.Fatalf("snapshot incorporated checkpoint = %v, want 1", result.opened.Checkpoint)
	}
}

func TestCoordinatorCoveredReuseRetainsAppliedCheckpoint(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	relation := storage.RelationKey{Source: "test", Name: "tasks"}
	plan := fakePlan{fingerprint: "all", relation: relation, routing: routing.RoutingPlan{Fallback: true}}
	record := func(id string) storage.Record {
		return storage.Record{"id": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte(id)}}
	}
	feed := newCoordinatorFeed()
	feed.events <- datastore.CommittedChanges{Checkpoint: testPosition(1)}
	source := &coordinatorSource{plan: plan, records: []storage.Record{record("1"), record("2")}, feed: feed, fence: testPosition(1)}
	coordinator, err := StartCoordinator(ctx, source)
	if err != nil {
		t.Fatal(err)
	}
	first, err := coordinator.Subscribe(ctx, "first", storage.QuerySpec{}, InitialWindow{Count: 2}, 4)
	if err != nil {
		t.Fatal(err)
	}
	if first.Checkpoint != testPosition(1) || first.Fence == nil {
		t.Fatalf("first provenance: checkpoint=%v fence=%v", first.Checkpoint, first.Fence)
	}
	second, err := coordinator.Subscribe(ctx, "second", storage.QuerySpec{}, InitialWindow{Count: 2}, 4)
	if err != nil {
		t.Fatal(err)
	}
	if second.Checkpoint != testPosition(1) {
		t.Fatalf("covered subscription lost provenance: checkpoint=%v fence=%v", second.Checkpoint, second.Fence)
	}
	if _, err := coordinator.QueueWindowUpdate(ctx, "first", []BoundaryChange{{Side: BoundaryRight, Operation: BoundaryContract, Count: 1}}, "contract"); err != nil {
		t.Fatal(err)
	}
	windowCtx, stop := context.WithTimeout(ctx, time.Second)
	defer stop()
	batch, err := nextTestBatch(windowCtx, first.Subscription)
	if err != nil {
		t.Fatal(err)
	}
	if batch.Window == nil || batch.Window.Checkpoint != testPosition(1) {
		t.Fatalf("covered window transition lost provenance: %+v", batch)
	}
}

func TestCoordinatorResolvesRepeatedInitialWindowFromSharedCache(t *testing.T) {
	for _, test := range []struct {
		name    string
		fromEnd bool
		want    []string
	}{
		{name: "prefix", want: []string{"1", "2"}},
		{name: "suffix", fromEnd: true, want: []string{"2", "3"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			relation := storage.RelationKey{Source: "test", Name: "tasks"}
			plan := fakePlan{fingerprint: "all", relation: relation, routing: routing.RoutingPlan{Fallback: true}}
			record := func(id string) storage.Record {
				return storage.Record{"id": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte(id)}}
			}
			feed := newCoordinatorFeed()
			feed.events <- datastore.CommittedChanges{Checkpoint: testPosition(1)}
			source := &coordinatorSource{
				plan: plan, records: []storage.Record{record("1"), record("2"), record("3")},
				feed: feed, fence: testPosition(1),
			}
			coordinator, err := StartCoordinator(ctx, source)
			if err != nil {
				t.Fatal(err)
			}
			first, err := coordinator.Subscribe(ctx, "first", storage.QuerySpec{}, InitialWindow{Count: 2, FromEnd: test.fromEnd}, 4)
			if err != nil {
				t.Fatal(err)
			}
			if first.Checkpoint != testPosition(1) {
				t.Fatalf("first checkpoint = %v, want 1", first.Checkpoint)
			}

			// A repeated request with sufficient ready edge coverage must not need
			// even a key-only datastore snapshot.
			source.snapshotErr = errors.New("unexpected datastore discovery")
			second, err := coordinator.Subscribe(ctx, "second", storage.QuerySpec{}, InitialWindow{Count: 2, FromEnd: test.fromEnd}, 4)
			if err != nil {
				t.Fatal(err)
			}
			if len(second.Initial) != len(test.want) {
				t.Fatalf("cached initial rows = %+v", second.Initial)
			}
			for index, want := range test.want {
				if second.Initial[index].Key.Value != want {
					t.Fatalf("cached initial row %d = %q, want %q", index, second.Initial[index].Key.Value, want)
				}
			}
		})
	}
}

func TestCoordinatorExplicitInitialBoundsUseOneColdReadThenCache(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	relation := storage.RelationKey{Source: "test", Name: "tasks"}
	plan := fakePlan{fingerprint: "all", relation: relation, routing: routing.RoutingPlan{Fallback: true}}
	record := func(id string) storage.Record {
		return storage.Record{"id": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte(id)}}
	}
	reads := make(chan datastore.WindowRequest, 4)
	feed := newCoordinatorFeed()
	feed.events <- datastore.CommittedChanges{Checkpoint: testPosition(1)}
	source := &coordinatorSource{plan: plan, records: []storage.Record{record("1"), record("2"), record("3")}, feed: feed, fence: testPosition(1), windowReads: reads}
	coordinator, err := StartCoordinator(ctx, source)
	if err != nil {
		t.Fatal(err)
	}
	bounds := storage.CursorBounds{Left: storage.CursorBoundary{Kind: storage.CursorKey, Cursor: []byte("1")}, Right: storage.CursorBoundary{Kind: storage.CursorKey, Cursor: []byte("2")}}
	first, err := coordinator.Subscribe(ctx, "cold", storage.QuerySpec{}, InitialWindow{Bounds: &bounds}, 4)
	if err != nil {
		t.Fatal(err)
	}
	if len(first.Initial) != 1 || first.Initial[0].Key.Value != "2" {
		t.Fatalf("explicit (left,right] bounds not respected: %+v", first.Initial)
	}
	if len(reads) != 1 {
		t.Fatalf("expected one range materialization, no count discovery; got %d reads", len(reads))
	}
	source.snapshotErr = errors.New("unexpected datastore read for covered interval")
	second, err := coordinator.Subscribe(ctx, "warm", storage.QuerySpec{}, InitialWindow{Bounds: &bounds}, 4)
	if err != nil || len(second.Initial) != 1 || second.Initial[0].Key != first.Initial[0].Key {
		t.Fatalf("warm explicit interval: %+v, %v", second, err)
	}
}

func TestCoordinatorLoadsOnlyMissingInitialWindowExtension(t *testing.T) {
	for _, test := range []struct {
		name    string
		fromEnd bool
		want    []string
	}{
		{name: "prefix", want: []string{"1", "2", "3", "4"}},
		{name: "suffix", fromEnd: true, want: []string{"2", "3", "4", "5"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			relation := storage.RelationKey{Source: "test", Name: "tasks"}
			plan := fakePlan{fingerprint: "all", relation: relation, routing: routing.RoutingPlan{Fallback: true}}
			record := func(id string) storage.Record {
				return storage.Record{"id": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte(id)}}
			}
			feed := newCoordinatorFeed()
			feed.events <- datastore.CommittedChanges{Checkpoint: testPosition(1)}
			source := &coordinatorSource{
				plan: plan, records: []storage.Record{record("1"), record("2"), record("3"), record("4"), record("5")},
				feed: feed, fence: testPosition(1),
			}
			coordinator, err := StartCoordinator(ctx, source)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := coordinator.Subscribe(ctx, "first", storage.QuerySpec{}, InitialWindow{Count: 2, FromEnd: test.fromEnd}, 4); err != nil {
				t.Fatal(err)
			}

			reads := make(chan datastore.WindowRequest, 2)
			source.windowReads = reads
			second, err := coordinator.Subscribe(ctx, "second", storage.QuerySpec{}, InitialWindow{Count: 4, FromEnd: test.fromEnd}, 4)
			if err != nil {
				t.Fatal(err)
			}
			if len(second.Initial) != len(test.want) {
				t.Fatalf("extended initial rows = %+v", second.Initial)
			}
			for index, want := range test.want {
				if second.Initial[index].Key.Value != want {
					t.Fatalf("extended initial row %d = %q, want %q", index, second.Initial[index].Key.Value, want)
				}
			}
			if len(reads) != 2 {
				t.Fatalf("datastore window reads = %d, want discovery plus snapshot", len(reads))
			}
			cursor := func(boundary datastore.Boundary) string {
				if boundary.Kind != datastore.BoundaryCursor {
					return ""
				}
				return string(boundary.Key.(datastore.EncodedSortKey))
			}
			discovery, snapshot := <-reads, <-reads
			if test.fromEnd {
				if discovery.Bounds.Left.Kind != datastore.BoundaryStart || cursor(discovery.Bounds.Right) != "3" || cursor(snapshot.Bounds.Left) != "1" || cursor(snapshot.Bounds.Right) != "3" {
					t.Fatalf("suffix reads overlap cached rows: discovery=%+v snapshot=%+v", discovery.Bounds, snapshot.Bounds)
				}
			} else if cursor(discovery.Bounds.Left) != "2" || discovery.Bounds.Right.Kind != datastore.BoundaryEnd || cursor(snapshot.Bounds.Left) != "2" || cursor(snapshot.Bounds.Right) != "4" {
				t.Fatalf("prefix reads overlap cached rows: discovery=%+v snapshot=%+v", discovery.Bounds, snapshot.Bounds)
			}
		})
	}
}

func TestCoordinatorReusesDisjointCoverageInsideInitialExtension(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	relation := storage.RelationKey{Source: "test", Name: "tasks"}
	plan := fakePlan{fingerprint: "all", relation: relation, routing: routing.RoutingPlan{Fallback: true}}
	record := func(id string) storage.Record {
		return storage.Record{"id": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte(id)}}
	}
	feed := newCoordinatorFeed()
	feed.events <- datastore.CommittedChanges{Checkpoint: testPosition(1)}
	source := &coordinatorSource{
		plan: plan, records: []storage.Record{record("1"), record("2"), record("3"), record("4"), record("5"), record("6")},
		feed: feed, fence: testPosition(1),
	}
	coordinator, err := StartCoordinator(ctx, source)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := coordinator.Subscribe(ctx, "prefix", storage.QuerySpec{}, InitialWindow{Count: 2}, 4); err != nil {
		t.Fatal(err)
	}
	if _, err := coordinator.Subscribe(ctx, "suffix", storage.QuerySpec{}, InitialWindow{Count: 2, FromEnd: true}, 4); err != nil {
		t.Fatal(err)
	}

	reads := make(chan datastore.WindowRequest, 2)
	source.windowReads = reads
	opened, err := coordinator.Subscribe(ctx, "extended", storage.QuerySpec{}, InitialWindow{Count: 5}, 8)
	if err != nil {
		t.Fatal(err)
	}
	if len(opened.Initial) != 5 {
		t.Fatalf("extended initial rows = %+v", opened.Initial)
	}
	if len(reads) != 2 {
		t.Fatalf("datastore window reads = %d, want discovery plus one gap snapshot", len(reads))
	}
	discovery, snapshot := <-reads, <-reads
	cursor := func(boundary datastore.Boundary) string {
		if boundary.Kind != datastore.BoundaryCursor {
			return ""
		}
		return string(boundary.Key.(datastore.EncodedSortKey))
	}
	if cursor(discovery.Bounds.Left) != "2" || discovery.Bounds.Right.Kind != datastore.BoundaryEnd {
		t.Fatalf("extension discovery = %+v", discovery.Bounds)
	}
	// The suffix cache already covers (4,END], so only (2,4] is loaded even
	// though the requested final prefix is (START,5].
	if cursor(snapshot.Bounds.Left) != "2" || cursor(snapshot.Bounds.Right) != "4" {
		t.Fatalf("gap snapshot = %+v, want (2,4]", snapshot.Bounds)
	}
}

func TestCoordinatorDiscoveryDoesNotInstallProvisionalCoverage(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	relation := storage.RelationKey{Source: "test", Name: "tasks"}
	plan := fakePlan{fingerprint: "all", relation: relation, routing: routing.RoutingPlan{Fallback: true}}
	record := func(id string) storage.Record {
		return storage.Record{"id": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte(id)}}
	}
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	feed := newCoordinatorFeed()
	source := &coordinatorSource{
		plan: plan, records: []storage.Record{record("1"), record("2"), record("3")}, feed: feed,
		loadStarted: started, loadRelease: release,
	}
	coordinator, err := StartCoordinator(ctx, source)
	if err != nil {
		t.Fatal(err)
	}
	opened := make(chan error, 1)
	go func() {
		_, err := coordinator.Subscribe(ctx, "viewer", storage.QuerySpec{}, InitialWindow{Count: 1}, 4)
		opened <- err
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("snapshot did not start")
	}
	key := storage.EntityKey{Relation: relation, Value: "9"}
	feed.events <- datastore.CommittedChanges{Checkpoint: testPosition(1), Changes: []datastore.Change{
		fakeChange{kind: datastore.ChangeInsert, relation: relation, after: key, record: record("9"), complete: true},
	}}
	deadline := time.Now().Add(time.Second)
	for !feed.Covers(testPosition(1)) && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if rows := coordinator.queryEntities(plan.Fingerprint()); len(rows) != 0 {
		t.Fatalf("discovery read installed query members: %+v", rows)
	}
	close(release)
	if err := <-opened; err != nil {
		t.Fatal(err)
	}
	rows := coordinator.queryEntities(plan.Fingerprint())
	if len(rows) != 1 || rows[0].Key.Value != "1" {
		t.Fatalf("resolved coverage contains unexpected rows: %+v", rows)
	}
}

func TestCoordinatorBuffersRelationCDCWhileRefreshIsBlocked(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	relation := storage.RelationKey{Source: "test", Name: "tasks"}
	plan := fakePlan{fingerprint: "all", relation: relation, routing: routing.RoutingPlan{Fallback: true}}
	record := func(title string) storage.Record {
		return storage.Record{"id": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("1")}, "title": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte(title)}}
	}
	feed := newCoordinatorFeed()
	source := &coordinatorSource{plan: plan, records: []storage.Record{record("old")}, feed: feed}
	coordinator, err := StartCoordinator(ctx, source)
	if err != nil {
		t.Fatal(err)
	}
	opened, err := coordinator.Subscribe(ctx, "viewer", storage.QuerySpec{}, InitialWindow{Count: 1}, 4)
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	source.records = []storage.Record{record("final")}
	source.loadStarted = started
	source.loadRelease = release
	key := storage.EntityKey{Relation: relation, Value: "1"}
	feed.events <- datastore.CommittedChanges{Checkpoint: testPosition(1), Changes: []datastore.Change{
		fakeChange{kind: datastore.ChangeUpdate, relation: relation, before: key, after: key, complete: false},
	}}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("refresh did not start")
	}
	feed.events <- datastore.CommittedChanges{Checkpoint: testPosition(2), Changes: []datastore.Change{
		fakeChange{kind: datastore.ChangeUpdate, relation: relation, before: key, after: key, record: record("final"), complete: true},
	}}
	deadline := time.Now().Add(time.Second)
	for !feed.Covers(testPosition(2)) && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if !feed.Covers(testPosition(2)) {
		t.Fatal("CDC consumer stopped at the blocked relation refresh")
	}
	close(release)
	eventCtx, stop := context.WithTimeout(ctx, time.Second)
	defer stop()
	pending, err := nextTestBatch(eventCtx, opened.Subscription)
	if err != nil || len(pending.Events) != 1 || !pending.Events[0].Pending {
		t.Fatalf("pending update = %+v, err=%v", pending, err)
	}
	batch, err := nextTestBatch(eventCtx, opened.Subscription)
	if err != nil || len(batch.Events) != 1 || batch.Events[0].Pending || string(batch.Events[0].Record["title"].Data) != "final" {
		t.Fatalf("newer CDC update = %+v, err=%v", batch, err)
	}
}

func TestCoordinatorDoesNotBacklogBusyRelationDuringEntityHydration(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	relation := storage.RelationKey{Source: "test", Name: "tasks"}
	plan := fakePlan{fingerprint: "all", relation: relation, routing: routing.RoutingPlan{Fallback: true}}
	record := func(title string) storage.Record {
		return storage.Record{"id": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("1")}, "title": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte(title)}}
	}
	feed := newCoordinatorFeed()
	source := &coordinatorSource{plan: plan, records: []storage.Record{record("old")}, feed: feed}
	coordinator, err := StartCoordinatorWithOptions(ctx, source, CoordinatorOptions{Limits: CoordinatorLimits{MaxResyncTransactions: 1, MaxResyncChanges: 1}})
	if err != nil {
		t.Fatal(err)
	}
	opened, err := coordinator.Subscribe(ctx, "viewer", storage.QuerySpec{}, InitialWindow{Count: 1}, 4)
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	defer close(release)
	source.loadStarted = started
	source.loadRelease = release
	key := storage.EntityKey{Relation: relation, Value: "1"}
	feed.events <- datastore.CommittedChanges{Checkpoint: testPosition(1), Changes: []datastore.Change{
		fakeChange{kind: datastore.ChangeUpdate, relation: relation, before: key, after: key, complete: false},
	}}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("refresh did not start")
	}
	for position := 2; position <= 3; position++ {
		feed.events <- datastore.CommittedChanges{Checkpoint: testPosition(position), Changes: []datastore.Change{
			fakeChange{kind: datastore.ChangeUpdate, relation: relation, before: key, after: key, record: record(fmt.Sprintf("v%d", position)), complete: true},
		}}
	}
	deadline := time.Now().Add(time.Second)
	for !feed.Covers(testPosition(3)) && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if !feed.Covers(testPosition(3)) {
		t.Fatal("CDC consumer stopped during entity hydration")
	}
	queueCtx, stop := context.WithTimeout(ctx, time.Second)
	defer stop()
	for index, expected := range []string{"", "v2", "v3"} {
		batch, err := nextTestBatch(queueCtx, opened.Subscription)
		if err != nil || len(batch.Events) != 1 {
			t.Fatalf("batch %d = %+v, err=%v", index, batch, err)
		}
		if index == 0 {
			if !batch.Events[0].Pending {
				t.Fatalf("first batch was not pending: %+v", batch)
			}
		} else if batch.Events[0].Pending || string(batch.Events[0].Record["title"].Data) != expected {
			t.Fatalf("batch %d = %+v", index, batch)
		}
	}
}

func TestCoordinatorRestartsHydrationSupersededWithinLaterTransaction(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	relation := storage.RelationKey{Source: "test", Name: "tasks"}
	record := func(title string) storage.Record {
		return storage.Record{
			"id":    {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("1")},
			"title": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte(title)},
		}
	}
	partial := storage.Record{
		"id":    {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("1")},
		"title": {State: storage.ValueUnavailable},
	}
	feed := newCoordinatorFeed()
	source := &coordinatorSource{
		plan:    fakePlan{fingerprint: "all", relation: relation, routing: routing.RoutingPlan{Fallback: true}},
		records: []storage.Record{record("old")},
		feed:    feed,
	}
	coordinator, err := StartCoordinator(ctx, source)
	if err != nil {
		t.Fatal(err)
	}
	opened, err := coordinator.Subscribe(ctx, "viewer", storage.QuerySpec{}, InitialWindow{Count: 1}, 16)
	if err != nil {
		t.Fatal(err)
	}
	firstStarted := make(chan struct{}, 1)
	firstRelease := make(chan struct{})
	source.records = []storage.Record{record("obsolete fetch")}
	source.loadStarted, source.loadRelease = firstStarted, firstRelease
	key := storage.EntityKey{Relation: relation, Value: "1"}
	feed.events <- datastore.CommittedChanges{Checkpoint: testPosition(1), Changes: []datastore.Change{
		fakeChange{kind: datastore.ChangeUpdate, relation: relation, before: key, after: key, record: partial},
	}}
	readCtx, stop := context.WithTimeout(ctx, time.Second)
	defer stop()
	if batch, err := nextTestBatch(readCtx, opened.Subscription); err != nil || len(batch.Events) != 1 || !batch.Events[0].Pending {
		t.Fatalf("first partial = %+v, err=%v", batch, err)
	}
	select {
	case <-firstStarted:
	case <-time.After(time.Second):
		t.Fatal("first hydration did not start")
	}

	secondStarted := make(chan struct{}, 1)
	secondRelease := make(chan struct{})
	source.records = []storage.Record{record("fresh fetch")}
	source.loadStarted, source.loadRelease = secondStarted, secondRelease
	feed.events <- datastore.CommittedChanges{Checkpoint: testPosition(2), Changes: []datastore.Change{
		fakeChange{kind: datastore.ChangeUpdate, relation: relation, before: key, after: key, record: record("intermediate"), complete: true},
		fakeChange{kind: datastore.ChangeUpdate, relation: relation, before: key, after: key, record: partial},
	}}
	select {
	case <-secondStarted:
	case <-time.After(time.Second):
		t.Fatal("final partial state reused a hydration superseded by the intermediate complete tuple")
	}
	close(secondRelease)
	close(firstRelease)
	var hydrated bool
	deadline := time.Now().Add(time.Second)
	for !hydrated && time.Now().Before(deadline) {
		batch, err := nextTestBatch(readCtx, opened.Subscription)
		if err != nil {
			t.Fatal(err)
		}
		for _, event := range batch.Events {
			if !event.Pending && string(event.Record["title"].Data) == "fresh fetch" {
				hydrated = true
			}
		}
	}
	if !hydrated {
		t.Fatal("replacement hydration never published its authoritative value")
	}
}

func TestCoordinatorCancelsHydrationSupersededByCompleteTuple(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	relation := storage.RelationKey{Source: "test", Name: "tasks"}
	record := func(title string) storage.Record {
		return storage.Record{
			"id":    {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("1")},
			"title": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte(title)},
		}
	}
	feed := newCoordinatorFeed()
	source := &coordinatorSource{
		plan:    fakePlan{fingerprint: "all", relation: relation, routing: routing.RoutingPlan{Fallback: true}},
		records: []storage.Record{record("initial")},
		feed:    feed,
	}
	coordinator, err := StartCoordinator(ctx, source)
	if err != nil {
		t.Fatal(err)
	}
	opened, err := coordinator.Subscribe(ctx, "viewer", storage.QuerySpec{}, InitialWindow{Count: 1}, 8)
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{}, 1)
	canceled := make(chan struct{}, 1)
	source.loadStarted = started
	source.loadRelease = make(chan struct{})
	source.loadCanceled = canceled
	key := storage.EntityKey{Relation: relation, Value: "1"}
	partial := storage.Record{
		"id":    {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("1")},
		"title": {State: storage.ValueUnavailable},
	}
	feed.events <- datastore.CommittedChanges{Checkpoint: testPosition(1), Changes: []datastore.Change{
		fakeChange{kind: datastore.ChangeUpdate, relation: relation, before: key, after: key, record: partial},
	}}
	readCtx, stop := context.WithTimeout(ctx, time.Second)
	defer stop()
	if batch, err := nextTestBatch(readCtx, opened.Subscription); err != nil || len(batch.Events) != 1 || !batch.Events[0].Pending {
		t.Fatalf("partial update = %+v, err=%v", batch, err)
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("hydration did not start")
	}
	feed.events <- datastore.CommittedChanges{Checkpoint: testPosition(2), Changes: []datastore.Change{
		fakeChange{kind: datastore.ChangeUpdate, relation: relation, before: key, after: key, record: record("complete"), complete: true},
	}}
	if _, err := nextTestBatch(readCtx, opened.Subscription); err != nil {
		t.Fatal(err)
	}
	select {
	case <-canceled:
	case <-time.After(time.Second):
		t.Fatal("superseded hydration database read was not canceled")
	}
}

func TestCoordinatorFailsPotentiallyAffectedNonMemberWhenHydrationFails(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	relation := storage.RelationKey{Source: "test", Name: "tasks"}
	plan := fakePlan{
		fingerprint: "project-a", relation: relation, routing: routing.RoutingPlan{Fallback: true},
		matchError: storage.ErrIncompleteRecord,
	}
	feed := newCoordinatorFeed()
	source := &coordinatorSource{plan: plan, feed: feed}
	coordinator, err := StartCoordinator(ctx, source)
	if err != nil {
		t.Fatal(err)
	}
	opened, err := coordinator.Subscribe(ctx, "viewer", storage.QuerySpec{}, InitialWindow{Count: 10}, 4)
	if err != nil {
		t.Fatal(err)
	}
	source.snapshotErr = errors.New("database read failed")
	key := storage.EntityKey{Relation: relation, Value: "new"}
	partial := storage.Record{"id": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("new")}}
	feed.events <- datastore.CommittedChanges{Checkpoint: testPosition(1), Changes: []datastore.Change{
		fakeChange{kind: datastore.ChangeInsert, relation: relation, before: key, after: key, record: partial, complete: false},
	}}
	nextCtx, stop := context.WithTimeout(ctx, time.Second)
	defer stop()
	if _, err := nextTestBatch(nextCtx, opened.Subscription); err == nil || !strings.Contains(err.Error(), "database read failed") {
		t.Fatalf("subscription hydration error = %v", err)
	}
	if _, exists := coordinator.entities.Get(key); exists {
		t.Fatal("failed hydration left a non-current cache entry reusable")
	}
	deadline := time.Now().Add(time.Second)
	for {
		if _, exists := coordinator.queries.Plan(plan.Fingerprint()); !exists {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("failed hydration left its shared query registered")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestCoordinatorDoesNotHydratePartialRejectedByLiveQuery(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	relation := storage.RelationKey{Source: "test", Name: "tasks"}
	plan := fakePlan{
		fingerprint: "project-a", relation: relation, routing: routing.RoutingPlan{Fallback: true},
		matches: func(record storage.Record) bool { return string(record["project"].Data) == "a" },
	}
	feed := newCoordinatorFeed()
	source := &coordinatorSource{plan: plan, feed: feed}
	coordinator, err := StartCoordinator(ctx, source)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := coordinator.Subscribe(ctx, "viewer", storage.QuerySpec{}, InitialWindow{Count: 10}, 4); err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{}, 1)
	source.loadStarted = started
	key := storage.EntityKey{Relation: relation, Value: "new"}
	partial := storage.Record{
		"id":      {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("new")},
		"project": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("other")},
		"body":    {State: storage.ValueUnavailable},
	}
	feed.events <- datastore.CommittedChanges{Checkpoint: testPosition(1), Changes: []datastore.Change{
		fakeChange{kind: datastore.ChangeInsert, relation: relation, before: key, after: key, record: partial},
	}}
	deadline := time.Now().Add(time.Second)
	for feed.acknowledged() != testPosition(1) && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if feed.acknowledged() != testPosition(1) {
		t.Fatal("partial transaction was not processed")
	}
	select {
	case <-started:
		t.Fatal("rejected partial tuple started a database hydration")
	default:
	}
	if _, exists := coordinator.entities.Get(key); exists {
		t.Fatal("rejected partial tuple remained in the shared entity cache")
	}
}
func (s *coordinatorSnapshot) Finish(context.Context) (storage.Fence, error) { return s.fence, nil }
func (*coordinatorSnapshot) Abort(context.Context) error                     { return nil }

func sortRecords(plan datastore.PreparedQuery, records []storage.Record) {
	sort.Slice(records, func(i, j int) bool {
		left, _ := plan.Key(records[i])
		right, _ := plan.Key(records[j])
		compared, _ := plan.Compare(left, right)
		return compared < 0
	})
}

func TestCoordinatorSharesQuerySnapshotAndPublishesCDC(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	relation := storage.RelationKey{Source: "test", Name: "tasks"}
	plan := fakePlan{
		fingerprint: "project-a", relation: relation,
		routing: routing.RoutingPlan{Exact: []routing.RoutingExact{{Field: "project", Token: "a"}}},
		matches: func(record storage.Record) bool { return string(record["project"].Data) == "a" },
	}
	record := func(id, project string) storage.Record {
		return storage.Record{"id": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte(id)}, "project": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte(project)}}
	}
	feed := newCoordinatorFeed()
	source := &coordinatorSource{plan: plan, records: []storage.Record{record("1", "a"), record("2", "a")}, feed: feed}
	coordinator, err := StartCoordinator(ctx, source)
	if err != nil {
		t.Fatal(err)
	}
	first, err := coordinator.Subscribe(ctx, "first", storage.QuerySpec{}, InitialWindow{Count: 2}, 4)
	if err != nil || len(first.Initial) != 2 {
		t.Fatalf("first = %+v, %v", first, err)
	}
	second, err := coordinator.Subscribe(ctx, "second", storage.QuerySpec{}, InitialWindow{Count: 2}, 4)
	if err != nil || len(second.Initial) != 2 {
		t.Fatalf("second = %+v, %v", second, err)
	}
	key := storage.EntityKey{Relation: relation, Value: "1"}
	if entity, ok := coordinator.entities.Get(key); !ok || entity.References != 1 {
		t.Fatalf("entity was not shared by query: %+v, %v", entity, ok)
	}
	updated := record("1", "a")
	updated["title"] = storage.Value{Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("changed")}
	feed.events <- datastore.CommittedChanges{Checkpoint: testPosition(1), Atomic: true, Changes: []datastore.Change{fakeChange{kind: datastore.ChangeUpdate, relation: relation, before: key, after: key, record: updated, complete: true}}}
	for _, subscription := range []*Subscription{first.Subscription, second.Subscription} {
		nextCtx, stop := context.WithTimeout(ctx, time.Second)
		batch, eventErr := nextTestBatch(nextCtx, subscription)
		stop()
		event := batch.Events[0]
		if eventErr != nil || event.Kind != QueryUpdated || string(event.Record["title"].Data) != "changed" {
			t.Fatalf("event = %+v, %v", event, eventErr)
		}
	}
	if _, err := coordinator.Unsubscribe(ctx, "first"); err != nil {
		t.Fatal(err)
	}
	if _, err := coordinator.Unsubscribe(ctx, "second"); err != nil {
		t.Fatal(err)
	}
}

func TestCoordinatorPublishesOneBatchPerCommittedTransaction(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	relation := storage.RelationKey{Source: "test", Name: "tasks"}
	plan := fakePlan{fingerprint: "all", relation: relation, routing: routing.RoutingPlan{Fallback: true}}
	record := func(id, title string) storage.Record {
		return storage.Record{"id": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte(id)}, "title": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte(title)}}
	}
	feed := newCoordinatorFeed()
	source := &coordinatorSource{plan: plan, records: []storage.Record{record("1", "old-1"), record("2", "old-2")}, feed: feed}
	coordinator, err := StartCoordinator(ctx, source)
	if err != nil {
		t.Fatal(err)
	}
	opened, err := coordinator.Subscribe(ctx, "viewer", storage.QuerySpec{}, InitialWindow{Count: 10}, 4)
	if err != nil {
		t.Fatal(err)
	}
	key1 := storage.EntityKey{Relation: relation, Value: "1"}
	key2 := storage.EntityKey{Relation: relation, Value: "2"}
	feed.events <- datastore.CommittedChanges{Checkpoint: testPosition(12), Atomic: true, Changes: []datastore.Change{
		fakeChange{kind: datastore.ChangeUpdate, relation: relation, before: key1, after: key1, record: record("1", "new-1"), complete: true},
		fakeChange{kind: datastore.ChangeUpdate, relation: relation, before: key2, after: key2, record: record("2", "new-2"), complete: true},
	}}
	nextCtx, stop := context.WithTimeout(ctx, time.Second)
	defer stop()
	batch, err := nextTestBatch(nextCtx, opened.Subscription)
	if err != nil || !batch.Atomic || batch.Checkpoint != testPosition(12) || len(batch.Events) != 2 {
		t.Fatalf("batch = %+v, err=%v", batch, err)
	}
	noSecondCtx, noSecondCancel := context.WithTimeout(ctx, 20*time.Millisecond)
	defer noSecondCancel()
	if _, err := nextTestBatch(noSecondCtx, opened.Subscription); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected no second batch, got %v", err)
	}
}

func TestCoordinatorPreservesOriginalChangeIndexesAcrossRelations(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	projects := storage.RelationKey{Source: "test", Name: "projects"}
	tasks := storage.RelationKey{Source: "test", Name: "tasks"}
	record := func(id, title string) storage.Record {
		return storage.Record{"id": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte(id)}, "title": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte(title)}}
	}
	feed := newCoordinatorFeed()
	source := &coordinatorSource{plan: fakePlan{fingerprint: "projects", relation: projects, routing: routing.RoutingPlan{Fallback: true}}, records: []storage.Record{record("p", "old")}, feed: feed}
	coordinator, err := StartCoordinator(ctx, source)
	if err != nil {
		t.Fatal(err)
	}
	projectSub, err := coordinator.Subscribe(ctx, "projects", storage.QuerySpec{}, InitialWindow{Count: 10}, 4)
	if err != nil {
		t.Fatal(err)
	}
	source.plan = fakePlan{fingerprint: "tasks", relation: tasks, routing: routing.RoutingPlan{Fallback: true}}
	source.records = []storage.Record{record("t", "old")}
	taskSub, err := coordinator.Subscribe(ctx, "tasks", storage.QuerySpec{}, InitialWindow{Count: 10}, 4)
	if err != nil {
		t.Fatal(err)
	}
	projectKey := storage.EntityKey{Relation: projects, Value: "p"}
	taskKey := storage.EntityKey{Relation: tasks, Value: "t"}
	feed.events <- datastore.CommittedChanges{Checkpoint: testPosition(13), Atomic: true, SourceOrdinals: []uint32{4, 9, 12}, Changes: []datastore.Change{
		fakeChange{kind: datastore.ChangeUpdate, relation: projects, before: projectKey, after: projectKey, record: record("p", "first"), complete: true},
		fakeChange{kind: datastore.ChangeUpdate, relation: tasks, before: taskKey, after: taskKey, record: record("t", "second"), complete: true},
		fakeChange{kind: datastore.ChangeUpdate, relation: projects, before: projectKey, after: projectKey, record: record("p", "third"), complete: true},
	}}
	readCtx, stop := context.WithTimeout(ctx, time.Second)
	defer stop()
	projectBatch, err := nextTestBatch(readCtx, projectSub.Subscription)
	if err != nil {
		t.Fatal(err)
	}
	taskBatch, err := nextTestBatch(readCtx, taskSub.Subscription)
	if err != nil {
		t.Fatal(err)
	}
	if len(projectBatch.Events) != 2 || projectBatch.Events[0].ChangeIndex != 4 || projectBatch.Events[1].ChangeIndex != 12 {
		t.Fatalf("project change indexes = %+v, want 4 and 12", projectBatch.Events)
	}
	if len(taskBatch.Events) != 1 || taskBatch.Events[0].ChangeIndex != 9 {
		t.Fatalf("task change indexes = %+v, want 9", taskBatch.Events)
	}
	if !projectBatch.Atomic || !taskBatch.Atomic || projectBatch.Revision == 0 || projectBatch.Revision != taskBatch.Revision {
		t.Fatalf("cross-relation transaction metadata: projects=%+v tasks=%+v", projectBatch, taskBatch)
	}
}

func TestSplitBatchByRelationRejectsMalformedSourceBatches(t *testing.T) {
	relation := storage.RelationKey{Source: "test", Name: "tasks"}
	key := storage.EntityKey{Relation: relation, Value: "1"}
	tests := []datastore.CommittedChanges{
		{Changes: []datastore.Change{nil}},
		{Changes: []datastore.Change{fakeChange{kind: datastore.ChangeDelete, relation: storage.RelationKey{Source: "test"}, before: key}}},
		{SourceOrdinals: []uint32{1, 2}, Changes: []datastore.Change{fakeChange{kind: datastore.ChangeDelete, relation: relation, before: key}}},
		{SourceOrdinals: []uint32{2, 2}, Changes: []datastore.Change{
			fakeChange{kind: datastore.ChangeDelete, relation: relation, before: key},
			fakeChange{kind: datastore.ChangeDelete, relation: relation, before: key},
		}},
	}
	for index, batch := range tests {
		if _, err := splitBatchByRelation(batch); !errors.Is(err, changeprocessor.ErrFatalSource) {
			t.Fatalf("case %d error = %v, want ErrFatalSource", index, err)
		}
	}
}

func TestCoordinatorPublishesPendingThenHydratesIncompleteTuple(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	relation := storage.RelationKey{Source: "test", Name: "tasks"}
	plan := fakePlan{fingerprint: "all", relation: relation, routing: routing.RoutingPlan{Fallback: true}}
	record := func(title string) storage.Record {
		return storage.Record{"id": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("1")}, "title": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte(title)}}
	}
	feed := newCoordinatorFeed()
	source := &coordinatorSource{plan: plan, records: []storage.Record{record("old")}, feed: feed}
	coordinator, err := StartCoordinator(ctx, source)
	if err != nil {
		t.Fatal(err)
	}
	opened, err := coordinator.Subscribe(ctx, "viewer", storage.QuerySpec{}, InitialWindow{Count: 10}, 4)
	if err != nil {
		t.Fatal(err)
	}
	source.records = []storage.Record{record("new")}
	key := storage.EntityKey{Relation: relation, Value: "1"}
	feed.events <- datastore.CommittedChanges{Checkpoint: testPosition(1), Changes: []datastore.Change{fakeChange{kind: datastore.ChangeUpdate, relation: relation, before: key, after: key, complete: false}}}
	eventCtx, stop := context.WithTimeout(ctx, time.Second)
	defer stop()
	pending, err := nextTestBatch(eventCtx, opened.Subscription)
	if err != nil || len(pending.Events) != 1 || !pending.Events[0].Pending {
		t.Fatalf("pending batch = %+v, %v", pending, err)
	}
	batch, err := nextTestBatch(eventCtx, opened.Subscription)
	if err != nil || len(batch.Events) != 1 || batch.Events[0].Pending {
		t.Fatalf("hydrated batch = %+v, %v", batch, err)
	}
	event := batch.Events[0]
	if event.Kind != QueryUpdated || string(event.Record["title"].Data) != "new" {
		t.Fatalf("hydrated event = %+v, %v", event, err)
	}
}

func TestCoordinatorPreservesTransactionBatchWithPendingEntity(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	relation := storage.RelationKey{Source: "test", Name: "tasks"}
	plan := fakePlan{fingerprint: "all", relation: relation, routing: routing.RoutingPlan{Fallback: true}}
	record := func(id, title string) storage.Record {
		return storage.Record{
			"id":    {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte(id)},
			"title": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte(title)},
		}
	}
	feed := newCoordinatorFeed()
	source := &coordinatorSource{
		plan: plan, records: []storage.Record{record("1", "old-1"), record("2", "old-2")}, feed: feed,
	}
	coordinator, err := StartCoordinator(ctx, source)
	if err != nil {
		t.Fatal(err)
	}
	opened, err := coordinator.Subscribe(ctx, "viewer", storage.QuerySpec{}, InitialWindow{Count: 10}, 4)
	if err != nil {
		t.Fatal(err)
	}

	source.records = []storage.Record{record("1", "new-1"), record("2", "new-2")}
	key1 := storage.EntityKey{Relation: relation, Value: "1"}
	key2 := storage.EntityKey{Relation: relation, Value: "2"}
	feed.events <- datastore.CommittedChanges{Checkpoint: testPosition(9), Atomic: true, Changes: []datastore.Change{
		fakeChange{kind: datastore.ChangeUpdate, relation: relation, before: key1, after: key1, complete: false},
		fakeChange{kind: datastore.ChangeUpdate, relation: relation, before: key2, after: key2, record: record("2", "new-2"), complete: true},
	}}

	nextCtx, stop := context.WithTimeout(ctx, time.Second)
	defer stop()
	batch, err := nextTestBatch(nextCtx, opened.Subscription)
	if err != nil || !batch.Atomic || batch.Checkpoint != testPosition(9) || len(batch.Events) != 2 {
		t.Fatalf("transaction batch = %+v, err=%v", batch, err)
	}
	if !batch.Events[0].Pending || batch.Events[1].Pending || string(batch.Events[1].Record["title"].Data) != "new-2" {
		t.Fatalf("transaction did not preserve pending/full structure: %+v", batch.Events)
	}
	hydrated, err := nextTestBatch(nextCtx, opened.Subscription)
	if err != nil || hydrated.Atomic || len(hydrated.Events) != 1 || hydrated.Events[0].Pending || string(hydrated.Events[0].Record["title"].Data) != "new-1" {
		t.Fatalf("hydration batch = %+v, err=%v", hydrated, err)
	}
}

func TestCoordinatorExtendsAndContractsWindowAtomically(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	relation := storage.RelationKey{Source: "test", Name: "tasks"}
	plan := fakePlan{fingerprint: "all", relation: relation, routing: routing.RoutingPlan{Fallback: true}}
	record := func(id string) storage.Record {
		return storage.Record{"id": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte(id)}}
	}
	feed := newCoordinatorFeed()
	feed.events <- datastore.CommittedChanges{Checkpoint: testPosition(1)}
	source := &coordinatorSource{plan: plan, records: []storage.Record{record("1"), record("2"), record("3")}, feed: feed, fence: testPosition(1)}
	coordinator, err := StartCoordinator(ctx, source)
	if err != nil {
		t.Fatal(err)
	}
	opened, err := coordinator.Subscribe(ctx, "viewer", storage.QuerySpec{}, InitialWindow{Count: 2}, 4)
	if err != nil || len(opened.Initial) != 2 {
		t.Fatalf("initial = %+v, %v", opened, err)
	}
	reads := make(chan datastore.WindowRequest, 2)
	source.windowReads = reads
	extendedBounds, err := coordinator.QueueWindowUpdate(ctx, "viewer", []BoundaryChange{{Side: BoundaryRight, Operation: BoundaryExtend, Count: 1}}, "extend")
	extended, queueErr := nextTestBatch(ctx, opened.Subscription)
	if err != nil || queueErr != nil || extended.Window == nil || extended.Window.Token != "extend" || compareBoundary(plan, extended.Window.Bounds.Left, extendedBounds.Left) != 0 || compareBoundary(plan, extended.Window.Bounds.Right, extendedBounds.Right) != 0 || len(extended.Events) != 1 || extended.Events[0].Kind != QueryAdded || extended.Events[0].Key.Value != "3" {
		t.Fatalf("extend bounds=%+v batch=%+v updateErr=%v queueErr=%v", extendedBounds, extended, err, queueErr)
	}
	if len(extended.Membership.Insertions) != 1 || extended.Membership.Insertions[0].Key.Value != "3" || extended.Membership.Insertions[0].FinalIndex != 2 {
		t.Fatalf("extend membership = %+v", extended.Membership)
	}
	if len(reads) != 2 {
		t.Fatalf("extension datastore reads=%d, want discovery plus missing-gap snapshot", len(reads))
	}
	discovery, snapshot := <-reads, <-reads
	cursor := func(boundary datastore.Boundary) string {
		if boundary.Kind != datastore.BoundaryCursor {
			return ""
		}
		return string(boundary.Key.(datastore.EncodedSortKey))
	}
	if cursor(discovery.Bounds.Left) != "2" || discovery.Bounds.Right.Kind != datastore.BoundaryEnd || cursor(snapshot.Bounds.Left) != "2" || snapshot.Bounds.Right.Kind != datastore.BoundaryEnd {
		t.Fatalf("extension reloaded covered rows: discovery=%+v snapshot=%+v", discovery.Bounds, snapshot.Bounds)
	}
	contractedBounds, err := coordinator.QueueWindowUpdate(ctx, "viewer", []BoundaryChange{{Side: BoundaryLeft, Operation: BoundaryContract, Count: 1}}, "contract")
	contracted, queueErr := nextTestBatch(ctx, opened.Subscription)
	if err != nil || queueErr != nil || contracted.Window == nil || contracted.Window.Token != "contract" || compareBoundary(plan, contracted.Window.Bounds.Left, contractedBounds.Left) != 0 || compareBoundary(plan, contracted.Window.Bounds.Right, contractedBounds.Right) != 0 || len(contracted.Events) != 1 || contracted.Events[0].Kind != QueryRemoved || contracted.Events[0].Key.Value != "1" {
		t.Fatalf("contract bounds=%+v batch=%+v updateErr=%v queueErr=%v", contractedBounds, contracted, err, queueErr)
	}
	if len(contracted.Membership.Removed) != 1 || contracted.Membership.Removed[0].Value != "1" || len(contracted.Membership.Insertions) != 0 {
		t.Fatalf("contract membership = %+v", contracted.Membership)
	}
}

func TestCoordinatorResolvesExtensionFromSharedReadyCoverage(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	relation := storage.RelationKey{Source: "test", Name: "tasks"}
	plan := fakePlan{fingerprint: "all", relation: relation, routing: routing.RoutingPlan{Fallback: true}}
	record := func(id string) storage.Record {
		return storage.Record{"id": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte(id)}}
	}
	feed := newCoordinatorFeed()
	feed.events <- datastore.CommittedChanges{Checkpoint: testPosition(1)}
	source := &coordinatorSource{plan: plan, records: []storage.Record{record("1"), record("2"), record("3"), record("4"), record("5")}, feed: feed, fence: testPosition(1)}
	coordinator, err := StartCoordinator(ctx, source)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := coordinator.Subscribe(ctx, "wide", storage.QuerySpec{}, InitialWindow{Count: 4}, 8); err != nil {
		t.Fatal(err)
	}
	narrow, err := coordinator.Subscribe(ctx, "narrow", storage.QuerySpec{}, InitialWindow{Count: 2}, 8)
	if err != nil {
		t.Fatal(err)
	}
	reads := make(chan datastore.WindowRequest, 1)
	source.windowReads = reads
	if _, err := coordinator.QueueWindowUpdate(ctx, "narrow", []BoundaryChange{{Side: BoundaryRight, Operation: BoundaryExtend, Count: 2}}, "cached"); err != nil {
		t.Fatal(err)
	}
	if len(reads) != 0 {
		t.Fatalf("cached extension performed %d datastore reads", len(reads))
	}
	batch, err := nextTestBatch(ctx, narrow.Subscription)
	if err != nil || batch.Window == nil || len(batch.Events) != 2 || batch.Events[0].Key.Value != "3" || batch.Events[1].Key.Value != "4" {
		t.Fatalf("cached extension batch=%+v err=%v", batch, err)
	}
	if len(batch.Membership.Insertions) != 2 || batch.Membership.Insertions[0].FinalIndex != 2 || batch.Membership.Insertions[1].FinalIndex != 3 {
		t.Fatalf("cached extension membership=%+v", batch.Membership)
	}
}

func installPendingQueryHydration(t *testing.T, coordinator *Coordinator, key storage.EntityKey, fingerprint datastore.QueryFingerprint) func() {
	t.Helper()
	load := coordinator.entities.BeginRefresh(key)
	installed := make(chan error, 1)
	coordinator.commands <- coordinatorCommand{run: func() {
		_, _, err := coordinator.recovery.StartHydration(key, load, []datastore.QueryFingerprint{fingerprint}, func() {})
		installed <- err
	}}
	if err := <-installed; err != nil {
		t.Fatal(err)
	}
	return func() {
		removed := make(chan struct{}, 1)
		coordinator.commands <- coordinatorCommand{run: func() {
			coordinator.recovery.RemoveHydration(key)
			removed <- struct{}{}
		}}
		<-removed
		_ = coordinator.entities.Fail(load, context.Canceled)
	}
}

func TestCoordinatorDoesNotReuseQueryMembershipWithPendingHydration(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	relation := storage.RelationKey{Source: "test", Name: "tasks"}
	plan := fakePlan{fingerprint: "all", relation: relation, routing: routing.RoutingPlan{Fallback: true}}
	record := func(id string) storage.Record {
		return storage.Record{"id": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte(id)}}
	}
	feed := newCoordinatorFeed()
	feed.events <- datastore.CommittedChanges{Checkpoint: testPosition(1)}
	source := &coordinatorSource{plan: plan, records: []storage.Record{record("1"), record("2"), record("3")}, feed: feed, fence: testPosition(1)}
	coordinator, err := StartCoordinator(ctx, source)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := coordinator.Subscribe(ctx, "warm", storage.QuerySpec{}, InitialWindow{Count: 2}, 8); err != nil {
		t.Fatal(err)
	}

	defer installPendingQueryHydration(t, coordinator, storage.EntityKey{Relation: relation, Value: "pending"}, plan.Fingerprint())()

	reads := make(chan datastore.WindowRequest, 4)
	source.windowReads = reads
	opened, err := coordinator.Subscribe(ctx, "late", storage.QuerySpec{}, InitialWindow{Count: 1}, 8)
	if err != nil {
		t.Fatal(err)
	}
	if len(opened.Initial) != 1 {
		t.Fatalf("initial rows=%d, want 1", len(opened.Initial))
	}
	if len(reads) != 2 {
		t.Fatalf("snapshot reads=%d, want key discovery plus fenced materialization", len(reads))
	}
}

func TestCoordinatorRetainsInitialDiscoveryFenceAfterHydrationCompletes(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	relation := storage.RelationKey{Source: "test", Name: "tasks"}
	plan := fakePlan{fingerprint: "all", relation: relation, routing: routing.RoutingPlan{Fallback: true}}
	record := func(id string) storage.Record {
		return storage.Record{"id": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte(id)}}
	}
	feed := newCoordinatorFeed()
	feed.events <- datastore.CommittedChanges{Checkpoint: testPosition(1)}
	source := &coordinatorSource{plan: plan, records: []storage.Record{record("1"), record("2"), record("3")}, feed: feed, fence: testPosition(1)}
	coordinator, err := StartCoordinator(ctx, source)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := coordinator.Subscribe(ctx, "warm", storage.QuerySpec{}, InitialWindow{Count: 2}, 8); err != nil {
		t.Fatal(err)
	}

	// Force datastore discovery, then complete the hydration before the
	// subscription installs its already-covered final bounds. The discovery
	// itself is unfenced, so installation must still perform a snapshot read.
	source.windowKeysLoaded = installPendingQueryHydration(t, coordinator, storage.EntityKey{Relation: relation, Value: "pending"}, plan.Fingerprint())
	reads := make(chan datastore.WindowRequest, 2)
	source.windowReads = reads
	opened, err := coordinator.Subscribe(ctx, "late", storage.QuerySpec{}, InitialWindow{Count: 1}, 8)
	if err != nil {
		t.Fatal(err)
	}
	if len(opened.Initial) != 1 {
		t.Fatalf("initial rows=%d, want 1", len(opened.Initial))
	}
	if len(reads) != 2 {
		t.Fatalf("snapshot reads=%d, want discovery plus retained fenced materialization", len(reads))
	}
}

func TestCoordinatorDoesNotResolveCachedExtensionWithPendingHydration(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	relation := storage.RelationKey{Source: "test", Name: "tasks"}
	plan := fakePlan{fingerprint: "all", relation: relation, routing: routing.RoutingPlan{Fallback: true}}
	record := func(id string) storage.Record {
		return storage.Record{"id": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte(id)}}
	}
	feed := newCoordinatorFeed()
	feed.events <- datastore.CommittedChanges{Checkpoint: testPosition(1)}
	source := &coordinatorSource{plan: plan, records: []storage.Record{record("1"), record("2"), record("3"), record("4")}, feed: feed, fence: testPosition(1)}
	coordinator, err := StartCoordinator(ctx, source)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := coordinator.Subscribe(ctx, "wide", storage.QuerySpec{}, InitialWindow{Count: 4}, 8); err != nil {
		t.Fatal(err)
	}
	if _, err := coordinator.Subscribe(ctx, "narrow", storage.QuerySpec{}, InitialWindow{Count: 2}, 8); err != nil {
		t.Fatal(err)
	}

	defer installPendingQueryHydration(t, coordinator, storage.EntityKey{Relation: relation, Value: "pending"}, plan.Fingerprint())()

	reads := make(chan datastore.WindowRequest, 4)
	source.windowReads = reads
	bounds, err := coordinator.QueueWindowUpdate(ctx, "narrow", []BoundaryChange{{Side: BoundaryRight, Operation: BoundaryExtend, Count: 1}}, "authoritative")
	if err != nil {
		t.Fatal(err)
	}
	if len(reads) != 2 {
		t.Fatalf("window reads=%d, want key discovery plus fenced materialization", len(reads))
	}
	if bounds.Right.Kind != datastore.BoundaryCursor || string(bounds.Right.Key.(datastore.EncodedSortKey)) != "3" {
		t.Fatalf("extended bounds = %+v, want right cursor 3", bounds)
	}
	bounds, err = coordinator.QueueWindowUpdate(ctx, "narrow", []BoundaryChange{{Side: BoundaryRight, Operation: BoundaryContract, Count: 1}}, "authoritative-contract")
	if err != nil {
		t.Fatal(err)
	}
	if len(reads) != 4 {
		t.Fatalf("window reads=%d, want discovery and materialization for both updates", len(reads))
	}
	if bounds.Right.Kind != datastore.BoundaryCursor || string(bounds.Right.Key.(datastore.EncodedSortKey)) != "2" {
		t.Fatalf("contracted bounds = %+v, want right cursor 2", bounds)
	}
}

func TestCoordinatorRetainsWindowDiscoveryFenceAfterHydrationCompletes(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	relation := storage.RelationKey{Source: "test", Name: "tasks"}
	plan := fakePlan{fingerprint: "all", relation: relation, routing: routing.RoutingPlan{Fallback: true}}
	record := func(id string) storage.Record {
		return storage.Record{"id": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte(id)}}
	}
	feed := newCoordinatorFeed()
	feed.events <- datastore.CommittedChanges{Checkpoint: testPosition(1)}
	source := &coordinatorSource{plan: plan, records: []storage.Record{record("1"), record("2"), record("3"), record("4")}, feed: feed, fence: testPosition(1)}
	coordinator, err := StartCoordinator(ctx, source)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := coordinator.Subscribe(ctx, "wide", storage.QuerySpec{}, InitialWindow{Count: 4}, 8); err != nil {
		t.Fatal(err)
	}
	if _, err := coordinator.Subscribe(ctx, "narrow", storage.QuerySpec{}, InitialWindow{Count: 2}, 8); err != nil {
		t.Fatal(err)
	}

	source.windowKeysLoaded = installPendingQueryHydration(t, coordinator, storage.EntityKey{Relation: relation, Value: "pending"}, plan.Fingerprint())
	reads := make(chan datastore.WindowRequest, 2)
	source.windowReads = reads
	bounds, err := coordinator.QueueWindowUpdate(ctx, "narrow", []BoundaryChange{{Side: BoundaryRight, Operation: BoundaryExtend, Count: 1}}, "authoritative")
	if err != nil {
		t.Fatal(err)
	}
	if len(reads) != 2 {
		t.Fatalf("window reads=%d, want discovery plus retained fenced materialization", len(reads))
	}
	if bounds.Right.Kind != datastore.BoundaryCursor || string(bounds.Right.Key.(datastore.EncodedSortKey)) != "3" {
		t.Fatalf("extended bounds = %+v, want right cursor 3", bounds)
	}
}

func TestCoordinatorDiscoversAndLoadsOnlyBeyondPartialExtensionCoverage(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	relation := storage.RelationKey{Source: "test", Name: "tasks"}
	plan := fakePlan{fingerprint: "all", relation: relation, routing: routing.RoutingPlan{Fallback: true}}
	record := func(id string) storage.Record {
		return storage.Record{"id": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte(id)}}
	}
	feed := newCoordinatorFeed()
	feed.events <- datastore.CommittedChanges{Checkpoint: testPosition(1)}
	source := &coordinatorSource{plan: plan, records: []storage.Record{record("1"), record("2"), record("3"), record("4"), record("5")}, feed: feed, fence: testPosition(1)}
	coordinator, err := StartCoordinator(ctx, source)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := coordinator.Subscribe(ctx, "wide", storage.QuerySpec{}, InitialWindow{Count: 3}, 8); err != nil {
		t.Fatal(err)
	}
	narrow, err := coordinator.Subscribe(ctx, "narrow", storage.QuerySpec{}, InitialWindow{Count: 2}, 8)
	if err != nil {
		t.Fatal(err)
	}
	reads := make(chan datastore.WindowRequest, 2)
	source.windowReads = reads
	if _, err := coordinator.QueueWindowUpdate(ctx, "narrow", []BoundaryChange{{Side: BoundaryRight, Operation: BoundaryExtend, Count: 2}}, "partial"); err != nil {
		t.Fatal(err)
	}
	if len(reads) != 2 {
		t.Fatalf("partial extension datastore reads=%d, want discovery plus gap snapshot", len(reads))
	}
	discovery, snapshot := <-reads, <-reads
	cursor := func(boundary datastore.Boundary) string {
		if boundary.Kind != datastore.BoundaryCursor {
			return ""
		}
		return string(boundary.Key.(datastore.EncodedSortKey))
	}
	if cursor(discovery.Bounds.Left) != "3" || cursor(snapshot.Bounds.Left) != "3" || cursor(snapshot.Bounds.Right) != "4" {
		t.Fatalf("partial extension reads=%+v then %+v", discovery.Bounds, snapshot.Bounds)
	}
	batch, err := nextTestBatch(ctx, narrow.Subscription)
	if err != nil || batch.Window == nil || len(batch.Events) != 2 || batch.Events[0].Key.Value != "3" || batch.Events[1].Key.Value != "4" {
		t.Fatalf("partial extension batch=%+v err=%v", batch, err)
	}
}

func TestCoordinatorAllowsSoftWindowGrowthAboveRequestLimit(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	relation := storage.RelationKey{Source: "test", Name: "tasks"}
	plan := fakePlan{fingerprint: "all", relation: relation, routing: routing.RoutingPlan{Fallback: true}}
	record := func(id string) storage.Record {
		return storage.Record{"id": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte(id)}}
	}
	feed := newCoordinatorFeed()
	source := &coordinatorSource{plan: plan, records: []storage.Record{record("1"), record("2")}, feed: feed}
	coordinator, err := StartCoordinatorWithOptions(ctx, source, CoordinatorOptions{Limits: CoordinatorLimits{MaxWindowRows: 2, SnapshotPageRows: 2}})
	if err != nil {
		t.Fatal(err)
	}
	opened, err := coordinator.Subscribe(ctx, "viewer", storage.QuerySpec{}, InitialWindow{Count: 2}, 8)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"3", "4"} {
		key := storage.EntityKey{Relation: relation, Value: id}
		feed.events <- datastore.CommittedChanges{Checkpoint: testPosition(id[0] - '0'), Changes: []datastore.Change{
			fakeChange{kind: datastore.ChangeInsert, relation: relation, before: key, after: key, record: record(id), complete: true},
		}}
		if _, err := nextTestBatch(ctx, opened.Subscription); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := coordinator.QueueWindowUpdate(ctx, "viewer", []BoundaryChange{{Side: BoundaryLeft, Operation: BoundaryContract, Count: 1}}, "contract-grown"); err != nil {
		t.Fatalf("contract soft-grown window: %v", err)
	}
	batch, err := nextTestBatch(ctx, opened.Subscription)
	if err != nil || batch.Window == nil || batch.Window.Token != "contract-grown" || len(batch.Events) != 1 || batch.Events[0].Key.Value != "1" {
		t.Fatalf("soft-grown contraction batch=%+v err=%v", batch, err)
	}
}

func TestCoordinatorResolvesBothBoundaryChangesFromOriginalWindow(t *testing.T) {
	for _, test := range []struct {
		name    string
		changes []BoundaryChange
	}{
		{
			name: "left then right",
			changes: []BoundaryChange{
				{Side: BoundaryLeft, Operation: BoundaryExtend, Count: 2},
				{Side: BoundaryRight, Operation: BoundaryContract, Count: 3},
			},
		},
		{
			name: "right then left",
			changes: []BoundaryChange{
				{Side: BoundaryRight, Operation: BoundaryContract, Count: 3},
				{Side: BoundaryLeft, Operation: BoundaryExtend, Count: 2},
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			relation := storage.RelationKey{Source: "test", Name: "tasks"}
			plan := fakePlan{fingerprint: "all", relation: relation, routing: routing.RoutingPlan{Fallback: true}}
			record := func(id string) storage.Record {
				return storage.Record{"id": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte(id)}}
			}
			source := &coordinatorSource{
				plan:    plan,
				records: []storage.Record{record("1"), record("2"), record("3"), record("4")},
				feed:    newCoordinatorFeed(),
			}
			coordinator, err := StartCoordinator(ctx, source)
			if err != nil {
				t.Fatal(err)
			}
			opened, err := coordinator.Subscribe(ctx, "viewer", storage.QuerySpec{}, InitialWindow{FromEnd: true, Count: 2}, 8)
			if err != nil {
				t.Fatal(err)
			}
			if opened.Bounds.Left.Kind != datastore.BoundaryCursor || string(opened.Bounds.Left.Key.(datastore.EncodedSortKey)) != "2" || opened.Bounds.Right.Kind != datastore.BoundaryEnd {
				t.Fatalf("initial bounds = %+v, want (2, END]", opened.Bounds)
			}

			bounds, err := coordinator.QueueWindowUpdate(ctx, "viewer", test.changes, "atomic")
			if err != nil {
				t.Fatal(err)
			}
			if bounds.Left.Kind != datastore.BoundaryStart || bounds.Right.Kind != datastore.BoundaryCursor || string(bounds.Right.Key.(datastore.EncodedSortKey)) != "2" {
				t.Fatalf("updated bounds = %+v, want (START, 2]", bounds)
			}
			batch, err := nextTestBatch(ctx, opened.Subscription)
			if err != nil {
				t.Fatal(err)
			}
			added, removed := map[string]bool{}, map[string]bool{}
			for _, event := range batch.Events {
				switch event.Kind {
				case QueryAdded:
					added[event.Key.Value] = true
				case QueryRemoved:
					removed[event.Key.Value] = true
				}
			}
			if !added["1"] || !added["2"] || !removed["3"] || !removed["4"] || len(batch.Events) != 4 {
				t.Fatalf("window delta = %+v", batch.Events)
			}
		})
	}
}

func TestCoordinatorDiscoversBothBoundaryChangesFromOneSnapshot(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	relation := storage.RelationKey{Source: "test", Name: "tasks"}
	plan := fakePlan{fingerprint: "all", relation: relation, routing: routing.RoutingPlan{Fallback: true}}
	record := func(id string) storage.Record {
		return storage.Record{"id": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte(id)}}
	}
	feed := newCoordinatorFeed()
	feed.events <- datastore.CommittedChanges{Checkpoint: testPosition(1)}
	var snapshotBegins int
	source := &coordinatorSource{
		plan: plan, records: []storage.Record{record("1"), record("2"), record("3"), record("4")},
		feed: feed, fence: testPosition(1), snapshotBegins: &snapshotBegins,
	}
	coordinator, err := StartCoordinator(ctx, source)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := coordinator.Subscribe(ctx, "viewer", storage.QuerySpec{}, InitialWindow{FromEnd: true, Count: 2}, 8); err != nil {
		t.Fatal(err)
	}
	defer installPendingQueryHydration(t, coordinator, storage.EntityKey{Relation: relation, Value: "pending"}, plan.Fingerprint())()

	before := snapshotBegins
	_, err = coordinator.QueueWindowUpdate(ctx, "viewer", []BoundaryChange{
		{Side: BoundaryLeft, Operation: BoundaryExtend, Count: 2},
		{Side: BoundaryRight, Operation: BoundaryContract, Count: 1},
	}, "atomic-discovery")
	if err != nil {
		t.Fatal(err)
	}
	// Both key lookups share one discovery snapshot; the second snapshot is the
	// fenced materialization required before the transition can be published.
	if used := snapshotBegins - before; used != 2 {
		t.Fatalf("window update used %d snapshots, want one discovery and one materialization", used)
	}
}

func TestCoordinatorDoesNotMixCachedAndDiscoveredBoundaryCounts(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	relation := storage.RelationKey{Source: "test", Name: "tasks"}
	plan := fakePlan{fingerprint: "all", relation: relation, routing: routing.RoutingPlan{Fallback: true}}
	record := func(id string) storage.Record {
		return storage.Record{"id": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte(id)}}
	}
	feed := newCoordinatorFeed()
	feed.events <- datastore.CommittedChanges{Checkpoint: testPosition(1)}
	source := &coordinatorSource{
		plan: plan, records: []storage.Record{record("1"), record("2"), record("3"), record("4")},
		feed: feed, fence: testPosition(1),
	}
	coordinator, err := StartCoordinator(ctx, source)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := coordinator.Subscribe(ctx, "viewer", storage.QuerySpec{}, InitialWindow{FromEnd: true, Count: 2}, 8); err != nil {
		t.Fatal(err)
	}

	reads := make(chan datastore.WindowRequest, 3)
	source.windowReads = reads
	bounds, err := coordinator.QueueWindowUpdate(ctx, "viewer", []BoundaryChange{
		{Side: BoundaryLeft, Operation: BoundaryExtend, Count: 1},
		{Side: BoundaryRight, Operation: BoundaryContract, Count: 1},
	}, "mixed")
	if err != nil {
		t.Fatal(err)
	}
	if len(reads) != 3 {
		t.Fatalf("window reads=%d, want both boundary discoveries plus materialization", len(reads))
	}
	if bounds.Left.Kind != datastore.BoundaryCursor || string(bounds.Left.Key.(datastore.EncodedSortKey)) != "1" || bounds.Right.Kind != datastore.BoundaryCursor || string(bounds.Right.Key.(datastore.EncodedSortKey)) != "3" {
		t.Fatalf("resolved bounds = %+v, want (1, 3]", bounds)
	}
}

func TestCoordinatorRejectsMultipleUpdatesToSameBoundary(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	relation := storage.RelationKey{Source: "test", Name: "tasks"}
	plan := fakePlan{fingerprint: "all", relation: relation, routing: routing.RoutingPlan{Fallback: true}}
	record := func(id string) storage.Record {
		return storage.Record{"id": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte(id)}}
	}
	source := &coordinatorSource{plan: plan, records: []storage.Record{record("1"), record("2"), record("3"), record("4")}, feed: newCoordinatorFeed()}
	coordinator, err := StartCoordinator(ctx, source)
	if err != nil {
		t.Fatal(err)
	}
	opened, err := coordinator.Subscribe(ctx, "viewer", storage.QuerySpec{}, InitialWindow{Count: 2}, 4)
	if err != nil {
		t.Fatal(err)
	}

	_, err = coordinator.QueueWindowUpdate(ctx, "viewer", []BoundaryChange{
		{Side: BoundaryRight, Operation: BoundaryExtend, Count: 2},
		{Side: BoundaryRight, Operation: BoundaryContract, Count: 1},
	}, "ambiguous")
	if err == nil {
		t.Fatal("expected repeated right-boundary update to be rejected")
	}
	active, ok := coordinator.subscriptions.Active("viewer")
	if !ok || compareBoundary(plan, active.Bounds.Left, opened.Bounds.Left) != 0 || compareBoundary(plan, active.Bounds.Right, opened.Bounds.Right) != 0 {
		t.Fatalf("rejected update changed active bounds: %+v", active.Bounds)
	}

	// Rejection must also release the in-progress token so a later valid update
	// can proceed normally.
	if _, err := coordinator.QueueWindowUpdate(ctx, "viewer", []BoundaryChange{{Side: BoundaryRight, Operation: BoundaryExtend, Count: 1}}, "valid"); err != nil {
		t.Fatalf("valid update after rejection: %v", err)
	}
}

func TestCoordinatorConsumesCDCWhileWindowUpdateSnapshotIsBlocked(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	relation := storage.RelationKey{Source: "test", Name: "tasks"}
	plan := fakePlan{fingerprint: "all", relation: relation, routing: routing.RoutingPlan{Fallback: true}}
	record := func(id, title string) storage.Record {
		return storage.Record{
			"id":    {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte(id)},
			"title": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte(title)},
		}
	}
	feed := newCoordinatorFeed()
	source := &coordinatorSource{plan: plan, records: []storage.Record{record("1", "old"), record("2", "two"), record("3", "three")}, feed: feed}
	coordinator, err := StartCoordinator(ctx, source)
	if err != nil {
		t.Fatal(err)
	}
	opened, err := coordinator.Subscribe(ctx, "viewer", storage.QuerySpec{}, InitialWindow{Count: 2}, 4)
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	source.loadStarted = started
	source.loadRelease = release
	updated := make(chan error, 1)
	go func() {
		_, err := coordinator.QueueWindowUpdate(ctx, "viewer", []BoundaryChange{{Side: BoundaryRight, Operation: BoundaryExtend, Count: 1}}, "extend")
		updated <- err
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("window snapshot did not start")
	}
	key := storage.EntityKey{Relation: relation, Value: "1"}
	feed.events <- datastore.CommittedChanges{Checkpoint: testPosition(1), Changes: []datastore.Change{
		fakeChange{kind: datastore.ChangeUpdate, relation: relation, before: key, after: key, record: record("1", "new"), complete: true},
	}}
	eventCtx, stop := context.WithTimeout(ctx, time.Second)
	batch, err := nextTestBatch(eventCtx, opened.Subscription)
	stop()
	if err != nil || len(batch.Events) != 1 || string(batch.Events[0].Record["title"].Data) != "new" {
		t.Fatalf("CDC was blocked by window snapshot: batch=%+v err=%v", batch, err)
	}
	close(release)
	if err := <-updated; err != nil {
		t.Fatal(err)
	}
	windowCtx, windowStop := context.WithTimeout(ctx, time.Second)
	defer windowStop()
	windowBatch, err := nextTestBatch(windowCtx, opened.Subscription)
	if err != nil || windowBatch.Window == nil || windowBatch.Window.Token != "extend" {
		t.Fatalf("queued window transition = %+v, err=%v", windowBatch, err)
	}
}

func TestCoordinatorResolvesInitialCountFromRawRows(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	relation := storage.RelationKey{Source: "test", Name: "tasks"}
	plan := fakePlan{fingerprint: "all", relation: relation, routing: routing.RoutingPlan{Fallback: true}}
	var records []storage.Record
	for i := 0; i < 70; i++ {
		records = append(records, storage.Record{
			"id":      {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte(fmt.Sprintf("%03d", i))},
			"visible": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("no")},
		})
	}
	for i := 70; i < 73; i++ {
		records = append(records, storage.Record{
			"id":      {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte(fmt.Sprintf("%03d", i))},
			"visible": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("yes")},
		})
	}
	source := &coordinatorSource{plan: plan, records: records, feed: newCoordinatorFeed()}
	coordinator, err := StartCoordinator(ctx, source)
	if err != nil {
		t.Fatal(err)
	}
	opened, err := coordinator.Subscribe(ctx, "viewer", storage.QuerySpec{}, InitialWindow{Count: 2}, 4)
	if err != nil {
		t.Fatal(err)
	}
	if len(opened.Initial) != 2 || opened.Initial[0].Key.Value != "000" || opened.Initial[1].Key.Value != "001" {
		t.Fatalf("initial window = %+v", opened.Initial)
	}
	compared, compareErr := plan.Compare(opened.Bounds.Right.Key, datastore.EncodedSortKey("001"))
	if opened.Bounds.Right.Kind != datastore.BoundaryCursor || compareErr != nil || compared != 0 {
		t.Fatalf("right boundary was not resolved from raw rows: %+v", opened.Bounds.Right)
	}
}

func TestCoordinatorRollsBackStagedWindowWhenLaterOperationFails(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	relation := storage.RelationKey{Source: "test", Name: "tasks"}
	plan := fakePlan{fingerprint: "all", relation: relation, routing: routing.RoutingPlan{Fallback: true}}
	record := func(id string) storage.Record {
		return storage.Record{"id": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte(id)}}
	}
	source := &coordinatorSource{plan: plan, records: []storage.Record{record("1"), record("2"), record("3")}, feed: newCoordinatorFeed()}
	coordinator, err := StartCoordinator(ctx, source)
	if err != nil {
		t.Fatal(err)
	}
	opened, err := coordinator.Subscribe(ctx, "viewer", storage.QuerySpec{}, InitialWindow{Count: 2}, 4)
	if err != nil {
		t.Fatal(err)
	}
	_, err = coordinator.QueueWindowUpdate(ctx, "viewer", []BoundaryChange{
		{Side: BoundaryRight, Operation: BoundaryExtend, Count: 1},
		{Side: BoundaryRight},
	}, "invalid")
	if err == nil {
		t.Fatal("expected the second operation to fail")
	}
	active, ok := coordinator.subscriptions.Active("viewer")
	if !ok {
		t.Fatal("viewer subscription is not active")
	}
	if compareBoundary(plan, active.Bounds.Right, opened.Bounds.Right) != 0 || len(coordinator.queryEntities(plan.Fingerprint())) != 2 {
		t.Fatalf("failed update changed installed window: bounds=%+v entities=%v", active.Bounds, coordinator.queryEntities(plan.Fingerprint()))
	}
}

func TestCoordinatorEvictsIdleEntitiesOnSchedule(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	relation := storage.RelationKey{Source: "test", Name: "tasks"}
	plan := fakePlan{fingerprint: "all", relation: relation, routing: routing.RoutingPlan{Fallback: true}}
	var records []storage.Record
	for n := 0; n < 100; n++ {
		records = append(records, storage.Record{"id": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte(fmt.Sprint(n))}})
	}
	source := &coordinatorSource{plan: plan, records: records, feed: newCoordinatorFeed()}
	// One periodic tick must drain the backlog through separate actor turns,
	// rather than taking one batch per tick (which would take 20 seconds).
	coordinator, err := StartCoordinatorWithOptions(ctx, source, CoordinatorOptions{Limits: CoordinatorLimits{EntityIdleTTL: time.Millisecond, EvictionPeriod: 200 * time.Millisecond, EvictionBatchSize: 1, QueryIdleTTL: time.Millisecond, QueryMaintenancePeriod: 5 * time.Millisecond}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := coordinator.Subscribe(ctx, "viewer", storage.QuerySpec{}, InitialWindow{Count: 100}, 4); err != nil {
		t.Fatal(err)
	}
	if _, err := coordinator.Unsubscribe(ctx, "viewer"); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		remaining := false
		for n := 0; n < 100; n++ {
			if _, exists := coordinator.entities.Get(storage.EntityKey{Relation: relation, Value: fmt.Sprint(n)}); exists {
				remaining = true
				break
			}
		}
		if !remaining {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("idle entity was not evicted")
}

func TestCoordinatorReleasesSlowSubscriptionAfterQueueOverflow(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	relation := storage.RelationKey{Source: "test", Name: "tasks"}
	plan := fakePlan{fingerprint: "all", relation: relation, routing: routing.RoutingPlan{Fallback: true}}
	record := func(title string) storage.Record {
		return storage.Record{"id": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("1")}, "title": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte(title)}}
	}
	feed := newCoordinatorFeed()
	source := &coordinatorSource{plan: plan, records: []storage.Record{record("old")}, feed: feed}
	coordinator, err := StartCoordinator(ctx, source)
	if err != nil {
		t.Fatal(err)
	}
	opened, err := coordinator.Subscribe(ctx, "slow", storage.QuerySpec{}, InitialWindow{Count: 1}, 1)
	if err != nil {
		t.Fatal(err)
	}
	key := storage.EntityKey{Relation: relation, Value: "1"}
	for i := 1; i <= 4; i++ {
		feed.events <- datastore.CommittedChanges{Checkpoint: testPosition(i), Changes: []datastore.Change{
			fakeChange{kind: datastore.ChangeUpdate, relation: relation, before: key, after: key, record: record(fmt.Sprint(i)), complete: true},
		}}
	}
	time.Sleep(10 * time.Millisecond)
	nextCtx, stop := context.WithTimeout(ctx, time.Second)
	defer stop()
	if _, err := nextTestBatch(nextCtx, opened.Subscription); !errors.Is(err, ErrClientQueueFull) {
		t.Fatalf("slow client did not receive resync-required closure: %v", err)
	}
	if _, err := coordinator.Unsubscribe(ctx, "slow"); err == nil {
		t.Fatal("overflowed subscription remained active in the coordinator")
	}
}

func TestCoordinatorRelationFailureCannotReuseStaleWarmEntity(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	relation := storage.RelationKey{Source: "test", Name: "tasks"}
	plan := fakePlan{fingerprint: "all", relation: relation, routing: routing.RoutingPlan{Fallback: true}}
	record := func(title string) storage.Record {
		return storage.Record{"id": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("1")}, "title": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte(title)}}
	}
	feed := newCoordinatorFeed()
	source := &coordinatorSource{plan: plan, records: []storage.Record{record("old")}, feed: feed}
	coordinator, err := StartCoordinator(ctx, source)
	if err != nil {
		t.Fatal(err)
	}
	opened, err := coordinator.Subscribe(ctx, "first", storage.QuerySpec{}, InitialWindow{Count: 1}, 4)
	if err != nil {
		t.Fatal(err)
	}
	key := storage.EntityKey{Relation: relation, Value: "1"}
	feed.events <- datastore.CommittedChanges{Checkpoint: testPosition(1), Changes: []datastore.Change{
		fakeChange{kind: datastore.ChangeUpdate, relation: relation, before: key, after: key, beforeErr: errors.New("decode identity failed")},
	}}
	errCtx, stop := context.WithTimeout(ctx, time.Second)
	_, queueErr := nextTestBatch(errCtx, opened.Subscription)
	stop()
	if queueErr == nil {
		t.Fatal("relation failure did not close its subscriber")
	}
	source.records = []storage.Record{record("fresh")}
	recovered, err := coordinator.Subscribe(ctx, "recovered", storage.QuerySpec{}, InitialWindow{Count: 1}, 4)
	if err != nil {
		t.Fatal(err)
	}
	if len(recovered.Initial) != 1 || string(recovered.Initial[0].Record["title"].Data) != "fresh" {
		t.Fatalf("recovery reused stale cache: %+v", recovered.Initial)
	}
}

func TestCoordinatorIsolatesBadRelationWithinCommittedTransaction(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	badRelation := storage.RelationKey{Source: "test", Name: "broken_tasks"}
	goodRelation := storage.RelationKey{Source: "test", Name: "healthy_tasks"}
	record := func(id, title string) storage.Record {
		return storage.Record{
			"id":    {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte(id)},
			"title": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte(title)},
		}
	}
	feed := newCoordinatorFeed()
	source := &coordinatorSource{
		plan:    fakePlan{fingerprint: "broken", relation: badRelation, routing: routing.RoutingPlan{Fallback: true}},
		records: []storage.Record{record("bad", "old-bad")},
		feed:    feed,
	}
	coordinator, err := StartCoordinator(ctx, source)
	if err != nil {
		t.Fatal(err)
	}
	broken, err := coordinator.Subscribe(ctx, "broken", storage.QuerySpec{}, InitialWindow{Count: 1}, 4)
	if err != nil {
		t.Fatal(err)
	}
	source.plan = fakePlan{fingerprint: "healthy", relation: goodRelation, routing: routing.RoutingPlan{Fallback: true}}
	source.records = []storage.Record{record("good", "old-good")}
	healthy, err := coordinator.Subscribe(ctx, "healthy", storage.QuerySpec{}, InitialWindow{Count: 1}, 4)
	if err != nil {
		t.Fatal(err)
	}

	badKey := storage.EntityKey{Relation: badRelation, Value: "bad"}
	goodKey := storage.EntityKey{Relation: goodRelation, Value: "good"}
	feed.events <- datastore.CommittedChanges{
		Checkpoint: testPosition(1),
		Atomic:     true,
		Changes: []datastore.Change{
			fakeChange{kind: datastore.ChangeUpdate, relation: badRelation, before: badKey, after: badKey, beforeErr: errors.New("decode identity failed")},
			fakeChange{kind: datastore.ChangeUpdate, relation: goodRelation, before: goodKey, after: goodKey, record: record("good", "new-good"), complete: true},
		},
	}

	readCtx, stop := context.WithTimeout(ctx, time.Second)
	defer stop()
	if _, err := nextTestBatch(readCtx, broken.Subscription); err == nil {
		t.Fatal("broken relation subscription remained active")
	}
	batch, err := nextTestBatch(readCtx, healthy.Subscription)
	if err != nil {
		t.Fatalf("healthy relation did not receive its transaction change: %v", err)
	}
	if batch.Atomic {
		t.Fatal("partially isolated source transaction was reported as atomic")
	}
	if len(batch.Events) != 1 || string(batch.Events[0].Record["title"].Data) != "new-good" {
		t.Fatalf("healthy relation batch = %+v", batch)
	}
	deadline := time.Now().Add(time.Second)
	for feed.acknowledged() < testPosition(1) && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := feed.acknowledged(); got != testPosition(1) {
		t.Fatalf("acknowledged checkpoint = %v, want 1", got)
	}

	feed.events <- datastore.CommittedChanges{
		Checkpoint: testPosition(2),
		Atomic:     true,
		Changes: []datastore.Change{
			fakeChange{kind: datastore.ChangeUpdate, relation: goodRelation, before: goodKey, after: goodKey, record: record("good", "newer-good"), complete: true},
		},
	}
	batch, err = nextTestBatch(readCtx, healthy.Subscription)
	if err != nil || len(batch.Events) != 1 || string(batch.Events[0].Record["title"].Data) != "newer-good" {
		t.Fatalf("coordinator did not survive relation failure: batch=%+v err=%v", batch, err)
	}
}

func TestCoordinatorPublishesTruncateWithoutRelationResnapshot(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	relation := storage.RelationKey{Source: "test", Name: "tasks"}
	record := storage.Record{"id": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("1")}}
	feed := newCoordinatorFeed()
	source := &coordinatorSource{
		plan:    fakePlan{fingerprint: "all", relation: relation, routing: routing.RoutingPlan{Fallback: true}},
		records: []storage.Record{record},
		feed:    feed,
	}
	coordinator, err := StartCoordinator(ctx, source)
	if err != nil {
		t.Fatal(err)
	}
	opened, err := coordinator.Subscribe(ctx, "viewer", storage.QuerySpec{}, InitialWindow{Count: 10}, 8)
	if err != nil {
		t.Fatal(err)
	}
	// Any old relation-recovery implementation would now fail. First-class
	// truncate processing must not open another snapshot.
	source.snapshotErr = errors.New("unexpected truncate resnapshot")
	feed.events <- datastore.CommittedChanges{Checkpoint: testPosition(1), Atomic: true, Changes: []datastore.Change{
		fakeChange{kind: datastore.ChangeClearRelation, relation: relation},
	}}
	readCtx, stop := context.WithTimeout(ctx, time.Second)
	defer stop()
	batch, err := nextTestBatch(readCtx, opened.Subscription)
	if err != nil {
		t.Fatalf("truncate delivery: %v", err)
	}
	if !batch.Clear || batch.ClearChangeIndex != 1 || len(batch.Events) != 0 || !batch.Atomic {
		t.Fatalf("truncate batch = %+v", batch)
	}
	if coordinator.hasTerminalError() {
		t.Fatalf("truncate stopped coordinator: %v", coordinator.currentTerminalError())
	}
}

func TestCoordinatorTruncateCancelsBlockedHydrationWithoutResurrection(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	relation := storage.RelationKey{Source: "test", Name: "tasks"}
	record := func(title string) storage.Record {
		return storage.Record{
			"id":    {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("1")},
			"title": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte(title)},
		}
	}
	feed := newCoordinatorFeed()
	source := &coordinatorSource{
		plan:    fakePlan{fingerprint: "all", relation: relation, routing: routing.RoutingPlan{Fallback: true}},
		records: []storage.Record{record("old")},
		feed:    feed,
	}
	coordinator, err := StartCoordinator(ctx, source)
	if err != nil {
		t.Fatal(err)
	}
	opened, err := coordinator.Subscribe(ctx, "viewer", storage.QuerySpec{}, InitialWindow{Count: 10}, 8)
	if err != nil {
		t.Fatal(err)
	}

	started := make(chan struct{}, 1)
	release := make(chan struct{})
	source.records = []storage.Record{record("stale hydration")}
	source.loadStarted, source.loadRelease = started, release
	key := storage.EntityKey{Relation: relation, Value: "1"}
	feed.events <- datastore.CommittedChanges{Checkpoint: testPosition(1), Changes: []datastore.Change{
		fakeChange{kind: datastore.ChangeUpdate, relation: relation, before: key, after: key},
	}}

	readCtx, stop := context.WithTimeout(ctx, time.Second)
	defer stop()
	pending, err := nextTestBatch(readCtx, opened.Subscription)
	if err != nil || len(pending.Events) != 1 || !pending.Events[0].Pending {
		t.Fatalf("pending hydration event = %+v, err=%v", pending, err)
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("hydration did not start")
	}

	feed.events <- datastore.CommittedChanges{Checkpoint: testPosition(2), Atomic: true, Changes: []datastore.Change{
		fakeChange{kind: datastore.ChangeClearRelation, relation: relation},
	}}
	cleared, err := nextTestBatch(readCtx, opened.Subscription)
	if err != nil || !cleared.Clear || cleared.ClearChangeIndex != 1 {
		t.Fatalf("truncate while hydration blocked = %+v, err=%v", cleared, err)
	}

	close(release)
	noResurrectionCtx, noResurrectionCancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer noResurrectionCancel()
	if resurrected, err := nextTestBatch(noResurrectionCtx, opened.Subscription); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("stale hydration was published after truncate: batch=%+v err=%v", resurrected, err)
	}
	if coordinator.hasTerminalError() {
		t.Fatalf("truncate during hydration stopped coordinator: %v", coordinator.currentTerminalError())
	}
}

func TestCoordinatorTruncateDuringRelationResyncDoesNotResurrectSnapshotRows(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	relation := storage.RelationKey{Source: "test", Name: "tasks"}
	record := storage.Record{"id": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("1")}}
	feed := newCoordinatorFeed()
	source := &coordinatorSource{
		plan:    fakePlan{fingerprint: "all", relation: relation, routing: routing.RoutingPlan{Fallback: true}},
		records: []storage.Record{record},
		feed:    feed,
	}
	coordinator, err := StartCoordinator(ctx, source)
	if err != nil {
		t.Fatal(err)
	}
	opened, err := coordinator.Subscribe(ctx, "viewer", storage.QuerySpec{}, InitialWindow{Count: 10}, 8)
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	source.loadStarted, source.loadRelease = started, release
	feed.events <- datastore.CommittedChanges{Checkpoint: testPosition(1), Changes: []datastore.Change{
		fakeChange{kind: datastore.ChangeInvalidate, relation: relation},
	}}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("relation resync snapshot did not start")
	}
	feed.events <- datastore.CommittedChanges{Checkpoint: testPosition(2), Atomic: true, Changes: []datastore.Change{
		fakeChange{kind: datastore.ChangeClearRelation, relation: relation},
	}}
	deadline := time.Now().Add(time.Second)
	for !feed.Covers(testPosition(2)) && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if !feed.Covers(testPosition(2)) {
		t.Fatal("truncate was not buffered while resync snapshot was blocked")
	}
	close(release)
	readCtx, stop := context.WithTimeout(ctx, time.Second)
	defer stop()
	batch, err := nextTestBatch(readCtx, opened.Subscription)
	if err != nil || !batch.Clear || len(batch.Events) != 0 {
		t.Fatalf("truncate recovered through resync = %+v, err=%v", batch, err)
	}
	quietCtx, quietCancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer quietCancel()
	if resurrected, err := nextTestBatch(quietCtx, opened.Subscription); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("stale resync snapshot row was resurrected: %+v, err=%v", resurrected, err)
	}
}

func TestCoordinatorRelationInvalidationSupersedesOlderEntityHydration(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	relation := storage.RelationKey{Source: "test", Name: "tasks"}
	record := func(title string) storage.Record {
		return storage.Record{
			"id":    {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("1")},
			"title": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte(title)},
		}
	}
	feed := newCoordinatorFeed()
	source := &coordinatorSource{
		plan:    fakePlan{fingerprint: "all", relation: relation, routing: routing.RoutingPlan{Fallback: true}},
		records: []storage.Record{record("initial")},
		feed:    feed,
	}
	coordinator, err := StartCoordinator(ctx, source)
	if err != nil {
		t.Fatal(err)
	}
	opened, err := coordinator.Subscribe(ctx, "viewer", storage.QuerySpec{}, InitialWindow{Count: 10}, 8)
	if err != nil {
		t.Fatal(err)
	}

	hydrationStarted := make(chan struct{}, 1)
	hydrationRelease := make(chan struct{})
	hydrationCanceled := make(chan struct{}, 1)
	source.records = []storage.Record{record("obsolete hydration")}
	source.loadStarted = hydrationStarted
	source.loadRelease = hydrationRelease
	source.loadCanceled = hydrationCanceled
	key := storage.EntityKey{Relation: relation, Value: "1"}
	partial := storage.Record{
		"id":    {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("1")},
		"title": {State: storage.ValueUnavailable},
	}
	feed.events <- datastore.CommittedChanges{Checkpoint: testPosition(1), Changes: []datastore.Change{
		fakeChange{kind: datastore.ChangeUpdate, relation: relation, before: key, after: key, record: partial},
	}}
	readCtx, stop := context.WithTimeout(ctx, time.Second)
	defer stop()
	if batch, err := nextTestBatch(readCtx, opened.Subscription); err != nil || len(batch.Events) != 1 || !batch.Events[0].Pending {
		t.Fatalf("partial update = %+v, err=%v", batch, err)
	}
	select {
	case <-hydrationStarted:
	case <-time.After(time.Second):
		t.Fatal("entity hydration did not start")
	}

	resyncStarted := make(chan struct{}, 1)
	source.records = []storage.Record{record("authoritative refresh")}
	source.loadStarted = resyncStarted
	source.loadRelease = nil
	feed.events <- datastore.CommittedChanges{Checkpoint: testPosition(2), Changes: []datastore.Change{
		fakeChange{kind: datastore.ChangeInvalidate, relation: relation},
	}}
	select {
	case <-hydrationCanceled:
	case <-time.After(time.Second):
		t.Fatal("relation invalidation did not cancel the older entity hydration")
	}
	select {
	case <-resyncStarted:
	case <-time.After(time.Second):
		t.Fatal("authoritative relation refresh did not start")
	}
	batch, err := nextTestBatch(readCtx, opened.Subscription)
	if err != nil || len(batch.Events) != 1 || batch.Events[0].Pending || string(batch.Events[0].Record["title"].Data) != "authoritative refresh" {
		t.Fatalf("relation refresh result = %+v, err=%v", batch, err)
	}
	close(hydrationRelease)
}

func TestPruneCoverageDefersPendingEntityUntilHydration(t *testing.T) {
	relation := storage.RelationKey{Source: "test", Name: "tasks"}
	key := storage.EntityKey{Relation: relation, Value: "1"}
	entities := NewEntityCache()
	queries := NewQueryRegistry(entities)
	plan := fakePlan{fingerprint: "all", relation: relation, routing: routing.RoutingPlan{Fallback: true}}
	queries.Acquire(plan)
	if err := queries.SetWindow("all", "viewer", datastore.Bounds{Left: datastore.Boundary{Kind: datastore.BoundaryStart}, Right: datastore.Boundary{Kind: datastore.BoundaryEnd}}); err != nil {
		t.Fatal(err)
	}
	entities.StorePartial(key, storage.Record{"id": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("1")}})
	if _, _, err := queries.AddMember("all", key); err != nil {
		t.Fatal(err)
	}
	coordinator := &Coordinator{entities: entities, queries: queries}
	if err := coordinator.pruneCoverage("all"); err != nil {
		t.Fatalf("prune evaluated a pending entity: %v", err)
	}
	if !queries.IsMember("all", key) {
		t.Fatal("prune removed pending entity before hydration")
	}
}

func TestCoordinatorRelationResyncHydratesBufferedPartialRows(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	relation := storage.RelationKey{Source: "test", Name: "tasks"}
	plan := fakePlan{fingerprint: "all", relation: relation, routing: routing.RoutingPlan{Fallback: true}}
	record := func(id, document string) storage.Record {
		return storage.Record{
			"id":       {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte(id)},
			"project":  {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("a")},
			"document": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte(document)},
		}
	}
	feed := newCoordinatorFeed()
	source := &coordinatorSource{plan: plan, records: []storage.Record{record("1", "old")}, feed: feed}
	coordinator, err := StartCoordinator(ctx, source)
	if err != nil {
		t.Fatal(err)
	}
	opened, err := coordinator.Subscribe(ctx, "viewer", storage.QuerySpec{}, InitialWindow{Count: 10}, 8)
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	source.loadStarted, source.loadRelease = started, release
	feed.events <- datastore.CommittedChanges{Checkpoint: testPosition(1), Changes: []datastore.Change{
		fakeChange{kind: datastore.ChangeInvalidate, relation: relation},
	}}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("relation resync snapshot did not start")
	}
	full := record("2", "hydrated")
	source.records = []storage.Record{record("1", "old"), full}
	key := storage.EntityKey{Relation: relation, Value: "2"}
	partial := storage.Record{
		"id":       {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("2")},
		"project":  {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("a")},
		"document": {State: storage.ValueUnavailable},
	}
	feed.events <- datastore.CommittedChanges{Checkpoint: testPosition(2), Changes: []datastore.Change{
		fakeChange{kind: datastore.ChangeInsert, relation: relation, before: key, after: key, record: partial},
	}}
	close(release)
	eventCtx, stop := context.WithTimeout(ctx, time.Second)
	defer stop()
	pending, err := nextTestBatch(eventCtx, opened.Subscription)
	if err != nil || len(pending.Events) != 1 || pending.Events[0].Key != key || !pending.Events[0].Pending {
		t.Fatalf("resync pending event = %+v, err=%v", pending, err)
	}
	hydrated, err := nextTestBatch(eventCtx, opened.Subscription)
	if err != nil || len(hydrated.Events) != 1 || hydrated.Events[0].Pending || string(hydrated.Events[0].Record["document"].Data) != "hydrated" {
		t.Fatalf("resync hydration event = %+v, err=%v", hydrated, err)
	}
	// A completed resync keeps existing subscriptions alive. Their next window
	// operation must capture the new relation epoch rather than remaining
	// permanently fenced by the epoch at subscription creation.
	if _, err := coordinator.QueueWindowUpdate(ctx, "viewer", []BoundaryChange{{Side: BoundaryRight, Operation: BoundaryExtend, Count: 1}}, "after-resync"); err != nil {
		t.Fatalf("window update after completed resync: %v", err)
	}
	windowBatch, err := nextTestBatch(eventCtx, opened.Subscription)
	if err != nil || windowBatch.Window == nil || windowBatch.Window.Token != "after-resync" {
		t.Fatalf("window transition after completed resync = %+v, err=%v", windowBatch, err)
	}
}

func TestCoordinatorRelationResyncPagesAcrossStableCoverage(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	relation := storage.RelationKey{Source: "test", Name: "tasks"}
	plan := fakePlan{fingerprint: "all", relation: relation, routing: routing.RoutingPlan{Fallback: true}}
	record := func(id string) storage.Record {
		return storage.Record{"id": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte(id)}}
	}
	feed := newCoordinatorFeed()
	source := &coordinatorSource{plan: plan, records: []storage.Record{record("1"), record("2")}, feed: feed}
	coordinator, err := StartCoordinatorWithOptions(ctx, source, CoordinatorOptions{Limits: CoordinatorLimits{MaxWindowRows: 2, SnapshotPageRows: 2}})
	if err != nil {
		t.Fatal(err)
	}
	opened, err := coordinator.Subscribe(ctx, "viewer", storage.QuerySpec{}, InitialWindow{Count: 2}, 8)
	if err != nil {
		t.Fatal(err)
	}

	// Counts are soft, so rows that later enter the stable (START, "2"]
	// coverage make its shared refresh larger than one subscription window.
	source.records = []storage.Record{record("0a"), record("0b"), record("1"), record("2")}
	feed.events <- datastore.CommittedChanges{Checkpoint: testPosition(1), Atomic: true, Changes: []datastore.Change{
		fakeChange{kind: datastore.ChangeInvalidate, relation: relation},
	}}

	readCtx, stop := context.WithTimeout(ctx, time.Second)
	defer stop()
	batch, err := nextTestBatch(readCtx, opened.Subscription)
	if err != nil {
		t.Fatalf("paged relation refresh failed: %v", err)
	}
	if batch.Atomic {
		t.Fatal("asynchronous relation refresh was reported as an atomic source transaction")
	}
	added := make(map[string]struct{})
	for _, event := range batch.Events {
		if event.Kind != QueryAdded {
			t.Fatalf("paged relation refresh emitted an unexpected event: %+v", event)
		}
		added[event.Key.Value] = struct{}{}
	}
	if len(added) != 2 {
		t.Fatalf("paged relation refresh additions = %v, want 0a and 0b", added)
	}
	if _, ok := added["0a"]; !ok {
		t.Fatalf("paged relation refresh additions = %v, missing 0a", added)
	}
	if _, ok := added["0b"]; !ok {
		t.Fatalf("paged relation refresh additions = %v, missing 0b", added)
	}
}

func TestCoordinatorRelationResyncIsolatesQueryEvaluationFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	relation := storage.RelationKey{Source: "test", Name: "tasks"}
	failBadQuery := false
	badPlan := fakePlan{
		fingerprint: "bad", relation: relation, routing: routing.RoutingPlan{Fallback: true},
		matchErr: func() error {
			if failBadQuery {
				return errors.New("bad query evaluation")
			}
			return nil
		},
	}
	healthyPlan := fakePlan{fingerprint: "healthy", relation: relation, routing: routing.RoutingPlan{Fallback: true}}
	record := func(title string) storage.Record {
		return storage.Record{
			"id":    {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("1")},
			"title": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte(title)},
		}
	}
	feed := newCoordinatorFeed()
	source := &coordinatorSource{plan: badPlan, records: []storage.Record{record("old")}, feed: feed}
	coordinator, err := StartCoordinator(ctx, source)
	if err != nil {
		t.Fatal(err)
	}
	broken, err := coordinator.Subscribe(ctx, "broken", storage.QuerySpec{}, InitialWindow{Count: 1}, 8)
	if err != nil {
		t.Fatal(err)
	}
	source.plan = healthyPlan
	healthy, err := coordinator.Subscribe(ctx, "healthy", storage.QuerySpec{}, InitialWindow{Count: 1}, 8)
	if err != nil {
		t.Fatal(err)
	}

	failBadQuery = true
	feed.events <- datastore.CommittedChanges{Checkpoint: testPosition(1), Changes: []datastore.Change{
		fakeChange{kind: datastore.ChangeInvalidate, relation: relation},
	}}
	readCtx, stop := context.WithTimeout(ctx, time.Second)
	defer stop()
	if _, err := nextTestBatch(readCtx, broken.Subscription); err == nil || !strings.Contains(err.Error(), "bad query evaluation") {
		t.Fatalf("broken query error = %v", err)
	}

	key := storage.EntityKey{Relation: relation, Value: "1"}
	feed.events <- datastore.CommittedChanges{Checkpoint: testPosition(2), Atomic: true, Changes: []datastore.Change{
		fakeChange{kind: datastore.ChangeUpdate, relation: relation, before: key, after: key, record: record("new"), complete: true},
	}}
	batch, err := nextTestBatch(readCtx, healthy.Subscription)
	if err != nil || len(batch.Events) != 1 || string(batch.Events[0].Record["title"].Data) != "new" {
		t.Fatalf("healthy query after isolated resync failure = %+v, err=%v", batch, err)
	}
	if coordinator.hasTerminalError() {
		t.Fatalf("isolated resync query failure stopped coordinator: %v", coordinator.currentTerminalError())
	}
}

func TestCoordinatorRelationResyncIsolatesEarlyQueryReadFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	relation := storage.RelationKey{Source: "test", Name: "tasks"}
	badPlan := fakePlan{fingerprint: "bad-read", relation: relation, routing: routing.RoutingPlan{Fallback: true}}
	healthyPlan := fakePlan{fingerprint: "healthy-read", relation: relation, routing: routing.RoutingPlan{Fallback: true}}
	record := func(title string) storage.Record {
		return storage.Record{
			"id":    {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("1")},
			"title": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte(title)},
		}
	}
	feed := newCoordinatorFeed()
	source := &coordinatorSource{plan: badPlan, records: []storage.Record{record("old")}, feed: feed}
	coordinator, err := StartCoordinator(ctx, source)
	if err != nil {
		t.Fatal(err)
	}
	broken, err := coordinator.Subscribe(ctx, "broken-read", storage.QuerySpec{}, InitialWindow{Count: 1}, 8)
	if err != nil {
		t.Fatal(err)
	}
	source.plan = healthyPlan
	healthy, err := coordinator.Subscribe(ctx, "healthy-read", storage.QuerySpec{}, InitialWindow{Count: 1}, 8)
	if err != nil {
		t.Fatal(err)
	}

	readFailure := errors.New("bad query snapshot SQL")
	source.queryErrors = map[datastore.QueryFingerprint]error{badPlan.Fingerprint(): readFailure}
	feed.events <- datastore.CommittedChanges{Checkpoint: testPosition(1), Changes: []datastore.Change{
		fakeChange{kind: datastore.ChangeInvalidate, relation: relation},
	}}
	readCtx, stop := context.WithTimeout(ctx, time.Second)
	defer stop()
	if _, err := nextTestBatch(readCtx, broken.Subscription); err == nil || !strings.Contains(err.Error(), readFailure.Error()) {
		t.Fatalf("broken query error = %v", err)
	}

	key := storage.EntityKey{Relation: relation, Value: "1"}
	feed.events <- datastore.CommittedChanges{Checkpoint: testPosition(2), Atomic: true, Changes: []datastore.Change{
		fakeChange{kind: datastore.ChangeUpdate, relation: relation, before: key, after: key, record: record("new"), complete: true},
	}}
	batch, err := nextTestBatch(readCtx, healthy.Subscription)
	if err != nil || len(batch.Events) != 1 || string(batch.Events[0].Record["title"].Data) != "new" {
		t.Fatalf("healthy query after isolated read failure = %+v, err=%v", batch, err)
	}
	if coordinator.hasTerminalError() {
		t.Fatalf("isolated recovery read failure stopped coordinator: %v", coordinator.currentTerminalError())
	}
}

func TestCoordinatorRejectsSubscriptionAndWindowChangeDuringRelationResync(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	relation := storage.RelationKey{Source: "test", Name: "tasks"}
	plan := fakePlan{fingerprint: "all", relation: relation, routing: routing.RoutingPlan{Fallback: true}}
	record := storage.Record{"id": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("1")}}
	feed := newCoordinatorFeed()
	source := &coordinatorSource{plan: plan, records: []storage.Record{record}, feed: feed}
	coordinator, err := StartCoordinator(ctx, source)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := coordinator.Subscribe(ctx, "viewer", storage.QuerySpec{}, InitialWindow{Count: 1}, 8); err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	defer close(release)
	source.loadStarted, source.loadRelease = started, release
	feed.events <- datastore.CommittedChanges{Checkpoint: testPosition(1), Changes: []datastore.Change{
		fakeChange{kind: datastore.ChangeInvalidate, relation: relation},
	}}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("relation resync snapshot did not start")
	}
	if _, err := coordinator.Subscribe(ctx, "late", storage.QuerySpec{}, InitialWindow{Count: 1}, 8); !errors.Is(err, storage.ErrResumeRequired) {
		t.Fatalf("subscription during resync error = %v", err)
	}
	if _, err := coordinator.QueueWindowUpdate(ctx, "viewer", []BoundaryChange{{Side: BoundaryRight, Operation: BoundaryExtend, Count: 1}}, "during-resync"); !errors.Is(err, storage.ErrResumeRequired) {
		t.Fatalf("window update during resync error = %v", err)
	}
}
