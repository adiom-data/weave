package projection

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/adiom-data/weave/internal/datastore"
	"github.com/adiom-data/weave/internal/engine"
	"github.com/adiom-data/weave/internal/engine/changeprocessor"
	"github.com/adiom-data/weave/internal/engine/delivery"
	"github.com/adiom-data/weave/internal/engine/deliverytest"
	"github.com/adiom-data/weave/internal/engine/entitycache"
	querylocal "github.com/adiom-data/weave/internal/querysubscription/local"
	"github.com/adiom-data/weave/internal/querysubscription/model"
	sourceposition "github.com/adiom-data/weave/internal/querysubscription/position"
	"github.com/adiom-data/weave/internal/storage"
)

type fakePlan struct {
	fingerprint datastore.QueryFingerprint
	relation    storage.RelationKey
}

type projectionTestCheckpoint string

func (projectionTestCheckpoint) StorageCheckpoint()       {}
func (projectionTestCheckpoint) Source() storage.SourceID { return "test" }
func (p projectionTestCheckpoint) StableID() string       { return string(p) }
func (p projectionTestCheckpoint) Order() uint64 {
	parts := strings.Split(string(p), "-")
	value, _ := strconv.ParseUint(parts[len(parts)-1], 10, 64)
	return value
}

func (p fakePlan) Fingerprint() datastore.QueryFingerprint { return p.fingerprint }
func (p fakePlan) Relation() storage.RelationKey           { return p.relation }
func (p fakePlan) EntityKey(record storage.Record) (storage.EntityKey, error) {
	return storage.EntityKey{Relation: p.relation, Value: string(record["id"].Data)}, nil
}
func (p fakePlan) Matches(storage.Record) (bool, error) { return true, nil }
func (p fakePlan) Key(record storage.Record) (datastore.SortKey, error) {
	return datastore.EncodedSortKey(record["id"].Data), nil
}
func (p fakePlan) Compare(a, b datastore.SortKey) (int, error) {
	return bytes.Compare(a.(datastore.EncodedSortKey), b.(datastore.EncodedSortKey)), nil
}
func (p fakePlan) EncodeKey(key datastore.SortKey) ([]byte, error) {
	return append([]byte(nil), key.(datastore.EncodedSortKey)...), nil
}
func (p fakePlan) DecodeKey(encoded []byte) (datastore.SortKey, error) {
	return datastore.EncodedSortKey(append([]byte(nil), encoded...)), nil
}
func (p fakePlan) Query() storage.QuerySpec { return storage.QuerySpec{} }

type projectionMock struct {
	mu            sync.Mutex
	subscriptions []*deliverytest.Interest
	childParents  []string
	unsubscribed  int
	blockParent   string
	childStarted  chan struct{}
	childRelease  chan struct{}
	groupQueue    *delivery.ClientQueue[delivery.GroupMessage]
}

func (m *projectionMock) OpenGroup(ctx context.Context, _ string, limit int) (*delivery.ClientQueue[delivery.GroupMessage], error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.groupQueue = delivery.NewClientQueue[delivery.GroupMessage](limit)
	return m.groupQueue, nil
}

func (m *projectionMock) SubscribeEntities(ctx context.Context, group string, requests []engine.EntitySubscribe, install func(int, engine.SubscribeResult)) {
	for i, request := range requests {
		opened, err := m.SubscribeGroupedWithFetch(ctx, request.ID, group, request.Query, engine.InitialWindow{Count: 1}, engine.FetchOptions{Entity: true, From: request.From})
		install(i, engine.SubscribeResult{Opened: opened, Err: err})
	}
}

func (*projectionMock) ReportProgress(context.Context, string) (*delivery.ProgressReport, error) {
	return nil, nil
}

func projectionSession(t *testing.T, coordinator *projectionMock) querysubscription.Feed {
	return projectionSessionWithLimit(t, coordinator, 32)
}

func projectionSessionWithLimit(t *testing.T, coordinator *projectionMock, limit int) querysubscription.Feed {
	t.Helper()
	session, err := querylocal.New(coordinator).OpenFeed(context.Background(), querysubscription.FeedOptions{BufferLimit: limit})
	if err != nil {
		t.Fatalf("open query feed: %v", err)
	}
	t.Cleanup(func() { _ = session.Close(context.Background()) })
	return session
}

func startTestProjection(ctx context.Context, queries querysubscription.Feed, root *ProjectionNode, options StartOptions) (*Projection, []ProjectionEvent, error) {
	p, err := BeginProjection(ctx, queries, root, options)
	if err != nil {
		return nil, nil, err
	}
	go pumpProjectionFeed(p, queries)
	initial, _, err := p.TakeInitial(ctx)
	if err != nil {
		p.Close()
		return nil, nil, err
	}
	if err := p.releaseOutput(p.queue.Offer); err != nil {
		p.Close()
		return nil, nil, err
	}
	return p, initial, nil
}

func pumpProjectionFeed(p *Projection, queries querysubscription.Feed) {
	for {
		event, err := queries.Next(p.ctx)
		if err != nil {
			if p.ctx.Err() == nil && !errors.Is(err, querysubscription.ErrClosed) {
				p.shutdown(err)
			}
			return
		}
		batch, ok, err := p.ApplyEvent(event)
		if err != nil {
			return
		}
		if ok {
			if err := p.queue.Offer(batch); err != nil {
				p.shutdown(err)
				return
			}
		}
	}
}

func completeTestPublication(t *testing.T, p *Projection, batch ProjectionBatch) ProjectionBatch {
	t.Helper()
	if batch.publication == nil {
		return batch
	}
	completed, err := p.completePublication(*batch.publication)
	if err != nil {
		t.Fatalf("complete projection publication: %v", err)
	}
	return completed
}

func TestProjectionDrainsTransactionsWhileChildSnapshotLoads(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mock := &projectionMock{blockParent: "p1", childStarted: make(chan struct{}, 1), childRelease: make(chan struct{})}
	tasks := &ProjectionNode{Name: "tasks", BuildQuery: func(parent storage.Record) (storage.QuerySpec, error) {
		return storage.QuerySpec{Relation: storage.RelationName{Name: "tasks"}, Filter: &storage.Predicate{Comparison: &storage.Comparison{Field: "project_id", Operator: storage.ComparisonEqual, Value: storage.Literal{Data: append([]byte(nil), parent["id"].Data...)}}}}, nil
	}}
	root := &ProjectionNode{Name: "projects", Children: []*ProjectionNode{tasks}, BuildQuery: func(storage.Record) (storage.QuerySpec, error) {
		return storage.QuerySpec{Relation: storage.RelationName{Name: "projects"}}, nil
	}}
	queries := projectionSessionWithLimit(t, mock, 4)
	p, err := BeginProjection(ctx, queries, root, StartOptions{QueueLimit: 8})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	go pumpProjectionFeed(p, queries)
	if err := p.releaseOutput(p.queue.Offer); err == nil {
		t.Fatal("output release succeeded before initialization completed")
	}
	select {
	case <-mock.childStarted:
	case <-time.After(time.Second):
		t.Fatal("child snapshot did not start")
	}
	mock.mu.Lock()
	rootSubscription := mock.subscriptions[0]
	mock.mu.Unlock()
	key := storage.EntityKey{Relation: storage.RelationKey{Source: "test", Name: "projects"}, Value: "p1"}
	for revision := uint64(1); revision <= 3; revision++ {
		record := storage.Record{
			"id":    {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("p1")},
			"title": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte(fmt.Sprintf("version-%d", revision))},
		}
		if err := rootSubscription.Queue.Offer(delivery.SubscriptionBatch{
			Checkpoint: projectionTestCheckpoint(strconv.FormatUint(revision, 10)), Revision: revision, PreviousGroupRevision: revision - 1, Delivery: delivery.DeliveryCommit, Events: []delivery.SubscriptionEvent{{Kind: changeprocessor.QueryUpdated, Key: key, Record: record}},
		}); err != nil {
			t.Fatalf("offer revision %d: %v", revision, err)
		}
	}
	close(mock.childRelease)
	readyCtx, stopReady := context.WithTimeout(ctx, time.Second)
	initial, _, err := p.TakeInitial(readyCtx)
	stopReady()
	if err != nil {
		t.Fatal(err)
	}
	var rootRow *ProjectionEvent
	for index := range initial {
		if initial[index].Type == ProjectionRowChanged && initial[index].Node == "projects" {
			rootRow = &initial[index]
		}
	}
	if rootRow == nil {
		t.Fatalf("reconciled root row = %+v", rootRow)
	}

	postReady := storage.CloneRecord(rootRow.Record)
	postReady["title"] = storage.Value{Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("live")}
	if err := rootSubscription.Queue.Offer(delivery.SubscriptionBatch{
		Checkpoint: projectionTestCheckpoint("4"), Revision: 4, PreviousGroupRevision: 3, Delivery: delivery.DeliveryCommit, Events: []delivery.SubscriptionEvent{{Kind: changeprocessor.QueryUpdated, Key: key, Record: postReady}},
	}); err != nil {
		t.Fatal(err)
	}
	blockedCtx, stopBlocked := context.WithTimeout(ctx, 20*time.Millisecond)
	if batch, err := p.Next(blockedCtx); !errors.Is(err, context.DeadlineExceeded) {
		stopBlocked()
		t.Fatalf("pre-release delivery = %+v, err=%v", batch, err)
	}
	stopBlocked()
	if err := p.releaseOutput(p.queue.Offer); err != nil {
		t.Fatal(err)
	}
	if err := p.releaseOutput(p.queue.Offer); err != nil {
		t.Fatalf("idempotent output release: %v", err)
	}
	liveCtx, stopLive := context.WithTimeout(ctx, time.Second)
	defer stopLive()
	var live ProjectionBatch
	rootUpdated := false
	for live.Delivery.Revision < 4 {
		live, err = p.Next(liveCtx)
		if err != nil {
			t.Fatal(err)
		}
		for _, event := range live.Events {
			rootUpdated = rootUpdated || event.Type == ProjectionRowChanged && event.Node == "projects" && event.Kind == querysubscription.Updated
		}
	}
	if !rootUpdated {
		t.Fatalf("live delivery = %+v", live)
	}
}

func TestProjectionDefinitionLimitsAreConfigurable(t *testing.T) {
	leaf := &ProjectionNode{
		Name: "leaf",
		BuildQuery: func(storage.Record) (storage.QuerySpec, error) {
			return storage.QuerySpec{Relation: storage.RelationName{Name: "leaf"}}, nil
		},
	}
	root := &ProjectionNode{
		Name:     "root",
		Children: []*ProjectionNode{leaf},
		BuildQuery: func(storage.Record) (storage.QuerySpec, error) {
			return storage.QuerySpec{Relation: storage.RelationName{Name: "root"}}, nil
		},
	}

	mock := &projectionMock{}
	_, _, err := startTestProjection(context.Background(), projectionSession(t, mock), root, StartOptions{
		QueueLimit: 8,
		Limits: Limits{
			MaxDefinitionDepth: 1,
			MaxDefinitionNodes: 8,
			MaxActiveInstances: 8,
		},
	})
	if err == nil || err.Error() != "projection exceeds maximum depth of 1" {
		t.Fatalf("error = %v", err)
	}
}

func (m *projectionMock) SubscribeGroupedWithFetch(_ context.Context, id, group string, spec storage.QuerySpec, _ engine.InitialWindow, _ engine.FetchOptions) (*engine.OpenSubscription, error) {
	relation := storage.RelationKey{Source: "test", Name: spec.Relation.Name}
	plan := fakePlan{fingerprint: datastore.QueryFingerprint(id), relation: relation}
	var record storage.Record
	if spec.Relation.Name == "projects" {
		record = storage.Record{"id": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("p1")}}
	} else {
		parent := string(spec.Filter.Comparison.Value.Data)
		if parent == m.blockParent && m.childRelease != nil {
			select {
			case m.childStarted <- struct{}{}:
			default:
			}
			<-m.childRelease
		}
		m.mu.Lock()
		m.childParents = append(m.childParents, parent)
		m.mu.Unlock()
		record = storage.Record{"id": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("task-" + parent)}}
	}
	key, _ := plan.EntityKey(record)
	subscription := deliverytest.New(&delivery.Subscription{ID: id, Group: group, Query: plan.Fingerprint()}, m.groupQueue)
	m.mu.Lock()
	m.subscriptions = append(m.subscriptions, subscription)
	m.mu.Unlock()
	return &engine.OpenSubscription{Subscription: subscription.Subscription, Plan: plan, Bounds: datastore.Bounds{Left: datastore.Boundary{Kind: datastore.BoundaryStart}, Right: datastore.Boundary{Kind: datastore.BoundaryEnd}}, Initial: []entitycache.EntitySnapshot{{Key: key, Record: record, State: entitycache.EntityCurrent}}}, nil
}

func (*projectionMock) ReleaseGroup(context.Context, string) error { return nil }

func TestUnpublishedProjectionInstancePublishesCurrentState(t *testing.T) {
	relation := storage.RelationKey{Source: "test", Name: "tasks"}
	key := storage.EntityKey{Relation: relation, Value: "task"}
	oldRecord := storage.Record{
		"id":    {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("task")},
		"title": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("old")},
	}
	newRecord := storage.CloneRecord(oldRecord)
	newRecord["title"] = storage.Value{Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("new")}
	snapshotCheckpoint := sourceposition.FromSource(projectionTestCheckpoint("authorization-1"))
	updateCheckpoint := sourceposition.FromSource(projectionTestCheckpoint("authorization-2"))
	instance := &projectionInstance{
		id: "projection:child", node: &ProjectionNode{Name: "tasks"}, initial: projectionInitialState{Revision: 1, Checkpoint: snapshotCheckpoint},
		members: map[storage.EntityKey]*projectionMember{key: {record: oldRecord, version: querysubscription.RecordVersion{Revision: 1}, cursor: []byte("task")}}, order: []storage.EntityKey{key},
		revision: 1, checkpoint: snapshotCheckpoint,
	}
	p := &Projection{
		ctx: context.Background(), prefix: "projection", instances: map[string]*projectionInstance{instance.id: instance},
		outputReleased: true,
	}

	batch, outgoing, err := p.ApplyDelivery(querysubscription.DeliveryEnvelope{
		Delivery: querysubscription.DeliveryMetadata{Kind: querysubscription.DeliveryCommit, Revision: 2, Checkpoint: updateCheckpoint},
		Effects: []querysubscription.SubscriptionEffect{{SubscriptionID: instance.id, Batch: querysubscription.Batch{
			Changes: []querysubscription.Change{{Kind: querysubscription.Updated, Row: querysubscription.Row{Key: key, Record: newRecord, Cursor: []byte("task")}}},
		}}},
	})
	if err != nil || !outgoing || len(batch.Events) != 0 {
		t.Fatalf("unpublished delivery: batch=%+v outgoing=%v err=%v", batch, outgoing, err)
	}
	checkpoint := sourceposition.FromSource(projectionTestCheckpoint("authorization-3"))
	_, outgoing, err = p.ApplyDelivery(querysubscription.DeliveryEnvelope{Delivery: querysubscription.DeliveryMetadata{
		Kind: querysubscription.DeliveryCommit, Revision: 3, Checkpoint: checkpoint,
	}})
	if err != nil || outgoing {
		t.Fatalf("progress-only delivery: outgoing=%v err=%v", outgoing, err)
	}
	_, outgoing, err = p.ApplyDelivery(querysubscription.DeliveryEnvelope{Delivery: querysubscription.DeliveryMetadata{
		Kind: querysubscription.DeliveryHydration, Revision: 4, Checkpoint: updateCheckpoint,
	}})
	if err != nil || outgoing {
		t.Fatalf("older local checkpoint delivery: outgoing=%v err=%v", outgoing, err)
	}
	completed, err := p.completePublication(projectionPublication{InstanceIDs: []string{instance.id}, CausalRevision: 1})
	if err != nil || len(completed.Events) != 2 {
		t.Fatalf("current image: events=%+v err=%v", completed.Events, err)
	}
	if title := string(completed.Events[1].Record["title"].Data); title != "new" || completed.Events[1].Revision != 2 {
		t.Fatalf("published row title=%q revision=%d", title, completed.Events[1].Revision)
	}
	if completed.Events[0].Checkpoint == nil || completed.Events[0].Checkpoint.Value != 3 {
		t.Fatalf("publication did not use latest authorization checkpoint=%v", completed.Events[0].Checkpoint)
	}
	duplicate, err := p.completePublication(projectionPublication{InstanceIDs: []string{instance.id}})
	if err != nil || len(duplicate.Events) != 0 {
		t.Fatalf("duplicate publication=%+v err=%v", duplicate, err)
	}

	liveRecord := storage.CloneRecord(newRecord)
	liveRecord["title"] = storage.Value{Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("live")}
	live, outgoing, err := p.ApplyDelivery(querysubscription.DeliveryEnvelope{
		Delivery: querysubscription.DeliveryMetadata{Kind: querysubscription.DeliveryCommit, Checkpoint: &querysubscription.Position{SourceID: "test", Value: 4}, Revision: 4},
		Effects: []querysubscription.SubscriptionEffect{{SubscriptionID: instance.id, Batch: querysubscription.Batch{
			Changes: []querysubscription.Change{{Kind: querysubscription.Updated, Row: querysubscription.Row{Key: key, Record: liveRecord}}},
		}}},
	})
	if err != nil || !outgoing || len(live.Events) != 1 || string(live.Events[0].Record["title"].Data) != "live" {
		t.Fatalf("live delivery: batch=%+v outgoing=%v err=%v", live, outgoing, err)
	}
}

func TestProjectionWindowTransitionIsPreserved(t *testing.T) {
	left := querysubscription.Boundary{Kind: querysubscription.BoundaryStart}
	right := querysubscription.Boundary{Kind: querysubscription.BoundaryEnd}
	instance := &projectionInstance{
		id: "projection:list", node: &ProjectionNode{Name: "tasks"}, published: true,
		members: make(map[storage.EntityKey]*projectionMember),
	}
	p := &Projection{
		ctx: context.Background(), prefix: "projection", instances: map[string]*projectionInstance{instance.id: instance},
		outputReleased: true,
	}
	batch, outgoing, err := p.ApplyDelivery(querysubscription.DeliveryEnvelope{
		Delivery: querysubscription.DeliveryMetadata{Kind: querysubscription.DeliveryWindow, Revision: 1},
		Effects: []querysubscription.SubscriptionEffect{{SubscriptionID: instance.id, Batch: querysubscription.Batch{
			Window: &querysubscription.WindowTransition{Token: "extend", Bounds: querysubscription.Bounds{Left: left, Right: right}},
		}}},
	})
	if err != nil || !outgoing || batch.Window == nil || batch.Window.Token != "extend" || batch.Window.InstanceID != instance.id {
		t.Fatalf("window batch=%+v outgoing=%v err=%v", batch, outgoing, err)
	}
	if instance.initial.Bounds.Left.Kind != querysubscription.BoundaryStart || instance.initial.Bounds.Right.Kind != querysubscription.BoundaryEnd {
		t.Fatalf("stored bounds=%+v", instance.initial.Bounds)
	}
}

func (m *projectionMock) Unsubscribe(_ context.Context, id string) (uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, subscription := range m.subscriptions {
		if subscription.ID == id {
			m.unsubscribed++
			return subscription.Queue.RetirementRevision(), nil
		}
	}
	return 0, nil
}

func TestRetiredProjectionWatcherPreservesProgressWithoutStaleEvents(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mock := &projectionMock{}
	tasks := &ProjectionNode{Name: "tasks", Window: querysubscription.InitialWindow{Count: 10}, BuildQuery: func(parent storage.Record) (storage.QuerySpec, error) {
		return storage.QuerySpec{Relation: storage.RelationName{Name: "tasks"}, Filter: &storage.Predicate{Comparison: &storage.Comparison{Field: "project_id", Operator: storage.ComparisonEqual, Value: storage.Literal{Data: append([]byte(nil), parent["id"].Data...)}}}}, nil
	}}
	root := &ProjectionNode{Name: "projects", Window: querysubscription.InitialWindow{Count: 10}, Children: []*ProjectionNode{tasks}, BuildQuery: func(storage.Record) (storage.QuerySpec, error) {
		return storage.QuerySpec{Relation: storage.RelationName{Name: "projects"}}, nil
	}}
	p, _, err := startTestProjection(ctx, projectionSession(t, mock), root, StartOptions{QueueLimit: 8})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	mock.mu.Lock()
	child := mock.subscriptions[1]
	mock.mu.Unlock()
	childID := strings.SplitN(child.ID, "\x00", 2)[1]
	record := storage.Record{"id": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("late")}}
	key := storage.EntityKey{Relation: storage.RelationKey{Source: "test", Name: "tasks"}, Value: "late"}

	// Hold the projection serializer so the watcher can accept these batches but
	// cannot apply them until after its instance has been retired.
	p.processMu.Lock()
	if err := child.Queue.Offer(delivery.SubscriptionBatch{
		Checkpoint: projectionTestCheckpoint("11"), Revision: 11, Delivery: delivery.DeliveryCommit, Events: []delivery.SubscriptionEvent{{Kind: changeprocessor.QueryAdded, Key: key, Record: record}},
	}); err != nil {
		p.processMu.Unlock()
		t.Fatal(err)
	}
	if err := child.Queue.Offer(delivery.SubscriptionBatch{
		Revision: 12, PreviousGroupRevision: 11, Delivery: delivery.DeliveryWindow, Window: &delivery.SubscriptionWindowTransition{Token: "move", Bounds: datastore.Bounds{}},
	}); err != nil {
		p.processMu.Unlock()
		t.Fatal(err)
	}
	p.closeInstanceEvents(childID)
	p.processMu.Unlock()

	waitCtx, stop := context.WithTimeout(ctx, time.Second)
	defer stop()
	commit, err := p.Next(waitCtx)
	if err != nil {
		t.Fatal(err)
	}
	if commit.Delivery.Revision != 11 || commit.Delivery.Kind != querysubscription.DeliveryCommit || len(commit.Events) != 0 {
		t.Fatalf("retired commit metadata = %+v", commit)
	}
	window, err := p.Next(waitCtx)
	if err != nil {
		t.Fatal(err)
	}
	if window.Delivery.Revision != 12 || window.Delivery.Kind != querysubscription.DeliveryWindow || window.Window != nil || window.WindowFailure == nil || window.WindowFailure.Token != "move" || window.WindowFailure.InstanceID != childID {
		t.Fatalf("retired window result = %+v", window)
	}
}

func (m *projectionMock) QueueWindowUpdate(context.Context, string, []engine.BoundaryChange, string) (datastore.Bounds, error) {
	return datastore.Bounds{}, nil
}

func TestProjectionExpandsParameterizedChildrenAndTracksNewParents(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mock := &projectionMock{}
	tasks := &ProjectionNode{Name: "tasks", Window: querysubscription.InitialWindow{Count: 10}, BuildQuery: func(parent storage.Record) (storage.QuerySpec, error) {
		return storage.QuerySpec{Relation: storage.RelationName{Name: "tasks"}, Filter: &storage.Predicate{Comparison: &storage.Comparison{Field: "project_id", Operator: storage.ComparisonEqual, Value: storage.Literal{Data: append([]byte(nil), parent["id"].Data...)}}}}, nil
	}}
	projects := &ProjectionNode{Name: "projects", Window: querysubscription.InitialWindow{Count: 10}, Children: []*ProjectionNode{tasks}, BuildQuery: func(storage.Record) (storage.QuerySpec, error) {
		return storage.QuerySpec{Relation: storage.RelationName{Name: "projects"}}, nil
	}}
	projection, initial, err := startTestProjection(ctx, projectionSession(t, mock), projects, StartOptions{QueueLimit: 16})
	if err != nil {
		t.Fatal(err)
	}
	defer projection.Close()
	if len(initial) != 4 || len(mock.childParents) != 1 || mock.childParents[0] != "p1" {
		t.Fatalf("initial=%+v child parents=%v", initial, mock.childParents)
	}
	if got := initial[3].Lineage; len(got) != 1 || got[0].Node != "projects" || got[0].Key.Value != "p1" {
		t.Fatalf("task lineage = %+v", got)
	}

	root := mock.subscriptions[0]
	record := storage.Record{"id": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("p2")}}
	key := storage.EntityKey{Relation: storage.RelationKey{Source: "test", Name: "projects"}, Value: "p2"}
	if err := root.Queue.Offer(delivery.SubscriptionBatch{Checkpoint: projectionTestCheckpoint("7"), Atomic: true, Revision: 7, Delivery: delivery.DeliveryCommit, Events: []delivery.SubscriptionEvent{{Kind: changeprocessor.QueryAdded, Key: key, Record: record}}}); err != nil {
		t.Fatal(err)
	}
	eventCtx, stop := context.WithTimeout(ctx, time.Second)
	defer stop()
	batch, err := projection.Next(eventCtx)
	if err != nil || len(batch.Events) != 1 {
		t.Fatalf("batch = %+v, %v", batch, err)
	}
	if batch.Events[0].Node != "projects" || batch.Events[0].Type != ProjectionRowChanged {
		t.Fatalf("unexpected projection batch: %+v", batch)
	}
	if batch.Delivery.Revision != 7 || batch.Delivery.Kind != querysubscription.DeliveryCommit {
		t.Fatalf("parent delivery metadata = %+v", batch)
	}
	expanded, err := projection.Next(eventCtx)
	expanded = completeTestPublication(t, projection, expanded)
	if err != nil || len(expanded.Events) != 2 || expanded.Events[0].Node != "tasks" || expanded.Events[0].Type != ProjectionInstanceOpened || expanded.Events[1].Node != "tasks" || expanded.Events[1].Type != ProjectionRowChanged {
		t.Fatalf("expanded child batch = %+v, err=%v", expanded, err)
	}
	if expanded.Delivery.Kind != 0 {
		t.Fatalf("expansion incorrectly claimed source transaction metadata = %+v", expanded)
	}
	mock.mu.Lock()
	parents := fmt.Sprint(mock.childParents)
	mock.mu.Unlock()
	if parents != "[p1 p2]" {
		t.Fatalf("child parents = %s", parents)
	}

	updated := storage.Record{"id": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("p1")}, "title": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("updated")}}
	p1 := storage.EntityKey{Relation: storage.RelationKey{Source: "test", Name: "projects"}, Value: "p1"}
	if err := root.Queue.Offer(delivery.SubscriptionBatch{Checkpoint: projectionTestCheckpoint("8"), Atomic: true, Revision: 8, PreviousGroupRevision: 7, Delivery: delivery.DeliveryCommit, Events: []delivery.SubscriptionEvent{{Kind: changeprocessor.QueryUpdated, Key: p1, Record: updated}}}); err != nil {
		t.Fatal(err)
	}
	rebuilt, err := projection.Next(eventCtx)
	if err != nil || len(rebuilt.Events) != 2 || rebuilt.Events[0].Type != ProjectionInstanceClosed || rebuilt.Events[1].Type != ProjectionRowChanged {
		t.Fatalf("rebuilt subtree batch = %+v, err=%v", rebuilt, err)
	}
	reopened, err := projection.Next(eventCtx)
	reopened = completeTestPublication(t, projection, reopened)
	if err != nil || len(reopened.Events) != 2 || reopened.Events[0].Type != ProjectionInstanceOpened || reopened.Events[1].Type != ProjectionRowChanged {
		t.Fatalf("reopened subtree batch = %+v, err=%v", reopened, err)
	}
	if rebuilt.Events[0].InstanceID == reopened.Events[0].InstanceID {
		t.Fatal("rebuilt child reused a closed server instance ID")
	}
}

func TestProjectionClosesExpandedChildrenWhenParentQueryIsCleared(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mock := &projectionMock{}
	tasks := &ProjectionNode{Name: "tasks", Window: querysubscription.InitialWindow{Count: 10}, BuildQuery: func(parent storage.Record) (storage.QuerySpec, error) {
		return storage.QuerySpec{Relation: storage.RelationName{Name: "tasks"}, Filter: &storage.Predicate{Comparison: &storage.Comparison{Field: "project_id", Operator: storage.ComparisonEqual, Value: storage.Literal{Data: append([]byte(nil), parent["id"].Data...)}}}}, nil
	}}
	projects := &ProjectionNode{Name: "projects", Window: querysubscription.InitialWindow{Count: 10}, Children: []*ProjectionNode{tasks}, BuildQuery: func(storage.Record) (storage.QuerySpec, error) {
		return storage.QuerySpec{Relation: storage.RelationName{Name: "projects"}}, nil
	}}
	projection, _, err := startTestProjection(ctx, projectionSession(t, mock), projects, StartOptions{QueueLimit: 16})
	if err != nil {
		t.Fatal(err)
	}
	defer projection.Close()
	root := mock.subscriptions[0]
	if err := root.Queue.Offer(delivery.SubscriptionBatch{Checkpoint: projectionTestCheckpoint("9"), Atomic: true, Revision: 9, Delivery: delivery.DeliveryCommit, Clear: true, ClearChangeIndex: 1}); err != nil {
		t.Fatal(err)
	}

	eventCtx, stop := context.WithTimeout(ctx, time.Second)
	defer stop()
	batch, err := projection.Next(eventCtx)
	if err != nil {
		t.Fatal(err)
	}
	if len(batch.Events) != 2 || batch.Events[0].Type != ProjectionInstanceClosed || batch.Events[1].Type != ProjectionRowChanged || batch.Events[1].Kind != querysubscription.Removed || batch.Events[1].ChangeIndex != 1 {
		t.Fatalf("projection clear batch = %+v", batch)
	}
	if batch.Delivery.Revision != 9 || !batch.Delivery.Atomic || batch.Delivery.Kind != querysubscription.DeliveryCommit {
		t.Fatalf("projection clear metadata = %+v", batch)
	}
}

func TestProjectionWatcherFailureUnsubscribesEveryInstance(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mock := &projectionMock{}
	tasks := &ProjectionNode{Name: "tasks", Window: querysubscription.InitialWindow{Count: 10}, BuildQuery: func(parent storage.Record) (storage.QuerySpec, error) {
		return storage.QuerySpec{Relation: storage.RelationName{Name: "tasks"}, Filter: &storage.Predicate{Comparison: &storage.Comparison{Field: "project_id", Operator: storage.ComparisonEqual, Value: storage.Literal{Data: append([]byte(nil), parent["id"].Data...)}}}}, nil
	}}
	root := &ProjectionNode{Name: "projects", Window: querysubscription.InitialWindow{Count: 10}, Children: []*ProjectionNode{tasks}, BuildQuery: func(storage.Record) (storage.QuerySpec, error) {
		return storage.QuerySpec{Relation: storage.RelationName{Name: "projects"}}, nil
	}}
	projection, _, err := startTestProjection(ctx, projectionSession(t, mock), root, StartOptions{QueueLimit: 8})
	if err != nil {
		t.Fatal(err)
	}
	mock.mu.Lock()
	want := len(mock.subscriptions)
	rootSubscription := mock.subscriptions[0]
	mock.mu.Unlock()
	watchErr := errors.New("subscription failed")
	rootSubscription.Queue.Close(watchErr)

	waitCtx, stop := context.WithTimeout(ctx, time.Second)
	defer stop()
	if _, err := projection.Next(waitCtx); !errors.Is(err, watchErr) {
		t.Fatalf("projection error = %v, want %v", err, watchErr)
	}
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		mock.mu.Lock()
		got := mock.unsubscribed
		mock.mu.Unlock()
		if got == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	mock.mu.Lock()
	got := mock.unsubscribed
	mock.mu.Unlock()
	t.Fatalf("unsubscribed %d of %d projection instances", got, want)
}

func TestProjectionChildSnapshotDoesNotBlockParentDelivery(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mock := &projectionMock{blockParent: "p2", childStarted: make(chan struct{}, 1), childRelease: make(chan struct{})}
	defer close(mock.childRelease)
	tasks := &ProjectionNode{Name: "tasks", Window: querysubscription.InitialWindow{Count: 10}, BuildQuery: func(parent storage.Record) (storage.QuerySpec, error) {
		return storage.QuerySpec{Relation: storage.RelationName{Name: "tasks"}, Filter: &storage.Predicate{Comparison: &storage.Comparison{Field: "project_id", Operator: storage.ComparisonEqual, Value: storage.Literal{Data: append([]byte(nil), parent["id"].Data...)}}}}, nil
	}}
	rootNode := &ProjectionNode{Name: "projects", Window: querysubscription.InitialWindow{Count: 10}, Children: []*ProjectionNode{tasks}, BuildQuery: func(storage.Record) (storage.QuerySpec, error) {
		return storage.QuerySpec{Relation: storage.RelationName{Name: "projects"}}, nil
	}}
	projection, _, err := startTestProjection(ctx, projectionSession(t, mock), rootNode, StartOptions{QueueLimit: 16})
	if err != nil {
		t.Fatal(err)
	}
	defer projection.Close()
	mock.mu.Lock()
	root := mock.subscriptions[0]
	mock.mu.Unlock()

	p2 := storage.EntityKey{Relation: storage.RelationKey{Source: "test", Name: "projects"}, Value: "p2"}
	if err := root.Queue.Offer(delivery.SubscriptionBatch{Checkpoint: projectionTestCheckpoint("1"), Revision: 1, Delivery: delivery.DeliveryCommit, Events: []delivery.SubscriptionEvent{{Kind: changeprocessor.QueryAdded, Key: p2, Record: storage.Record{"id": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("p2")}}}}}); err != nil {
		t.Fatal(err)
	}
	eventCtx, stop := context.WithTimeout(ctx, time.Second)
	defer stop()
	first, err := projection.Next(eventCtx)
	if err != nil || len(first.Events) != 1 || first.Events[0].Key != p2 {
		t.Fatalf("parent delivery = %+v, err=%v", first, err)
	}
	select {
	case <-mock.childStarted:
	case <-eventCtx.Done():
		t.Fatal("child snapshot did not start")
	}

	p1 := storage.EntityKey{Relation: storage.RelationKey{Source: "test", Name: "projects"}, Value: "p1"}
	updated := storage.Record{"id": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("p1")}, "title": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("updated")}}
	if err := root.Queue.Offer(delivery.SubscriptionBatch{Checkpoint: projectionTestCheckpoint("2"), Revision: 2, PreviousGroupRevision: 1, Delivery: delivery.DeliveryCommit, Events: []delivery.SubscriptionEvent{{Kind: changeprocessor.QueryUpdated, Key: p1, Record: updated}}}); err != nil {
		t.Fatal(err)
	}
	second, err := projection.Next(eventCtx)
	if err != nil || len(second.Events) != 2 || second.Events[1].Key != p1 {
		t.Fatalf("delivery while child blocked = %+v, err=%v", second, err)
	}
}
