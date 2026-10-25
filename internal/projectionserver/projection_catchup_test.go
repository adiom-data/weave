package projectionserver

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/adiom-data/weave/internal/accesscontrol"
	"github.com/adiom-data/weave/internal/querysubscription/model"
	"github.com/adiom-data/weave/internal/storage"
	reactivev1 "github.com/adiom-data/weave/pkg/gen/reactive/v1"
)

// Hold only the initial slow image. Live authorization can continue using
// immutable views while that evaluation is outstanding.
type catchupVisibility struct {
	accesscontrol.Controller
	candidates []accesscontrol.Candidate
	started    chan struct{}
	release    chan struct{}
	once       sync.Once
	observed   chan uint64
}

func (c *catchupVisibility) Prepare(ctx context.Context, request accesscontrol.PrepareRequest) (accesscontrol.Prepared, error) {
	prepared, err := c.Controller.Prepare(ctx, request)
	if err != nil {
		return nil, err
	}
	return &catchupPrepared{Prepared: prepared, owner: c}, nil
}

func (c *catchupVisibility) Synchronize(_ context.Context, _ accesscontrol.PreparedID, position storage.Checkpoint) (accesscontrol.Checkpoint, error) {
	if c.observed != nil {
		select {
		case c.observed <- position.Order():
		default:
		}
	}
	return accesscontrol.Checkpoint{Position: position, View: authorizationCommitView{allowed: position.Order() < 3}, Invalidation: accesscontrol.Invalidation{Candidates: c.candidates}}, nil
}

func TestProjectionInitialAuthorizationIncludesCommitsWhileDescendantsLoad(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	key := modelKey(snapshotTestRelation(), "project")
	record := modelRecord("project", "1", "Project")
	policy := &catchupVisibility{Controller: accesscontrol.NewAllowAll(), observed: make(chan uint64, 8), candidates: []accesscontrol.Candidate{{Key: key, Record: record, Complete: true}}}
	queries := newProjectionLifecycleClient(querysubscription.QuerySnapshot{})
	childRelease := make(chan struct{})
	queries.snapshotFor = func(request querysubscription.QuerySubscribeRequest) querysubscription.QuerySnapshot {
		snapshot := querysubscription.QuerySnapshot{Bounds: snapshotTestBounds(), Revision: 1, Checkpoint: &querysubscription.Position{SourceID: "test", Value: 1}}
		if request.Query.Relation.Name == "tasks" {
			select {
			case <-childRelease:
			case <-ctx.Done():
			}
		} else {
			snapshot.Rows = []querysubscription.QueryRow{{Row: querysubscription.Row{Key: key, Record: record, Cursor: []byte("project")}}}
		}
		return snapshot
	}
	stream := &authorizationCommitStream{ctx: ctx, received: make(chan *reactivev1.ClientMessage, 1), sent: make(chan *reactivev1.ServerMessage, 8)}
	done := make(chan error, 1)
	go func() {
		done <- newServer(queries, Options{AccessController: policy}).Open(stream)
	}()
	defer func() {
		cancel()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Error("service did not stop")
		}
	}()
	stream.received <- &reactivev1.ClientMessage{SubscriptionId: "projection", RequestId: "subscribe", Operation: &reactivev1.ClientMessage_SubscribeProjection{SubscribeProjection: &reactivev1.SubscribeProjection{Root: snapshotTestRequest(false, true).Root}}}
	opened := func() *projectionLifecycleSubscription {
		t.Helper()
		select {
		case sub := <-queries.opened:
			return sub
		case <-ctx.Done():
			t.Fatal("subscription did not open")
			return nil
		}
	}
	root := opened()
	opened() // Child snapshot is now held open.
	if err := root.offer(querysubscription.DeliveryEnvelope{Delivery: querysubscription.DeliveryMetadata{Kind: querysubscription.DeliveryCommit, Checkpoint: &querysubscription.Position{SourceID: "test", Value: 3}, Revision: 3, Atomic: true}}); err != nil {
		t.Fatal(err)
	}
	select {
	case position := <-policy.observed:
		if position != 3 {
			t.Fatalf("unexpected live policy position: %d", position)
		}
	case <-ctx.Done():
		t.Fatal("revocation was not consumed")
	}
	close(childRelease)
	initial := receiveAuthorizationMessage(t, stream.sent).GetModelBatch()
	if initial == nil || initial.Kind != reactivev1.ModelBatch_SNAPSHOT || len(initial.Entities) != 0 {
		t.Fatalf("initial image exposed a project revoked while loading: %v", initial)
	}
	if message := receiveAuthorizationMessage(t, stream.sent); message.GetReady() == nil {
		t.Fatalf("projection readiness = %v", message)
	}
}

type catchupPrepared struct {
	accesscontrol.Prepared
	owner *catchupVisibility
}

func (p *catchupPrepared) Evaluate(ctx context.Context, view accesscontrol.View, candidates []accesscontrol.Candidate) ([]bool, error) {
	for _, candidate := range candidates {
		if string(candidate.Record["title"].Data) == "Slow" {
			p.owner.once.Do(func() {
				close(p.owner.started)
				select {
				case <-p.owner.release:
				case <-ctx.Done():
				}
			})
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	allowed := make([]bool, len(candidates))
	for i := range allowed {
		allowed[i] = view.(authorizationCommitView).allowed
	}
	return allowed, nil
}

func TestProjectionAuthorizationCatchupIsScopedAndIncludesEmptyCommits(t *testing.T) {
	t.Run("authorization_only", func(t *testing.T) { testProjectionAuthorizationCatchup(t, false) })
	t.Run("raw_removal", func(t *testing.T) { testProjectionAuthorizationCatchup(t, true) })
}

func testProjectionAuthorizationCatchup(t *testing.T, remove bool) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	queries := newProjectionLifecycleClient(querysubscription.QuerySnapshot{})
	policy := &catchupVisibility{Controller: accesscontrol.NewAllowAll(), started: make(chan struct{}), release: make(chan struct{})}
	for _, table := range []string{"tasks", "projects"} {
		key := modelKey(storage.RelationKey{Source: "test", Namespace: "app", Name: table, OpaqueID: table}, table+"-1")
		title := "Healthy"
		if table == "projects" {
			title = "Slow"
		}
		policy.candidates = append(policy.candidates, accesscontrol.Candidate{Key: key, Record: modelRecord(key.Value, "1", title), Complete: true})
	}
	queries.snapshotFor = func(request querysubscription.QuerySubscribeRequest) querysubscription.QuerySnapshot {
		for _, candidate := range policy.candidates {
			if candidate.Key.Relation.Name == request.Query.Relation.Name {
				return querysubscription.QuerySnapshot{Bounds: snapshotTestBounds(), Checkpoint: &querysubscription.Position{SourceID: "test", Value: 1}, Rows: []querysubscription.QueryRow{{Row: querysubscription.Row{Key: candidate.Key, Record: candidate.Record, Cursor: []byte(candidate.Key.Value)}}}}
			}
		}
		panic("unexpected relation")
	}
	service := newServer(queries, Options{AccessController: policy, SubscriptionQueueLimit: 8})
	stream := &authorizationCommitStream{ctx: ctx, received: make(chan *reactivev1.ClientMessage, 1), sent: make(chan *reactivev1.ServerMessage, 32)}
	done := make(chan error, 1)
	go func() { done <- service.Open(stream) }()
	defer func() {
		cancel()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Error("service did not stop")
		}
	}()
	subscribe := func(id, table string) {
		root := snapshotTestRequest(false, false).Root
		root.GetList().Query.Table = table
		stream.received <- &reactivev1.ClientMessage{SubscriptionId: id, RequestId: id, Operation: &reactivev1.ClientMessage_SubscribeProjection{SubscribeProjection: &reactivev1.SubscribeProjection{Root: root}}}
	}
	subscribe("healthy", "tasks")
	if message := receiveAuthorizationMessage(t, stream.sent); message.GetModelBatch() == nil {
		t.Fatalf("healthy snapshot = %v", message)
	}
	if message := receiveAuthorizationMessage(t, stream.sent); message.GetReady() == nil {
		t.Fatalf("healthy ready = %v", message)
	}
	healthy := <-queries.opened
	subscribe("slow", "projects")
	select {
	case <-policy.started:
	case <-ctx.Done():
		t.Fatal("initial authorization did not start")
	}
	slow := <-queries.opened
	// T2 has no raw effects. T3 revokes both rows and optionally removes slow
	// from the raw list too. Replaying T2 must not re-grant healthy, and the
	// invalidation at T3 must not resurrect a removed raw member.
	for _, position := range []uint64{2, 3} {
		envelope := querysubscription.DeliveryEnvelope{Delivery: querysubscription.DeliveryMetadata{Kind: querysubscription.DeliveryCommit, Checkpoint: &querysubscription.Position{SourceID: "test", Value: position}, Revision: position, Atomic: true}}
		if remove && position == 3 {
			key := policy.candidates[1].Key
			envelope.Effects = []querysubscription.SubscriptionEffect{{SubscriptionID: slow.id, Batch: querysubscription.Batch{
				Changes:    []querysubscription.Change{{Kind: querysubscription.Removed, Row: querysubscription.Row{Key: key}}},
				Membership: querysubscription.MembershipDelta{Removed: []storage.EntityKey{key}},
			}}}
		}
		if err := healthy.offer(envelope); err != nil {
			t.Fatal(err)
		}
	}
	message := receiveAuthorizationMessage(t, stream.sent)
	if message.GetModelBatch() == nil || message.GetModelBatch().Checkpoint != checkpointString(&querysubscription.Position{SourceID: "test", Value: 3}) {
		t.Fatalf("live revocation = %v", message)
	}
	close(policy.release)
	if message := receiveAuthorizationMessage(t, stream.sent); message.GetModelBatch() == nil || message.GetModelBatch().Kind != reactivev1.ModelBatch_SNAPSHOT {
		t.Fatalf("slow snapshot = %v", message)
	}
	if message := receiveAuthorizationMessage(t, stream.sent); message.GetReady() == nil {
		t.Fatalf("slow ready = %v", message)
	}
	// T2 leaves slow unchanged and must not re-grant healthy. Only T3 emits.
	message = receiveAuthorizationMessage(t, stream.sent)
	batch := message.GetModelBatch()
	if batch == nil || batch.Checkpoint != checkpointString(&querysubscription.Position{SourceID: "test", Value: 3}) {
		t.Fatalf("catch-up revocation = %v", message)
	}
	if len(batch.Entities) != 1 || batch.Entities[0].EntityId != publicEntityID(policy.candidates[1].Key) {
		t.Fatalf("catch-up touched an unrelated projection: %v", message)
	}
	if batch.Entities[0].GetReleased() == nil {
		t.Fatalf("authorization-only revocation was lost: %v", message)
	}
}
