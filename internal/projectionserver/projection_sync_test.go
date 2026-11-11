package projectionserver

import (
	"context"
	"testing"
	"time"

	"github.com/adiom-data/weave/internal/projection"
	"github.com/adiom-data/weave/internal/querysubscription/model"
	"github.com/adiom-data/weave/internal/storage"
	reactivev1 "github.com/adiom-data/weave/pkg/gen/reactive/v1"
)

func TestReactiveProjectionWaitForSyncFoldsCatchupAndPreservesLiveHandoff(t *testing.T) {
	projectKey := modelKey(snapshotTestRelation(), "project-1")
	taskRelation := storage.RelationKey{Source: "test", Namespace: "app", Name: "tasks", OpaqueID: "tasks"}
	taskKey := modelKey(taskRelation, "task-1")
	queries := newProjectionLifecycleClient(querysubscription.QuerySnapshot{})
	queries.snapshotFor = func(request querysubscription.QuerySubscribeRequest) querysubscription.QuerySnapshot {
		key, checkpoint, title := projectKey, uint64(5), "Original project"
		if request.Query.Relation.Name == "tasks" {
			key, checkpoint, title = taskKey, 10, "Original task"
		}
		return querysubscription.QuerySnapshot{
			Bounds:     snapshotTestBounds(),
			Rows:       []querysubscription.QueryRow{{Row: querysubscription.Row{Key: key, Record: modelRecord(key.Value, "1", title), Cursor: []byte(key.Value)}}},
			Checkpoint: &querysubscription.Position{SourceID: "test", Value: checkpoint},
			Fence:      &querysubscription.Position{SourceID: "test", Value: checkpoint},
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream := &authorizationCommitStream{ctx: ctx, received: make(chan *reactivev1.ClientMessage, 1), sent: make(chan *reactivev1.ServerMessage, 16)}
	done := make(chan error, 1)
	go func() { done <- newWithDefaults(queries).Open(stream) }()
	defer func() {
		cancel()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Error("service did not stop")
		}
	}()
	root := snapshotTestRequest(true, true).Root
	stream.received <- &reactivev1.ClientMessage{
		SubscriptionId: "projection", RequestId: "subscribe",
		Operation: &reactivev1.ClientMessage_SubscribeProjection{SubscribeProjection: &reactivev1.SubscribeProjection{Root: root, WaitForSync: true}},
	}
	opened := func() *projectionLifecycleSubscription {
		t.Helper()
		select {
		case subscription := <-queries.opened:
			return subscription
		case <-ctx.Done():
			t.Fatal("projection subscription did not open")
			return nil
		}
	}
	project := opened()
	task := opened()
	select {
	case <-queries.progress:
	case <-ctx.Done():
		t.Fatal("initialization did not request known feed progress")
	}
	select {
	case message := <-stream.sent:
		t.Fatalf("projection emitted before synchronization: %v", message)
	default:
	}
	// Update the older parent while initialization waits for the child's newer
	// snapshot. Its unchanged child selector must keep the original subscription.
	if err := project.offer(querysubscription.DeliveryEnvelope{
		Delivery: querysubscription.DeliveryMetadata{Kind: querysubscription.DeliveryCommit, Checkpoint: &querysubscription.Position{SourceID: "test", Value: 9}, Revision: 1, Atomic: true},
		Effects: []querysubscription.SubscriptionEffect{{SubscriptionID: project.id, Batch: querysubscription.Batch{
			Changes: []querysubscription.Change{{Kind: querysubscription.Updated, Row: querysubscription.Row{Key: projectKey, Record: modelRecord("project-1", "1", "Caught-up project"), Cursor: []byte("project-1")}}},
		}}},
	}); err != nil {
		t.Fatal(err)
	}
	project.owner.events <- querysubscription.Event{Progress: &querysubscription.Progress{
		Position: &querysubscription.Position{SourceID: "test", Value: 10}, Revision: 2, PreviousRevision: 1,
	}}
	initial := receiveAuthorizationMessage(t, stream.sent).GetModelBatch()
	if initial == nil || initial.Kind != reactivev1.ModelBatch_KIND_SNAPSHOT || initial.Checkpoint != "10" {
		t.Fatalf("synchronized initial image = %v", initial)
	}
	found := false
	for _, entity := range initial.Entities {
		if entity.EntityId == publicEntityID(projectKey) && entity.GetUpsert().Values["title"].GetTextValue() == "Caught-up project" {
			found = true
		}
	}
	if !found {
		t.Fatal("initial snapshot did not fold in the pre-cut parent update")
	}
	if ready := receiveAuthorizationMessage(t, stream.sent); ready.GetReady() == nil {
		t.Fatalf("expected Ready after synchronized snapshot, got %v", ready)
	}
	if err := task.offer(querysubscription.DeliveryEnvelope{
		Delivery: querysubscription.DeliveryMetadata{Kind: querysubscription.DeliveryCommit, Checkpoint: &querysubscription.Position{SourceID: "test", Value: 11}, Revision: 3, PreviousRevision: 2, Atomic: true},
		Effects: []querysubscription.SubscriptionEffect{{SubscriptionID: task.id, Batch: querysubscription.Batch{
			Changes: []querysubscription.Change{{Kind: querysubscription.Updated, Row: querysubscription.Row{Key: taskKey, Record: modelRecord("task-1", "1", "Live task")}}},
		}}},
	}); err != nil {
		t.Fatal(err)
	}
	// Replaying the catch-up transaction would produce an older commit here.
	live := receiveAuthorizationMessage(t, stream.sent).GetModelBatch()
	if live == nil || live.Kind != reactivev1.ModelBatch_KIND_COMMIT || live.Checkpoint != "11" || !live.Atomic {
		t.Fatalf("first live batch = %v", live)
	}
	if len(live.Entities) != 1 || live.Entities[0].GetUpsert().Values["title"].GetTextValue() != "Live task" {
		t.Fatalf("live update = %v", live)
	}
}

func TestSynchronizedProjectionBuffersUpdatesBetweenInitialCutAndOutputRelease(t *testing.T) {
	key := modelKey(snapshotTestRelation(), "project-1")
	queries := newProjectionLifecycleClient(querysubscription.QuerySnapshot{
		Bounds:     snapshotTestBounds(),
		Rows:       []querysubscription.QueryRow{{Row: querysubscription.Row{Key: key, Record: modelRecord("project-1", "1", "Initial"), Cursor: []byte("project-1")}}},
		Checkpoint: &querysubscription.Position{SourceID: "test", Value: 5},
	})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	feed, err := queries.OpenFeed(ctx, querysubscription.FeedOptions{BufferLimit: 8})
	if err != nil {
		t.Fatal(err)
	}
	defer feed.Close(context.Background())
	p, err := projection.BeginProjection(ctx, feed, &projection.ProjectionNode{
		Name: "project", Entity: true,
		BuildQuery: func(storage.Record) (storage.QuerySpec, bool, error) {
			return storage.QuerySpec{Relation: storage.RelationName{Name: "projects"}}, true, nil
		},
	}, projection.StartOptions{QueueLimit: 8, WaitForSync: true})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if err := p.WaitReady(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case <-queries.progress:
		t.Fatal("single-instance initialization unnecessarily requested progress")
	default:
	}
	subscription := <-queries.opened
	_, outgoing, err := p.ApplyDelivery(querysubscription.DeliveryEnvelope{
		Delivery: querysubscription.DeliveryMetadata{Kind: querysubscription.DeliveryCommit, Checkpoint: &querysubscription.Position{SourceID: "test", Value: 6}, Revision: 1, Atomic: true},
		Effects: []querysubscription.SubscriptionEffect{{SubscriptionID: subscription.id, Batch: querysubscription.Batch{
			Changes: []querysubscription.Change{{Kind: querysubscription.Updated, Row: querysubscription.Row{Key: key, Record: modelRecord("project-1", "1", "Between cut and send")}}},
		}}},
	})
	if err != nil || outgoing {
		t.Fatalf("unreleased delivery escaped: outgoing=%v, err=%v", outgoing, err)
	}
	// The initial image remains its original cut even when ownership is taken
	// after newer state has arrived. The newer transaction is drained below.
	initial, positions, err := p.TakeInitial(ctx)
	if err != nil || len(initial) != 2 || initial[0].Checkpoint.Value != 5 || string(initial[1].Record["title"].Data) != "Initial" {
		t.Fatalf("single-instance synchronized snapshot = %+v, %v", initial, err)
	}
	if len(positions) != 1 || positions["test"] == nil || positions["test"].Value != 5 {
		t.Fatalf("initial image advertised a newer cut than its data: %+v", positions)
	}
	var staged []projection.ProjectionBatch
	var output projection.OutputScheduler[struct{}]
	if err := output.Release(p, struct{}{}, func(item projection.Output[struct{}]) error {
		staged = append(staged, item.Batch)
		return item.Err
	}); err != nil {
		t.Fatal(err)
	}
	if len(staged) != 1 || staged[0].Delivery.Checkpoint.Value != 6 || len(staged[0].Events) != 1 || string(staged[0].Events[0].Record["title"].Data) != "Between cut and send" {
		t.Fatalf("post-cut delivery = %+v", staged)
	}
}

func TestReactiveProjectionCanUnsubscribeWhileWaitingForSync(t *testing.T) {
	key := modelKey(snapshotTestRelation(), "project-1")
	queries := newProjectionLifecycleClient(querysubscription.QuerySnapshot{
		Bounds:     snapshotTestBounds(),
		Rows:       []querysubscription.QueryRow{{Row: querysubscription.Row{Key: key, Record: modelRecord("project-1", "1", "Initial"), Cursor: []byte("project-1")}}},
		Checkpoint: &querysubscription.Position{SourceID: "test", Value: 10},
	})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	stream := &authorizationCommitStream{ctx: ctx, received: make(chan *reactivev1.ClientMessage, 1), sent: make(chan *reactivev1.ServerMessage, 8)}
	done := make(chan error, 1)
	go func() { done <- newWithDefaults(queries).Open(stream) }()
	defer func() {
		cancel()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Error("service did not stop")
		}
	}()
	stream.received <- &reactivev1.ClientMessage{
		SubscriptionId: "waiting", RequestId: "subscribe-waiting",
		Operation: &reactivev1.ClientMessage_SubscribeProjection{SubscribeProjection: &reactivev1.SubscribeProjection{Root: snapshotTestRequest(true, true).Root, WaitForSync: true}},
	}
	select {
	case <-queries.progress:
	case <-ctx.Done():
		t.Fatal("projection did not reach synchronization wait")
	}
	stream.received <- &reactivev1.ClientMessage{
		SubscriptionId: "waiting", RequestId: "unsubscribe-waiting",
		Operation: &reactivev1.ClientMessage_Unsubscribe{Unsubscribe: &reactivev1.Unsubscribe{}},
	}
	ack := receiveAuthorizationMessage(t, stream.sent)
	if ack.RequestId != "unsubscribe-waiting" || ack.GetModelBatch() == nil {
		t.Fatalf("unsubscribe acknowledgment = %v", ack)
	}
	for index := 0; index < 2; index++ {
		subscription := <-queries.opened
		select {
		case <-subscription.closed:
		case <-ctx.Done():
			t.Fatal("canceling synchronization did not close its query subscription")
		}
	}
	// The same connection can still open an ordinary projection immediately.
	stream.received <- &reactivev1.ClientMessage{
		SubscriptionId: "ordinary", RequestId: "subscribe-ordinary",
		Operation: &reactivev1.ClientMessage_SubscribeProjection{SubscribeProjection: &reactivev1.SubscribeProjection{Root: snapshotTestRequest(false, false).Root}},
	}
	snapshot := receiveAuthorizationMessage(t, stream.sent)
	if snapshot.SubscriptionId != "ordinary" || snapshot.GetModelBatch() == nil || snapshot.GetModelBatch().Kind != reactivev1.ModelBatch_KIND_SNAPSHOT {
		t.Fatalf("ordinary projection after cancellation = %v", snapshot)
	}
	if ready := receiveAuthorizationMessage(t, stream.sent); ready.SubscriptionId != "ordinary" || ready.GetReady() == nil {
		t.Fatalf("ordinary projection readiness = %v", ready)
	}
}
