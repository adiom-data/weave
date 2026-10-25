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

type stalledInitialVisibility struct {
	accesscontrol.Controller
	started chan struct{}
	once    sync.Once
}

func (c *stalledInitialVisibility) Prepare(ctx context.Context, request accesscontrol.PrepareRequest) (accesscontrol.Prepared, error) {
	prepared, err := c.Controller.Prepare(ctx, request)
	if err != nil {
		return nil, err
	}
	return &stalledInitialPrepared{Prepared: prepared, owner: c}, nil
}

type stalledInitialPrepared struct {
	accesscontrol.Prepared
	owner *stalledInitialVisibility
}

func (p *stalledInitialPrepared) Evaluate(ctx context.Context, view accesscontrol.View, candidates []accesscontrol.Candidate) ([]bool, error) {
	for _, candidate := range candidates {
		if string(candidate.Record["title"].Data) == "Slow" {
			p.owner.once.Do(func() { close(p.owner.started) })
			<-ctx.Done()
			return nil, ctx.Err()
		}
	}
	return p.Prepared.Evaluate(ctx, view, candidates)
}

func TestProjectionInitializationOverflowKeepsOtherProjectionsLive(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	queries := newProjectionLifecycleClient(querysubscription.QuerySnapshot{})
	keyFor := func(table string) storage.EntityKey {
		return modelKey(storage.RelationKey{Source: "test", Namespace: "app", Name: table, OpaqueID: table}, table+"-1")
	}
	queries.snapshotFor = func(request querysubscription.QuerySubscribeRequest) querysubscription.QuerySnapshot {
		key := keyFor(request.Query.Relation.Name)
		title := "Healthy"
		if request.Query.Relation.Name == "projects" {
			title = "Slow"
		}
		return querysubscription.QuerySnapshot{
			Bounds: snapshotTestBounds(), Checkpoint: &querysubscription.Position{SourceID: "test", Value: 10},
			Rows: []querysubscription.QueryRow{{Row: querysubscription.Row{Key: key, Record: modelRecord(key.Value, "1", title), Cursor: []byte(key.Value)}}},
		}
	}
	policy := &stalledInitialVisibility{Controller: accesscontrol.NewAllowAll(), started: make(chan struct{})}
	service := newServer(queries, Options{AccessController: policy, SubscriptionQueueLimit: 1})
	stream := &authorizationCommitStream{ctx: ctx, received: make(chan *reactivev1.ClientMessage, 1), sent: make(chan *reactivev1.ServerMessage, 16)}
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
		stream.received <- &reactivev1.ClientMessage{
			SubscriptionId: id, RequestId: "subscribe-" + id,
			Operation: &reactivev1.ClientMessage_SubscribeProjection{SubscribeProjection: &reactivev1.SubscribeProjection{Root: root}},
		}
	}
	subscribe("healthy", "tasks")
	if message := receiveAuthorizationMessage(t, stream.sent); message.GetModelBatch() == nil {
		t.Fatalf("healthy snapshot = %v", message)
	}
	if message := receiveAuthorizationMessage(t, stream.sent); message.GetReady() == nil {
		t.Fatalf("healthy readiness = %v", message)
	}
	healthy := <-queries.opened
	subscribe("slow", "projects")
	select {
	case <-policy.started:
	case <-ctx.Done():
		t.Fatal("slow initial authorization did not start")
	}
	slow := <-queries.opened
	update := func(subscription *projectionLifecycleSubscription, table, title string, revision uint64) {
		key := keyFor(table)
		if err := subscription.offer(querysubscription.DeliveryEnvelope{
			Delivery: querysubscription.DeliveryMetadata{Kind: querysubscription.DeliveryCommit, Checkpoint: &querysubscription.Position{SourceID: "test", Value: 10 + revision}, Revision: revision, PreviousRevision: revision - 1, Atomic: true},
			Effects: []querysubscription.SubscriptionEffect{{SubscriptionID: subscription.id, Batch: querysubscription.Batch{
				Changes: []querysubscription.Change{{Kind: querysubscription.Updated, Row: querysubscription.Row{Key: key, Record: modelRecord(key.Value, "1", title), Cursor: []byte(key.Value)}}},
			}}},
		}); err != nil {
			t.Fatal(err)
		}
	}
	update(slow, "projects", "Slow", 1)
	update(slow, "projects", "Slow", 2)
	failure := receiveAuthorizationMessage(t, stream.sent)
	if failure.SubscriptionId != "slow" || failure.GetError() == nil || failure.GetError().Code != "SUBSCRIBE_FAILED" {
		t.Fatalf("isolated overflow = %v", failure)
	}
	select {
	case <-slow.closed:
	case <-ctx.Done():
		t.Fatal("overflowed projection retained its query subscription")
	}
	update(healthy, "tasks", "Still live", 3)
	live := receiveAuthorizationMessage(t, stream.sent)
	if live.GetModelBatch() == nil || live.GetModelBatch().Kind != reactivev1.ModelBatch_COMMIT || len(live.GetModelBatch().Entities) != 1 {
		t.Fatalf("healthy projection stopped after another projection overflowed: %v", live)
	}
	if live.GetModelBatch().Entities[0].GetUpsert().Values["title"].GetTextValue() != "Still live" {
		t.Fatalf("healthy update = %v", live)
	}
}
