package projectionserver

import (
	"context"
	"testing"
	"time"

	"github.com/adiom-data/weave/internal/accesscontrol/policy"
	q "github.com/adiom-data/weave/internal/querysubscription/model"
	"github.com/adiom-data/weave/internal/storage"
	reactivev1 "github.com/adiom-data/weave/pkg/gen/reactive/v1"
)

func TestInitialVisibilityWaitDoesNotRetainProcessedTransactions(t *testing.T) {
	for _, synchronized := range []bool{false, true} {
		name := "first-image"
		if synchronized {
			name = "synchronized"
		}
		t.Run(name, func(t *testing.T) { testInitialVisibilityWaitDoesNotRetainProcessedTransactions(t, synchronized) })
	}
}

func testInitialVisibilityWaitDoesNotRetainProcessedTransactions(t *testing.T, synchronized bool) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	release := make(chan struct{})
	backend := newPolicyBackend(func(ctx context.Context, request q.QuerySubscribeRequest) q.QuerySnapshot {
		switch request.Query.Relation.Name {
		case "grants":
			select {
			case <-release:
			case <-ctx.Done():
			}
			return policySnapshot(10, policyRow("grants", "g1", "grant"))
		case "notes":
			return policySnapshot(10, policyRow("notes", "n1", "note"))
		default:
			return policySnapshot(10, policyRow("projects", "p1", "old"))
		}
	})
	options := policyOptions()
	options.SubscriptionQueueLimit = 2
	options.Policies.Definitions[storage.RelationName{Namespace: "app", Name: "notes"}] = policy.Relation{Allow: []policy.Rule{{ID: "public", When: "true"}}}
	service, err := New(ctx, backend, options)
	if err != nil {
		t.Fatal(err)
	}
	defer service.Close()
	stream := &authorizationCommitStream{ctx: ctx, received: make(chan *reactivev1.ClientMessage, 10), sent: make(chan *reactivev1.ServerMessage, 100)}
	done := make(chan error, 1)
	go func() { done <- service.(*server).Open(stream) }()
	defer func() {
		cancel()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Error("stream failed to stop")
		}
	}()
	root := snapshotTestRequest(false, false).Root
	stream.received <- &reactivev1.ClientMessage{SubscriptionId: "project", RequestId: "project", Operation: &reactivev1.ClientMessage_SubscribeProjection{SubscribeProjection: &reactivev1.SubscribeProjection{Root: root, WaitForSync: synchronized}}}
	project := receivePolicyRequest(t, ctx, backend, "projects")
	receivePolicyRequest(t, ctx, backend, "grants")
	// This independent public subscription is a processing witness: each
	// received commit proves the same connection consumed the corresponding
	// update while the projection's initial authorization remains unresolved.
	stream.received <- &reactivev1.ClientMessage{SubscriptionId: "note", RequestId: "note", Operation: &reactivev1.ClientMessage_Subscribe{Subscribe: &reactivev1.Subscribe{Query: &reactivev1.Query{Schema: "app", Table: "notes", Columns: root.GetList().Query.Columns}, Window: root.GetList().Window}}}
	note := receivePolicyRequest(t, ctx, backend, "notes")
	receivePolicyBatch(t, ctx, stream, reactivev1.ModelBatch_KIND_SNAPSHOT, "")
	select {
	case ready := <-stream.sent:
		if ready.GetReady() == nil {
			t.Fatalf("expected note readiness: %v", ready)
		}
	case <-ctx.Done():
		t.Fatal("no note readiness")
	}
	for point := uint64(11); point <= 30; point++ {
		title := checkpointString(&q.Position{Value: point})
		effects := []q.SubscriptionEffect{{SubscriptionID: note.ID, Batch: q.Batch{Changes: []q.Change{{Kind: q.Updated, Row: policyRow("notes", "n1", title)}}}}}
		if point%2 == 0 {
			effects = append(effects, q.SubscriptionEffect{SubscriptionID: project.ID, Batch: q.Batch{Changes: []q.Change{{Kind: q.Updated, Row: policyRow("projects", "p1", title)}}}})
		}
		backend.commit(t, point, effects...)
		batch := receivePolicyBatch(t, ctx, stream, reactivev1.ModelBatch_KIND_COMMIT, title)
		if len(batch.Entities) != 1 || batch.Entities[0].GetEntityId() != publicEntityID(policyRow("notes", "n1", "").Key) {
			t.Fatalf("published project before authorization resolved: %v", batch)
		}
	}
	close(release)
	initial := receivePolicyBatch(t, ctx, stream, reactivev1.ModelBatch_KIND_SNAPSHOT, "")
	if len(initial.Entities) != 1 || initial.Entities[0].GetUpsert().GetValues()["title"].GetTextValue() != "30" {
		t.Fatalf("initial snapshot lost processed updates: %v", initial)
	}
	select {
	case ready := <-stream.sent:
		if ready.GetReady() == nil {
			t.Fatalf("expected project readiness: %v", ready)
		}
	case <-ctx.Done():
		t.Fatal("no project readiness")
	}
	backend.commit(t, 31, q.SubscriptionEffect{SubscriptionID: project.ID, Batch: q.Batch{Changes: []q.Change{{Kind: q.Updated, Row: policyRow("projects", "p1", "live")}}}})
	live := receivePolicyBatch(t, ctx, stream, reactivev1.ModelBatch_KIND_COMMIT, "31")
	if len(live.Entities) != 1 || live.Entities[0].GetUpsert().GetValues()["title"].GetTextValue() != "live" {
		t.Fatalf("live handoff lost update: %v", live)
	}
}
