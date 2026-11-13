package projectionserver

import (
	"context"
	"testing"
	"time"

	q "github.com/adiom-data/weave/internal/querysubscription/model"
	relationdef "github.com/adiom-data/weave/internal/relation"
	"github.com/adiom-data/weave/internal/storage"
	pb "github.com/adiom-data/weave/pkg/gen/reactive/v1"
)

func TestConnectionProgressRetainsOnlyConsumedSourceProof(t *testing.T) {
	var progress connectionProgress
	position := &q.Position{SourceID: "test", Value: 10}
	progress.observe(q.Event{Progress: &q.Progress{Position: position, Revision: 1}})
	position.Value = 999 // Feed metadata cannot mutate retained proof.
	for _, kind := range []q.DeliveryKind{q.DeliveryHydration, q.DeliveryWindow} {
		progress.observe(q.Event{Delivery: &q.DeliveryEnvelope{Delivery: q.DeliveryMetadata{
			Kind: kind, Checkpoint: &q.Position{SourceID: "test", Value: 30}, Revision: 2,
		}}})
		seed, revision := progress.current()
		if seed == nil || seed.Value != 10 || revision != 2 {
			t.Fatalf("local delivery became source proof: %+v revision=%d", seed, revision)
		}
		seed.Value = 999 // Consumers also receive independent seed metadata.
	}
	progress.observe(q.Event{Delivery: &q.DeliveryEnvelope{Delivery: q.DeliveryMetadata{
		Kind: q.DeliveryCommit, Checkpoint: &q.Position{SourceID: "test", Value: 20}, Revision: 3,
	}}})
	seed, revision := progress.current()
	if seed == nil || seed.Value != 20 || revision != 3 {
		t.Fatalf("commit did not advance the consumed cut: %+v revision=%d", seed, revision)
	}
}

func TestProjectionReusesProgressConsumedBeforeItWasOpened(t *testing.T) {
	for _, policy := range []bool{false, true} {
		t.Run(map[bool]string{false: "allow-all", true: "policy"}[policy], func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
			defer cancel()
			backend := newPolicyBackend(func(_ context.Context, request q.QuerySubscribeRequest) q.QuerySnapshot {
				return policySnapshot(20, policyRow(request.Query.Relation.Name, "one", "Item"))
			})
			options := Options{}
			if policy {
				options = policyOptions()
				options.Relations.Definitions = relationdef.Definitions{
					{Namespace: "app", Name: "projects"}: {Visibility: "true"},
					{Namespace: "app", Name: "tasks"}:    {Visibility: "true"},
				}
			}
			service, err := New(ctx, backend, options)
			if err != nil {
				t.Fatal(err)
			}
			defer service.Close()
			stream := &authorizationCommitStream{ctx: ctx, sent: make(chan *pb.ServerMessage, 16)}
			session := &connectionSession{service: service.(*server), stream: stream, ctx: ctx}
			if err := session.initialize(); err != nil {
				t.Fatal(err)
			}
			defer session.close()
			// Use the production dependency cache to establish and consume a
			// quiet feed's progress before a projection has registered interest.
			if err := session.queries.SubscribeQuery(ctx, q.QuerySubscribeRequest{
				ID: "warm", Query: storage.QuerySpec{Relation: storage.RelationName{Namespace: "app", Name: "projects"}}, Window: q.InitialWindow{Count: 10},
			}, func(q.QuerySubscription, q.QuerySnapshot) error { return nil }); err != nil {
				t.Fatal(err)
			}
			if err := session.queries.RequestProgress(ctx); err != nil {
				t.Fatal(err)
			}
			event, err := session.queries.Next(ctx)
			if err != nil || event.Progress == nil {
				t.Fatalf("expected source proof before opening: %+v %v", event, err)
			}
			if err := session.handleOutbound(queryEventMessage{event: event}); err != nil {
				t.Fatal(err)
			}
			request := snapshotTestRequest(true, true) // Two snapshots require source coverage.
			if err := session.handleRequest(&pb.ClientMessage{SubscriptionId: "view", RequestId: "open", Operation: &pb.ClientMessage_SubscribeProjection{SubscribeProjection: &pb.SubscribeProjection{Root: request.Root, WaitForSync: true}}}); err != nil {
				t.Fatal(err)
			}
			// No more feed events are delivered. The projection must use the
			// retained cut, rather than wait for another report at that position.
			for {
				select {
				case item := <-session.outbound:
					if err := session.handleOutbound(item); err != nil {
						t.Fatal(err)
					}
					if err := session.retryPolicy(); err != nil {
						t.Fatal(err)
					}
				case message := <-stream.sent:
					if message.GetError() != nil {
						t.Fatal(message.GetError())
					}
					if message.GetReady() != nil {
						return
					}
				case <-ctx.Done():
					t.Fatal("projection waited for source proof already consumed by its connection")
				}
			}
		})
	}
}
