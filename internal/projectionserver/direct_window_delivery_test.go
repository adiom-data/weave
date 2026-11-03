package projectionserver

import (
	"context"
	"testing"
	"time"

	"github.com/adiom-data/weave/internal/accesscontrol"
	"github.com/adiom-data/weave/internal/querysubscription/model"
	"github.com/adiom-data/weave/internal/storage"
	reactivev1 "github.com/adiom-data/weave/pkg/gen/reactive/v1"
)

type directInvalidationController struct {
	visibilityTestController
	key storage.EntityKey
}

type directInvalidationPrepared struct {
	accesscontrol.Prepared
	id accesscontrol.PreparedID
}

func (p directInvalidationPrepared) ID() accesscontrol.PreparedID { return p.id }

func (c directInvalidationController) Prepare(ctx context.Context, request accesscontrol.PrepareRequest) (accesscontrol.Prepared, error) {
	prepared, err := c.visibilityTestController.Prepare(ctx, request)
	return directInvalidationPrepared{Prepared: prepared, id: request.ID}, err
}

func (c directInvalidationController) Synchronize(_ context.Context, _ accesscontrol.PreparedID, position storage.Checkpoint) (accesscontrol.Checkpoint, error) {
	return accesscontrol.Checkpoint{Position: position, View: visibilityTestView{}, Invalidation: accesscontrol.Invalidation{
		Candidates: []accesscontrol.Candidate{{Key: c.key, Record: visibilityRecord(position.Order() != 3), Complete: true}},
	}}, nil
}

func TestDirectSubscriptionPolicyOnlyCommitsAndCombinedUpdates(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	key := modelKey(snapshotTestRelation(), "task")
	record := modelRecord("task", "1", "original")
	record["visible"] = visibilityRecord(true)["visible"]
	queries := newProjectionLifecycleClient(querysubscription.QuerySnapshot{
		Bounds: snapshotTestBounds(), Revision: 1, Checkpoint: &querysubscription.Position{SourceID: "test", Value: 1},
		Rows: []querysubscription.QueryRow{{Row: querysubscription.Row{Key: key, Record: record}}},
	})
	stream := &authorizationCommitStream{ctx: ctx, received: make(chan *reactivev1.ClientMessage, 1), sent: make(chan *reactivev1.ServerMessage, 16)}
	done := make(chan error, 1)
	go func() {
		done <- newServer(queries, Options{AccessController: directInvalidationController{key: key}}).Open(stream)
	}()
	defer func() {
		cancel()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Error("service did not stop")
		}
	}()
	stream.received <- &reactivev1.ClientMessage{SubscriptionId: "direct", RequestId: "subscribe", Operation: &reactivev1.ClientMessage_Subscribe{Subscribe: &reactivev1.Subscribe{
		Query: snapshotTestRequest(false, false).Root.GetList().Query, Window: &reactivev1.InitialWindow{From: reactivev1.InitialWindow_FROM_START, Count: 10},
	}}}
	if initial := receiveAuthorizationMessage(t, stream.sent).GetModelBatch(); initial == nil || len(initial.Entities) != 1 {
		t.Fatalf("initial = %v", initial)
	}
	if message := receiveAuthorizationMessage(t, stream.sent); message.GetReady() == nil {
		t.Fatalf("ready = %v", message)
	}
	sub := <-queries.opened
	for _, revision := range []uint64{3, 4, 5} {
		envelope := querysubscription.DeliveryEnvelope{Delivery: testDelivery(revision)}
		if revision == 5 {
			updated := modelRecord("task", "1", "updated")
			updated["visible"] = visibilityRecord(false)["visible"]
			envelope.Effects = []querysubscription.SubscriptionEffect{{SubscriptionID: sub.id, Batch: querysubscription.Batch{
				Changes: []querysubscription.Change{{Kind: querysubscription.Updated, Row: querysubscription.Row{Key: key, Record: updated}}},
			}}}
		}
		if err := sub.offer(envelope); err != nil {
			t.Fatal(err)
		}
		batch := receiveAuthorizationMessage(t, stream.sent).GetModelBatch()
		if batch == nil || batch.Kind != reactivev1.ModelBatch_KIND_COMMIT {
			t.Fatalf("commit %d = %v", revision, batch)
		}
		switch revision {
		case 3:
			if len(batch.Queries) != 1 || len(batch.Queries[0].GetDelta().GetRemovedEntityIds()) != 1 {
				t.Fatalf("policy revocation = %v", batch)
			}
		case 4:
			if len(batch.Queries) != 1 || len(batch.Entities) != 1 || batch.Entities[0].GetUpsert().GetValues()["title"].GetTextValue() != "original" {
				t.Fatalf("policy grant replaced raw image: %v", batch)
			}
		case 5:
			if len(batch.Queries) != 0 || len(batch.Entities) != 1 || batch.Entities[0].GetUpsert().GetValues()["title"].GetTextValue() != "updated" {
				t.Fatalf("combined update flickered or lost raw payload: %v", batch)
			}
		}
	}
}

func TestDirectWindowDeliveryPreservesFreshnessAcrossPublicationPaths(t *testing.T) {
	for _, pending := range []bool{false, true} {
		name := "complete_record_refresh"
		if pending {
			name = "pending_hydration"
		}
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			key := modelKey(snapshotTestRelation(), "shared")
			record := modelRecord("shared", "1", "before")
			if pending {
				record = nil
			}
			queries := newProjectionLifecycleClient(querysubscription.QuerySnapshot{Bounds: snapshotTestBounds(), Revision: 2, Checkpoint: &querysubscription.Position{SourceID: "test", Value: 2}, Rows: []querysubscription.QueryRow{{Row: querysubscription.Row{Key: key, Record: record, Pending: pending, Cursor: []byte("shared")}}}})
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
			stream.received <- &reactivev1.ClientMessage{SubscriptionId: "direct", RequestId: "subscribe", Operation: &reactivev1.ClientMessage_Subscribe{Subscribe: &reactivev1.Subscribe{Query: snapshotTestRequest(false, false).Root.GetList().Query, Window: &reactivev1.InitialWindow{From: reactivev1.InitialWindow_FROM_START, Count: 10}}}}
			initial := receiveAuthorizationMessage(t, stream.sent).GetModelBatch()
			if initial == nil || len(initial.Entities) != 1 {
				t.Fatalf("initial snapshot = %v", initial)
			}
			if message := receiveAuthorizationMessage(t, stream.sent); message.GetReady() == nil {
				t.Fatalf("initial readiness = %v", message)
			}
			sub := <-queries.opened
			position := &querysubscription.Position{SourceID: "test", Value: 3}
			if err := sub.offer(querysubscription.DeliveryEnvelope{Delivery: querysubscription.DeliveryMetadata{Kind: querysubscription.DeliveryWindow, Revision: 3, PreviousRevision: 2, Checkpoint: position, Fence: position}, Effects: []querysubscription.SubscriptionEffect{{SubscriptionID: sub.id, Batch: querysubscription.Batch{Changes: []querysubscription.Change{{Kind: querysubscription.Updated, Row: querysubscription.Row{Key: key, Record: modelRecord("shared", "1", "from window"), Cursor: []byte("shared")}}}, Window: &querysubscription.WindowTransition{Token: "window", Bounds: snapshotTestBounds(), Checkpoint: position, Fence: position}}}}}); err != nil {
				t.Fatal(err)
			}
			message := receiveAuthorizationMessage(t, stream.sent)
			batch := message.GetModelBatch()
			if message.RequestId != "window" || batch == nil || batch.Kind != reactivev1.ModelBatch_KIND_WINDOW || batch.Checkpoint != "3" || len(batch.Entities) != 1 || batch.Entities[0].GetUpsert() == nil || batch.Entities[0].GetUpsert().Values["title"].GetTextValue() != "from window" {
				t.Fatalf("window lost its source context or payload: %v", message)
			}
			if len(batch.Queries) != 1 || batch.Queries[0].GetWindowChanged() == nil {
				t.Fatalf("window acknowledgment = %v", batch)
			}
			// The same entity then appears in a slower projection's older image.
			// It must not replace the record or completeness accepted at revision 3.
			stream.received <- &reactivev1.ClientMessage{SubscriptionId: "late", RequestId: "late", Operation: &reactivev1.ClientMessage_SubscribeProjection{SubscribeProjection: &reactivev1.SubscribeProjection{Root: snapshotTestRequest(false, false).Root}}}
			late := receiveAuthorizationMessage(t, stream.sent).GetModelBatch()
			if late == nil || late.Kind != reactivev1.ModelBatch_KIND_SNAPSHOT || len(late.Entities) != 0 {
				t.Fatalf("older projection image replaced newer window state: %v", late)
			}
			if message := receiveAuthorizationMessage(t, stream.sent); message.GetReady() == nil {
				t.Fatalf("late readiness = %v", message)
			}
			if err := sub.offer(querysubscription.DeliveryEnvelope{Delivery: querysubscription.DeliveryMetadata{Kind: querysubscription.DeliveryCommit, Revision: 4, PreviousRevision: 3, Checkpoint: &querysubscription.Position{SourceID: "test", Value: 4}, Atomic: true}, Effects: []querysubscription.SubscriptionEffect{{SubscriptionID: sub.id, Batch: querysubscription.Batch{Changes: []querysubscription.Change{{Kind: querysubscription.Updated, Row: querysubscription.Row{Key: key, Record: modelRecord("shared", "1", "live"), Cursor: []byte("shared")}}}}}}}); err != nil {
				t.Fatal(err)
			}
			live := receiveAuthorizationMessage(t, stream.sent).GetModelBatch()
			if live == nil || live.Kind != reactivev1.ModelBatch_KIND_COMMIT || live.Checkpoint != "4" || !live.Atomic || len(live.Entities) != 1 || live.Entities[0].GetUpsert() == nil || live.Entities[0].GetUpsert().Values["title"].GetTextValue() != "live" {
				t.Fatalf("live delivery after window = %v", live)
			}
		})
	}
}

func TestDirectDeliveryRoutingKeepsOriginalMetadata(t *testing.T) {
	for _, kind := range []querysubscription.DeliveryKind{querysubscription.DeliveryCommit, querysubscription.DeliveryHydration, querysubscription.DeliveryWindow} {
		metadata := querysubscription.DeliveryMetadata{Kind: kind, Revision: 7, PreviousRevision: 6, Checkpoint: &querysubscription.Position{SourceID: "test", Value: 5}, Fence: &querysubscription.Position{SourceID: "test", Value: 4}, Atomic: kind == querysubscription.DeliveryCommit}
		called := false
		err := handleQueryEvent(querysubscription.Event{Delivery: &querysubscription.DeliveryEnvelope{Delivery: metadata, Effects: []querysubscription.SubscriptionEffect{{SubscriptionID: "internal"}}}}, func(deliveries []feedDelivery) error {
			called = true
			if len(deliveries) != 1 || deliveries[0].metadata != metadata || len(deliveries[0].parts) != 1 {
				t.Fatalf("feed delivery lost metadata: %+v", deliveries)
			}
			part, ok := deliveries[0].parts[0].(subscriptionBatchMessage)
			if !ok || part.delivery != metadata {
				t.Fatalf("direct payload lost metadata: %+v", deliveries[0].parts[0])
			}
			return nil
		}, nil, nil, map[string]*sharedSubscription{"direct": {externalID: "direct", internalID: "internal"}}, nil, nil)
		if err != nil || !called {
			t.Fatalf("routing kind %d: called=%v err=%v", kind, called, err)
		}
	}
}
