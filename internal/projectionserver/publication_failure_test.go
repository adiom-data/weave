package projectionserver

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/adiom-data/weave/internal/querysubscription/model"
	"github.com/adiom-data/weave/internal/storage"
	reactivev1 "github.com/adiom-data/weave/pkg/gen/reactive/v1"
)

func TestFailedInitialProjectionPublishesDeferredEntityChangesForSurvivingScope(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	queries := newProjectionLifecycleClient(querysubscription.QuerySnapshot{})
	var projectReads atomic.Uint64
	projectKey := modelKey(snapshotTestRelation(), "shared")
	queries.snapshotFor = func(request querysubscription.QuerySubscribeRequest) querysubscription.QuerySnapshot {
		key, revision, record := projectKey, uint64(2), modelRecord("shared", "1", "after")
		if request.Query.Relation.Name == "projects" {
			revision = projectReads.Add(1)
			if revision == 1 {
				record = modelRecord("shared", "1", "before")
			}
		} else {
			key = modelKey(storage.RelationKey{Source: "test", Namespace: "app", Name: "tasks", OpaqueID: "tasks"}, "bad")
			record = modelRecord("bad", "1", "bad")
			record["title"] = storage.Value{Type: "unsupported", State: storage.ValuePresent}
		}
		return querysubscription.QuerySnapshot{Bounds: snapshotTestBounds(), Revision: revision, Checkpoint: &querysubscription.Position{SourceID: "test", Value: revision}, Rows: []querysubscription.QueryRow{{Row: querysubscription.Row{Key: key, Record: record, Cursor: []byte(key.Value)}}}}
	}
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
	subscribe := func(id string, child bool) {
		stream.received <- &reactivev1.ClientMessage{SubscriptionId: id, RequestId: id, Operation: &reactivev1.ClientMessage_SubscribeProjection{SubscribeProjection: &reactivev1.SubscribeProjection{Root: snapshotTestRequest(false, child).Root}}}
	}
	subscribe("healthy", false)
	if batch := receiveAuthorizationMessage(t, stream.sent).GetModelBatch(); batch == nil || batch.Entities[0].GetUpsert().Values["title"].GetTextValue() != "before" {
		t.Fatalf("healthy initial image = %v", batch)
	}
	if message := receiveAuthorizationMessage(t, stream.sent); message.GetReady() == nil {
		t.Fatalf("healthy readiness = %v", message)
	}
	healthy := <-queries.opened
	subscribe("failed", true)
	refresh := receiveAuthorizationMessage(t, stream.sent)
	if refresh.GetModelBatch() == nil || refresh.ConnectionSequence == 0 || len(refresh.GetModelBatch().Entities) != 1 || refresh.GetModelBatch().Entities[0].GetUpsert().Values["title"].GetTextValue() != "after" || len(refresh.GetModelBatch().Projections) != 0 || len(refresh.GetModelBatch().Queries) != 0 {
		t.Fatalf("surviving entity output was lost or failed scope was published: %v", refresh)
	}
	failure := receiveAuthorizationMessage(t, stream.sent)
	if failure.SubscriptionId != "failed" || failure.GetError() == nil || failure.GetError().Code != "SUBSCRIBE_FAILED" {
		t.Fatalf("initial failure = %v", failure)
	}
	if err := healthy.offer(querysubscription.DeliveryEnvelope{Delivery: querysubscription.DeliveryMetadata{Kind: querysubscription.DeliveryCommit, Checkpoint: &querysubscription.Position{SourceID: "test", Value: 3}, Revision: 3}, Effects: []querysubscription.SubscriptionEffect{{SubscriptionID: healthy.id, Batch: querysubscription.Batch{Changes: []querysubscription.Change{{Kind: querysubscription.Updated, Row: querysubscription.Row{Key: projectKey, Record: modelRecord("shared", "1", "live")}}}}}}}); err != nil {
		t.Fatal(err)
	}
	live := receiveAuthorizationMessage(t, stream.sent)
	if live.GetModelBatch() == nil || len(live.GetModelBatch().Entities) != 1 || live.GetModelBatch().Entities[0].GetUpsert().Values["title"].GetTextValue() != "live" {
		t.Fatalf("surviving projection stopped: %v", live)
	}
}
