package projectionserver

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/adiom-data/weave/internal/projection/transactionmessages"
	q "github.com/adiom-data/weave/internal/querysubscription/model"
	"github.com/adiom-data/weave/internal/storage"
	reactivev1 "github.com/adiom-data/weave/pkg/gen/reactive/v1"
)

func TestConnectionScopeFailurePreservesOtherScopesAndPublication(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	stream := &authorizationCommitStream{ctx: ctx, sent: make(chan *reactivev1.ServerMessage, 8)}
	prepared := &recordingReleasePrepared{}
	session := &connectionSession{ctx: ctx, stream: stream, prepared: prepared, subscriptions: make(map[string]*sharedSubscription)}
	session.publisher = &connectionPublisher{connection: session, model: newConnectionModel()}
	key := modelKey(snapshotTestRelation(), "shared")
	for _, id := range []string{"failed", "healthy"} {
		handle := &projectionLifecycleSubscription{id: id, closed: make(chan struct{})}
		state := &sharedSubscription{externalID: id, queryID: id, subscription: handle, visibility: newSubscriptionVisibility()}
		session.subscriptions[id] = state
		if _, err := session.publisher.model.openSubscription(id, id, "", q.QuerySnapshot{
			Bounds: q.Bounds{Left: q.Boundary{Kind: q.BoundaryStart}, Right: q.Boundary{Kind: q.BoundaryEnd}},
			Rows:   []q.QueryRow{{Row: q.Row{Key: key, Record: storage.Record{}}}},
		}, nil); err != nil {
			t.Fatal(err)
		}
	}
	failed, healthy := session.subscriptions["failed"], session.subscriptions["healthy"]
	if err := session.closeDirect(failed, errors.New("query failed")); err != nil {
		t.Fatal(err)
	}
	if session.subscriptions["failed"] != nil || session.subscriptions["healthy"] != healthy {
		t.Fatal("scope failure removed the wrong interest")
	}
	select {
	case <-failed.subscription.(*projectionLifecycleSubscription).closed:
	default:
		t.Fatal("failed interest was not closed")
	}
	select {
	case <-healthy.subscription.(*projectionLifecycleSubscription).closed:
		t.Fatal("scope failure closed a healthy interest")
	default:
	}
	if prepared.released[failed.visibility.id] != 1 || prepared.released[healthy.visibility.id] != 0 {
		t.Fatal("scope failure released another scope's authorization dependencies")
	}
	if session.publisher.model.entityRefs[publicEntityID(key)] != 1 {
		t.Fatal("scope failure lost the shared entity's remaining reference")
	}
	if err := session.handleOutbound(transactionMessage{transaction: &transactionmessages.Transaction{Position: &q.Position{SourceID: "test", Value: 20}}}); err != nil {
		t.Fatal(err)
	}
	cleanup := receiveAuthorizationMessage(t, stream.sent)
	failure := receiveAuthorizationMessage(t, stream.sent)
	later := receiveAuthorizationMessage(t, stream.sent)
	if cleanup.SubscriptionId != "failed" || failure.SubscriptionId != "failed" || failure.GetError() == nil || later.ConnectionSequence != 1 || later.GetModelBatch().GetCheckpoint() == "" {
		t.Fatalf("scope cleanup disrupted publication: %+v, %+v, %+v", cleanup, failure, later)
	}
	if ctx.Err() != nil {
		t.Fatal("scope failure canceled the connection")
	}
}
