package projectionserver

import (
	"context"
	"errors"
	"testing"
	"time"

	proj "github.com/adiom-data/weave/internal/projection"
	q "github.com/adiom-data/weave/internal/querysubscription/model"
	"github.com/adiom-data/weave/internal/storage"
)

func TestProjectionFailureRoutingUsesActiveInterests(t *testing.T) {
	for _, pending := range []bool{false, true} {
		t.Run(map[bool]string{false: "published", true: "initializing"}[pending], func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			backend := newPolicyBackend(func(context.Context, q.QuerySubscribeRequest) q.QuerySnapshot {
				return policySnapshot(10)
			})
			feed, err := backend.OpenFeed(ctx, q.FeedOptions{BufferLimit: 10})
			if err != nil {
				t.Fatal(err)
			}
			defer feed.Close(ctx)
			p, err := proj.BeginProjection(ctx, feed, &proj.ProjectionNode{Name: "root", BuildQuery: func(storage.Record) (storage.QuerySpec, error) {
				return storage.QuerySpec{Relation: storage.RelationName{Name: "projects"}}, nil
			}}, proj.StartOptions{Prefix: "view", QueueLimit: 10})
			if err != nil {
				t.Fatal(err)
			}
			defer p.Close()
			initial, _, err := p.TakeInitial(ctx)
			if err != nil {
				t.Fatal(err)
			}
			activeID := initial[0].InstanceID
			closed := false
			projections := map[string]*sharedProjection{}
			loading := map[string]*pendingSharedSubscription{}
			if pending {
				loading["view"] = &pendingSharedSubscription{projection: p, owns: p.OwnsSubscription, cancel: func() { closed = true }}
			} else {
				projections["view"] = &sharedProjection{projection: p}
			}
			failure := errors.New("subscription failed")
			handle := func(id string) error {
				return handleQueryEvent(q.Event{Failure: &q.SubscriptionFailure{SubscriptionID: id, Cause: failure}}, nil, nil,
					func(*sharedProjection, error) error { closed = true; return nil }, nil, projections, loading)
			}
			if err := handle("view:retired"); err != nil || closed || p.Err() != nil {
				t.Fatalf("historical prefix closed projection: %v", err)
			}
			if err := handle(activeID); err != nil || !closed || !errors.Is(p.Err(), failure) {
				t.Fatalf("active failure was not routed: closed=%v err=%v terminal=%v", closed, err, p.Err())
			}
		})
	}
}
