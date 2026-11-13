package projection

import (
	"context"
	"testing"
	"time"

	"github.com/adiom-data/weave/internal/projection/plan"
	q "github.com/adiom-data/weave/internal/querysubscription/model"
	"github.com/adiom-data/weave/internal/storage"
)

type nullableBindingFeed struct {
	childBindingFeed
	parentFence *q.Position
}

func (f *nullableBindingFeed) SubscribeQuery(ctx context.Context, request q.QuerySubscribeRequest, install q.QuerySnapshotInstaller) error {
	return f.childBindingFeed.SubscribeQuery(ctx, request, func(subscription q.QuerySubscription, snapshot q.QuerySnapshot) error {
		if request.Query.Relation.Name == "parents" {
			snapshot.Rows[0].Record["target"] = storage.Value{Type: storage.TypeText, State: storage.ValueNull}
			snapshot.Fence = f.parentFence
		}
		return install(subscription, snapshot)
	})
}

func TestNullableBranchAdmissionStillRequiresItsParentFence(t *testing.T) {
	for _, synchronized := range []bool{false, true} {
		t.Run(map[bool]string{false: "first-image", true: "synchronized"}[synchronized], func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			feed := &nullableBindingFeed{childBindingFeed: childBindingFeed{loads: map[string]int{}}, parentFence: &q.Position{SourceID: "test", Value: 20}}
			definition := &plan.Node{Name: "parents", Query: storage.QuerySpec{Relation: storage.RelationName{Name: "parents"}}, Children: []*plan.Node{{
				Name: "related", Entity: true, Query: storage.QuerySpec{Relation: storage.RelationName{Name: "related"}},
				Bindings: []plan.Binding{{Field: "id", Operator: storage.ComparisonEqual, Parent: "target"}},
			}}}
			p, err := BeginProjection(ctx, feed, FromPlan(definition, nil, nil), StartOptions{QueueLimit: 16, WaitForSync: synchronized, CurrentImageAdmission: true})
			if err != nil {
				t.Fatal(err)
			}
			defer p.Close()
			// Only the parent read has a snapshot. Empty children must not add
			// another source or require feed progress merely to finish loading.
			if err := p.WaitReady(ctx); err != nil {
				t.Fatal(err)
			}
			accepted := false
			accept := func(batch ProjectionBatch) (bool, error) {
				accepted = true
				if len(batch.Events) != 3 {
					t.Errorf("expected parent, row, and empty child: %+v", batch.Events)
				}
				return true, nil
			}
			for _, point := range []uint64{10, 19, 20} {
				if _, _, err := p.ApplyEvent(q.Event{Progress: &q.Progress{Position: &q.Position{SourceID: "test", Value: point}, Revision: point}}); err != nil {
					t.Fatal(err)
				}
				wait, err := p.TryAdmitInitial(accept)
				if err != nil || wait.Accepted != (point == 20) || accepted != (point == 20) {
					t.Fatalf("admission at %d: %+v accepted=%v err=%v", point, wait, accepted, err)
				}
				if point < 20 && len(wait.Progress) != 1 {
					t.Fatalf("lost the parent's progress requirement: %+v", wait)
				}
			}
			if wait, err := p.TryPublishCurrent(func([]ProjectionEvent) (bool, error) { return true, nil }); err != nil || !wait.Accepted {
				t.Fatalf("current publication with an empty branch: %+v %v", wait, err)
			}
		})
	}
}

func TestOnlyLocallyEmptyInstancesMayOmitSnapshotPositions(t *testing.T) {
	for _, local := range []bool{false, true} {
		for _, admission := range []bool{false, true} {
			t.Run(map[bool]string{false: "source/", true: "local/"}[local]+map[bool]string{false: "synchronized", true: "admission"}[admission], func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				node := &ProjectionNode{Name: "empty", BuildQuery: func(storage.Record) (storage.QuerySpec, bool, error) {
					return storage.QuerySpec{}, !local, nil
				}}
				p, err := BeginProjection(ctx, &boundarySession{}, node, StartOptions{QueueLimit: 4, WaitForSync: !admission, CurrentImageAdmission: admission})
				if err != nil {
					t.Fatal(err)
				}
				defer p.Close()
				err = p.WaitReady(ctx)
				if err == nil && admission {
					_, err = p.TryAdmitInitial(func(ProjectionBatch) (bool, error) { return true, nil })
				}
				if (err == nil) != local {
					t.Fatalf("local=%v admission=%v: %v", local, admission, err)
				}
				if !local && err.Error() != "snapshot requires a source position" {
					t.Fatalf("source snapshot failed for the wrong reason: %v", err)
				}
			})
		}
	}
}

func (f *nullableBindingFeed) SubscribeEntity(ctx context.Context, request q.EntitySubscribeRequest, install q.EntitySnapshotInstaller) error {
	return f.SubscribeQuery(ctx, q.QuerySubscribeRequest{ID: request.ID, Query: request.Query}, func(subscription q.QuerySubscription, snapshot q.QuerySnapshot) error {
		initial := q.EntitySnapshot{Checkpoint: snapshot.Checkpoint, Fence: snapshot.Fence, Revision: snapshot.Revision}
		if len(snapshot.Rows) > 0 {
			initial.Row = &snapshot.Rows[0].Row
		}
		return install(boundaryEntitySubscription{Subscription: subscription}, initial)
	})
}

func TestNullableBindingsPublishEmptyBranchesAndReactToParentChanges(t *testing.T) {
	for _, entity := range []bool{false, true} {
		t.Run(map[bool]string{false: "list", true: "entity"}[entity], func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			feed := &nullableBindingFeed{childBindingFeed: childBindingFeed{loads: map[string]int{}}}
			definition := &plan.Node{Name: "parents", Query: storage.QuerySpec{Relation: storage.RelationName{Name: "parents"}}, Children: []*plan.Node{{
				Name: "related", Entity: entity, Query: storage.QuerySpec{Relation: storage.RelationName{Name: "related"}},
				Bindings: []plan.Binding{{Field: "id", Operator: storage.ComparisonEqual, Parent: "target"}},
			}}}
			p, initial, err := startTestProjection(ctx, feed, FromPlan(definition, nil, nil), StartOptions{QueueLimit: 16})
			if err != nil {
				t.Fatal(err)
			}
			defer p.Close()
			for _, event := range initial {
				if event.Failure != nil {
					t.Fatalf("null binding produced a branch error: %+v", event)
				}
			}
			key := p.graph.instances[p.graph.rootID].order[0]
			check := func(wantRows, wantLoads int) {
				t.Helper()
				p.mu.Lock()
				children := p.graph.instances[p.graph.rootID].members[key].children
				if len(children) != 1 {
					p.mu.Unlock()
					t.Fatalf("expected one complete child slot, got %v", children)
				}
				child := p.graph.instances[children[0]]
				rows, failed, subscribed := len(child.order), child.failure, child.subscription != nil
				p.mu.Unlock()
				feed.mu.Lock()
				loads := feed.loads["related"]
				feed.mu.Unlock()
				if rows != wantRows || loads != wantLoads || failed != nil || subscribed != (wantRows > 0) {
					t.Fatalf("rows=%d loads=%d failed=%v subscribed=%v; want rows=%d loads=%d", rows, loads, failed, subscribed, wantRows, wantLoads)
				}
			}
			check(0, 0)
			for i, target := range []string{"one", "", "two"} {
				record := memberTarget(target)
				if target == "" {
					record["target"] = storage.Value{Type: storage.TypeText, State: storage.ValueNull}
				}
				_, _, err := p.ApplyDelivery(q.DeliveryEnvelope{Delivery: q.DeliveryMetadata{Kind: q.DeliveryCommit, Revision: uint64(11 + i), Checkpoint: &q.Position{SourceID: "test", Value: uint64(11 + i)}, Atomic: true}, Effects: []q.SubscriptionEffect{{SubscriptionID: p.graph.rootID, Batch: q.Batch{Changes: []q.Change{{Kind: q.Updated, Row: q.Row{Key: key, Record: record}}}}}}})
				if err != nil {
					t.Fatal(err)
				}
				if err := p.expansions.waitIdle(ctx); err != nil {
					t.Fatal(err)
				}
				batch, err := p.Next(ctx)
				if err != nil {
					t.Fatal(err)
				}
				completeTestPublication(t, p, batch)
				rows := 1
				if target == "" {
					rows = 0
				}
				check(rows, 1+i/2)
			}
		})
	}
}
