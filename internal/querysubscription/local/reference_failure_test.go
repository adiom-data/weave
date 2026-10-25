package local

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/adiom-data/weave/internal/engine/delivery"
	q "github.com/adiom-data/weave/internal/querysubscription/model"
	"github.com/adiom-data/weave/internal/storage"
)

func TestOwnedReferenceFailureRetiresWholeInterest(t *testing.T) {
	for _, mode := range []string{"child-query", "child-conversion", "parent-query"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			retired := make(chan string, 8)
			coordinator := coordinatorStub{
				unsubscribe: func(_ context.Context, id string) error { retired <- id; return nil },
				progress:    func(context.Context, string) (*delivery.ProgressReport, error) { return nil, nil },
			}
			feed, refs, key, _ := referenceFixture(t, ctx, coordinator)
			child := refs.parents[key]
			other := key
			other.Value = "sibling"
			sibling, _, err := refs.bindLocked(other, storage.Record{"task_id": {Type: storage.TypeInteger, State: storage.ValuePresent, Data: []byte("2")}})
			if err != nil {
				t.Fatal(err)
			}
			for _, interest := range []*ownedReferenceChild{child, sibling} {
				interest.native = &localSubscription{id: interest.id, nativeID: feed.nativeID(interest.id), owner: feed, coordinator: coordinator, referenceChild: interest}
				feed.subscriptions[interest.id] = interest.native
			}
			feed.subscriptions[refs.parent.id] = refs.parent
			loadCtx, stopLoad := context.WithCancel(ctx)
			defer stopLoad()
			sibling.cancel = stopLoad
			native := child.native
			if mode == "parent-query" {
				native = refs.parent
			}
			cause := errors.New("adapter query failed")
			if mode == "child-conversion" {
				native.selector = &entitySelector{}
				// An invalid final index fails entity batch conversion.
				if err := feed.groupQueue.Offer(delivery.GroupMessage{Batches: []delivery.SubscriptionBatch{{SubscriptionID: native.nativeID,
					Delivery: delivery.DeliveryCommit, Checkpoint: &q.Position{SourceID: "test", Value: 1}, Revision: 1,
					Membership: delivery.MembershipDelta{Insertions: []delivery.MembershipInsertion{{Key: key, FinalIndex: 2}}},
				}}}); err != nil {
					t.Fatal(err)
				}
			} else {
				if err := feed.groupQueue.Offer(delivery.GroupMessage{Failures: []delivery.SubscriptionFailure{{ID: native.nativeID, Cause: cause}}}); err != nil {
					t.Fatal(err)
				}
			}
			for {
				event, err := feed.Next(ctx)
				if err != nil {
					t.Fatal(err)
				}
				if event.Failure == nil {
					continue
				}
				if event.Failure.SubscriptionID != refs.parent.id || (mode != "child-conversion" && !errors.Is(event.Failure.Cause, cause)) {
					t.Fatalf("wrong failure: %+v", event.Failure)
				}
				break
			}
			seen := map[string]bool{}
			for len(seen) < 3 {
				select {
				case id := <-retired:
					if seen[id] {
						t.Fatalf("duplicate retirement: %q", id)
					}
					seen[id] = true
				case <-ctx.Done():
					t.Fatalf("parent and siblings not retired: %v", seen)
				}
			}
			if !seen[refs.parent.nativeID] || !seen[child.native.nativeID] || !seen[sibling.native.nativeID] {
				t.Fatalf("wrong retirements: %v", seen)
			}
			for _, interest := range []*localSubscription{refs.parent, child.native, sibling.native} {
				if !interest.discardsQueuedEffects() {
					t.Fatal("failed owned interest still accepts effects", interest.id)
				}
			}
			select {
			case <-loadCtx.Done():
			default:
				t.Fatal("sibling's pending load was not canceled")
			}
			if change := child.advance(q.DeliveryMetadata{Revision: 2}, q.Batch{Clear: true}); change != nil {
				t.Fatal("failed interest continued emitting changes")
			}
			// Already queued parent batches must be discarded while asynchronous
			// cleanup is finishing, not forwarded after the terminal failure.
			effects, entities, _, err := feed.referenceEffects(&pendingDelivery{
				owners:  map[string]*localSubscription{refs.parent.id: refs.parent},
				effects: []q.SubscriptionEffect{{SubscriptionID: refs.parent.id, Batch: q.Batch{Clear: true}}},
			})
			if err != nil || len(effects) != 0 || len(entities) != 0 {
				t.Fatalf("failed parent forwarded queued effects: %+v, %v", effects, err)
			}
			if err := feed.RequestProgress(ctx); err != nil {
				t.Fatalf("unrelated feed operations failed: %v", err)
			}
		})
	}
}
