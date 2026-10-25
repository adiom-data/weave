package delivery

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/adiom-data/weave/internal/datastore"
	"github.com/adiom-data/weave/internal/engine/changeprocessor"
)

func TestGroupQueuesOneTransactionForManyInterests(t *testing.T) {
	hub := newTestSubscriptionHub()
	queue := hub.OpenGroup("feed", 1)
	plan := fakePlan{fingerprint: "items"}
	bounds := datastore.Bounds{Left: datastore.Boundary{Kind: datastore.BoundaryStart}, Right: datastore.Boundary{Kind: datastore.BoundaryEnd}}
	for i := range 500 {
		sub, err := hub.SubscribePreparedGrouped(fmt.Sprint(i), "feed", plan, bounds)
		if err != nil {
			t.Fatalf("shared interest acquired private queue: %+v, %v", sub, err)
		}
	}
	entity := hubEntity(plan, "item", true)
	changes := []changeprocessor.QueryChange{{Query: plan.Fingerprint(), Key: entity.Key, Kind: changeprocessor.QueryUpdated, PreviousRecord: entity.Record, Record: entity.Record}}
	if dropped := hub.PublishRevision(hubCheckpoint(1), true, 1, changes); len(dropped) != 0 {
		t.Fatal("one transaction exhausted a one-slot queue", dropped)
	}
	message, err := queue.Next(context.Background())
	if err != nil || len(message.Batches) != 500 {
		t.Fatalf("group transaction has %d batches: %v", len(message.Batches), err)
	}
	for _, batch := range message.Batches {
		if batch.SubscriptionID == "" || batch.Revision != 1 || !batch.Atomic {
			t.Fatal("incomplete transaction metadata", batch.SubscriptionID)
		}
	}
}

func TestGroupOverflowDoesNotBlockOrCloseOtherGroups(t *testing.T) {
	hub := newTestSubscriptionHub()
	slow := hub.OpenGroup("slow", 1)
	fast := hub.OpenGroup("fast", 1)
	plan := fakePlan{fingerprint: "items"}
	bounds := datastore.Bounds{Left: datastore.Boundary{Kind: datastore.BoundaryStart}, Right: datastore.Boundary{Kind: datastore.BoundaryEnd}}
	for _, id := range []string{"slow", "fast"} {
		if _, err := hub.SubscribePreparedGrouped(id, id, plan, bounds); err != nil {
			t.Fatal(err)
		}
	}
	hub.PublishRevision(hubCheckpoint(1), true, 1, nil)
	if _, err := fast.Next(context.Background()); err != nil {
		t.Fatal(err)
	}
	dropped := hub.PublishRevision(hubCheckpoint(2), true, 2, nil)
	if len(dropped) != 1 || dropped[0] != "slow" {
		t.Fatal("overflow did not isolate the slow group", dropped)
	}
	if _, err := slow.Next(context.Background()); !errors.Is(err, ErrClientQueueFull) {
		t.Fatal("slow group must resynchronize", err)
	}
	message, err := fast.Next(context.Background())
	if err != nil || message.Batches[0].Revision != 2 || message.Batches[0].PreviousGroupRevision != 1 {
		t.Fatal("healthy group's transaction chain broke", message, err)
	}
	if !hub.ReleaseGroup("slow") || hub.ReleaseGroup("fast") {
		t.Fatal("group membership was not cleaned up")
	}
}

func TestGroupRetirementFollowsAcceptedDeliveries(t *testing.T) {
	for _, failure := range []bool{false, true} {
		t.Run(fmt.Sprint(failure), func(t *testing.T) {
			hub := newTestSubscriptionHub()
			queue := hub.OpenGroup("feed", 8)
			plan := fakePlan{fingerprint: "items"}
			bounds := datastore.Bounds{Left: datastore.Boundary{Kind: datastore.BoundaryStart}, Right: datastore.Boundary{Kind: datastore.BoundaryEnd}}
			if _, err := hub.SubscribePreparedGrouped("item", "feed", plan, bounds); err != nil {
				t.Fatal(err)
			}
			hub.PublishRevision(hubCheckpoint(1), true, 1, nil)
			cause := errors.New("query failure")
			if failure {
				hub.FailQuery(plan.Fingerprint(), cause)
			} else if revision, ok := hub.Unsubscribe("item"); !ok || revision != 1 {
				t.Fatal("unsubscribe failed")
			}
			message, err := queue.Next(context.Background())
			if err != nil || len(message.Batches) != 1 || message.Batches[0].Revision != 1 {
				t.Fatal("retirement discarded an accepted transaction", message, err)
			}
			if !failure {
				return
			}
			message, err = queue.Next(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if failure {
				if len(message.Failures) != 1 || message.Failures[0].ID != "item" || message.Failures[0].Cause != cause {
					t.Fatal("missing isolated failure", message)
				}
			}
		})
	}
}

func TestGroupQueryFailureUsesOneQueueEntry(t *testing.T) {
	hub := newTestSubscriptionHub()
	queue := hub.OpenGroup("feed", 2)
	plan := fakePlan{fingerprint: "broken"}
	bounds := datastore.Bounds{Left: datastore.Boundary{Kind: datastore.BoundaryStart}, Right: datastore.Boundary{Kind: datastore.BoundaryEnd}}
	for i := range 500 {
		if _, err := hub.SubscribePreparedGrouped(fmt.Sprint(i), "feed", plan, bounds); err != nil {
			t.Fatal(err)
		}
	}
	healthy := fakePlan{fingerprint: "healthy"}
	if _, err := hub.SubscribePreparedGrouped("healthy", "feed", healthy, bounds); err != nil {
		t.Fatal(err)
	}
	hub.PublishRevision(hubCheckpoint(1), true, 1, nil)
	hub.FailQuery(plan.Fingerprint(), errors.New("query failed"))
	if _, err := queue.Next(context.Background()); err != nil {
		t.Fatal("one failure fanned out into queue overflow", err)
	}
	message, err := queue.Next(context.Background())
	if err != nil || len(message.Failures) != 500 {
		t.Fatal("missing grouped failures", len(message.Failures), err)
	}
	if hub.byID["healthy"] == nil || len(hub.byGroup["feed"]) != 1 {
		t.Fatal("query failure removed an unrelated interest")
	}
}

func TestRetiringManyInterestsDoesNotConsumeFullQueue(t *testing.T) {
	hub := newTestSubscriptionHub()
	queue := hub.OpenGroup("feed", 1)
	plan := fakePlan{fingerprint: "items"}
	bounds := datastore.Bounds{Left: datastore.Boundary{Kind: datastore.BoundaryStart}, Right: datastore.Boundary{Kind: datastore.BoundaryEnd}}
	for i := range 501 {
		if _, err := hub.SubscribePreparedGrouped(fmt.Sprint(i), "feed", plan, bounds); err != nil {
			t.Fatal(err)
		}
	}
	hub.PublishRevision(hubCheckpoint(1), true, 1, nil)
	for i := range 500 {
		if revision, ok := hub.Unsubscribe(fmt.Sprint(i)); !ok || revision != 1 {
			t.Fatal("wrong retirement floor", revision, ok)
		}
	}
	message, err := queue.Next(context.Background())
	if err != nil || message.Batches[0].Revision != 1 {
		t.Fatal("retirement discarded the accepted transaction", message, err)
	}
	if dropped := hub.PublishRevision(hubCheckpoint(2), true, 2, nil); len(dropped) != 0 {
		t.Fatal(dropped)
	}
	message, err = queue.Next(context.Background())
	if err != nil || len(message.Batches) != 1 || message.Batches[0].SubscriptionID != "500" || message.Batches[0].PreviousGroupRevision != 1 {
		t.Fatal("remaining interest lost its ordering chain", message, err)
	}
}

func TestSubscriptionRequiresExplicitDeliveryGroup(t *testing.T) {
	hub := newTestSubscriptionHub()
	plan := fakePlan{fingerprint: "items"}
	bounds := datastore.Bounds{Left: datastore.Boundary{Kind: datastore.BoundaryStart}, Right: datastore.Boundary{Kind: datastore.BoundaryEnd}}
	if _, err := hub.SubscribePreparedGrouped("item", "feed", plan, bounds); err == nil {
		t.Fatal("subscription silently created another delivery mode")
	}
	hub.OpenGroup("feed", 1)
	if _, err := hub.SubscribePreparedGrouped("item", "feed", plan, bounds); err != nil {
		t.Fatal(err)
	}
}
