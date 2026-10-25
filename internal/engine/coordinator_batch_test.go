package engine

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/adiom-data/weave/internal/engine/delivery"
)

func batchFixture(t *testing.T) (*Coordinator, *ReferenceSource, context.Context) {
	t.Helper()
	c, plan, key := initialCacheFixture(t)
	c.options = DefaultCoordinatorOptions()
	c.options.SubscriptionBatchSize = 2
	c.hub.OpenGroup("feed", 16)
	if _, err := c.startSubscribeActor(initialWindowOperation{subscriptionID: "parent", group: "feed", plan: plan}, InitialWindow{Count: 1}); err != nil {
		t.Fatal(err)
	}
	parent, _ := c.subscriptions.Active("parent")
	parent.Reference = fixedPreparedReference{plan: plan}
	c.subscriptions.Activate("parent", parent)
	c.commands, c.done = make(chan coordinatorCommand), make(chan struct{})
	stop, stopped := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(stopped)
		for {
			select {
			case command := <-c.commands:
				command.run()
			case <-stop:
				return
			}
		}
	}()
	t.Cleanup(func() { close(stop); <-stopped })
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	t.Cleanup(cancel)
	return c, &ReferenceSource{SubscriptionID: "parent", ParentKey: key}, ctx
}

func TestEntityBatchRegistersCachedInterestsAndCallsInstallerOutsideActor(t *testing.T) {
	c, from, ctx := batchFixture(t)
	requests := make([]EntitySubscribe, 5) // Cross multiple bounded actor turns.
	for i := range requests {
		requests[i] = EntitySubscribe{ID: fmt.Sprint(i), From: from}
	}
	seen := make(map[int]bool)
	c.SubscribeEntities(ctx, "feed", requests, func(index int, result SubscribeResult) {
		if seen[index] || result.Err != nil || result.Opened == nil || len(result.Opened.Initial) != 1 {
			t.Fatalf("result %d: %+v", index, result)
		}
		seen[index] = true
		// An installer may synchronously call the coordinator. This would
		// deadlock if it accidentally ran inside the registration actor.
		done := make(chan error, 1)
		if err := c.waitCommand(ctx, coordinatorCommand{run: func() { done <- nil }}, done); err != nil {
			t.Fatal(err)
		}
	})
	if len(seen) != len(requests) {
		t.Fatal("missing results", seen)
	}
}

func TestEntityBatchCancellationRollsBackOnlyItsOwnRegistrations(t *testing.T) {
	c, from, parentCtx := batchFixture(t)
	ctx, cancel := context.WithCancel(parentCtx)
	defer cancel()
	requests := []EntitySubscribe{{ID: "parent", From: from}, {ID: "cancelled", From: from}, {ID: "never-started", From: from}}
	seen := make(map[int]error)
	c.SubscribeEntities(ctx, "feed", requests, func(index int, result SubscribeResult) {
		seen[index] = result.Err
		if result.Opened != nil {
			t.Fatal("cancelled result transferred an active subscription")
		}
		cancel()
	})
	if len(seen) != 3 || !errors.Is(seen[0], delivery.ErrSubscriptionExists) || !errors.Is(seen[1], context.Canceled) || !errors.Is(seen[2], context.Canceled) {
		t.Fatal("unexpected cancellation results", seen)
	}
	if !c.subscriptions.Exists("parent") || c.subscriptions.Exists("cancelled") || c.subscriptions.Exists("never-started") {
		t.Fatal("cancellation removed an existing interest or leaked its own")
	}
}

func TestEntityBatchEmptyDoesNotCallInstaller(t *testing.T) {
	var c Coordinator
	c.SubscribeEntities(context.Background(), "", nil, func(int, SubscribeResult) { t.Fatal("unexpected result") })
}

type cancelDuringRetirement struct {
	subscriptionDelivery
	cancel context.CancelFunc
}

func (h cancelDuringRetirement) Unsubscribe(id string) (uint64, bool) {
	h.cancel()
	return h.subscriptionDelivery.Unsubscribe(id)
}

func TestAcceptedRetirementReturnsFloorDespiteCancellation(t *testing.T) {
	c, _, parentCtx := batchFixture(t)
	ctx, cancel := context.WithCancel(parentCtx)
	defer cancel()
	c.hub = cancelDuringRetirement{c.hub, cancel}
	revision, err := c.Unsubscribe(ctx, "parent")
	if err != nil || revision != 0 || ctx.Err() == nil || c.subscriptions.Exists("parent") {
		t.Fatal("accepted retirement lost its result", revision, err)
	}
}
