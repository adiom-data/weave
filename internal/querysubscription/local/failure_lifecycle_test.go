package local

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/adiom-data/weave/internal/datastore"
	"github.com/adiom-data/weave/internal/engine/changeprocessor"
	"github.com/adiom-data/weave/internal/engine/delivery"
	q "github.com/adiom-data/weave/internal/querysubscription/model"
	"github.com/adiom-data/weave/internal/storage"
)

type conditionalConversionPlan struct {
	nonComparingPlan
	calls *int
}

func (p conditionalConversionPlan) Key(record storage.Record) (datastore.SortKey, error) {
	*p.calls++
	if string(record["id"].Data) == "bad" {
		return nil, errors.New("adapter conversion failed")
	}
	return p.nonComparingPlan.Key(record)
}

type retirementFloorCoordinator struct {
	coordinatorStub
	floor uint64
	calls int
}

func (c *retirementFloorCoordinator) Unsubscribe(context.Context, string) (uint64, error) {
	c.calls++
	return c.floor, nil
}

func failureLifecycleFixture() (*localFeed, *localSubscription, *retirementFloorCoordinator, *int) {
	feed := newFeedTestFeed()
	feed.ctx, feed.cancel = context.Background(), func() {}
	relation := storage.RelationKey{Source: "test", Name: "items"}
	coordinator := &retirementFloorCoordinator{floor: 4}
	calls := new(int)
	failed := &localSubscription{id: "failed", nativeID: feed.nativeID("failed"), owner: feed, coordinator: coordinator, installed: true,
		plan: conditionalConversionPlan{nonComparingPlan{relation: relation}, calls}}
	healthy := &localSubscription{id: "healthy", nativeID: feed.nativeID("healthy"), owner: feed, installed: true, plan: nonComparingPlan{relation: relation}}
	feed.subscriptions[failed.id], feed.subscriptions[healthy.id] = failed, healthy
	_ = feed.events.RegisterSubscription(failed.id)
	_ = feed.events.RegisterSubscription(healthy.id)
	return feed, failed, coordinator, calls
}

func failureLifecycleGroup(feed *localFeed, revision, previous uint64, value string, healthy bool) delivery.GroupMessage {
	ids := []string{"failed"}
	if healthy {
		ids = append(ids, "healthy")
	}
	message := testGroup(feed, revision, previous, ids...)
	for index, id := range ids {
		message.Batches[index].Events = []delivery.SubscriptionEvent{{Kind: changeprocessor.QueryUpdated,
			Key:    storage.EntityKey{Relation: storage.RelationKey{Source: "test", Name: "items"}, Value: id},
			Record: storage.Record{"id": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte(value)}},
		}}
	}
	return message
}

func requireOnlyHealthyEffects(t *testing.T, event q.Event) {
	t.Helper()
	if event.Failure != nil {
		t.Fatal("duplicate terminal failure", event.Failure)
	}
	if event.Delivery == nil {
		return
	}
	for _, effect := range event.Delivery.Effects {
		if effect.SubscriptionID != "healthy" {
			t.Fatal("failed subscription published after failure", effect)
		}
	}
	if len(event.Delivery.Entities) != 0 {
		t.Fatal("failed entity published after failure")
	}
}

func TestFailedSubscriptionDrainsQueuedSuccessorsWithoutEffectsOrRepeatedErrors(t *testing.T) {
	feed, failed, coordinator, calls := failureLifecycleFixture()
	if err := feed.acceptGroup(failureLifecycleGroup(feed, 1, 0, "bad", false)); err != nil {
		t.Fatal(err)
	}
	if _, err := feed.Next(context.Background()); err != nil {
		t.Fatal(err)
	} // T1 metadata.
	event, err := feed.Next(context.Background())
	if err != nil || event.Failure == nil {
		t.Fatal("missing terminal failure", event, err)
	}
	if !failed.discardsQueuedEffects() || feed.subscriptions[failed.id] == nil {
		t.Fatal("failed handle must remain metadata-only until its floor")
	}
	// A successful conversion and another invalid tuple were already accepted
	// upstream. Neither may execute conversion or emit another failure now.
	for revision, value := range []string{"good", "bad", "good"} {
		position := uint64(revision + 2)
		if err := feed.acceptGroup(failureLifecycleGroup(feed, position, position-1, value, true)); err != nil {
			t.Fatal(err)
		}
		event, err = feed.Next(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		requireOnlyHealthyEffects(t, event)
		if len(event.Delivery.Effects) != 1 {
			t.Fatal("healthy subscription lost its update", event)
		}
	}
	if *calls != 1 || coordinator.calls != 1 {
		t.Fatal("terminal failure was reevaluated or unsubscribed twice", *calls, coordinator.calls)
	}
	if feed.subscriptions[failed.id] != nil || len(feed.retirements) != 0 || feed.flushedRevision != 4 {
		t.Fatal("failed handle did not retire at its floor")
	}
}

func TestFailureSuppressesPreviouslyConvertedEffectsWaitingForProgress(t *testing.T) {
	feed, failed, _, calls := failureLifecycleFixture()
	// T2 is converted but cannot publish until the outstanding T1 progress
	// response arrives. T3 then fails, making the earlier buffered image stale
	// with respect to the already-published terminal failure notification.
	if err := feed.acceptGroup(failureLifecycleGroup(feed, 2, 1, "good", true)); err != nil {
		t.Fatal(err)
	}
	if err := feed.acceptGroup(failureLifecycleGroup(feed, 3, 2, "bad", false)); err != nil {
		t.Fatal(err)
	}
	event, err := feed.Next(context.Background())
	if err != nil || event.Failure == nil {
		t.Fatal("missing terminal failure", event, err)
	}
	if err := feed.acceptProgress(delivery.ProgressReport{Checkpoint: &q.Position{SourceID: "test", Value: 1}, Revision: 1}); err != nil {
		t.Fatal(err)
	}
	for range 3 {
		event, err = feed.Next(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		requireOnlyHealthyEffects(t, event)
	}
	if err := feed.acceptGroup(failureLifecycleGroup(feed, 4, 3, "good", true)); err != nil {
		t.Fatal(err)
	}
	event, err = feed.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	requireOnlyHealthyEffects(t, event)
	if *calls != 2 || feed.subscriptions[failed.id] != nil {
		t.Fatal("failure resumed conversion or leaked routing", *calls)
	}
}

func TestSubscriptionFailureNotificationIsTerminalAndIdempotent(t *testing.T) {
	feed, failed, coordinator, _ := failureLifecycleFixture()
	failed.failSubscription(errors.New("first"))
	failed.failSubscription(errors.New("second"))
	if coordinator.calls != 1 || feed.events.Len() != 1 {
		t.Fatal("failure was not idempotent")
	}
}

func TestFailedSubscriptionRejectsNewWindowOperationsBeforeCleanup(t *testing.T) {
	subscription := &localSubscription{installed: true, coordinator: coordinatorStub{}}
	subscription.markFailed()
	if err := subscription.UpdateWindow(context.Background(), nil, "request"); err != q.ErrClosed {
		t.Fatal("failed subscription accepted a new window operation", err)
	}
}

func TestFailureTransitionDoesNotWaitForWindowOperation(t *testing.T) {
	subscription := &localSubscription{installed: true}
	subscription.lifecycle.RLock() // UpdateWindow retains this lock across IO.
	done := make(chan bool, 1)
	go func() { done <- subscription.markFailed() }()
	select {
	case changed := <-done:
		subscription.lifecycle.RUnlock()
		if !changed || !subscription.discardsQueuedEffects() {
			t.Fatal("failure did not become terminal")
		}
	case <-time.After(time.Second):
		subscription.lifecycle.RUnlock()
		<-done
		t.Fatal("failure transition blocked on an in-flight window operation")
	}
}
