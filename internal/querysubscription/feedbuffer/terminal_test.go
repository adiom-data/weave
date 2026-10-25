package feedbuffer

import (
	"context"
	"errors"
	"sync"
	"testing"

	q "github.com/adiom-data/weave/internal/querysubscription/model"
	"github.com/adiom-data/weave/internal/storage"
)

func failure(id string) q.Event {
	return q.Event{Failure: &q.SubscriptionFailure{SubscriptionID: id, Cause: errors.New("subscription failed")}}
}

func image(id string) q.Event {
	return q.Event{Entity: &q.EntityInstallation{Target: q.EntityTarget{SubscriptionID: id}}}
}

func TestFailurePrecedesDelayedImageWithoutConsumingCapacity(t *testing.T) {
	for _, staging := range []bool{false, true} {
		b := New(2)
		if staging {
			b.BeginInstallation()
		}
		captured, release, done := make(chan struct{}), make(chan struct{}), make(chan error, 1)
		go func() {
			prepared := image("failed") // Prepared before another worker fails.
			close(captured)
			<-release
			done <- b.Offer(prepared)
		}()
		<-captured
		if err := b.Offer(failure("failed")); err != nil {
			t.Fatal(err)
		}
		close(release)
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		// A stale image must not use the second queue/staging slot.
		if err := b.Offer(progress(1)); err != nil {
			t.Fatal("discarded image consumed capacity", err)
		}
		if staging {
			if err := b.FinishInstallation(); err != nil {
				t.Fatal(err)
			}
		}
		event, err := b.Next(context.Background())
		if err != nil || event.Failure == nil {
			t.Fatal("missing failure", event, err)
		}
		event, err = b.Next(context.Background())
		if err != nil || event.Progress == nil {
			t.Fatal("late image escaped after failure", event, err)
		}
		if b.Len() != 0 {
			t.Fatal("unexpected late event")
		}
	}
}

func TestTerminalFilteringPreservesHealthyEffectsAndTransactionClock(t *testing.T) {
	b := New(5)
	if err := b.Offer(failure("failed")); err != nil {
		t.Fatal(err)
	}
	if err := b.Offer(failure("failed-entity")); err != nil {
		t.Fatal(err)
	}
	failed, healthy := q.EntityTarget{SubscriptionID: "failed-entity"}, q.EntityTarget{SubscriptionID: "healthy"}
	owned := q.EntityTarget{SubscriptionID: "failed", Reference: &q.ReferenceRoute{Selector: "selector", Parents: []storage.EntityKey{{Relation: storage.RelationKey{Source: "test", Name: "items"}, Value: "parent"}}}}
	envelope := &q.DeliveryEnvelope{
		Delivery: q.DeliveryMetadata{Kind: q.DeliveryCommit, Checkpoint: &q.Position{SourceID: "test", Value: 1}, Revision: 1},
		Effects:  []q.SubscriptionEffect{{SubscriptionID: "failed"}, {SubscriptionID: "healthy-query"}},
		Entities: []q.EntityEffect{{Target: failed}, {Target: owned}, {Target: healthy}},
	}
	if err := b.Offer(q.Event{Delivery: envelope}); err != nil {
		t.Fatal(err)
	}
	// Removing the last effect still preserves the next complete transaction.
	empty := &q.DeliveryEnvelope{Delivery: q.DeliveryMetadata{Kind: q.DeliveryCommit, Checkpoint: &q.Position{SourceID: "test", Value: 2}, Revision: 2, PreviousRevision: 1}, Effects: []q.SubscriptionEffect{{SubscriptionID: "failed"}}}
	if err := b.Offer(q.Event{Delivery: empty}); err != nil {
		t.Fatal(err)
	}
	if err := b.Offer(progress(3)); err != nil {
		t.Fatal(err)
	}
	if len(envelope.Effects) != 2 || len(envelope.Entities) != 3 || len(empty.Effects) != 1 {
		t.Fatal("filter mutated caller-owned slices")
	}
	_, _ = b.Next(context.Background()) // Failure.
	_, _ = b.Next(context.Background()) // Explicit entity failure.
	event, err := b.Next(context.Background())
	if err != nil || event.Delivery == nil {
		t.Fatal(event, err)
	}
	if len(event.Delivery.Effects) != 1 || event.Delivery.Effects[0].SubscriptionID != "healthy-query" || len(event.Delivery.Entities) != 1 || event.Delivery.Entities[0].Target != healthy {
		t.Fatal("healthy effects were lost or terminal effects escaped", event)
	}
	event, err = b.Next(context.Background())
	if err != nil || event.Delivery == nil || event.Delivery.Delivery.Revision != 2 || len(event.Delivery.Effects) != 0 {
		t.Fatal("empty transaction clock was lost", event, err)
	}
	event, err = b.Next(context.Background())
	if err != nil || event.Progress == nil || event.Progress.Revision != 3 {
		t.Fatal("progress was lost", event, err)
	}
}

func TestFailureNotificationRemainsTerminalAcrossInstallations(t *testing.T) {
	b := New(2)
	first := failure("failed")
	if err := b.Offer(first); err != nil {
		t.Fatal(err)
	}
	b.BeginInstallation()
	if err := b.Offer(failure("failed")); err != nil {
		t.Fatal(err)
	}
	if err := b.Offer(image("failed")); err != nil {
		t.Fatal(err)
	}
	if err := b.Offer(image("healthy")); err != nil {
		t.Fatal(err)
	}
	if err := b.FinishInstallation(); err != nil {
		t.Fatal(err)
	}
	event, err := b.Next(context.Background())
	if err != nil || event.Failure != first.Failure {
		t.Fatal("original failure was replaced", event, err)
	}
	event, err = b.Next(context.Background())
	if err != nil || event.Entity == nil || event.Entity.Target.SubscriptionID != "healthy" {
		t.Fatal(event, err)
	}
	if b.Len() != 0 {
		t.Fatal("duplicate notification escaped")
	}
	b.Close(nil)
	if b.terminal != nil {
		t.Fatal("closed buffer retained terminal IDs")
	}
}

func TestDiscardedInstallationSuppressesFutureWork(t *testing.T) {
	b := New(2)
	b.BeginInstallation()
	if err := b.Offer(image("failed")); err != nil {
		t.Fatal(err)
	}
	b.DiscardSubscription("failed")
	if err := b.Offer(image("failed")); err != nil {
		t.Fatal(err)
	}
	if err := b.Offer(failure("failed")); err != nil {
		t.Fatal(err)
	}
	if err := b.Offer(image("healthy")); err != nil {
		t.Fatal(err)
	}
	if err := b.Offer(progress(1)); err != nil {
		t.Fatal(err)
	}
	if err := b.FinishInstallation(); err != nil {
		t.Fatal(err)
	}
	event, err := b.Next(context.Background())
	if err != nil || event.Entity == nil || event.Entity.Target.SubscriptionID != "healthy" {
		t.Fatal(event, err)
	}
	event, err = b.Next(context.Background())
	if err != nil || event.Progress == nil {
		t.Fatal("discarded installation resumed publication", event, err)
	}
}

func TestConcurrentFailureAndImageHaveOnePublicationOrder(t *testing.T) {
	for range 500 {
		b := New(2)
		start := make(chan struct{})
		var workers sync.WaitGroup
		for _, event := range []q.Event{failure("failed"), image("failed")} {
			workers.Add(1)
			go func() {
				defer workers.Done()
				<-start
				if err := b.Offer(event); err != nil {
					t.Error(err)
				}
			}()
		}
		close(start)
		workers.Wait()
		seenFailure := false
		for range b.Len() {
			event, err := b.Next(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if seenFailure {
				t.Fatal("image published after terminal failure", event)
			}
			seenFailure = event.Failure != nil
		}
		if !seenFailure {
			t.Fatal("failure notification lost")
		}
	}
}

func TestDiscardPreservesOtherSubscriptionsAcceptedHistory(t *testing.T) {
	b := New(4)
	b.BeginInstallation()
	for _, event := range []q.Event{image("other"), failure("other"), image("discarded"), failure("discarded")} {
		if err := b.Offer(event); err != nil {
			t.Fatal(err)
		}
	}
	b.DiscardSubscription("discarded")
	if err := b.Offer(image("other")); err != nil {
		t.Fatal(err)
	}
	if err := b.Offer(progress(1)); err != nil {
		t.Fatal(err)
	}
	if err := b.FinishInstallation(); err != nil {
		t.Fatal(err)
	}
	event, err := b.Next(context.Background())
	if err != nil || event.Entity == nil || event.Entity.Target.SubscriptionID != "other" {
		t.Fatal("accepted image history changed", event, err)
	}
	event, err = b.Next(context.Background())
	if err != nil || event.Failure == nil || event.Failure.SubscriptionID != "other" {
		t.Fatal("unrelated terminal notification lost", event, err)
	}
	event, err = b.Next(context.Background())
	if err != nil || event.Progress == nil {
		t.Fatal("terminal subscription resumed after installation", event, err)
	}
	if b.Len() != 0 {
		t.Fatal("discard left stale events")
	}
}
