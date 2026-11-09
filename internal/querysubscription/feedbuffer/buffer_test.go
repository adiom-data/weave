package feedbuffer

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/adiom-data/weave/internal/querysubscription/model"
)

func progress(n uint64) querysubscription.Event {
	return querysubscription.Event{Progress: &querysubscription.Progress{Position: &querysubscription.Position{SourceID: "test", Value: n}, Revision: n}}
}

func TestOverlappingInstallationsHoldCompleteEnvelopes(t *testing.T) {
	b := New(4)
	b.BeginInstallation()
	b.BeginInstallation()
	if err := b.Offer(progress(1)); err != nil {
		t.Fatal(err)
	}
	if err := b.FinishInstallation(); err != nil {
		t.Fatal(err)
	}
	if b.Len() != 0 {
		t.Fatal("event escaped the second installation")
	}
	if err := b.FinishInstallation(); err != nil {
		t.Fatal(err)
	}
	event, err := b.Next(context.Background())
	if err != nil || event.Progress.Position.Value != 1 {
		t.Fatalf("event=%+v err=%v", event, err)
	}
}

func TestInstallationStartingDuringPublicationCannotLeakItsEvents(t *testing.T) {
	for attempt := 0; attempt < 100; attempt++ {
		b := New(1025)
		b.BeginInstallation()
		for i := uint64(1); i <= 1024; i++ {
			if err := b.Offer(progress(i)); err != nil {
				t.Fatal(err)
			}
		}
		var workers sync.WaitGroup
		workers.Add(1)
		go func() {
			defer workers.Done()
			if err := b.FinishInstallation(); err != nil {
				t.Error(err)
			}
		}()
		b.BeginInstallation()
		if err := b.Offer(progress(1025)); err != nil {
			t.Fatal(err)
		}
		workers.Wait()
		if b.Len() > 1024 {
			t.Fatal("new installation's event escaped during an earlier drain")
		}
		if err := b.FinishInstallation(); err != nil {
			t.Fatal(err)
		}
		for i := uint64(1); i <= 1025; i++ {
			event, err := b.Next(context.Background())
			if err != nil || event.Progress.Position.Value != i {
				t.Fatalf("want %d: %+v %v", i, event, err)
			}
		}
	}
}

func TestFailedInstallationOnlyDiscardsItsEffects(t *testing.T) {
	b := New(4)
	register(t, b, "failed", "healthy")
	b.BeginInstallation()
	envelope := &querysubscription.DeliveryEnvelope{Delivery: querysubscription.DeliveryMetadata{Kind: querysubscription.DeliveryCommit, Checkpoint: &querysubscription.Position{SourceID: "test", Value: 1}, Revision: 1}, Effects: []querysubscription.SubscriptionEffect{{SubscriptionID: "failed"}, {SubscriptionID: "healthy"}}}
	if err := b.Offer(querysubscription.Event{Delivery: envelope}); err != nil {
		t.Fatal(err)
	}
	if err := b.Offer(querysubscription.Event{Failure: &querysubscription.SubscriptionFailure{SubscriptionID: "failed", Cause: errors.New("failed")}}); err != nil {
		t.Fatal(err)
	}
	if err := b.Offer(progress(2)); err != nil {
		t.Fatal(err)
	}
	b.DiscardSubscription("failed")
	if len(envelope.Effects) != 2 {
		t.Fatal("discard mutated caller-owned envelope")
	}
	if err := b.FinishInstallation(); err != nil {
		t.Fatal(err)
	}
	event, err := b.Next(context.Background())
	if err != nil || len(event.Delivery.Effects) != 1 || event.Delivery.Effects[0].SubscriptionID != "healthy" {
		t.Fatalf("retained event=%+v err=%v", event, err)
	}
	event, err = b.Next(context.Background())
	if err != nil || event.Progress == nil || event.Progress.Position.Value != 2 {
		t.Fatalf("progress=%+v err=%v", event, err)
	}
}

func TestDiscardRetainsOtherEntityEffectsAndTransactionClock(t *testing.T) {
	b := New(4)
	register(t, b, "failed", "healthy")
	b.BeginInstallation()
	failed := querysubscription.EntityTarget{SubscriptionID: "failed"}
	healthy := querysubscription.EntityTarget{SubscriptionID: "healthy"}
	if err := b.Offer(querysubscription.Event{Entity: &querysubscription.EntityInstallation{Target: failed}}); err != nil {
		t.Fatal(err)
	}
	envelope := &querysubscription.DeliveryEnvelope{
		Delivery: querysubscription.DeliveryMetadata{Kind: querysubscription.DeliveryCommit, Checkpoint: &querysubscription.Position{SourceID: "test", Value: 1}, Revision: 1},
		Entities: []querysubscription.EntityEffect{{Target: failed}, {Target: healthy}},
	}
	if err := b.Offer(querysubscription.Event{Delivery: envelope}); err != nil {
		t.Fatal(err)
	}
	if err := b.Offer(progress(2)); err != nil {
		t.Fatal(err)
	}
	b.DiscardSubscription("failed")
	if len(envelope.Entities) != 2 {
		t.Fatal("discard mutated caller-owned entity payloads")
	}
	if err := b.FinishInstallation(); err != nil {
		t.Fatal(err)
	}
	event, err := b.Next(context.Background())
	if err != nil || event.Delivery == nil || len(event.Delivery.Entities) != 1 || event.Delivery.Entities[0].Target.SubscriptionID != "healthy" || event.Delivery.Delivery.Revision != 1 {
		t.Fatalf("lost transaction or healthy entity: %+v %v", event, err)
	}
	event, err = b.Next(context.Background())
	if err != nil || event.Progress == nil || event.Progress.Revision != 2 {
		t.Fatalf("lost progress: %+v %v", event, err)
	}
}

func TestOverflowIsTerminalAndReleasesStagedRecords(t *testing.T) {
	b := New(1)
	b.BeginInstallation()
	if err := b.Offer(progress(1)); err != nil {
		t.Fatal(err)
	}
	if err := b.Offer(progress(2)); !errors.Is(err, querysubscription.ErrBackpressure) {
		t.Fatalf("overflow=%v", err)
	}
	if len(b.staged) != 0 {
		t.Fatal("closed buffer retained staged records")
	}
	if err := b.FinishInstallation(); !errors.Is(err, querysubscription.ErrBackpressure) {
		t.Fatalf("finish=%v", err)
	}
	if _, err := b.Next(context.Background()); !errors.Is(err, querysubscription.ErrBackpressure) {
		t.Fatalf("next=%v", err)
	}
}

func TestPositionValidationAndCloseShareOneTerminalState(t *testing.T) {
	b := New(2)
	if err := b.Offer(progress(2)); err != nil {
		t.Fatal(err)
	}
	if err := b.Offer(progress(1)); err == nil {
		t.Fatal("regressing progress accepted")
	}
	b.Close(querysubscription.ErrClosed)
	if _, err := b.Next(context.Background()); err == nil || errors.Is(err, querysubscription.ErrClosed) {
		t.Fatalf("first failure lost: %v", err)
	}
	closed := New(1)
	closed.BeginInstallation()
	closed.Close(nil)
	if err := closed.FinishInstallation(); !errors.Is(err, querysubscription.ErrClosed) {
		t.Fatalf("finish after close=%v", err)
	}
}
