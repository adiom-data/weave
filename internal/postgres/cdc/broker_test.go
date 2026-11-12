package cdc

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pglogrepl"
)

func TestBrokerSuppressesReplicationReplay(t *testing.T) {
	broker := NewBroker()
	subscription, err := broker.Subscribe()
	if err != nil {
		t.Fatal(err)
	}
	defer subscription.Close()

	for _, lsn := range []pglogrepl.LSN{10, 20, 10, 20, 30} {
		if err := broker.Publish(context.Background(), Transaction{CommitLSN: lsn}); err != nil {
			t.Fatal(err)
		}
	}

	for _, want := range []pglogrepl.LSN{10, 20, 30} {
		select {
		case event := <-subscription.Events:
			if event.Transaction == nil || event.Transaction.CommitLSN != want {
				t.Fatalf("event = %+v, want commit %v", event, want)
			}
		default:
			t.Fatalf("missing commit %v", want)
		}
	}
	select {
	case event := <-subscription.Events:
		t.Fatalf("unexpected replayed event: %+v", event)
	default:
	}
}

func TestBrokerStartupPositionIsOrderedAndNotReappliedOnReconnect(t *testing.T) {
	receive := func(subscription Subscription) StreamEvent {
		t.Helper()
		select {
		case event := <-subscription.Events:
			return event
		default:
			t.Fatal("expected queued stream event")
			return StreamEvent{}
		}
	}
	broker := NewBroker()
	broker.MarkReady(10)
	if err := broker.WaitReady(t.Context()); err != nil {
		t.Fatal(err)
	}
	if broker.Progress() != 10 {
		t.Fatal("readiness preceded the startup position")
	}
	subscription, err := broker.Subscribe()
	if err != nil {
		t.Fatal(err)
	}
	defer subscription.Close()
	initial := receive(subscription)
	if initial.Transaction != nil || initial.Position != 10 {
		t.Fatalf("initial position: %+v", initial)
	}
	if err := broker.Publish(t.Context(), Transaction{CommitLSN: 20, EndLSN: 21}); err != nil {
		t.Fatal(err)
	}
	// Reconnection can inspect an older confirmed position. Neither that nor
	// a newer slot position may replace the running broker's ordered history.
	broker.MarkReady(5)
	broker.MarkReady(100)
	if broker.Progress() != 21 {
		t.Fatalf("reconnect replaced live progress: %v", broker.Progress())
	}
	commit := receive(subscription)
	if commit.Transaction == nil || commit.Position != 21 {
		t.Fatalf("queued commit changed on reconnect: %+v", commit)
	}
	select {
	case event := <-subscription.Events:
		t.Fatalf("reconnect published another startup position: %+v", event)
	default:
	}
	joined, err := broker.Subscribe()
	if err != nil {
		t.Fatal(err)
	}
	defer joined.Close()
	if event := receive(joined); event.Transaction != nil || event.Position != 21 {
		t.Fatalf("later consumer did not join at live progress: %+v", event)
	}
}

func TestBrokerFatalFailureReachesCurrentAndFutureConsumers(t *testing.T) {
	broker := NewBroker()
	broker.MarkReady(0)
	subscription, err := broker.Subscribe()
	if err != nil {
		t.Fatal(err)
	}
	want := errors.New("cannot decode WAL")
	broker.Fail(want)
	if got := <-subscription.Err; !errors.Is(got, want) {
		t.Fatalf("consumer error = %v, want %v", got, want)
	}
	if _, err := broker.Subscribe(); !errors.Is(err, want) {
		t.Fatalf("future subscription error = %v, want %v", err, want)
	}
	if err := broker.WaitReady(context.Background()); !errors.Is(err, want) {
		t.Fatalf("ready error = %v, want %v", err, want)
	}
}
