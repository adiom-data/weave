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

func TestBrokerFatalFailureReachesCurrentAndFutureConsumers(t *testing.T) {
	broker := NewBroker()
	broker.MarkReady()
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
