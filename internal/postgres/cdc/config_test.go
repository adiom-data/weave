package cdc

import (
	"log/slog"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

func TestReplicationConfigurationDefaultsOverridesAndValidation(t *testing.T) {
	if got := (ReplicationOptions{}).WithDefaults(); got != DefaultReplicationOptions() {
		t.Fatalf("unexpected defaults: %+v", got)
	}
	custom := ReplicationOptions{RetryMinDelay: 3 * time.Millisecond, RetryMaxDelay: 7 * time.Millisecond, StatusInterval: 11 * time.Millisecond}
	base, err := pgconn.ParseConfig("postgres://localhost/app?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	r, err := NewReplicator(base, "publication", "slot", NewBroker(), slog.Default(), ReplicatorOptions{Replication: custom})
	if err != nil {
		t.Fatal(err)
	}
	if r.replication != custom {
		t.Fatalf("replicator ignored custom timing: %+v", r.replication)
	}
	for _, invalid := range []ReplicationOptions{
		{RetryMinDelay: -1}, {RetryMaxDelay: -1}, {StatusInterval: -1},
		{RetryMinDelay: 5 * time.Second, RetryMaxDelay: time.Second},
	} {
		if _, err := NewReplicator(base, "publication", "slot", NewBroker(), slog.Default(), ReplicatorOptions{Replication: invalid}); err == nil {
			t.Fatalf("accepted invalid timing: %+v", invalid)
		}
	}
	delay := custom.RetryMinDelay
	for _, want := range []time.Duration{6 * time.Millisecond, 7 * time.Millisecond, 7 * time.Millisecond} {
		delay = custom.nextRetryDelay(delay)
		if delay != want {
			t.Fatalf("retry delay = %v, want %v", delay, want)
		}
	}
	// A near-limit duration must saturate without overflowing negative.
	large := ReplicationOptions{RetryMaxDelay: time.Duration(1<<63 - 1)}
	if got := large.nextRetryDelay(large.RetryMaxDelay/2 + 1); got != large.RetryMaxDelay {
		t.Fatalf("retry delay overflowed: %v", got)
	}
}

func TestBrokerUsesConfiguredConsumerCapacity(t *testing.T) {
	b, err := NewBrokerWithOptions(BrokerOptions{ConsumerBufferLimit: 3})
	if err != nil {
		t.Fatal(err)
	}
	s, err := b.Subscribe()
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if got := cap(s.Events); got != 3 {
		t.Fatalf("consumer capacity = %d, want 3", got)
	}
	if _, err := NewBrokerWithOptions(BrokerOptions{ConsumerBufferLimit: -1}); err == nil {
		t.Fatal("negative consumer capacity accepted")
	}
}
