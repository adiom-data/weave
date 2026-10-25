package engine

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/adiom-data/weave/internal/datastore"
	"github.com/adiom-data/weave/internal/engine/delivery"
	"github.com/adiom-data/weave/internal/engine/recovery"
	"github.com/adiom-data/weave/internal/engine/windowloader"
)

func TestReceivedProgressCannotAdmitSnapshotBeforeApplication(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	feed := &coordinatorFeed{events: make(chan datastore.CommittedChanges, 1)}
	c := &Coordinator{feed: feed, source: &coordinatorSource{}, lastCheckpoint: testPosition(1),
		hub: delivery.NewSubscriptionHub(nil), recovery: recovery.NewManager(1, 1, 1)}
	results, readerDone := c.readFeed(ctx)
	defer func() { cancel(); <-readerDone }()
	feed.events <- datastore.CommittedChanges{Checkpoint: testPosition(2)}
	for !feed.Covers(testPosition(2)) {
		select {
		case <-ctx.Done():
			t.Fatal("reader did not receive event")
		default:
		}
		time.Sleep(time.Millisecond)
	}
	window := &windowloader.PendingWindow{Fence: testPosition(2)}
	if c.appliedThrough(window.Fence) {
		t.Fatal("receive advanced the actor's applied watermark")
	}
	if _, err := c.activateSnapshot(ctx, window); !errors.Is(err, errSnapshotNotApplied) {
		t.Fatalf("early activation=%v", err)
	}
	if err := c.reconcileSnapshot(ctx, window); !errors.Is(err, errSnapshotNotApplied) {
		t.Fatalf("early reconciliation=%v", err)
	}
	var result feedResult
	select {
	case result = <-results:
	case <-ctx.Done():
		t.Fatal("reader did not hand off event")
	}
	if result.err != nil {
		t.Fatal(result.err)
	}
	if err := c.applyFeedEvent(ctx, result.event); err != nil {
		t.Fatal(err)
	}
	if err := c.admitSnapshot(window); err != nil {
		t.Fatalf("applied snapshot rejected: %v", err)
	}
	if c.appliedThrough(foreignPosition(1)) || c.appliedThrough(testPosition(3)) || c.appliedThrough(nil) {
		t.Fatal("foreign, future, or absent fence admitted")
	}
	if feed.acknowledged() != testPosition(2) {
		t.Fatal("applied checkpoint was not acknowledged")
	}
}

func TestAppliedProgressEventAdmitsSnapshotWithoutRowActivity(t *testing.T) {
	feed := &coordinatorFeed{events: make(chan datastore.CommittedChanges)}
	c := &Coordinator{feed: feed, source: &coordinatorSource{}, lastCheckpoint: testPosition(1),
		hub: delivery.NewSubscriptionHub(nil), recovery: recovery.NewManager(1, 1, 1)}
	window := &windowloader.PendingWindow{Fence: testPosition(2)}
	if err := c.applyFeedEvent(context.Background(), datastore.ProgressEvent(testPosition(2))); err != nil {
		t.Fatal(err)
	}
	if err := c.admitSnapshot(window); err != nil {
		t.Fatalf("applied barrier did not admit snapshot: %v", err)
	}
	if feed.acknowledged() != testPosition(2) {
		t.Fatal("barrier was not acknowledged")
	}
}

func TestReadyCDCPrecedesBarrierFailureAndRemainsBounded(t *testing.T) {
	for _, ready := range []bool{false, true} {
		name := "failure_without_coverage"
		if ready {
			name = "already_ready_coverage"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			feed := &coordinatorFeed{events: make(chan datastore.CommittedChanges)}
			barrierErr := errors.New("barrier unavailable")
			activation := &pendingInitialActivation{
				materialized: &materializedWindow{pending: &windowloader.PendingWindow{Fence: testPosition(2)}},
				result:       make(chan initialActivationResult, 1),
			}
			c := &Coordinator{feed: feed, source: &coordinatorSource{}, lastCheckpoint: testPosition(1),
				hub: delivery.NewSubscriptionHub(nil), recovery: recovery.NewManager(1, 1, 1),
				pendingInitial:   map[string]*pendingInitialActivation{"waiting": activation},
				progressStarting: true, progressFence: testPosition(2),
			}
			received := make(chan feedResult, 2)
			if ready {
				received <- feedResult{event: datastore.ProgressEvent(testPosition(2))}
				received <- feedResult{event: datastore.ProgressEvent(testPosition(3))}
			}
			applied, err := c.applyReadyFeed(ctx, received)
			if err != nil || applied != ready {
				t.Fatalf("ready CDC: applied=%v err=%v", applied, err)
			}
			c.completeProgressRequest(progressResult{fence: testPosition(2), err: barrierErr})
			if c.progressStarting || c.progressFence != nil {
				t.Fatal("failed request retained active lifecycle state")
			}
			if ready {
				if c.lastCheckpoint != testPosition(2) || len(received) != 1 {
					t.Fatal("auxiliary action drained more than one CDC event")
				}
				if c.pendingInitial["waiting"] != activation || len(activation.result) != 0 {
					t.Fatal("barrier failure aborted a snapshot already covered by CDC")
				}
			} else {
				select {
				case result := <-activation.result:
					if !errors.Is(result.err, barrierErr) {
						t.Fatalf("isolated barrier error=%v", result.err)
					}
				default:
					t.Fatal("barrier failure did not fail uncovered snapshot")
				}
			}
		})
	}
}

type observedReader struct {
	*coordinatorFeed
	calls   atomic.Int64
	started chan struct{}
}

func (f *observedReader) Receive(ctx context.Context) (datastore.ChangeEvent, error) {
	f.calls.Add(1)
	f.started <- struct{}{}
	return f.coordinatorFeed.Receive(ctx)
}

type readerSource struct {
	*coordinatorSource
	reader datastore.ChangeFeed
}

func (s readerSource) OpenFeed(context.Context) (datastore.ChangeFeed, error) { return s.reader, nil }

func TestCommandsDoNotRestartIdleSourceReader(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	f := &observedReader{coordinatorFeed: newCoordinatorFeed(), started: make(chan struct{}, 16)}
	c, err := StartCoordinator(ctx, readerSource{coordinatorSource: &coordinatorSource{}, reader: f})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { cancel(); <-c.done }()
	// The baseline is received, followed by one long-lived idle Receive.
	for i := 0; i < 2; i++ {
		select {
		case <-f.started:
		case <-ctx.Done():
			t.Fatal("reader did not start")
		}
	}
	for i := 0; i < 8; i++ {
		report, err := c.ReportProgress(ctx, "connection")
		if err != nil || report.Checkpoint != testPosition(0) {
			t.Fatalf("command=%+v err=%v", report, err)
		}
	}
	if got := f.calls.Load(); got != 2 {
		t.Fatalf("commands restarted the idle reader: calls=%d", got)
	}
}
