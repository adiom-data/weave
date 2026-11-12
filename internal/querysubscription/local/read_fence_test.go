package local

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/adiom-data/weave/internal/engine/delivery"
	q "github.com/adiom-data/weave/internal/querysubscription/model"
	"github.com/adiom-data/weave/internal/storage"
)

func TestPublishedMetadataCannotRewriteSourceProof(t *testing.T) {
	for _, kind := range []string{"progress", "commit"} {
		t.Run(kind, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
			defer cancel()
			const count = 256
			feed, err := New(coordinatorStub{
				waitFence: func(context.Context, string) (storage.Fence, error) {
					return &q.Position{SourceID: "test", Value: count}, nil
				},
			}).openFeed(ctx, q.FeedOptions{BufferLimit: count})
			if err != nil {
				t.Fatal(err)
			}
			defer feed.Close(context.Background())
			done := make(chan error, 1)
			go func() {
				for range count {
					event, err := feed.Next(ctx)
					if err != nil {
						done <- err
						return
					}
					if event.Progress != nil {
						event.Progress.Position.Value = 1 << 62
					} else {
						event.Delivery.Delivery.Checkpoint.Value = 1 << 62
					}
				}
				done <- nil
			}()
			for revision := uint64(1); revision <= count; revision++ {
				position := &q.Position{SourceID: "test", Value: revision}
				if kind == "progress" {
					err = feed.acceptProgress(delivery.ProgressReport{Checkpoint: position, Revision: revision, PreviousGroupRevision: revision - 1})
				} else {
					feed.feedMu.Lock()
					err = feed.addDeliveryLocked(revision, &pendingDelivery{metadata: q.DeliveryMetadata{Kind: q.DeliveryCommit, Checkpoint: position, Revision: revision, PreviousRevision: revision - 1}})
					if err == nil {
						err = feed.drainFeedLocked()
					}
					feed.feedMu.Unlock()
				}
				if err != nil {
					t.Fatal(err)
				}
			}
			if err := <-done; err != nil {
				t.Fatal(err)
			}
			proof, err := feed.WaitForFence(ctx, "opaque")
			if err != nil || proof == nil || proof.SourceID != "test" || proof.Value != count {
				t.Fatalf("consumer metadata changed source proof: %+v, %v", proof, err)
			}
		})
	}
}

func TestCoveredFenceReusesOneOrderedReportAcrossWaiters(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	var reports atomic.Int32
	started, release := make(chan struct{}), make(chan struct{})
	validated := make(chan struct{}, 16)
	coordinator := coordinatorStub{
		waitFence: func(context.Context, string) (storage.Fence, error) {
			validated <- struct{}{}
			return &q.Position{SourceID: "test", Value: 20}, nil
		},
		progress: func(ctx context.Context, _ string) (*delivery.ProgressReport, error) {
			n := reports.Add(1)
			if n == 1 {
				close(started)
				select {
				case <-release:
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			}
			return &delivery.ProgressReport{Checkpoint: &q.Position{SourceID: "test", Value: 20}, Revision: uint64(n), PreviousGroupRevision: uint64(n - 1)}, nil
		},
	}
	feed, err := New(coordinator).openFeed(ctx, q.FeedOptions{BufferLimit: 16})
	if err != nil {
		t.Fatal(err)
	}
	defer feed.Close(context.Background())
	var workers sync.WaitGroup
	results := make(chan error, 16)
	for range 16 {
		workers.Add(1)
		go func() { defer workers.Done(); _, err := feed.WaitForFence(ctx, "same-fence"); results <- err }()
	}
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	for range 16 {
		select {
		case <-validated:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	close(release)
	workers.Wait()
	close(results)
	for err := range results {
		if err != nil {
			t.Fatal(err)
		}
	}
	if _, err := feed.WaitForFence(ctx, "same-fence"); err != nil {
		t.Fatal(err)
	}
	if got := reports.Load(); got != 1 {
		t.Fatalf("covered fence generated %d reports", got)
	}
	if feed.events.Len() != 1 {
		t.Fatalf("duplicate proof queued: %d", feed.events.Len())
	}
}

func TestFenceReportLeaderCancellationDoesNotCancelOtherWaiters(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	started, joined := make(chan struct{}), make(chan struct{})
	var reports atomic.Int32
	coordinator := coordinatorStub{
		waitFence: func(_ context.Context, token string) (storage.Fence, error) {
			if token == "follower" {
				close(joined)
			}
			return &q.Position{SourceID: "test", Value: 20}, nil
		},
		progress: func(ctx context.Context, _ string) (*delivery.ProgressReport, error) {
			if reports.Add(1) == 1 {
				close(started)
				<-ctx.Done()
				return nil, ctx.Err()
			}
			return &delivery.ProgressReport{Checkpoint: &q.Position{SourceID: "test", Value: 20}, Revision: 1}, nil
		},
	}
	feed, err := New(coordinator).openFeed(ctx, q.FeedOptions{BufferLimit: 16})
	if err != nil {
		t.Fatal(err)
	}
	defer feed.Close(context.Background())
	leader, stop := context.WithCancel(ctx)
	defer stop()
	first, second := make(chan error, 1), make(chan error, 1)
	go func() { _, err := feed.WaitForFence(leader, "leader"); first <- err }()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	go func() { _, err := feed.WaitForFence(ctx, "follower"); second <- err }()
	select {
	case <-joined:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	stop()
	if err := <-first; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err := <-second; err != nil {
		t.Fatalf("leader cancellation escaped to another waiter: %v", err)
	}
	if reports.Load() != 2 {
		t.Fatalf("expected one canceled attempt and one accepted report, got %d", reports.Load())
	}
}

func TestFenceReportFollowsNewerConcurrentDemand(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	started, release, joined := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var reports atomic.Int32
	coordinator := coordinatorStub{
		waitFence: func(_ context.Context, token string) (storage.Fence, error) {
			point := uint64(20)
			if token == "newer" {
				point = 30
				close(joined)
			}
			return &q.Position{SourceID: "test", Value: point}, nil
		},
		progress: func(ctx context.Context, _ string) (*delivery.ProgressReport, error) {
			n := reports.Add(1)
			point := uint64(30)
			if n == 1 {
				point = 20
				close(started)
				select {
				case <-release:
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			}
			return &delivery.ProgressReport{Checkpoint: &q.Position{SourceID: "test", Value: point}, Revision: uint64(n), PreviousGroupRevision: uint64(n - 1)}, nil
		},
	}
	feed, err := New(coordinator).openFeed(ctx, q.FeedOptions{BufferLimit: 16})
	if err != nil {
		t.Fatal(err)
	}
	defer feed.Close(context.Background())
	first, second := make(chan error, 1), make(chan error, 1)
	go func() { _, err := feed.WaitForFence(ctx, "older"); first <- err }()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	go func() {
		point, err := feed.WaitForFence(ctx, "newer")
		if err == nil && point.Value < 30 {
			err = errors.New("older report satisfied newer demand")
		}
		second <- err
	}()
	select {
	case <-joined:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	close(release)
	for _, done := range []chan error{first, second} {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
	if reports.Load() != 2 {
		t.Fatalf("newer demand must get one follow-up report: %d", reports.Load())
	}
}
