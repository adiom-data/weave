package engine

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/adiom-data/weave/internal/datastore"
	"github.com/adiom-data/weave/internal/storage"
)

type readFenceSource struct{ *progressCoordinatorSource }

func (*readFenceSource) DecodeReadFence(_ context.Context, token string) (storage.Fence, error) {
	if token != "test-fence" {
		return nil, errors.New("invalid fence")
	}
	return testPosition(2), nil
}

func TestReadFenceWaitUsesAppliedProgressWithoutBlockingActor(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	feed := newCoordinatorFeed()
	requested, release := make(chan struct{}), make(chan struct{})
	source := &readFenceSource{&progressCoordinatorSource{coordinatorSource: &coordinatorSource{feed: feed}, requestFn: func(ctx context.Context, _ storage.Fence) error {
		close(requested)
		select {
		case <-release:
		case <-ctx.Done():
			return ctx.Err()
		}
		feed.events <- datastore.CommittedChanges{Checkpoint: testPosition(2)}
		return nil
	}}}
	c, err := StartCoordinator(ctx, source)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { _, err := c.WaitForFence(ctx, "test-fence"); done <- err }()
	select {
	case <-requested:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if _, err := c.OpenGroup(ctx, "other", 8); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		t.Fatalf("wait completed before application: %v", err)
	default:
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if _, err := c.WaitForFence(ctx, "test-fence"); err != nil {
		t.Fatal(err)
	}
	source.mu.Lock()
	calls := source.requests
	source.mu.Unlock()
	if calls != 1 {
		t.Fatalf("already covered fence requested another barrier: %d", calls)
	}
	if _, err := c.WaitForFence(ctx, "foreign"); err == nil {
		t.Fatal("invalid fence accepted")
	}
}

func TestReadFenceCancellationAndBarrierFailureAreLocal(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(map[bool]string{false: "cancel", true: "barrier-error"}[fail], func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			requested := make(chan struct{})
			sentinel := errors.New("barrier failed")
			source := &readFenceSource{&progressCoordinatorSource{coordinatorSource: &coordinatorSource{feed: newCoordinatorFeed()}, requestFn: func(context.Context, storage.Fence) error {
				close(requested)
				if fail {
					return sentinel
				}
				return nil
			}}}
			c, err := StartCoordinator(ctx, source)
			if err != nil {
				t.Fatal(err)
			}
			call, stop := context.WithCancel(ctx)
			defer stop()
			done := make(chan error, 1)
			go func() { _, err := c.WaitForFence(call, "test-fence"); done <- err }()
			select {
			case <-requested:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			if !fail {
				stop()
			}
			want := error(context.Canceled)
			if fail {
				want = sentinel
			}
			if err := <-done; !errors.Is(err, want) {
				t.Fatalf("wait: %v", err)
			}
			if _, err := c.OpenGroup(ctx, "healthy", 8); err != nil {
				t.Fatal(err)
			}
		})
	}
}
