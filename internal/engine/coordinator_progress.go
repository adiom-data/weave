package engine

import (
	"context"

	"github.com/adiom-data/weave/internal/storage"
)

// appliedThrough is actor-confined. Adapter receive progress may be ahead of
// shared state because the reader can already be handing off the next event.
// Only a successfully applied checkpoint admits snapshots or hydrations.
func (c *Coordinator) appliedThrough(fence storage.Fence) bool {
	covered, err := storage.CheckpointCovers(c.lastCheckpoint, fence)
	return err == nil && covered
}

func (c *Coordinator) applyFeedResult(ctx context.Context, result feedResult) error {
	if result.err != nil {
		return result.err
	}
	return c.applyFeedEvent(ctx, result.event)
}

// applyReadyFeed gives already-deliverable CDC one bounded turn before an
// auxiliary action. In particular a failed barrier request cannot fail reads
// before the actor applies source progress already waiting at its handoff.
// It never waits for IO and cannot starve commands by draining indefinitely.
func (c *Coordinator) applyReadyFeed(ctx context.Context, received <-chan feedResult) (bool, error) {
	select {
	case result := <-received:
		return true, c.applyFeedResult(ctx, result)
	default:
		return false, nil
	}
}

func (c *Coordinator) completeProgressRequest(result progressResult) {
	c.progressStarting = false
	if result.err != nil {
		c.progressFence = nil
		if !c.appliedThrough(result.fence) {
			c.failPendingProgress(result.fence, result.err)
		}
	}
}

// readFeed has one reader and an unbuffered handoff: at most one received event
// can await the actor. It never applies state or acknowledges source progress.
// Cancellation belongs to the coordinator lifetime, not individual commands.
func (c *Coordinator) readFeed(ctx context.Context) (<-chan feedResult, <-chan struct{}) {
	results := make(chan feedResult)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			event, err := c.feed.Receive(ctx)
			select {
			case results <- feedResult{event: event, err: err}:
			case <-ctx.Done():
				return
			}
			if err != nil {
				return
			}
		}
	}()
	return results, done
}
