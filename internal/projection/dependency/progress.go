package dependency

import (
	"context"

	q "github.com/adiom-data/weave/internal/querysubscription/model"
)

// sharedProgress belongs to one upstream generation and is guarded by Cache.mu.
// Completed snapshots establish demand, never proof. Proof comes only from
// dispatch, which queues the same consumed checkpoint to every logical feed.
type sharedProgress struct {
	completed *q.Position
	requested *q.Position
	inFlight  *progressRequest
}

type progressRequest struct {
	target *q.Position
	done   chan struct{}
	err    error // Published by closing done.
}

func covers(known, target *q.Position) bool {
	return known != nil && (target == nil || known.SourceID == target.SourceID && known.Value >= target.Value)
}

// complete records a finished read: a returned upstream subscription or a
// delivered hydration/window image. An installer callback alone cannot
// establish RequestProgress's snapshot floor.
func (p *sharedProgress) complete(at *q.Position) error {
	if at == nil {
		return nil
	}
	if p.completed != nil {
		compared, err := q.ComparePositions(at, p.completed)
		if err != nil {
			return err
		}
		if compared <= 0 {
			return nil
		}
	}
	p.completed = copyPosition(at)
	return nil
}

// RequestProgress reuses proof already queued on this logical feed or joins
// one shared upstream request. Higher snapshot demands arriving during a call
// require a follow-up; identical demands do not poll while its report is queued.
func (f *consumer) RequestProgress(ctx context.Context) error {
	g, c := f.generation, f.generation.owner
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		c.mu.Lock()
		if f.closed || g.stopping {
			c.mu.Unlock()
			return q.ErrClosed
		}
		target := copyPosition(g.progress.completed)
		if covers(g.checkpoint, target) {
			c.mu.Unlock()
			return nil
		}
		call := g.progress.inFlight
		if call == nil {
			if target != nil && covers(g.progress.requested, target) {
				c.mu.Unlock()
				return nil
			}
			call = &progressRequest{target: target, done: make(chan struct{})}
			g.progress.inFlight = call
			g.workers.Add(1)
			go g.reportProgress(call)
		}
		c.mu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-g.ctx.Done():
			return q.ErrClosed
		case <-call.done:
			if call.err != nil {
				return call.err
			}
			if target == nil || covers(call.target, target) {
				return nil
			}
			// This caller's completed snapshot was newer than the request it
			// joined. Recheck shared proof/demand before starting another call.
		}
	}
}

func (g *generation) reportProgress(call *progressRequest) {
	defer g.workers.Done()
	// One viewer canceling must not cancel a request shared with other viewers.
	err := g.upstream.RequestProgress(g.ctx)
	g.owner.mu.Lock()
	if err == nil && call.target != nil {
		g.progress.requested = copyPosition(call.target)
	}
	call.err = err
	g.progress.inFlight = nil
	close(call.done)
	g.owner.mu.Unlock()
}
