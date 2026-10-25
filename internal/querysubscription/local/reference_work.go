package local

import (
	"context"
	"sync"

	q "github.com/adiom-data/weave/internal/querysubscription/model"
)

// ReferenceOptions bounds asynchronous reference initialization per feed.
// Zero fields select defaults. Initial snapshots retain their sequential,
// caller-cancellable loading path; only live discoveries use this queue.
type ReferenceOptions struct {
	LoadConcurrency int
	LoadQueueLimit  int
}

func DefaultReferenceOptions() ReferenceOptions {
	return ReferenceOptions{LoadConcurrency: 4, LoadQueueLimit: 1024}
}

func (o ReferenceOptions) withDefaults() ReferenceOptions {
	d := DefaultReferenceOptions()
	if o.LoadConcurrency <= 0 {
		o.LoadConcurrency = d.LoadConcurrency
	}
	if o.LoadQueueLimit <= 0 {
		o.LoadQueueLimit = d.LoadQueueLimit
	}
	return o
}

// referenceWork owns bounded IO, not subscription state. State transitions and
// deduplication stay with ownedReferences. Cleanup has its own worker so a
// blocked load cannot prevent retirement/cancellation of another interest.
// Workers start lazily and are stopped by the feed's lifetime context.
type referenceWork struct {
	ctx                   context.Context
	options               ReferenceOptions
	loads                 chan func()
	cleanup               chan func()
	loadOnce, cleanupOnce sync.Once
}

func newReferenceWork(ctx context.Context, options ReferenceOptions, cleanupLimit int) *referenceWork {
	options = options.withDefaults()
	return &referenceWork{ctx: ctx, options: options, loads: make(chan func(), options.LoadQueueLimit), cleanup: make(chan func(), cleanupLimit)}
}

func (w *referenceWork) run(queue <-chan func()) {
	for {
		select {
		case <-w.ctx.Done():
			return
		case job := <-queue:
			if w.ctx.Err() != nil {
				return
			}
			job()
		}
	}
}

func (w *referenceWork) submit(job func(), cleanup bool) error {
	if w.ctx.Err() != nil {
		return q.ErrClosed
	}
	queue := w.loads
	if cleanup {
		queue = w.cleanup
		w.cleanupOnce.Do(func() { go w.run(w.cleanup) })
	} else {
		w.loadOnce.Do(func() {
			for range w.options.LoadConcurrency {
				go w.run(w.loads)
			}
		})
	}
	select {
	case <-w.ctx.Done():
		return q.ErrClosed
	case queue <- job:
		return nil
	default:
		return q.ErrBackpressure
	}
}

func (s *localFeed) retireReference(job func() error) {
	if err := s.referenceWork.submit(func() {
		if err := job(); err != nil {
			s.fail(err)
		}
	}, true); err != nil && s.ctx.Err() == nil {
		// If even bounded cleanup cannot keep up, retire this feed. Close owns
		// all remaining native subscriptions, including queued retirements.
		s.fail(err)
	}
}
