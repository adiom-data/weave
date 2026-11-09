package evaluation

import (
	"context"
	"strconv"
	"time"

	"github.com/adiom-data/weave/internal/projection/retry"
	q "github.com/adiom-data/weave/internal/querysubscription/model"
)

// Failed selectors retain their dependency identity but receive a new upstream
// interest on retry. Existing views keep their memoized error; only newly fenced
// evidence can recover an occurrence. No healthy selector is scanned or reloaded.
func (f *Facts) recoverQueries() {
	defer f.workers.Done()
	for {
		f.mu.Lock()
		if f.closed || f.ctx.Err() != nil {
			f.mu.Unlock()
			return
		}
		now := time.Now()
		var next time.Time
		var closeHandles []q.QuerySubscription
		for e := range f.failed {
			if e.loading || e.job != nil || e.refs == 0 {
				continue
			}
			if !e.recovery.Take(now) {
				next = retry.Earlier(next, e.recovery.Next())
				continue
			}
			delete(f.failed, e)
			delete(f.byID, e.subscriptionID)
			delete(f.awaiting, e.id)
			if e.handle != nil {
				closeHandles = append(closeHandles, e.handle)
			}
			f.sequence++
			e.subscriptionID = "authorization-recovery-" + strconv.FormatUint(f.sequence, 10)
			e.handle, e.err, e.installed, e.completed, e.baseline, e.checkpoint, e.rows, e.order = nil, nil, false, false, nil, nil, nil, nil
			e.revision = 0
			f.byID[e.subscriptionID] = e
			e.job = f.pending.PushBack(e)
		}
		work := f.pending.Len() > 0
		f.mu.Unlock()
		for _, handle := range closeHandles {
			_ = handle.Close(context.WithoutCancel(f.ctx))
		}
		if work {
			f.signalJob()
		}
		if !retry.Wait(f.ctx, f.recoveryWake, next) {
			return
		}
	}
}

// Requires f.mu. Failures retain a bounded schedule, not an unbounded stream
// of attempts. Re-evaluating the same errored evidence never resets the budget.
func (f *Facts) failQueryLocked(e *factEntry) {
	f.failed[e] = struct{}{}
	if f.options.Recovery == (retry.Policy{}) {
		return
	}
	e.recovery.Failed(time.Now(), f.options.Recovery)
	select {
	case f.recoveryWake <- struct{}{}:
	default:
	}
}
