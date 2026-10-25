package projection

import (
	"context"
	"errors"
	"sync"

	"github.com/adiom-data/weave/internal/storage"
)

var errExpansionBacklogFull = errors.New("projection expansion backlog is full; resynchronization required")

// ExpansionLimiter bounds expansion reads across projections. It is optional:
// each projection also has its own worker bound, while a shared limiter keeps
// many concurrent projections from multiplying that bound without limit.
type ExpansionLimiter struct {
	slots chan struct{}
}

func NewExpansionLimiter(maxConcurrent int) *ExpansionLimiter {
	if maxConcurrent <= 0 {
		return nil
	}
	return &ExpansionLimiter{slots: make(chan struct{}, maxConcurrent)}
}

func (l *ExpansionLimiter) acquire(ctx context.Context) error {
	if l == nil {
		return nil
	}
	select {
	case l.slots <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (l *ExpansionLimiter) release() {
	if l != nil {
		<-l.slots
	}
}

type expansionKey struct {
	parentID string
	row      storage.EntityKey
}

// expansionScheduler is a bounded, coalescing work queue. Enqueue never waits
// for a snapshot read: query-feed processing can therefore continue while
// child subscriptions are loading. For one parent occurrence, a queued newer
// generation replaces an older one. A running generation may have at most one
// successor queued behind it.
type expansionScheduler struct {
	ctx    context.Context
	limit  int
	handle func(projectionExpansion)

	mu      sync.Mutex
	ready   *sync.Cond
	stopped bool
	order   []expansionKey
	queued  map[expansionKey]projectionExpansion
	running int
	idle    chan struct{}
}

func newExpansionScheduler(ctx context.Context, workers, pendingLimit int, handle func(projectionExpansion)) *expansionScheduler {
	s := &expansionScheduler{
		ctx: ctx, limit: pendingLimit, handle: handle,
		queued: make(map[expansionKey]projectionExpansion), idle: closedSignal(),
	}
	s.ready = sync.NewCond(&s.mu)
	for range workers {
		go s.work()
	}
	return s
}

func (s *expansionScheduler) enqueue(expansions []projectionExpansion) error {
	if len(expansions) == 0 {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopped {
		return s.ctx.Err()
	}
	additional := 0
	newKeys := make(map[expansionKey]struct{}, len(expansions))
	for _, expansion := range expansions {
		key := expansionKey{parentID: expansion.parent.id, row: expansion.key}
		if _, exists := s.queued[key]; !exists {
			if _, duplicate := newKeys[key]; duplicate {
				continue
			}
			newKeys[key] = struct{}{}
			additional++
		}
	}
	if len(s.queued)+additional > s.limit {
		return errExpansionBacklogFull
	}
	wasIdle := s.running == 0 && len(s.queued) == 0
	for _, expansion := range expansions {
		key := expansionKey{parentID: expansion.parent.id, row: expansion.key}
		if _, exists := s.queued[key]; exists {
			// Feed order makes the incoming generation at least as current.
			s.queued[key] = expansion
			continue
		}
		s.queued[key] = expansion
		s.order = append(s.order, key)
	}
	if wasIdle && len(s.queued) > 0 {
		s.idle = make(chan struct{})
	}
	s.ready.Broadcast()
	return nil
}

func (s *expansionScheduler) work() {
	for {
		expansion, ok := s.take()
		if !ok {
			return
		}
		s.handle(expansion)
		s.complete()
	}
}

func (s *expansionScheduler) take() (projectionExpansion, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for {
		for len(s.order) > 0 {
			key := s.order[0]
			s.order = s.order[1:]
			expansion, exists := s.queued[key]
			if !exists {
				continue
			}
			delete(s.queued, key)
			s.running++
			return expansion, true
		}
		if s.stopped {
			return projectionExpansion{}, false
		}
		s.ready.Wait()
	}
}

func (s *expansionScheduler) discard(parentID string, row storage.EntityKey) {
	s.mu.Lock()
	key := expansionKey{parentID: parentID, row: row}
	delete(s.queued, key)
	for index, queuedKey := range s.order {
		if queuedKey == key {
			s.order = append(s.order[:index], s.order[index+1:]...)
			break
		}
	}
	s.closeIdleIfDoneLocked()
	s.mu.Unlock()
}

func (s *expansionScheduler) discardParent(parentID string) {
	s.mu.Lock()
	for key := range s.queued {
		if key.parentID == parentID {
			delete(s.queued, key)
		}
	}
	retained := s.order[:0]
	for _, key := range s.order {
		if key.parentID != parentID {
			retained = append(retained, key)
		}
	}
	s.order = retained
	s.closeIdleIfDoneLocked()
	s.mu.Unlock()
}

func (s *expansionScheduler) complete() {
	s.mu.Lock()
	s.running--
	s.closeIdleIfDoneLocked()
	s.mu.Unlock()
}

func (s *expansionScheduler) closeIdleIfDoneLocked() {
	if s.running != 0 || len(s.queued) != 0 {
		return
	}
	select {
	case <-s.idle:
	default:
		close(s.idle)
	}
}

func (s *expansionScheduler) waitIdle(ctx context.Context) error {
	for {
		s.mu.Lock()
		idle := s.idle
		done := s.running == 0 && len(s.queued) == 0
		s.mu.Unlock()
		if done {
			return nil
		}
		select {
		case <-idle:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (s *expansionScheduler) isIdle() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.running == 0 && len(s.queued) == 0
}

func (s *expansionScheduler) stop() {
	s.mu.Lock()
	if !s.stopped {
		s.stopped = true
		s.order = nil
		clear(s.queued)
		s.ready.Broadcast()
		if s.running == 0 {
			select {
			case <-s.idle:
			default:
				close(s.idle)
			}
		}
	}
	s.mu.Unlock()
}

func closedSignal() chan struct{} {
	result := make(chan struct{})
	close(result)
	return result
}
