package policy

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/adiom-data/weave/internal/expiryindex"
)

type compileFlight struct {
	done   chan struct{}
	policy *compiledRelation
	err    error
}
type policyEntry struct {
	epoch    uint64
	version  string
	valid    bool
	compiled *compiledRelation
	err      error
	loads    int
	flights  map[string]*compileFlight
	watchers map[*Watcher]struct{}
}
type policyStore struct {
	mu       sync.Mutex
	ctx      context.Context
	cancel   context.CancelFunc
	done     chan struct{}
	provider Provider
	limits   Limits
	options  CacheOptions
	entries  map[Key]*policyEntry
	idle     expiryindex.Index[Key]
	loads    chan struct{}
	now      func() time.Time
}

func newPolicyStore(ctx context.Context, provider Provider, limits Limits, options CacheOptions, invalidations <-chan []Key) *policyStore {
	run, cancel := context.WithCancel(ctx)
	s := &policyStore{ctx: run, cancel: cancel, done: make(chan struct{}), provider: provider, limits: limits, options: options, entries: map[Key]*policyEntry{}, loads: make(chan struct{}, options.LoadConcurrency), now: time.Now}
	go s.maintain(invalidations)
	return s
}
func (s *policyStore) entryLocked(key Key) *policyEntry {
	e := s.entries[key]
	if e == nil {
		e = &policyEntry{flights: map[string]*compileFlight{}, watchers: map[*Watcher]struct{}{}}
		s.entries[key] = e
	}
	return e
}
func (s *policyStore) syncIdleLocked(key Key, e *policyEntry) {
	if e.loads == 0 && len(e.watchers) == 0 {
		s.idle.Set(key, s.now().Add(s.options.IdleTTL))
	} else {
		s.idle.Remove(key)
	}
}
func (s *policyStore) invalidateLocked(key Key, e *policyEntry) {
	e.epoch++
	for watcher := range e.watchers {
		watcher.notifyLocked(key)
	}
}
func (s *policyStore) Get(ctx context.Context, key Key) (*compiledRelation, Stamp, error) {
	if err := s.ctx.Err(); err != nil {
		return nil, Stamp{}, err
	}
	loadCtx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(s.ctx, cancel)
	defer func() { stop(); cancel() }()
	select {
	case s.loads <- struct{}{}:
	case <-loadCtx.Done():
		return nil, Stamp{}, loadCtx.Err()
	}
	defer func() { <-s.loads }()
	s.mu.Lock()
	e := s.entryLocked(key)
	e.loads++
	s.idle.Remove(key)
	stamp := Stamp{e, e.epoch}
	s.mu.Unlock()
	defer func() { s.mu.Lock(); e.loads--; s.syncIdleLocked(key, e); s.mu.Unlock() }()
	snapshot, err := s.provider.GetPolicy(loadCtx, key)
	if err != nil {
		return nil, stamp, err
	}
	if snapshot.Version == "" {
		return nil, stamp, errors.New("policy provider returned an empty version")
	}
	s.mu.Lock()
	if e.valid && e.version == snapshot.Version {
		compiled, err := e.compiled, e.err
		s.mu.Unlock()
		return compiled, stamp, err
	}
	flight := e.flights[snapshot.Version]
	owned := flight == nil
	if owned {
		flight = &compileFlight{done: make(chan struct{})}
		e.flights[snapshot.Version] = flight
	}
	s.mu.Unlock()
	if !owned {
		select {
		case <-flight.done:
			return flight.policy, stamp, flight.err
		case <-loadCtx.Done():
			return nil, stamp, loadCtx.Err()
		}
	}
	if snapshot.Policy != nil {
		flight.policy, flight.err = compileRelation(key, *snapshot.Policy, s.limits)
	}
	s.mu.Lock()
	if e.valid && e.version != snapshot.Version {
		before := e.epoch
		s.invalidateLocked(key, e)
		if stamp.epoch == before {
			stamp.epoch = e.epoch
		}
	}
	e.version, e.valid, e.compiled, e.err = snapshot.Version, true, flight.policy, flight.err
	delete(e.flights, snapshot.Version)
	close(flight.done)
	s.mu.Unlock()
	if err := loadCtx.Err(); err != nil {
		return nil, stamp, err
	}
	return flight.policy, stamp, flight.err
}
func (s *policyStore) expire(now time.Time) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for count := 0; count < s.options.EvictionBatchSize; count++ {
		key, at, ok := s.idle.First()
		if !ok || at.After(now) {
			break
		}
		s.idle.Remove(key)
		delete(s.entries, key)
	}
	_, at, ok := s.idle.First()
	return ok && !at.After(now)
}
func (s *policyStore) maintain(invalidations <-chan []Key) {
	defer close(s.done)
	ticker := time.NewTicker(s.options.MaintenancePeriod)
	defer ticker.Stop()
	next := make(chan struct{}, 1)
	for {
		select {
		case <-s.ctx.Done():
			return
		case keys, ok := <-invalidations:
			if !ok {
				invalidations = nil
				continue
			}
			s.mu.Lock()
			for _, key := range keys {
				if e := s.entries[key]; e != nil {
					s.invalidateLocked(key, e)
				}
			}
			s.mu.Unlock()
			continue
		case <-ticker.C:
		case <-next:
		}
		if s.expire(s.now()) {
			select {
			case next <- struct{}{}:
			default:
			}
		}
	}
}
func (s *policyStore) Close() error { s.cancel(); <-s.done; return nil }
