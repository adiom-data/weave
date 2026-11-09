package evaluation

import (
	"context"
	"errors"
	"sync"

	q "github.com/adiom-data/weave/internal/querysubscription/model"
	relationdef "github.com/adiom-data/weave/internal/relation"
	"github.com/adiom-data/weave/internal/storage"
)

// Occurrence identifies a row within one concrete projection instance (or one
// direct query subscription). Diamonds have distinct scopes, even when their
// final entity keys are identical.
type Occurrence struct {
	Scope string
	Key   storage.EntityKey
}

type Session struct {
	mu               sync.Mutex
	facts            *Facts
	evaluator        *relationdef.Evaluator
	policyWatch      *relationdef.Watcher
	policyDependents map[relationdef.Key]map[Occurrence]struct{}
	claims           relationdef.Claims
	scopes           map[Occurrence]*occurrenceState
	resources        map[string]map[Occurrence]struct{}
	pendingScopes    map[string]int
	dependents       map[string]map[Occurrence]struct{}
	dirty            map[Occurrence]struct{}
	changed          chan struct{}
	version          uint64
	factEpoch        uint64
	factChanges      map[Occurrence]uint64
	closed           bool
}
type occurrenceState struct {
	mu           sync.Mutex
	scope        *Scope
	dependencies []string
	pending      bool
	at           *q.Position
	policyKeys   []relationdef.Key
}

func NewSession(ctx context.Context, feed q.Feed, evaluator *relationdef.Evaluator, claims relationdef.Claims, options Options, allowAll bool) (*Session, error) {
	if evaluator == nil {
		return nil, errors.New("authorization session requires a compiled policy")
	}
	facts, err := NewObservedFacts(ctx, feed, options)
	if err != nil {
		return nil, err
	}
	facts.allowAll = allowAll
	claims = relationdef.Claims(storage.CloneRecord(storage.Record(claims)))
	return &Session{facts: facts, evaluator: evaluator, policyWatch: evaluator.Watch(), policyDependents: map[relationdef.Key]map[Occurrence]struct{}{}, claims: claims, scopes: make(map[Occurrence]*occurrenceState), resources: make(map[string]map[Occurrence]struct{}), pendingScopes: make(map[string]int), dependents: make(map[string]map[Occurrence]struct{}), dirty: make(map[Occurrence]struct{}), factChanges: make(map[Occurrence]uint64), changed: make(chan struct{})}, nil
}

func (s *Session) ComputedValues(occurrence Occurrence) storage.Record {
	s.mu.Lock()
	state := s.scopes[occurrence]
	s.mu.Unlock()
	if state == nil {
		return nil
	}
	return state.scope.ComputedValues()
}

func (s *Session) OpenView(at *q.Position) (*View, error)    { return s.facts.OpenView(at) }
func (s *Session) FactChanges() <-chan struct{}              { return s.facts.Changed() }
func (s *Session) PolicyChanges() <-chan struct{}            { return s.policyWatch.Changed() }
func (s *Session) RequestProgress(ctx context.Context) error { return s.facts.RequestProgress(ctx) }
func (s *Session) ProgressTargets() []*q.Position            { return s.facts.ProgressTargets() }

// Observe runs in the projection's ordered consumer before the corresponding
// public effects. It does not read ahead, acknowledge CDC, or block on loads.
func (s *Session) Observe(event q.Event) ([]Occurrence, error) {
	if err := s.facts.Observe(event); err != nil {
		return nil, err
	}
	return s.Refresh(), nil
}

// Refresh records asynchronous fact installation/failure notifications. The
// caller must still consume source progress before using their row evidence.
func (s *Session) Refresh() []Occurrence {
	ids := s.facts.DrainChanges()
	policies := s.policyWatch.Drain()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	// An ordered progress report is not itself new authorization evidence.
	// Only fact/policy changes may wake evaluation waiters or invalidate an
	// evaluation already in flight. Otherwise requesting progress can wake a
	// waiter which requests the same progress again, without any source work.
	if len(ids) == 0 && len(policies) == 0 {
		return nil
	}
	changed := make(map[Occurrence]struct{})
	s.factEpoch++
	for _, id := range ids {
		for occurrence := range s.dependents[id] {
			s.dirty[occurrence] = struct{}{}
			s.factChanges[occurrence] = s.factEpoch
			changed[occurrence] = struct{}{}
		}
	}
	for _, key := range policies {
		for occurrence := range s.policyDependents[key] {
			s.dirty[occurrence] = struct{}{}
			s.factChanges[occurrence] = s.factEpoch
			changed[occurrence] = struct{}{}
		}
	}
	s.version++
	close(s.changed)
	s.changed = make(chan struct{})
	result := make([]Occurrence, 0, len(changed))
	for occurrence := range changed {
		result = append(result, occurrence)
	}
	return result
}

// FactChangesAfter distinguishes new evidence notifications from an unresolved
// decision. Consumers can retry only affected occurrences while IO is pending;
// crossing an installed snapshot's baseline produces its own notification.
func (s *Session) FactChangesAfter(epoch uint64) (uint64, []Occurrence) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if epoch >= s.factEpoch {
		return s.factEpoch, nil
	}
	var result []Occurrence
	for occurrence, changed := range s.factChanges {
		if changed > epoch {
			result = append(result, occurrence)
		}
	}
	return s.factEpoch, result
}
func (s *Session) Changes() <-chan struct{} { s.mu.Lock(); defer s.mu.Unlock(); return s.changed }
func (s *Session) Invalidated() []Occurrence {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make([]Occurrence, 0, len(s.dirty))
	for occurrence := range s.dirty {
		result = append(result, occurrence)
	}
	return result
}
func (s *Session) Pending(scopes []string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, id := range scopes {
		if s.pendingScopes[id] > 0 {
			return true
		}
	}
	return false
}

// PendingIn checks only unresolved scopes, not every row or projection node.
// contains must be a nonblocking ownership lookup and must not call Session.
func (s *Session) PendingIn(contains func(string) bool) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for id := range s.pendingScopes {
		if contains(id) {
			return true
		}
	}
	return false
}

func (s *Session) setPending(scope string, state *occurrenceState, pending bool) {
	if state.pending == pending {
		return
	}
	state.pending = pending
	if pending {
		s.pendingScopes[scope]++
	} else {
		s.pendingScopes[scope]--
		if s.pendingScopes[scope] == 0 {
			delete(s.pendingScopes, scope)
		}
	}
}

// Evaluate receives the concrete path's immutable row images from the
// projection publication boundary. It never looks up ancestors by entity ID
// in a connection-global "latest row" map.
func (s *Session) Evaluate(ctx context.Context, occurrence Occurrence, candidate relationdef.Candidate, view *View) (relationdef.Decision, error) {
	if occurrence.Scope == "" || occurrence.Key != candidate.Key || view == nil || candidate.Context.Projection != nil && candidate.Context.Projection.Step().Key != candidate.Key {
		return relationdef.Decision{}, errors.New("authorization occurrence and concrete path are required")
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return relationdef.Decision{}, q.ErrClosed
	}
	state := s.scopes[occurrence]
	if state == nil {
		scope, err := NewScope(s.facts)
		if err != nil {
			s.mu.Unlock()
			return relationdef.Decision{}, err
		}
		state = &occurrenceState{scope: scope}
		s.setPending(occurrence.Scope, state, true)
		s.scopes[occurrence] = state
		if s.resources[occurrence.Scope] == nil {
			s.resources[occurrence.Scope] = make(map[Occurrence]struct{})
		}
		s.resources[occurrence.Scope][occurrence] = struct{}{}
	}
	version := s.version
	s.mu.Unlock()
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.at != nil {
		compared, err := q.ComparePositions(view.at, state.at)
		if err != nil {
			return relationdef.Decision{}, err
		}
		if compared < 0 {
			s.mu.Lock()
			if s.scopes[occurrence] == state {
				s.setPending(occurrence.Scope, state, true)
				s.dirty[occurrence] = struct{}{}
			}
			s.mu.Unlock()
			return relationdef.Decision{}, nil
		}
	}
	decision, err := state.scope.EvaluatePath(ctx, s.evaluator, s.claims, candidate, view)
	if err != nil {
		return relationdef.Decision{}, err
	}
	dependencies := state.scope.DependencyIDs()
	policyStamps := state.scope.PolicyStamps()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.scopes[occurrence] != state {
		return relationdef.Decision{}, q.ErrClosed
	}
	s.unlink(occurrence, state.dependencies)
	for _, id := range dependencies {
		if s.dependents[id] == nil {
			s.dependents[id] = make(map[Occurrence]struct{})
		}
		s.dependents[id][occurrence] = struct{}{}
	}
	state.dependencies = dependencies
	s.unlinkPolicies(occurrence, state.policyKeys)
	state.policyKeys = s.policyWatch.Replace(state.policyKeys, policyStamps)
	for _, key := range state.policyKeys {
		if s.policyDependents[key] == nil {
			s.policyDependents[key] = map[Occurrence]struct{}{}
		}
		s.policyDependents[key][occurrence] = struct{}{}
	}
	// Installation may race the first dependency registration, or a view may
	// have memoized unknown evidence before another occurrence used it. Once
	// linked, recheck only those unknown reads so neither case loses its wake.
	if !decision.Resolved && state.scope.needsRetry(view) {
		s.factEpoch++
		s.factChanges[occurrence] = s.factEpoch
		s.facts.notify()
	}
	s.setPending(occurrence.Scope, state, !decision.Resolved)
	state.at = copyPosition(view.at)
	if decision.Resolved && version == s.version {
		delete(s.dirty, occurrence)
		delete(s.factChanges, occurrence)
	} else {
		s.dirty[occurrence] = struct{}{}
	}
	return decision, nil
}

func (s *Session) unlink(occurrence Occurrence, dependencies []string) {
	for _, id := range dependencies {
		delete(s.dependents[id], occurrence)
		if len(s.dependents[id]) == 0 {
			delete(s.dependents, id)
		}
	}
}

func (s *Session) unlinkPolicies(occurrence Occurrence, keys []relationdef.Key) {
	for _, key := range keys {
		delete(s.policyDependents[key], occurrence)
		if len(s.policyDependents[key]) == 0 {
			delete(s.policyDependents, key)
		}
	}
}

func (s *Session) Forget(ctx context.Context, occurrence Occurrence) error {
	s.mu.Lock()
	state := s.scopes[occurrence]
	if state != nil {
		s.setPending(occurrence.Scope, state, false)
		delete(s.scopes, occurrence)
		delete(s.dirty, occurrence)
		delete(s.factChanges, occurrence)
		delete(s.resources[occurrence.Scope], occurrence)
		if len(s.resources[occurrence.Scope]) == 0 {
			delete(s.resources, occurrence.Scope)
		}
		s.unlink(occurrence, state.dependencies)
		s.unlinkPolicies(occurrence, state.policyKeys)
		s.policyWatch.Release(state.policyKeys)
	}
	s.mu.Unlock()
	if state == nil {
		return nil
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	return state.scope.Close(ctx)
}
func (s *Session) ForgetScope(ctx context.Context, id string) error {
	s.mu.Lock()
	var occurrences []Occurrence
	for occurrence := range s.resources[id] {
		occurrences = append(occurrences, occurrence)
	}
	s.mu.Unlock()
	var err error
	for _, occurrence := range occurrences {
		err = errors.Join(err, s.Forget(ctx, occurrence))
	}
	return err
}
func (s *Session) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	states := s.scopes
	s.scopes = make(map[Occurrence]*occurrenceState)
	clear(s.resources)
	clear(s.pendingScopes)
	clear(s.dependents)
	clear(s.dirty)
	clear(s.factChanges)
	clear(s.policyDependents)
	s.policyWatch.Close()
	close(s.changed)
	s.mu.Unlock()
	err := s.facts.Close()
	for _, state := range states {
		state.mu.Lock()
		err = errors.Join(err, state.scope.Close(context.Background()))
		state.mu.Unlock()
	}
	return err
}
