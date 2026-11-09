package relation

import (
	"errors"
	"sync"
	"sync/atomic"

	"github.com/adiom-data/weave/internal/storage"
)

// Key identifies a relation, not a tenant. The host chooses which projection
// servers share a Store; separate stores may use the same relation names.
type Key = storage.RelationName

type definitionSet struct {
	relations map[Key]*compiledRelation
}

// Store owns an immutable, compiled definition map. Replace validates a complete
// replacement before publishing it atomically. Evaluations already in progress
// retain their old map. Store must be constructed with NewStore and not copied.
// It owns no background workers and requires no Close.
type Store struct {
	updateMu sync.Mutex // serializes writers, never held by evaluations/watchers
	current  atomic.Pointer[definitionSet]
	limits   Limits
	mu       sync.Mutex // protects subscriptions and publication/registration races
	watchers map[Key]map[*Watcher]struct{}
}

// NewStore validates and copies definitions. Callers may modify their inputs
// after this returns, but must not mutate them concurrently with this call.
func NewStore(definitions Definitions, limits Limits) (*Store, error) {
	limits, err := limits.normalized()
	if err != nil {
		return nil, err
	}
	s := &Store{limits: limits, watchers: make(map[Key]map[*Watcher]struct{})}
	if err := s.Replace(definitions); err != nil {
		return nil, err
	}
	return s, nil
}

// Replace installs a complete map, reusing unchanged compiled definitions.
// An invalid replacement leaves the current map and its dependents untouched.
// Successful changes automatically wake only affected relation dependents.
// As with NewStore, inputs are copied and need only be stable during this call.
func (s *Store) Replace(definitions Definitions) error {
	if s == nil || s.watchers == nil {
		return errors.New("initialized relation definition store is required")
	}
	s.updateMu.Lock()
	defer s.updateMu.Unlock()
	previous := s.current.Load()
	next, err := compileDefinitions(definitions, s.limits, previous)
	if err != nil {
		return err
	}
	var changed []Key
	for key, definition := range next.relations {
		if previous == nil || previous.relations[key] != definition {
			changed = append(changed, key)
		}
	}
	if previous != nil {
		for key := range previous.relations {
			if next.relations[key] == nil {
				changed = append(changed, key)
			}
		}
	}
	// Compilation happens outside the watcher lock. Publication and registration
	// share this short critical section, so no subscriber can miss a replacement.
	s.mu.Lock()
	s.current.Store(next)
	for _, key := range changed {
		for watcher := range s.watchers[key] {
			watcher.notifyLocked(key)
		}
	}
	s.mu.Unlock()
	return nil
}

func NewEvaluator(store *Store) (*Evaluator, error) {
	if store == nil || store.current.Load() == nil {
		return nil, errors.New("initialized relation definition store is required")
	}
	return &Evaluator{store: store, maxSteps: store.limits.MaxEvaluationSteps}, nil
}
