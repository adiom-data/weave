package policy

import (
	"context"
	"errors"
	"time"

	"github.com/adiom-data/weave/internal/storage"
)

// Key is the canonical relation name, not a tenant identifier. Applications
// decide where policies live and how namespaces map to their tenancy model.
type Key = storage.RelationName

// Snapshot and all referenced definitions are immutable after return. Versions
// identify content uniquely per key, across all callers of a provider. A nil
// Policy is an explicit deny-by-default definition, also requiring a version.
type Snapshot struct {
	Version string
	Policy  *Relation
}
type Provider interface {
	GetPolicy(context.Context, Key) (Snapshot, error)
}
type ProviderFunc func(context.Context, Key) (Snapshot, error)

func (f ProviderFunc) GetPolicy(ctx context.Context, key Key) (Snapshot, error) { return f(ctx, key) }

type CacheOptions struct {
	IdleTTL           time.Duration
	MaintenancePeriod time.Duration
	EvictionBatchSize int
	LoadConcurrency   int
}

func DefaultCacheOptions() CacheOptions {
	return CacheOptions{IdleTTL: 5 * time.Minute, MaintenancePeriod: time.Second, EvictionBatchSize: 128, LoadConcurrency: 16}
}
func (o CacheOptions) normalized() (CacheOptions, error) {
	d := DefaultCacheOptions()
	if o.IdleTTL == 0 {
		o.IdleTTL = d.IdleTTL
	}
	if o.MaintenancePeriod == 0 {
		o.MaintenancePeriod = d.MaintenancePeriod
	}
	if o.EvictionBatchSize == 0 {
		o.EvictionBatchSize = d.EvictionBatchSize
	}
	if o.LoadConcurrency == 0 {
		o.LoadConcurrency = d.LoadConcurrency
	}
	if o.IdleTTL < 0 || o.MaintenancePeriod <= 0 || o.EvictionBatchSize < 1 || o.LoadConcurrency < 1 {
		return o, errors.New("policy cache options must be positive")
	}
	return o, nil
}

func NewEvaluator(ctx context.Context, provider Provider, limits Limits, options CacheOptions, invalidations <-chan []Key) (*Evaluator, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if provider == nil {
		return nil, errors.New("policy provider is required")
	}
	var err error
	limits, err = limits.normalized()
	if err != nil {
		return nil, err
	}
	options, err = options.normalized()
	if err != nil {
		return nil, err
	}
	store := newPolicyStore(ctx, provider, limits, options, invalidations)
	return &Evaluator{maxSteps: limits.MaxEvaluationSteps, store: store}, nil
}

type staticProvider struct{ policies map[Key]*Relation }

func NewStaticProvider(definitions Map, limits Limits) (Provider, error) {
	e, err := Compile(definitions, limits)
	if err != nil {
		return nil, err
	}
	definitionsCopy := map[Key]*Relation{}
	for key, value := range e.relations {
		definition := value.definition
		definitionsCopy[key] = &definition
	}
	return &staticProvider{policies: definitionsCopy}, nil
}
func (p *staticProvider) GetPolicy(ctx context.Context, key Key) (Snapshot, error) {
	if err := ctx.Err(); err != nil {
		return Snapshot{}, err
	}
	value := p.policies[key]
	if value == nil {
		return Snapshot{Version: "static"}, nil
	}
	return Snapshot{Version: "static", Policy: value}, nil
}

// Watcher coalesces optional provider invalidations. It does not call the
// provider or authorize anything; the owner retries at its next valid boundary.
type Watcher struct {
	store   *policyStore
	keys    map[Key]int
	pending map[Key]struct{}
	wake    chan struct{}
	closed  bool
}
type Stamp struct {
	entry *policyEntry
	epoch uint64
}

func (e *Evaluator) Watch() *Watcher {
	return &Watcher{store: e.store, keys: map[Key]int{}, pending: map[Key]struct{}{}, wake: make(chan struct{}, 1)}
}
func (w *Watcher) Changed() <-chan struct{} { return w.wake }
func (w *Watcher) Drain() []Key {
	if w.store == nil {
		return nil
	}
	w.store.mu.Lock()
	defer w.store.mu.Unlock()
	var result []Key
	for key := range w.pending {
		result = append(result, key)
	}
	clear(w.pending)
	return result
}
func (w *Watcher) Replace(previous []Key, stamps map[Key]Stamp) []Key {
	if w.store == nil {
		return nil
	}
	s := w.store
	s.mu.Lock()
	defer s.mu.Unlock()
	if w.closed {
		return nil
	}
	for _, key := range previous {
		w.releaseLocked(key)
	}
	var result []Key
	for key, stamp := range stamps {
		e := s.entryLocked(key)
		w.keys[key]++
		e.watchers[w] = struct{}{}
		s.idle.Remove(key)
		result = append(result, key)
		if e != stamp.entry || e.epoch != stamp.epoch {
			w.notifyLocked(key)
		}
	}
	return result
}
func (w *Watcher) notifyLocked(key Key) {
	if w.closed {
		return
	}
	w.pending[key] = struct{}{}
	select {
	case w.wake <- struct{}{}:
	default:
	}
}
func (w *Watcher) releaseLocked(key Key) {
	if w.keys[key] <= 1 {
		delete(w.keys, key)
		if e := w.store.entries[key]; e != nil {
			delete(e.watchers, w)
			w.store.syncIdleLocked(key, e)
		}
	} else {
		w.keys[key]--
	}
}
func (w *Watcher) Release(keys []Key) {
	if w.store == nil {
		return
	}
	w.store.mu.Lock()
	defer w.store.mu.Unlock()
	for _, key := range keys {
		w.releaseLocked(key)
	}
}
func (w *Watcher) Close() {
	if w.store == nil {
		return
	}
	w.store.mu.Lock()
	defer w.store.mu.Unlock()
	if w.closed {
		return
	}
	w.closed = true
	for key := range w.keys {
		if e := w.store.entries[key]; e != nil {
			delete(e.watchers, w)
			w.store.syncIdleLocked(key, e)
		}
	}
	clear(w.keys)
	clear(w.pending)
}
