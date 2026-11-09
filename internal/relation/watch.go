package relation

// Watcher coalesces definition changes. The owner reevaluates affected
// occurrences at its next valid data boundary, not in the publishing goroutine.
type Watcher struct {
	store   *Store
	keys    map[Key]int
	pending map[Key]struct{}
	wake    chan struct{}
	closed  bool
}

// A nil definition is intentional: dependencies on an absent relation must
// also be invalidated when that relation is subsequently added.
type Stamp struct {
	definition *compiledRelation
}

func (e *Evaluator) Watch() *Watcher {
	return &Watcher{store: e.store, keys: map[Key]int{}, pending: map[Key]struct{}{}, wake: make(chan struct{}, 1)}
}
func (w *Watcher) Changed() <-chan struct{} { return w.wake }
func (w *Watcher) Drain() []Key {
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
	s := w.store
	s.mu.Lock()
	defer s.mu.Unlock()
	if w.closed {
		return nil
	}
	for _, key := range previous {
		w.releaseLocked(key)
	}
	current := s.current.Load()
	result := make([]Key, 0, len(stamps))
	for key, stamp := range stamps {
		w.keys[key]++
		if s.watchers[key] == nil {
			s.watchers[key] = make(map[*Watcher]struct{})
		}
		s.watchers[key][w] = struct{}{}
		result = append(result, key)
		if current.relations[key] != stamp.definition {
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
		delete(w.store.watchers[key], w)
		if len(w.store.watchers[key]) == 0 {
			delete(w.store.watchers, key)
		}
	} else {
		w.keys[key]--
	}
}
func (w *Watcher) Release(keys []Key) {
	w.store.mu.Lock()
	defer w.store.mu.Unlock()
	for _, key := range keys {
		w.releaseLocked(key)
	}
}
func (w *Watcher) Close() {
	w.store.mu.Lock()
	defer w.store.mu.Unlock()
	if w.closed {
		return
	}
	w.closed = true
	for key := range w.keys {
		delete(w.store.watchers[key], w)
		if len(w.store.watchers[key]) == 0 {
			delete(w.store.watchers, key)
		}
	}
	clear(w.keys)
	clear(w.pending)
}
