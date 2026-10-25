// Package entitycache owns shared normalized entity state, coalesced loads,
// reference counts, and idle eviction. It has no knowledge of queries,
// subscriptions, transports, or a particular datastore.
package entitycache

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/adiom-data/weave/internal/datastore"
	"github.com/adiom-data/weave/internal/expiryindex"
	"github.com/adiom-data/weave/internal/storage"
)

var ErrStaleLoad = errors.New("entity load was superseded")

type EntityState uint8

const (
	EntityLoading EntityState = iota + 1
	EntityReconciling
	EntityCurrent
	EntityUnreferenced
)

type cacheEntry struct {
	Key        storage.EntityKey
	Record     storage.Record
	State      EntityState
	References int
	LastUsed   time.Time
	generation uint64
	load       *loadState
}

type loadState struct {
	done   chan struct{}
	err    error
	closed bool
	// partial is set when CDC installs a non-authoritative tuple while this
	// source snapshot is in flight. The snapshot remains the baseline, but its
	// concrete CDC values must win when the load completes.
	partial bool
}

type EntitySnapshot struct {
	Key        storage.EntityKey
	Record     storage.Record
	State      EntityState
	References int
	Generation uint64
}

type Load struct {
	key        storage.EntityKey
	generation uint64
	done       <-chan struct{}
	owner      bool
	refresh    bool
	previous   storage.Record
	state      *loadState
}

// Key and Generation identify the exact cache load attempt. Callers may use
// them to reject stale asynchronous results, but the remaining reconciliation
// state stays private to EntityCache.
func (l Load) Key() storage.EntityKey { return l.key }

func (l Load) Generation() uint64 { return l.generation }

// Done closes when the load owner completes or fails this attempt.
func (l Load) Done() <-chan struct{} { return l.done }

// Owned reports whether the caller that received this handle is responsible
// for completing or failing the load.
func (l Load) Owned() bool { return l.owner }

// SnapshotLoadRequest describes one authoritative row that a snapshot reader
// may reconcile. Batches are acquired atomically so overlapping readers cannot
// split ownership of the same set of entities and form wait cycles.
type SnapshotLoadRequest struct {
	Key          storage.EntityKey
	Record       storage.Record
	ForceRefresh bool
}

// BeginRefresh forces a new source snapshot even when a warm/current tuple is
// cached. Concurrent refreshes still coalesce onto one owner.
func (c *EntityCache) BeginRefresh(key storage.EntityKey) Load {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.beginRefreshLocked(key)
}

func (c *EntityCache) beginRefreshLocked(key storage.EntityKey) Load {
	entry := c.entries[key]
	if entry == nil {
		state := &loadState{done: make(chan struct{})}
		entry = &cacheEntry{Key: key, State: EntityLoading, LastUsed: c.now(), generation: c.nextGenerationLocked(), load: state}
		c.entries[key] = entry
		return Load{key: key, generation: entry.generation, done: state.done, owner: true, state: state}
	}
	if entry.load != nil {
		return Load{key: key, generation: entry.generation, done: entry.load.done, refresh: true, state: entry.load}
	}
	entry.generation = c.nextGenerationLocked()
	state := &loadState{done: make(chan struct{}), partial: entry.State == EntityLoading}
	entry.load = state
	entry.LastUsed = c.now()
	c.syncIdleLocked(entry)
	return Load{key: key, generation: entry.generation, done: state.done, owner: true, refresh: true, previous: storage.CloneRecord(entry.Record), state: state}
}

type EntityCache struct {
	mu             sync.RWMutex
	entries        map[storage.EntityKey]*cacheEntry
	nextGeneration uint64
	now            func() time.Time
	idle           expiryindex.Index[storage.EntityKey]
}

func NewEntityCache() *EntityCache {
	return &EntityCache{entries: make(map[storage.EntityKey]*cacheEntry), now: time.Now}
}

func (c *EntityCache) nextGenerationLocked() uint64 {
	c.nextGeneration++
	if c.nextGeneration == 0 {
		panic("entity cache generation overflow")
	}
	return c.nextGeneration
}

func (c *EntityCache) syncIdleLocked(entry *cacheEntry) {
	if entry.References == 0 && entry.State == EntityUnreferenced && entry.load == nil {
		c.idle.Set(entry.Key, entry.LastUsed)
	} else {
		c.idle.Remove(entry.Key)
	}
}

func (c *EntityCache) removeLocked(key storage.EntityKey) {
	c.idle.Remove(key)
	delete(c.entries, key)
}

func (c *EntityCache) Acquire(key storage.EntityKey) (EntitySnapshot, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry := c.acquireLocked(key)
	if entry == nil {
		return EntitySnapshot{}, false
	}
	return snapshot(entry), true
}

// Retain acquires a reference without copying a payload the caller does not
// need. Acquire has the same reference semantics and also returns an owned row.
func (c *EntityCache) Retain(key storage.EntityKey) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.acquireLocked(key) != nil
}

func (c *EntityCache) acquireLocked(key storage.EntityKey) *cacheEntry {
	entry := c.entries[key]
	if entry == nil {
		return nil
	}
	entry.References++
	entry.LastUsed = c.now()
	if entry.State == EntityUnreferenced {
		entry.State = EntityCurrent
	}
	c.syncIdleLocked(entry)
	return entry
}

func (c *EntityCache) Release(key storage.EntityKey) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry := c.entries[key]
	if entry == nil || entry.References == 0 {
		return false
	}
	entry.References--
	entry.LastUsed = c.now()
	if entry.References == 0 && entry.State == EntityCurrent {
		entry.State = EntityUnreferenced
	}
	c.syncIdleLocked(entry)
	return true
}

// ReleaseMany validates and releases a set of references atomically. It is
// used by query membership ownership so a partially released query can never
// leave cache reference counts out of sync with its member set.
func (c *EntityCache) ReleaseMany(keys []storage.EntityKey) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	counts := make(map[storage.EntityKey]int, len(keys))
	for _, key := range keys {
		counts[key]++
	}
	for key, count := range counts {
		entry := c.entries[key]
		if entry == nil || entry.References < count {
			return fmt.Errorf("entity %q has %d references, cannot release %d", key.Value, referenceCount(entry), count)
		}
	}
	now := c.now()
	for _, key := range keys {
		entry := c.entries[key]
		entry.References--
		entry.LastUsed = now
		if entry.References == 0 && entry.State == EntityCurrent {
			entry.State = EntityUnreferenced
		}
		c.syncIdleLocked(entry)
	}
	return nil
}

func referenceCount(entry *cacheEntry) int {
	if entry == nil {
		return 0
	}
	return entry.References
}

func (c *EntityCache) Get(key storage.EntityKey) (EntitySnapshot, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	entry := c.entries[key]
	if entry == nil {
		return EntitySnapshot{}, false
	}
	return snapshot(entry), true
}

// Inspect borrows a record only for synchronous, read-only evaluation. The
// visitor must not retain or mutate the record (including nested values), call
// back into the cache, perform IO, or block. Get remains the owned-image API
// for asynchronous work and publication. The read lock pins metadata and data
// together until the visitor returns; even an error releases the lock.
func (c *EntityCache) Inspect(key storage.EntityKey, visit func(storage.Record, EntityState) error) (bool, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	entry := c.entries[key]
	if entry == nil {
		return false, nil
	}
	return true, visit(entry.Record, entry.State)
}

// IsCurrent inspects readiness without copying the cached payload. Speculative
// prefetch only needs this predicate, not ownership of another full row image.
func (c *EntityCache) IsCurrent(key storage.EntityKey) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	entry := c.entries[key]
	return entry != nil && entry.State == EntityCurrent && entry.load == nil && entry.Record != nil
}

func (c *EntityCache) beginLoadLocked(key storage.EntityKey) Load {
	if entry := c.entries[key]; entry != nil {
		if entry.load != nil {
			return Load{key: key, generation: entry.generation, done: entry.load.done, state: entry.load}
		}
		closed := make(chan struct{})
		close(closed)
		return Load{key: key, generation: entry.generation, done: closed}
	}
	state := &loadState{done: make(chan struct{})}
	entry := &cacheEntry{Key: key, State: EntityLoading, LastUsed: c.now(), generation: c.nextGenerationLocked(), load: state}
	c.entries[key] = entry
	return Load{key: key, generation: entry.generation, done: state.done, owner: true, state: state}
}

// StageSnapshotBatch atomically assigns load ownership and installs the
// authoritative snapshot baseline for a complete window read. Automatic
// refresh is required for any non-current entry (including an unreferenced row
// that may have missed CDC) and for records whose authoritative shape changed.
//
// Callers must later Complete or Fail every load they own. Loads owned by
// another caller retain that caller's baseline and completion channel.
func (c *EntityCache) StageSnapshotBatch(requests []SnapshotLoadRequest) []Load {
	c.mu.Lock()
	defer c.mu.Unlock()
	loads := make([]Load, len(requests))
	for index, request := range requests {
		entry := c.entries[request.Key]
		refresh := request.ForceRefresh || entry != nil && (entry.State != EntityCurrent || !sameRecordShape(entry.Record, request.Record))
		if refresh {
			loads[index] = c.beginRefreshLocked(request.Key)
		} else {
			loads[index] = c.beginLoadLocked(request.Key)
		}
		load := loads[index]
		if load.owner && !load.refresh {
			entry = c.entries[request.Key]
			entry.Record = storage.CloneRecord(request.Record)
			entry.State = EntityReconciling
		}
	}
	return loads
}

func sameRecordShape(left, right storage.Record) bool {
	if len(left) != len(right) {
		return false
	}
	for name, leftValue := range left {
		rightValue, ok := right[name]
		if !ok || leftValue.Type != rightValue.Type {
			return false
		}
	}
	return true
}

func (c *EntityCache) Complete(load Load, record storage.Record) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry := c.entries[load.key]
	if entry == nil || entry.generation != load.generation || entry.load != load.state || load.state == nil {
		return ErrStaleLoad
	}
	completed := storage.CloneRecord(record)
	if load.state.partial {
		overlayConcrete(completed, entry.Record)
	}
	entry.Record = completed
	entry.State = EntityCurrent
	if entry.References == 0 {
		entry.State = EntityUnreferenced
	}
	entry.LastUsed = c.now()
	closeLoad(load.state, nil)
	entry.load = nil
	c.syncIdleLocked(entry)
	return nil
}

func (c *EntityCache) Fail(load Load, err error) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry := c.entries[load.key]
	if entry == nil || entry.generation != load.generation || entry.load != load.state || load.state == nil {
		return ErrStaleLoad
	}
	closeLoad(load.state, err)
	if entry.References > 0 {
		// Other selectors may already reference a CDC partial tuple learned
		// during this read. Preserve their baseline and reference ownership;
		// failure must not turn their later release into a dangling reference.
		// It is not authoritative and must be refreshed before snapshot reuse.
		entry.load = nil
		entry.State = EntityUnreferenced
		c.syncIdleLocked(entry)
		return nil
	}
	if load.refresh && load.previous != nil {
		entry.load = nil
		c.syncIdleLocked(entry)
		return nil
	}
	c.removeLocked(load.key)
	return nil
}

// PrepareRelationRefresh retires every older load for an explicitly invalidated
// relation. Referenced records remain as diff baselines, while unreferenced
// records are removed because they may have missed CDC while idle.
func (c *EntityCache) PrepareRelationRefresh(relation storage.RelationKey) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for key, entry := range c.entries {
		if key.Relation != relation {
			continue
		}
		if entry.load != nil {
			closeLoad(entry.load, datastore.ErrInvalidated)
			entry.load = nil
		}
		if entry.References == 0 {
			c.removeLocked(key)
		}
	}
}

func (c *EntityCache) LoadError(load Load) error {
	if load.state == nil {
		return nil
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	return load.state.err
}

func closeLoad(state *loadState, err error) {
	if state == nil || state.closed {
		return
	}
	state.err = err
	state.closed = true
	close(state.done)
}

// StoreCurrent installs a complete source-reconciled row.
func (c *EntityCache) StoreCurrent(key storage.EntityKey, record storage.Record) EntitySnapshot {
	c.mu.Lock()
	defer c.mu.Unlock()
	return snapshot(c.storeCurrentLocked(key, record))
}

func (c *EntityCache) storeCurrentLocked(key storage.EntityKey, record storage.Record) *cacheEntry {
	entry := c.entries[key]
	if entry == nil {
		entry = &cacheEntry{Key: key, generation: c.nextGenerationLocked()}
		c.entries[key] = entry
	} else if entry.load != nil {
		closeLoad(entry.load, nil)
		entry.load = nil
		// A complete source tuple supersedes the in-flight load. Advance the
		// generation so recovery owners can distinguish their now-stale token
		// from a later partial tuple that needs a fresh hydration.
		entry.generation = c.nextGenerationLocked()
	}
	entry.Record = storage.CloneRecord(record)
	entry.State = EntityCurrent
	entry.LastUsed = c.now()
	c.syncIdleLocked(entry)
	return entry
}

// PrefetchStatus reports readiness and presence, not an owned payload. Prefetch
// admission and reference pinning must not clone rows merely to discard them.
type PrefetchStatus struct {
	Usable  bool
	Present bool
}

// InstallPrefetched may fill an idle/missing entry only after the caller has
// fenced the read and ruled out intervening invalidations. A tracked current
// image wins unchanged; an unresolved load is never superseded by prefetch.
func (c *EntityCache) InstallPrefetched(key storage.EntityKey, record storage.Record) PrefetchStatus {
	c.mu.Lock()
	defer c.mu.Unlock()
	if entry := c.entries[key]; entry != nil {
		if entry.load != nil || entry.State == EntityLoading || entry.State == EntityReconciling {
			return PrefetchStatus{}
		}
		if entry.State == EntityCurrent {
			return PrefetchStatus{Usable: true, Present: entry.Record != nil}
		}
	}
	if record == nil {
		// An idle baseline may have missed CDC while no query referenced it.
		// Do not leave it available for merging a future partial source tuple
		// after an authoritative empty selector has been installed.
		c.removeLocked(key)
		return PrefetchStatus{Usable: true}
	}
	entry := c.storeCurrentLocked(key, record)
	entry.State = EntityUnreferenced
	c.syncIdleLocked(entry)
	return PrefetchStatus{Usable: true, Present: true}
}

// StorePartial records the newest fields known from CDC without presenting the
// tuple as an authoritative entity payload. An in-flight hydration remains
// active and will merge these newer fields when its fenced snapshot is ready.
func (c *EntityCache) StorePartial(key storage.EntityKey, record storage.Record) EntitySnapshot {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry := c.entries[key]
	if entry == nil {
		entry = &cacheEntry{Key: key, generation: c.nextGenerationLocked()}
		c.entries[key] = entry
	}
	if entry.load != nil {
		entry.load.partial = true
	}
	entry.Record = storage.CloneRecord(record)
	entry.State = EntityLoading
	entry.LastUsed = c.now()
	c.syncIdleLocked(entry)
	return snapshot(entry)
}

// CompleteMerged installs a fetched snapshot as a baseline, then overlays all
// concrete values learned from CDC while the fetch was in flight. Unavailable
// values never overwrite the fetched baseline.
func (c *EntityCache) CompleteMerged(load Load, fetched storage.Record) (storage.Record, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry := c.entries[load.key]
	if entry == nil || entry.generation != load.generation || entry.load != load.state || load.state == nil {
		return nil, ErrStaleLoad
	}
	merged := storage.CloneRecord(fetched)
	overlayConcrete(merged, entry.Record)
	entry.Record = merged
	entry.State = EntityCurrent
	if entry.References == 0 {
		entry.State = EntityUnreferenced
	}
	entry.LastUsed = c.now()
	closeLoad(load.state, nil)
	entry.load = nil
	c.syncIdleLocked(entry)
	return storage.CloneRecord(merged), nil
}

func overlayConcrete(target, overlay storage.Record) {
	for name, value := range overlay {
		if value.State == storage.ValueUnavailable || value.State == storage.ValueMissing {
			continue
		}
		target[name] = storage.CloneValue(value)
	}
}

// DeleteAuthoritative applies a source-confirmed deletion. Any in-flight load
// is superseded successfully so older snapshots cannot resurrect the row.
func (c *EntityCache) DeleteAuthoritative(key storage.EntityKey) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry := c.entries[key]
	if entry == nil {
		return nil
	}
	if entry.References != 0 {
		return fmt.Errorf("cannot delete referenced entity %q (%d references)", key.Value, entry.References)
	}
	if entry.load != nil {
		closeLoad(entry.load, nil)
	}
	c.removeLocked(key)
	return nil
}

// DiscardIfGeneration removes an incomplete or failed cache generation without
// touching a newer snapshot generation installed concurrently. Waiters on the
// discarded generation receive cause and may reacquire from their own
// authoritative snapshot.
func (c *EntityCache) DiscardIfGeneration(key storage.EntityKey, generation uint64, cause error) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry := c.entries[key]
	if entry == nil || entry.generation != generation {
		return false, nil
	}
	if entry.References != 0 {
		return false, fmt.Errorf("cannot discard referenced entity %q (%d references)", key.Value, entry.References)
	}
	if cause == nil {
		cause = ErrStaleLoad
	}
	if entry.load != nil {
		closeLoad(entry.load, cause)
	}
	c.removeLocked(key)
	return true, nil
}

func (c *EntityCache) MarkUnreferenced(key storage.EntityKey) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if entry := c.entries[key]; entry != nil && entry.References == 0 && entry.State == EntityCurrent {
		entry.State = EntityUnreferenced
		entry.LastUsed = c.now()
		c.syncIdleLocked(entry)
	}
}

// EvictIdle removes at most limit entries. more permits the caller to schedule
// another batch without waiting for the next periodic tick.
func (c *EntityCache) EvictIdle(before time.Time, limit int) (evicted []storage.EntityKey, more bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for len(evicted) < limit {
		key, at, ok := c.idle.First()
		if !ok || !at.Before(before) {
			break
		}
		c.removeLocked(key)
		evicted = append(evicted, key)
	}
	_, at, ok := c.idle.First()
	return evicted, ok && at.Before(before)
}

// InvalidateRelation removes every cached generation for a relation. It is
// used after an adapter failure where acknowledging the source event makes any
// previously cached tuple unsafe to reuse for a recovery snapshot.
func (c *EntityCache) InvalidateRelation(relation storage.RelationKey) []storage.EntityKey {
	c.mu.Lock()
	defer c.mu.Unlock()
	var removed []storage.EntityKey
	for key, entry := range c.entries {
		if key.Relation != relation {
			continue
		}
		if entry.load != nil {
			closeLoad(entry.load, datastore.ErrInvalidated)
		}
		c.removeLocked(key)
		removed = append(removed, key)
	}
	return removed
}

func snapshot(entry *cacheEntry) EntitySnapshot {
	return EntitySnapshot{Key: entry.Key, Record: storage.CloneRecord(entry.Record), State: entry.State, References: entry.References, Generation: entry.generation}
}
