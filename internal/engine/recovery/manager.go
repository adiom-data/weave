package recovery

import (
	"context"
	"errors"
	"fmt"

	"github.com/adiom-data/weave/internal/datastore"
	"github.com/adiom-data/weave/internal/engine/entitycache"
	"github.com/adiom-data/weave/internal/engine/windowloader"
	"github.com/adiom-data/weave/internal/storage"
)

var ErrBufferLimit = errors.New("relation refresh buffer limit exceeded")

type RelationState struct {
	generation uint64
	cancel     context.CancelFunc
	before     *RelationSnapshot
	buffered   []datastore.CommittedChanges
	checkpoint storage.Checkpoint
	atomic     bool
	windows    []*windowloader.PendingWindow
	ready      bool
	changes    int
}

// RelationSnapshot stores each entity image once while retaining its
// membership in every shared query. This avoids multiplying large record
// copies when overlapping queries cover the same entity.
type RelationSnapshot struct {
	records     map[storage.EntityKey]storage.Record
	memberships map[datastore.QueryFingerprint]map[storage.EntityKey]struct{}
}

func NewRelationSnapshot() *RelationSnapshot {
	return &RelationSnapshot{
		records:     make(map[storage.EntityKey]storage.Record),
		memberships: make(map[datastore.QueryFingerprint]map[storage.EntityKey]struct{}),
	}
}

func (s *RelationSnapshot) AddMembership(fingerprint datastore.QueryFingerprint, key storage.EntityKey) {
	members := s.memberships[fingerprint]
	if members == nil {
		members = make(map[storage.EntityKey]struct{})
		s.memberships[fingerprint] = members
	}
	members[key] = struct{}{}
}

func (s *RelationSnapshot) HasRecord(key storage.EntityKey) bool {
	_, exists := s.records[key]
	return exists
}

func (s *RelationSnapshot) StoreRecord(key storage.EntityKey, record storage.Record) {
	if !s.HasRecord(key) {
		s.records[key] = storage.CloneRecord(record)
	}
}

func (s *RelationSnapshot) Lookup(fingerprint datastore.QueryFingerprint, key storage.EntityKey) (storage.Record, bool) {
	if _, member := s.memberships[fingerprint][key]; !member {
		return nil, false
	}
	return s.records[key], true
}

func (s *RelationSnapshot) RangeMemberships(yield func(datastore.QueryFingerprint, storage.EntityKey, storage.Record) bool) {
	for fingerprint, members := range s.memberships {
		for key := range members {
			if !yield(fingerprint, key, s.records[key]) {
				return
			}
		}
	}
}

type HydrationState struct {
	load    entitycache.Load
	cancel  context.CancelFunc
	fence   storage.Fence
	record  storage.Record
	exists  bool
	ready   bool
	covered bool
	queries map[datastore.QueryFingerprint]struct{}
}

// Manager owns the transient state used to recover incomplete change tuples
// and relations that require a fresh snapshot. It is actor-confined and does
// not perform IO or publish subscription events.
type Manager struct {
	relations     map[storage.RelationKey]*RelationState
	hydrations    map[storage.EntityKey]*HydrationState
	maxHydrations int
	maxBatches    int
	maxChanges    int
	nextRelation  uint64
}

func NewManager(maxHydrations, maxBatches, maxChanges int) *Manager {
	return &Manager{
		relations:     make(map[storage.RelationKey]*RelationState),
		hydrations:    make(map[storage.EntityKey]*HydrationState),
		maxHydrations: maxHydrations,
		maxBatches:    maxBatches,
		maxChanges:    maxChanges,
	}
}

func (m *Manager) Relation(relation storage.RelationKey) (*RelationState, bool) {
	state, ok := m.relations[relation]
	return state, ok
}

func (m *Manager) StartRelation(relation storage.RelationKey, before *RelationSnapshot, trigger datastore.CommittedChanges, cancel context.CancelFunc) (*RelationState, bool) {
	if cancel == nil {
		cancel = func() {}
	}
	if _, exists := m.relations[relation]; exists {
		cancel()
		return nil, false
	}
	m.nextRelation++
	state := &RelationState{generation: m.nextRelation, cancel: cancel, before: before, checkpoint: trigger.Checkpoint, atomic: trigger.Atomic}
	m.relations[relation] = state
	return state, true
}

func (m *Manager) RemoveRelation(relation storage.RelationKey) (*RelationState, bool) {
	state, ok := m.relations[relation]
	delete(m.relations, relation)
	if ok && state.cancel != nil {
		state.cancel()
	}
	return state, ok
}

func (m *Manager) RangeRelations(yield func(storage.RelationKey, *RelationState) bool) {
	for relation, state := range m.relations {
		if !yield(relation, state) {
			return
		}
	}
}

func (m *Manager) Buffer(relation storage.RelationKey, batch datastore.CommittedChanges) error {
	state, ok := m.relations[relation]
	if !ok {
		return errors.New("relation is not recovering")
	}
	if len(state.buffered) >= m.maxBatches || state.changes+len(batch.Changes) > m.maxChanges {
		return ErrBufferLimit
	}
	state.buffered = append(state.buffered, batch)
	state.changes += len(batch.Changes)
	state.checkpoint = batch.Checkpoint
	state.atomic = false
	return nil
}

func (m *Manager) Hydration(key storage.EntityKey) (*HydrationState, bool) {
	state, ok := m.hydrations[key]
	return state, ok
}

// QueryHasPendingHydration reports whether a source tuple still needs an
// authoritative read before membership or ordering for query is decidable.
// The manager is actor-confined, so callers must ask from the coordinator
// actor rather than inspecting this state from snapshot worker goroutines.
func (m *Manager) QueryHasPendingHydration(query datastore.QueryFingerprint) bool {
	for _, state := range m.hydrations {
		if _, pending := state.queries[query]; pending {
			return true
		}
	}
	return false
}

// ReplaceHydrationQueries installs the dependencies computed from the newest
// partial state for an entity. Older dependencies must not survive after a
// later CDC tuple proves that a query no longer needs the hydration.
func (m *Manager) ReplaceHydrationQueries(key storage.EntityKey, queries []datastore.QueryFingerprint) bool {
	state, ok := m.hydrations[key]
	if !ok {
		return false
	}
	state.replaceQueries(queries)
	return true
}

func (m *Manager) CheckHydrationCapacity() error {
	if len(m.hydrations) >= m.maxHydrations {
		return fmt.Errorf("entity hydration limit of %d exceeded", m.maxHydrations)
	}
	return nil
}

func (m *Manager) StartHydration(key storage.EntityKey, load entitycache.Load, queries []datastore.QueryFingerprint, cancel context.CancelFunc) (*HydrationState, bool, error) {
	if cancel == nil {
		cancel = func() {}
	}
	if state, ok := m.hydrations[key]; ok {
		cancel()
		state.replaceQueries(queries)
		return state, false, nil
	}
	if err := m.CheckHydrationCapacity(); err != nil {
		cancel()
		return nil, false, err
	}
	querySet := make(map[datastore.QueryFingerprint]struct{}, len(queries))
	for _, fingerprint := range queries {
		querySet[fingerprint] = struct{}{}
	}
	state := &HydrationState{load: load, cancel: cancel, queries: querySet}
	m.hydrations[key] = state
	return state, true, nil
}

func (m *Manager) RemoveHydration(key storage.EntityKey) (*HydrationState, bool) {
	state, ok := m.hydrations[key]
	delete(m.hydrations, key)
	if ok && state.cancel != nil {
		state.cancel()
	}
	return state, ok
}

func (m *Manager) RangeHydrations(yield func(storage.EntityKey, *HydrationState) bool) {
	for key, state := range m.hydrations {
		if !yield(key, state) {
			return
		}
	}
}

func (m *Manager) RemoveRelationHydrations(relation storage.RelationKey) {
	for key, state := range m.hydrations {
		if key.Relation == relation {
			delete(m.hydrations, key)
			if state.cancel != nil {
				state.cancel()
			}
		}
	}
}

// CancelAll terminates every owned recovery attempt. The manager is
// actor-confined, so callers use this when the coordinator loop exits.
func (m *Manager) CancelAll() {
	for relation := range m.relations {
		m.RemoveRelation(relation)
	}
	for key := range m.hydrations {
		m.RemoveHydration(key)
	}
}

func (m *Manager) PendingFence(covers func(storage.Fence) bool) storage.Fence {
	for _, state := range m.hydrations {
		if state.ready && !state.covered && state.fence != nil && !covers(state.fence) {
			return state.fence
		}
	}
	for _, state := range m.relations {
		if !state.ready {
			continue
		}
		for _, window := range state.windows {
			if window.Fence != nil && !covers(window.Fence) {
				return window.Fence
			}
		}
	}
	return nil
}

func (s *RelationState) Before() *RelationSnapshot {
	return s.before
}
func (s *RelationState) Generation() uint64                     { return s.generation }
func (s *RelationState) Buffered() []datastore.CommittedChanges { return s.buffered }
func (s *RelationState) Checkpoint() storage.Checkpoint         { return s.checkpoint }
func (s *RelationState) Atomic() bool                           { return s.atomic }
func (s *RelationState) Windows() []*windowloader.PendingWindow {
	return s.windows
}
func (s *RelationState) Ready() bool { return s.ready }
func (s *RelationState) InstallWindows(windows []*windowloader.PendingWindow) {
	s.windows = windows
	s.ready = true
}

func (s *HydrationState) Load() entitycache.Load { return s.load }
func (s *HydrationState) Fence() storage.Fence   { return s.fence }
func (s *HydrationState) Record() storage.Record { return storage.CloneRecord(s.record) }
func (s *HydrationState) Exists() bool           { return s.exists }
func (s *HydrationState) Ready() bool            { return s.ready }
func (s *HydrationState) Covered() bool          { return s.covered }
func (s *HydrationState) AcceptRead(fence storage.Fence, record storage.Record, exists, covered bool) {
	s.fence = fence
	s.record = storage.CloneRecord(record)
	s.exists = exists
	s.covered = covered
	s.ready = true
}
func (s *HydrationState) AddQueries(queries []datastore.QueryFingerprint) {
	for _, fingerprint := range queries {
		s.queries[fingerprint] = struct{}{}
	}
}

func (s *HydrationState) replaceQueries(queries []datastore.QueryFingerprint) {
	s.queries = make(map[datastore.QueryFingerprint]struct{}, len(queries))
	for _, fingerprint := range queries {
		s.queries[fingerprint] = struct{}{}
	}
}
func (s *HydrationState) QueryFingerprints() []datastore.QueryFingerprint {
	result := make([]datastore.QueryFingerprint, 0, len(s.queries))
	for fingerprint := range s.queries {
		result = append(result, fingerprint)
	}
	return result
}
