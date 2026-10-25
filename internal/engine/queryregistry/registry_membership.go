package queryregistry

import (
	"errors"
	"fmt"

	"github.com/adiom-data/weave/internal/datastore"
	"github.com/adiom-data/weave/internal/engine/entitycache"
	"github.com/adiom-data/weave/internal/storage"
)

func (r *QueryRegistry) AddMember(fingerprint datastore.QueryFingerprint, key storage.EntityKey) (entitycache.EntitySnapshot, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	query := r.queries[fingerprint]
	if query == nil {
		return entitycache.EntitySnapshot{}, false, errors.New("live query not found")
	}
	_, exists := query.members.byEntity[key]
	if exists {
		entity, ok := r.entities.Get(key)
		if !ok {
			return entitycache.EntitySnapshot{}, false, errors.New("query member is missing from entity cache")
		}
		sortKey, err := query.plan.Key(entity.Record)
		if err != nil {
			return entitycache.EntitySnapshot{}, false, err
		}
		if _, err := query.members.upsert(key, sortKey); err != nil {
			return entitycache.EntitySnapshot{}, false, err
		}
		return entity, false, nil
	}
	entity, ok := r.entities.Acquire(key)
	if !ok {
		return entitycache.EntitySnapshot{}, false, errors.New("cannot add query member before caching entity")
	}
	sortKey, err := query.plan.Key(entity.Record)
	if err != nil {
		if releaseErr := r.entities.ReleaseMany([]storage.EntityKey{key}); releaseErr != nil {
			return entitycache.EntitySnapshot{}, false, fmt.Errorf("order query member: %v; release failed: %w", err, releaseErr)
		}
		return entitycache.EntitySnapshot{}, false, err
	}
	if _, err := query.members.upsert(key, sortKey); err != nil {
		if releaseErr := r.entities.ReleaseMany([]storage.EntityKey{key}); releaseErr != nil {
			return entitycache.EntitySnapshot{}, false, fmt.Errorf("index query member: %v; release failed: %w", err, releaseErr)
		}
		return entitycache.EntitySnapshot{}, false, err
	}
	addIndex(r.byEntity, key, fingerprint)
	return entity, true, nil
}

func (r *QueryRegistry) RemoveMember(fingerprint datastore.QueryFingerprint, key storage.EntityKey) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	query := r.queries[fingerprint]
	if query == nil {
		return false, errors.New("live query not found")
	}
	return r.removeMemberLocked(query, key)
}

func (r *QueryRegistry) removeMemberLocked(query *liveQuery, key storage.EntityKey) (bool, error) {
	if _, exists := query.members.byEntity[key]; !exists {
		return false, nil
	}
	oldRoot := query.members.root
	oldSortKey := query.members.byEntity[key]
	if _, err := query.members.remove(key); err != nil {
		return false, err
	}
	if err := r.entities.ReleaseMany([]storage.EntityKey{key}); err != nil {
		query.members.root = oldRoot
		query.members.byEntity[key] = oldSortKey
		return false, fmt.Errorf("release query member: %w", err)
	}
	removeIndex(r.byEntity, key, query.plan.Fingerprint())
	return true, nil
}

// ClearRelation atomically removes every materialized membership for a
// relation while retaining the live queries and their cursor coverage. This is
// the in-memory meaning of a committed datastore TRUNCATE: future inserts can
// immediately repopulate the same stable windows.
func (r *QueryRegistry) ClearRelation(relation storage.RelationKey) ([]datastore.QueryFingerprint, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	set := r.byRelation[relation]
	if len(set) == 0 {
		return nil, nil
	}
	fingerprints := make([]datastore.QueryFingerprint, 0, len(set))
	var keys []storage.EntityKey
	for fingerprint := range set {
		query := r.queries[fingerprint]
		if query == nil {
			continue
		}
		fingerprints = append(fingerprints, fingerprint)
		keys = append(keys, query.members.all()...)
	}
	if err := r.entities.ReleaseMany(keys); err != nil {
		return nil, fmt.Errorf("release cleared query memberships: %w", err)
	}
	for _, fingerprint := range fingerprints {
		query := r.queries[fingerprint]
		for _, key := range query.members.all() {
			removeIndex(r.byEntity, key, fingerprint)
		}
		query.members = newOrderedMembers(query.plan)
	}
	return fingerprints, nil
}

// RemoveEntity atomically detaches an entity from every shared query that owns
// it. This is the only safe precursor to forcibly discarding a referenced
// cache entry after hydration or adapter failure.
func (r *QueryRegistry) RemoveEntity(key storage.EntityKey) ([]datastore.QueryFingerprint, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	set := r.byEntity[key]
	if len(set) == 0 {
		return nil, nil
	}
	fingerprints := make([]datastore.QueryFingerprint, 0, len(set))
	keys := make([]storage.EntityKey, 0, len(set))
	for fingerprint := range set {
		fingerprints = append(fingerprints, fingerprint)
		keys = append(keys, key)
	}
	type removedMembership struct {
		query   *liveQuery
		root    *memberNode
		sortKey datastore.SortKey
	}
	removed := make([]removedMembership, 0, len(fingerprints))
	for _, fingerprint := range fingerprints {
		if query := r.queries[fingerprint]; query != nil {
			state := removedMembership{query: query, root: query.members.root, sortKey: query.members.byEntity[key]}
			if _, err := query.members.remove(key); err != nil {
				for _, prior := range removed {
					prior.query.members.root = prior.root
					prior.query.members.byEntity[key] = prior.sortKey
				}
				return nil, err
			}
			removed = append(removed, state)
		}
	}
	if err := r.entities.ReleaseMany(keys); err != nil {
		for _, prior := range removed {
			prior.query.members.root = prior.root
			prior.query.members.byEntity[key] = prior.sortKey
		}
		return nil, fmt.Errorf("release entity query memberships: %w", err)
	}
	delete(r.byEntity, key)
	return fingerprints, nil
}

func (r *QueryRegistry) IsMember(fingerprint datastore.QueryFingerprint, key storage.EntityKey) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	query := r.queries[fingerprint]
	if query == nil {
		return false
	}
	_, present := query.members.byEntity[key]
	return present
}

func (r *QueryRegistry) Members(fingerprint datastore.QueryFingerprint) []storage.EntityKey {
	r.mu.RLock()
	defer r.mu.RUnlock()
	query := r.queries[fingerprint]
	if query == nil {
		return nil
	}
	return query.members.all()
}

// PositionInBounds returns an entity's final zero-based position inside one
// subscription window. The query registry is the sole owner of canonical
// membership ordering, so delivery can produce indexed deltas without
// retaining another per-subscription membership copy.
func (r *QueryRegistry) PositionInBounds(fingerprint datastore.QueryFingerprint, bounds datastore.Bounds, key storage.EntityKey) (int, bool, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	query := r.queries[fingerprint]
	if query == nil {
		return 0, false, errors.New("live query not found")
	}
	return query.members.position(bounds, key)
}

// RefreshMember updates an existing member's position after its authoritative
// record changes. Entity ownership is unchanged.
func (r *QueryRegistry) RefreshMember(fingerprint datastore.QueryFingerprint, key storage.EntityKey) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	query := r.queries[fingerprint]
	if query == nil {
		return errors.New("live query not found")
	}
	if _, exists := query.members.byEntity[key]; !exists {
		return errors.New("query member not found")
	}
	entity, ok := r.entities.Get(key)
	if !ok {
		return errors.New("query member is missing from entity cache")
	}
	sortKey, err := query.plan.Key(entity.Record)
	if err != nil {
		return err
	}
	_, err = query.members.upsert(key, sortKey)
	return err
}

// EntitiesInBounds reads only the shared members intersecting the requested
// intervals. A single interval returns query order (including the internal
// identity tie-breaker). Multiple intervals are visited in input order, with
// overlapping entities returned only once. Returned records are owned copies;
// windows themselves do not duplicate records.
// EntityKeysInBounds returns an owned, ordered key image without reading or
// copying entity records. Callers coordinate subsequent reads with mutations.
func (r *QueryRegistry) EntityKeysInBounds(fingerprint datastore.QueryFingerprint, bounds []datastore.Bounds) ([]storage.EntityKey, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	query := r.queries[fingerprint]
	if query == nil {
		return nil, errors.New("live query not found")
	}
	return query.members.within(bounds)
}

func (r *QueryRegistry) EntitiesInBounds(fingerprint datastore.QueryFingerprint, bounds []datastore.Bounds) ([]entitycache.EntitySnapshot, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	query := r.queries[fingerprint]
	if query == nil {
		return nil, errors.New("live query not found")
	}
	keys, err := query.members.within(bounds)
	if err != nil {
		return nil, err
	}
	result := make([]entitycache.EntitySnapshot, 0, len(keys))
	for _, key := range keys {
		entity, ok := r.entities.Get(key)
		if !ok {
			return nil, errors.New("query member is missing from entity cache")
		}
		result = append(result, entity)
	}
	return result, nil
}

func (r *QueryRegistry) SortKeysInBounds(fingerprint datastore.QueryFingerprint, bounds datastore.Bounds, limit int, reverse bool) ([]datastore.SortKey, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	query := r.queries[fingerprint]
	if query == nil {
		return nil, errors.New("live query not found")
	}
	return query.members.sortKeysWithin(bounds, limit, reverse)
}
