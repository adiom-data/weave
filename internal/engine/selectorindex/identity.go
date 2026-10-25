// Package selectorindex composes windowed queries and exact-identity selectors
// behind the change processor's shared index. Identity selectors use one slot,
// direct key routing and an idle deadline, not interval or membership trees.
package selectorindex

import (
	"errors"
	"sync"
	"time"

	"github.com/adiom-data/weave/internal/datastore"
	"github.com/adiom-data/weave/internal/engine/entitycache"
	"github.com/adiom-data/weave/internal/engine/queryregistry"
	"github.com/adiom-data/weave/internal/expiryindex"
	"github.com/adiom-data/weave/internal/storage"
)

type identityEntry struct {
	plan          datastore.IdentityQuery
	key           storage.EntityKey
	refs          int
	member, ready bool
	owners        map[string]bool
}

type Index struct {
	*queryregistry.QueryRegistry
	mu         sync.Mutex
	identities map[datastore.QueryFingerprint]*identityEntry
	byKey      map[storage.EntityKey]map[datastore.QueryFingerprint]struct{}
	byRelation map[storage.RelationKey]map[datastore.QueryFingerprint]struct{}
	idle       expiryindex.Index[datastore.QueryFingerprint]
	entities   queryregistry.EntityReferences
	ttl        time.Duration
}

func New(entities queryregistry.EntityReferences, ttl time.Duration) *Index {
	return &Index{QueryRegistry: queryregistry.NewQueryRegistryWithRetention(entities, ttl), entities: entities, ttl: ttl,
		identities: map[datastore.QueryFingerprint]*identityEntry{}, byKey: map[storage.EntityKey]map[datastore.QueryFingerprint]struct{}{}, byRelation: map[storage.RelationKey]map[datastore.QueryFingerprint]struct{}{}}
}

func full() datastore.Bounds {
	return datastore.Bounds{Left: datastore.Boundary{Kind: datastore.BoundaryStart}, Right: datastore.Boundary{Kind: datastore.BoundaryEnd}}
}
func fullBounds(bounds datastore.Bounds) bool {
	return bounds.Left.Kind == datastore.BoundaryStart && bounds.Right.Kind == datastore.BoundaryEnd
}
func descriptor(e *identityEntry) queryregistry.QueryDescriptor {
	key := e.key
	return queryregistry.QueryDescriptor{Plan: e.plan, Identity: &key}
}

func (r *Index) Acquire(plan datastore.PreparedQuery) (queryregistry.QueryDescriptor, bool) {
	identity, ok := plan.(datastore.IdentityQuery)
	if !ok {
		return r.QueryRegistry.Acquire(plan)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	fp := plan.Fingerprint()
	if e := r.identities[fp]; e != nil {
		e.refs++
		r.idle.Remove(fp)
		return descriptor(e), false
	}
	e := &identityEntry{plan: identity, key: identity.LookupKey(), refs: 1, owners: map[string]bool{}}
	r.identities[fp] = e
	if r.byKey[e.key] == nil {
		r.byKey[e.key] = map[datastore.QueryFingerprint]struct{}{}
	}
	r.byKey[e.key][fp] = struct{}{}
	if r.byRelation[e.key.Relation] == nil {
		r.byRelation[e.key.Relation] = map[datastore.QueryFingerprint]struct{}{}
	}
	r.byRelation[e.key.Relation][fp] = struct{}{}
	return descriptor(e), true
}

func (r *Index) Plan(fp datastore.QueryFingerprint) (datastore.PreparedQuery, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if e := r.identities[fp]; e != nil {
		return e.plan, true
	}
	return r.QueryRegistry.Plan(fp)
}

func (r *Index) Release(fp datastore.QueryFingerprint) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	e := r.identities[fp]
	if e == nil {
		return r.QueryRegistry.Release(fp)
	}
	if e.refs > 0 {
		e.refs--
	}
	if e.refs > 0 {
		return false, nil
	}
	if e.ready && r.ttl > 0 {
		r.idle.Set(fp, time.Now().Add(r.ttl))
		return false, nil
	}
	return true, r.discard(e)
}

func (r *Index) discard(e *identityEntry) error {
	if _, err := r.remove(e); err != nil {
		return err
	}
	fp := e.plan.Fingerprint()
	delete(r.identities, fp)
	r.idle.Remove(fp)
	delete(r.byKey[e.key], fp)
	if len(r.byKey[e.key]) == 0 {
		delete(r.byKey, e.key)
	}
	delete(r.byRelation[e.key.Relation], fp)
	if len(r.byRelation[e.key.Relation]) == 0 {
		delete(r.byRelation, e.key.Relation)
	}
	return nil
}

func (r *Index) Discard(fp datastore.QueryFingerprint) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if e := r.identities[fp]; e != nil {
		return r.discard(e)
	}
	return r.QueryRegistry.Discard(fp)
}

func (r *Index) Maintain(now time.Time, budget int) ([]queryregistry.MaintenanceFailure, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	failures, more := r.QueryRegistry.Maintain(now, budget)
	for range budget {
		fp, at, exists := r.idle.First()
		if !exists || at.After(now) {
			break
		}
		r.idle.Remove(fp)
		if e := r.identities[fp]; e != nil && e.refs == 0 {
			if err := r.discard(e); err != nil {
				failures = append(failures, queryregistry.MaintenanceFailure{Fingerprint: fp, Cause: err})
			}
		}
	}
	_, at, exists := r.idle.First()
	return failures, more || exists && !at.After(now)
}

func (r *Index) add(e *identityEntry, key storage.EntityKey) (entitycache.EntitySnapshot, bool, error) {
	if key != e.key {
		return entitycache.EntitySnapshot{}, false, errors.New("identity selector received a foreign entity")
	}
	if e.member {
		row, ok := r.entities.Get(key)
		if !ok {
			return row, false, errors.New("identity member missing from cache")
		}
		return row, false, nil
	}
	row, ok := r.entities.Acquire(key)
	if !ok {
		return row, false, errors.New("identity member missing from cache")
	}
	e.member = true
	return row, true, nil
}

func (r *Index) remove(e *identityEntry) (bool, error) {
	if !e.member {
		return false, nil
	}
	if err := r.entities.ReleaseMany([]storage.EntityKey{e.key}); err != nil {
		return false, err
	}
	e.member = false
	return true, nil
}
