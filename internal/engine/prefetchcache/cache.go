// Package prefetchcache owns short-lived, fenced join results before any
// selector subscribes to them. Source changes invalidate pins before the shared
// processor runs, so speculative data never outlives its freshness proof.
package prefetchcache

import (
	"time"

	"github.com/adiom-data/weave/internal/engine/entitycache"
	"github.com/adiom-data/weave/internal/engine/referencefetch"
	"github.com/adiom-data/weave/internal/expiryindex"
	"github.com/adiom-data/weave/internal/storage"
)

type Entities interface {
	InstallPrefetched(storage.EntityKey, storage.Record) entitycache.PrefetchStatus
	Retain(storage.EntityKey) bool
	ReleaseMany([]storage.EntityKey) error
}

// Cache is coordinator-confined. It does not retain queries, bindings or
// negative selector results: a filtered-out target is not an absent entity.
type Cache struct {
	entities   Entities
	ttl        time.Duration
	byRelation map[storage.RelationKey]map[storage.EntityKey]struct{}
	idle       expiryindex.Index[storage.EntityKey]
	absent     map[storage.EntityKey]bool
}

func New(entities Entities, ttl time.Duration) *Cache {
	return &Cache{entities: entities, ttl: ttl, byRelation: map[storage.RelationKey]map[storage.EntityKey]struct{}{}, absent: map[storage.EntityKey]bool{}}
}
func (c *Cache) Absent(key storage.EntityKey) bool { return c.absent[key] }
func (c *Cache) PutAbsent(key storage.EntityKey) error {
	if c.ttl <= 0 {
		return nil
	}
	state := c.entities.InstallPrefetched(key, nil)
	if !state.Usable || state.Present {
		return nil
	}
	if c.byRelation[key.Relation] == nil {
		c.byRelation[key.Relation] = map[storage.EntityKey]struct{}{}
	}
	c.byRelation[key.Relation][key] = struct{}{}
	c.absent[key] = true
	c.idle.Set(key, time.Now().Add(c.ttl))
	return nil
}
func (c *Cache) HasRelation(relation storage.RelationKey) bool {
	return len(c.byRelation[relation]) > 0
}
func (c *Cache) Put(key storage.EntityKey, record storage.Record) error {
	if record == nil || c.ttl <= 0 {
		return nil
	}
	state := c.entities.InstallPrefetched(key, record)
	if !state.Usable || !state.Present {
		return nil
	}
	keys := c.byRelation[key.Relation]
	if keys == nil {
		keys = map[storage.EntityKey]struct{}{}
		c.byRelation[key.Relation] = keys
	}
	if _, exists := keys[key]; !exists || c.absent[key] {
		if !c.entities.Retain(key) {
			return nil
		}
		keys[key] = struct{}{}
	}
	delete(c.absent, key)
	c.idle.Set(key, time.Now().Add(c.ttl))
	return nil
}
func (c *Cache) remove(key storage.EntityKey) error {
	keys := c.byRelation[key.Relation]
	if _, exists := keys[key]; !exists {
		return nil
	}
	if !c.absent[key] {
		if err := c.entities.ReleaseMany([]storage.EntityKey{key}); err != nil {
			return err
		}
	}
	delete(c.absent, key)
	delete(keys, key)
	c.idle.Remove(key)
	if len(keys) == 0 {
		delete(c.byRelation, key.Relation)
	}
	return nil
}
func (c *Cache) InvalidateRelation(relation storage.RelationKey) error {
	for key := range c.byRelation[relation] {
		if err := c.remove(key); err != nil {
			return err
		}
	}
	return nil
}
func (c *Cache) Observe(change referencefetch.Invalidation) error {
	if change.All() {
		return c.InvalidateRelation(change.Relation())
	}
	before, after := change.Keys()
	if err := c.remove(before); err != nil {
		return err
	}
	return c.remove(after)
}
func (c *Cache) Maintain(now time.Time, budget int) (bool, error) {
	for range budget {
		key, at, exists := c.idle.First()
		if !exists || at.After(now) {
			break
		}
		if err := c.remove(key); err != nil {
			return false, err
		}
	}
	_, at, exists := c.idle.First()
	return exists && !at.After(now), nil
}
