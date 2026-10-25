// Package referencefetch tracks invalidations during speculative reference
// reads. It never publishes records or replays source history: a dirty target
// simply falls back to ordinary fenced subscription initialization.
package referencefetch

import (
	"github.com/adiom-data/weave/internal/datastore"
	"github.com/adiom-data/weave/internal/storage"
)

type Capture struct {
	relation storage.RelationKey
	limit    int
	keys     map[storage.EntityKey]struct{}
	all      bool
}

// Invalidation can be resolved once per source change and shared by every
// in-flight fetch. In particular, native tuple decoding is not repeated for
// each concurrent reader of the same target relation.
type Invalidation struct {
	relation      storage.RelationKey
	before, after storage.EntityKey
	all           bool
}

func (i Invalidation) Relation() storage.RelationKey                { return i.relation }
func (i Invalidation) All() bool                                    { return i.all }
func (i Invalidation) Keys() (storage.EntityKey, storage.EntityKey) { return i.before, i.after }

func Resolve(change datastore.Change) Invalidation {
	result := Invalidation{relation: change.Relation()}
	if change.Kind() == datastore.ChangeClearRelation || change.Kind() == datastore.ChangeInvalidate {
		result.all = true
		return result
	}
	before, err := change.BeforeKey()
	if err != nil {
		result.all = true
		return result
	}
	after, err := change.Apply(nil)
	if err != nil {
		result.all = true
		return result
	}
	if before.Relation != result.relation || before.Value == "" || after.Key.Relation != result.relation || after.Key.Value == "" {
		result.all = true
		return result
	}
	result.before, result.after = before, after.Key
	return result
}

func New(relation storage.RelationKey, limit int) *Capture {
	return &Capture{relation: relation, limit: limit, keys: map[storage.EntityKey]struct{}{}}
}

func (c *Capture) Dirty(key storage.EntityKey) bool {
	_, found := c.keys[key]
	return c.all || found
}

func (c *Capture) Observe(change datastore.Change) {
	if c.all || change.Relation() != c.relation {
		return
	}
	c.ObserveResolved(Resolve(change))
}

func (c *Capture) ObserveResolved(change Invalidation) {
	if c.all || change.relation != c.relation {
		return
	}
	if change.all {
		c.all, c.keys = true, nil
		return
	}
	if change.before.Value != "" {
		c.keys[change.before] = struct{}{}
	}
	// A primary-key update may move into another selector. Even a partial
	// change must identify its new entity; otherwise disable this optimization.
	if change.after.Value != "" {
		c.keys[change.after] = struct{}{}
	}
	if len(c.keys) > c.limit {
		c.all, c.keys = true, nil
	}
}
