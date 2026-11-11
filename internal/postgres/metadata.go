package postgres

import (
	"context"
	"errors"
	"sync"

	"github.com/adiom-data/weave/internal/datastore"
	"github.com/adiom-data/weave/internal/metrics"
	"github.com/adiom-data/weave/internal/storage"
)

type metadataEntry struct {
	done     chan struct{}
	plan     *plan
	err      error
	cancel   context.CancelFunc
	waiters  int
	finished bool
}

// Successful entries live until explicitly invalidated. Removing an entry also
// revokes an in-flight loader's right to publish; no generation can repopulate
// the cache after a concurrent invalidation. Waiters may cancel independently.
type metadataCache struct {
	ready, loading *metrics.Gauge
	mu             sync.Mutex
	entries        map[storage.RelationName]*metadataEntry
	cached         uint64
}

func (c *metadataCache) get(ctx context.Context, relation storage.RelationName, load func(context.Context) (*plan, error)) (*plan, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	c.mu.Lock()
	if c.entries == nil {
		c.entries = make(map[storage.RelationName]*metadataEntry)
	}
	entry, exists := c.entries[relation]
	if exists && entry.finished {
		c.mu.Unlock()
		return entry.plan, nil
	}
	if !exists {
		loadCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
		entry = &metadataEntry{done: make(chan struct{}), cancel: cancel}
		c.entries[relation] = entry
		go c.load(loadCtx, relation, entry, load)
	}
	entry.waiters++
	c.recordMetrics()
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		defer c.mu.Unlock()
		defer c.recordMetrics()
		entry.waiters--
		if entry.waiters == 0 && !entry.finished {
			if c.entries[relation] == entry {
				delete(c.entries, relation)
			}
			entry.cancel()
		}
	}()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-entry.done:
		if entry.err != nil {
			return nil, entry.err
		}
		return entry.plan, nil
	}
}

func (c *metadataCache) load(ctx context.Context, relation storage.RelationName, entry *metadataEntry, load func(context.Context) (*plan, error)) {
	defer entry.cancel()
	p, err := load(ctx)
	c.mu.Lock()
	defer c.mu.Unlock()
	defer c.recordMetrics()
	if c.entries[relation] != entry {
		err = datastore.ErrInvalidated
	} else if err != nil {
		delete(c.entries, relation)
	} else {
		c.cached++
	}
	entry.plan, entry.err, entry.finished = p, err, true
	// Successful entries may live indefinitely; do not retain the originating
	// request's context/claims through its cancellation closure after discovery.
	entry.cancel = nil
	close(entry.done)
}

func validateRelationName(relation storage.RelationName) error {
	if relation.Namespace == "" || relation.Name == "" {
		return errors.New("schema and table are required")
	}
	return nil
}

func (s *PostgresDataSource) InvalidateMetadata(ctx context.Context, relation *storage.RelationName) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if relation != nil {
		if err := validateRelationName(*relation); err != nil {
			return err
		}
	}
	s.metadata.mu.Lock()
	defer s.metadata.mu.Unlock()
	defer s.metadata.recordMetrics()
	if relation == nil {
		s.metadata.entries = nil
		s.metadata.cached = 0
	} else {
		if entry := s.metadata.entries[*relation]; entry != nil {
			if entry.cancel != nil {
				entry.cancel()
			}
		}
		if entry := s.metadata.entries[*relation]; entry != nil && entry.plan != nil && entry.err == nil {
			s.metadata.cached--
		}
		delete(s.metadata.entries, *relation)
	}
	return nil
}

// Caller holds mu. Collection itself never acquires the cache lock.
func (c *metadataCache) recordMetrics() {
	c.ready.Set(int(c.cached))
	c.loading.Set(len(c.entries) - int(c.cached))
}
