package dependency

import "github.com/adiom-data/weave/internal/storage"

type cleanupReference struct {
	key  storage.EntityKey
	cell *entityCell
}

// cleanupRows owns a detached row map exclusively. Iteration and map removal
// happen outside the shared mutex, including any traversal of sparse buckets.
// Only reference-count updates take the mutex, for at most one bounded batch.
// Shutdown discards the whole generation, so canceled cleanup need not drain.
func (g *generation) cleanupRows(rows map[storage.EntityKey]row) bool {
	batch := make([]cleanupReference, 0, g.owner.options.CleanupBatchSize)
	flush := func() bool {
		if g.ctx.Err() != nil {
			return false
		}
		c := g.owner
		c.mu.Lock()
		if g.stopping {
			c.mu.Unlock()
			return false
		}
		for _, reference := range batch {
			g.release(reference.key, reference.cell)
		}
		c.mu.Unlock()
		clear(batch)
		batch = batch[:0]
		return true
	}
	for key, value := range rows {
		batch = append(batch, cleanupReference{key: key, cell: value.cell})
		delete(rows, key)
		if len(batch) == cap(batch) && !flush() {
			return false
		}
	}
	return len(batch) == 0 || flush()
}
