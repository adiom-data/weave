package engine

import "github.com/adiom-data/weave/internal/metrics"

type coordinatorMetrics struct {
	active, initializing, queries, entities *metrics.Gauge
}

func newCoordinatorMetrics(provider metrics.Provider) coordinatorMetrics {
	return coordinatorMetrics{
		metrics.NewGauge(provider, metrics.ActiveSubscriptions), metrics.NewGauge(provider, metrics.InitializingSubscriptions),
		metrics.NewGauge(provider, metrics.CachedQueries), metrics.NewGauge(provider, metrics.CachedEntities),
	}
}

// Actor-owned sampling is constant time; exporters read only scalar atomics.
func (c *Coordinator) recordMetrics() {
	if c.metrics.active == nil {
		return
	}
	active, initializing := c.subscriptions.Counts()
	c.metrics.active.Set(active)
	c.metrics.initializing.Set(initializing)
	c.metrics.queries.Set(c.queries.Size())
	c.metrics.entities.Set(c.entities.Size())
}

func (m coordinatorMetrics) close() {
	m.active.Close()
	m.initializing.Close()
	m.queries.Close()
	m.entities.Close()
}
