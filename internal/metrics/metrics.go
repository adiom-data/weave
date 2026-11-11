// Package metrics owns provider-neutral runtime metric definitions and scalar handles.
package metrics

import "sync/atomic"

const (
	ActiveSubscriptions       = "weave.query.subscriptions.active"
	InitializingSubscriptions = "weave.query.subscriptions.initializing"
	CachedQueries             = "weave.query.cache.queries"
	CachedEntities            = "weave.query.cache.entities"
	CachedMetadata            = "weave.postgres.metadata.cached"
	LoadingMetadata           = "weave.postgres.metadata.loading"
)

// Provider registers component contributions to named gauges. A provider may
// call read concurrently; readers supplied by Weave only load scalar atomics.
// Contributions with the same name must be summed. The returned function
// unregisters this contribution; provider failures are reported by the provider
// without affecting reactive processing. Nil disables runtime metrics.
type Provider interface {
	RegisterGauge(name string, read func() int64) (unregister func())
}

// Gauge owns a component's current count. Set performs no provider calls. A nil
// handle is a no-op; Close unregisters once and late Set calls are ignored.
type Gauge struct {
	value      atomic.Int64
	unregister func()
}

func NewGauge(provider Provider, name string) *Gauge {
	if provider == nil {
		return nil
	}
	g := &Gauge{}
	g.unregister = provider.RegisterGauge(name, func() int64 {
		if n := g.value.Load(); n >= 0 {
			return n
		}
		return 0
	})
	return g
}

func (g *Gauge) Set(n int) {
	if g == nil || n < 0 {
		return
	}
	for old := g.value.Load(); old >= 0; old = g.value.Load() {
		if old == int64(n) || g.value.CompareAndSwap(old, int64(n)) {
			return
		}
	}
}

func (g *Gauge) Close() {
	if g == nil || g.value.Swap(-1) < 0 {
		return
	}
	if unregister := g.unregister; unregister != nil {
		g.unregister = nil
		unregister()
	}
}
