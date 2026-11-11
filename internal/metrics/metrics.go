// Package metrics owns Weave's metric definitions and OpenTelemetry integration.
// Components publish scalar counts; collection never calls back into components.
package metrics

import (
	"context"
	"sync"
	"sync/atomic"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

const (
	ActiveSubscriptions       = "weave.query.subscriptions.active"
	InitializingSubscriptions = "weave.query.subscriptions.initializing"
	CachedQueries             = "weave.query.cache.queries"
	CachedEntities            = "weave.query.cache.entities"
	CachedMetadata            = "weave.postgres.metadata.cached"
	LoadingMetadata           = "weave.postgres.metadata.loading"
)

// Gauge is one component's contribution to a process-wide count. A nil handle
// is a no-op. Close releases the contribution and is safe alongside Set.
// Values must be nonnegative; -1 is reserved for a closed handle.
type Gauge struct {
	value  atomic.Int64
	family *family
}

func (g *Gauge) Set(n int) {
	if g == nil || n < 0 {
		return
	}
	for old := g.value.Load(); old >= 0; old = g.value.Load() {
		if old == int64(n) {
			return
		}
		if g.value.CompareAndSwap(old, int64(n)) {
			return
		}
	}
}

func (g *Gauge) Close() {
	if g == nil || g.value.Swap(-1) < 0 {
		return
	}
	f := g.family
	r := f.registry
	r.mu.Lock()
	f.mu.Lock()
	delete(f.handles, g)
	last := len(f.handles) == 0
	if last {
		f.active = false
		delete(r.families, f.name)
	}
	f.mu.Unlock()
	if last && len(r.families) == 0 {
		r.meter = nil
	}
	r.mu.Unlock()
	if last {
		if err := f.registration.Unregister(); err != nil {
			// Even a callback that could not be unregistered is inert.
			otel.Handle(err)
		}
	}
}

type family struct {
	mu           sync.RWMutex
	handles      map[*Gauge]struct{}
	registry     *registry
	name         string
	active       bool
	registration metric.Registration
}

type registry struct {
	mu            sync.Mutex
	meter         metric.Meter
	meterProvider func() metric.Meter
	families      map[string]*family
}

// The embedding application owns the provider and its lifetime. Resolve the
// meter when the first gauge is created, after application telemetry setup.
const scope = "github.com/adiom-data/weave"

var defaults = registry{meterProvider: func() metric.Meter { return otel.Meter(scope) }, families: make(map[string]*family)}

func NewGauge(name string) *Gauge { return defaults.newGauge(name) }

func (r *registry) newGauge(name string) *Gauge {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.meter == nil {
		r.meter = r.meterProvider()
	}
	f := r.families[name]
	if f == nil {
		f = &family{handles: make(map[*Gauge]struct{}), registry: r, name: name, active: true}
		registration, err := register(r.meter, name, f)
		if err != nil {
			otel.Handle(err)
			return nil
		}
		f.registration = registration
		r.families[name] = f
	}
	g := &Gauge{family: f}
	f.mu.Lock()
	f.handles[g] = struct{}{}
	f.mu.Unlock()
	return g
}

func register(meter metric.Meter, name string, f *family) (metric.Registration, error) {
	instrument, err := meter.Int64ObservableGauge(name, metric.WithUnit("{item}"))
	if err != nil {
		return nil, err
	}
	return meter.RegisterCallback(func(_ context.Context, observer metric.Observer) error {
		// Only component handles are visited, never cached rows, queries or
		// subscriptions. Writers update atomics without acquiring this lock.
		var total int64
		f.mu.RLock()
		defer f.mu.RUnlock()
		if !f.active {
			return nil
		}
		for handle := range f.handles {
			if n := handle.value.Load(); n >= 0 {
				total += n
			}
		}
		observer.ObserveInt64(instrument, total)
		return nil
	}, instrument)
}
