// Package otel adapts Weave's scalar metrics to an application-owned OpenTelemetry meter.
package otel

import (
	"context"
	"sync"

	"github.com/adiom-data/weave/internal/metrics"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

const scope = "github.com/adiom-data/weave"

type observation struct{ read func() int64 }

type family struct {
	mu           sync.RWMutex
	observations map[*observation]struct{}
	active       bool
	registration metric.Registration
}

type provider struct {
	mu       sync.Mutex
	meter    metric.Meter
	families map[string]*family
}

// New borrows the meter provider. The caller keeps it alive until all Weave
// components using this adapter are closed. A nil provider disables metrics.
func New(source metric.MeterProvider) metrics.Provider {
	if source == nil {
		return nil
	}
	return &provider{meter: source.Meter(scope), families: make(map[string]*family)}
}

func (p *provider) RegisterGauge(name string, read func() int64) func() {
	p.mu.Lock()
	f := p.families[name]
	if f == nil {
		f = &family{observations: make(map[*observation]struct{}), active: true}
		registration, err := register(p.meter, name, f)
		if err != nil {
			p.mu.Unlock()
			otel.Handle(err)
			return nil
		}
		f.registration = registration
		p.families[name] = f
	}
	o := &observation{read: read}
	f.mu.Lock()
	f.observations[o] = struct{}{}
	f.mu.Unlock()
	p.mu.Unlock()
	var once sync.Once
	return func() {
		once.Do(func() {
			p.mu.Lock()
			f.mu.Lock()
			delete(f.observations, o)
			last := len(f.observations) == 0
			if last {
				f.active = false
				delete(p.families, name)
			}
			f.mu.Unlock()
			p.mu.Unlock()
			if last {
				if err := f.registration.Unregister(); err != nil {
					otel.Handle(err) // A retired callback remains inert even on failure.
				}
			}
		})
	}
}

func register(meter metric.Meter, name string, f *family) (metric.Registration, error) {
	instrument, err := meter.Int64ObservableGauge(name, metric.WithUnit("{item}"))
	if err != nil {
		return nil, err
	}
	return meter.RegisterCallback(func(_ context.Context, observer metric.Observer) error {
		f.mu.RLock()
		defer f.mu.RUnlock()
		if !f.active {
			return nil
		}
		var total int64
		for o := range f.observations {
			total += o.read()
		}
		observer.ObserveInt64(instrument, total)
		return nil
	}, instrument)
}
