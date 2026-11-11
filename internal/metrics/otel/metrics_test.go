package otel

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/adiom-data/weave/internal/metrics"
	otelmetric "go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func testProvider(t *testing.T) (metrics.Provider, *sdkmetric.ManualReader) {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	return New(provider), reader
}

func readGauge(t *testing.T, reader *sdkmetric.ManualReader, name string) (int64, bool) {
	t.Helper()
	var result metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &result); err != nil {
		t.Fatal(err)
	}
	for _, scope := range result.ScopeMetrics {
		for _, measurement := range scope.Metrics {
			if measurement.Name == name {
				gauge, ok := measurement.Data.(metricdata.Gauge[int64])
				if !ok || len(gauge.DataPoints) != 1 || gauge.DataPoints[0].Attributes.Len() != 0 {
					t.Fatalf("unexpected measurement: %#v", measurement)
				}
				return gauge.DataPoints[0].Value, true
			}
		}
	}
	return 0, false
}

func TestAggregateAndReleaseContributions(t *testing.T) {
	p, reader := testProvider(t)
	a, b := metrics.NewGauge(p, metrics.CachedEntities), metrics.NewGauge(p, metrics.CachedEntities)
	a.Set(3)
	b.Set(7)
	if got, ok := readGauge(t, reader, metrics.CachedEntities); !ok || got != 10 {
		t.Fatal(got)
	}
	a.Close()
	a.Set(99)
	if got, ok := readGauge(t, reader, metrics.CachedEntities); !ok || got != 7 {
		t.Fatal(got)
	}
	b.Close()
	if _, ok := readGauge(t, reader, metrics.CachedEntities); ok {
		t.Fatal("closed metric still reported")
	}
	if len(p.(*provider).families) != 0 {
		t.Fatal("closed metric registration retained")
	}
}

func TestConcurrentRegistrationCollectionAndClose(t *testing.T) {
	p, reader := testProvider(t)
	var workers sync.WaitGroup
	for range 4 {
		workers.Go(func() {
			for i := range 100 {
				g := metrics.NewGauge(p, metrics.ActiveSubscriptions)
				g.Set(i)
				g.Close()
			}
		})
	}
	for range 100 {
		readGauge(t, reader, metrics.ActiveSubscriptions)
	}
	workers.Wait()
	if _, ok := readGauge(t, reader, metrics.ActiveSubscriptions); ok {
		t.Fatal("closed metric still reported")
	}
}

func TestAdaptersUseIndependentApplicationProviders(t *testing.T) {
	first, firstReader := testProvider(t)
	second, secondReader := testProvider(t)
	a, b := metrics.NewGauge(first, metrics.CachedQueries), metrics.NewGauge(second, metrics.CachedQueries)
	a.Set(4)
	b.Set(9)
	defer b.Close()
	if got, ok := readGauge(t, firstReader, metrics.CachedQueries); !ok || got != 4 {
		t.Fatal(got)
	}
	if got, ok := readGauge(t, secondReader, metrics.CachedQueries); !ok || got != 9 {
		t.Fatal(got)
	}
	a.Close()
	if _, ok := readGauge(t, firstReader, metrics.CachedQueries); ok {
		t.Fatal("first adapter retained its gauge")
	}
	if got, ok := readGauge(t, secondReader, metrics.CachedQueries); !ok || got != 9 {
		t.Fatal(got)
	}
}

type retirementFailure struct{ otelmetric.Registration }

func (retirementFailure) Unregister() error { return errors.New("retirement failed") }

func TestFailedUnregisterLeavesOldCallbackInert(t *testing.T) {
	p, reader := testProvider(t)
	g := metrics.NewGauge(p, metrics.CachedEntities)
	g.Set(3)
	f := p.(*provider).families[metrics.CachedEntities]
	f.registration = retirementFailure{f.registration}
	g.Close()
	if _, ok := readGauge(t, reader, metrics.CachedEntities); ok {
		t.Fatal("retired callback still emitted")
	}
	next := metrics.NewGauge(p, metrics.CachedEntities)
	defer next.Close()
	next.Set(5)
	if got, ok := readGauge(t, reader, metrics.CachedEntities); !ok || got != 5 {
		t.Fatal(got)
	}
}
