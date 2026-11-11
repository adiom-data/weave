package metrics

import (
	"context"
	"sync"
	"testing"

	otelmetric "go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func testRegistry(t *testing.T) (*registry, func(string) (int64, bool)) {
	t.Helper()
	reader := metric.NewManualReader()
	provider := metric.NewMeterProvider(metric.WithReader(reader))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	r := &registry{meterProvider: func() otelmetric.Meter { return provider.Meter("weave-test") }, families: make(map[string]*family)}
	return r, func(name string) (int64, bool) {
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
}

func TestGaugesAggregateAndReleaseComponents(t *testing.T) {
	r, read := testRegistry(t)
	a, b := r.newGauge(CachedEntities), r.newGauge(CachedEntities)
	a.Set(3)
	b.Set(7)
	if got, ok := read(CachedEntities); !ok || got != 10 {
		t.Fatal(got)
	}
	a.Set(2)
	a.Close()
	a.Set(99) // A late worker cannot resurrect a closed component.
	a.Close()
	if got, ok := read(CachedEntities); !ok || got != 7 {
		t.Fatal(got)
	}
	b.Close()
	if _, ok := read(CachedEntities); ok {
		t.Fatal("closed metric still reported")
	}
	if len(r.families) != 0 || r.meter != nil {
		t.Fatal("closed metric registration retained")
	}
}

func TestConcurrentCollectionAndClose(t *testing.T) {
	r, read := testRegistry(t)
	g := r.newGauge(ActiveSubscriptions)
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			for i := range 1000 {
				g.Set(i)
			}
			g.Close()
		})
	}
	for range 100 {
		read(ActiveSubscriptions)
	}
	wg.Wait()
	if _, ok := read(ActiveSubscriptions); ok {
		t.Fatal("closed metric still reported")
	}
	var noop *Gauge
	noop.Set(1)
	noop.Close()
}

func TestLastHandleReleasesProviderRegistration(t *testing.T) {
	firstReader := metric.NewManualReader()
	first := metric.NewMeterProvider(metric.WithReader(firstReader))
	defer first.Shutdown(context.Background())
	secondReader := metric.NewManualReader()
	second := metric.NewMeterProvider(metric.WithReader(secondReader))
	defer second.Shutdown(context.Background())
	current := first
	r := &registry{
		meterProvider: func() otelmetric.Meter { return current.Meter("weave-test") },
		families:      make(map[string]*family),
	}
	one := r.newGauge(CachedQueries)
	one.Set(4)
	one.Close()
	current = second
	two := r.newGauge(CachedQueries)
	two.Set(9)
	defer two.Close()
	var result metricdata.ResourceMetrics
	if err := secondReader.Collect(context.Background(), &result); err != nil {
		t.Fatal(err)
	}
	var found bool
	for _, scope := range result.ScopeMetrics {
		for _, measurement := range scope.Metrics {
			if measurement.Name != CachedQueries {
				continue
			}
			gauge, ok := measurement.Data.(metricdata.Gauge[int64])
			if !ok || len(gauge.DataPoints) != 1 || gauge.DataPoints[0].Value != 9 {
				t.Fatalf("wrong new-provider metric: %#v", measurement)
			}
			found = true
		}
	}
	if !found {
		t.Fatal("new provider did not receive the gauge")
	}
	result = metricdata.ResourceMetrics{}
	if err := firstReader.Collect(context.Background(), &result); err != nil {
		t.Fatal(err)
	}
	for _, scope := range result.ScopeMetrics {
		for _, measurement := range scope.Metrics {
			if measurement.Name == CachedQueries {
				t.Fatal("old provider retained the gauge")
			}
		}
	}
}
