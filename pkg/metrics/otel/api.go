// Package otel exports Weave runtime metrics through an application-owned OpenTelemetry provider.
package otel

import (
	impl "github.com/adiom-data/weave/internal/metrics/otel"
	"github.com/adiom-data/weave/pkg/metrics"
	"go.opentelemetry.io/otel/metric"
)

// New creates a shared adapter without installing or taking ownership of the
// provider. Close its dependent Weave components before shutting the provider
// down. Pass the same adapter to components whose counts should be aggregated.
// A nil provider disables metrics.
func New(provider metric.MeterProvider) metrics.Provider { return impl.New(provider) }
