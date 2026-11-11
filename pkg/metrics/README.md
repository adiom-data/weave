# Runtime metrics

Weave's query engine and PostgreSQL storage accept an optional `metrics.Provider`.
Nil disables runtime metrics. The provider does not appear in datastore or
subscription contracts and does not own any Weave resources.

```go
type Provider interface {
    RegisterGauge(name string, read func() int64) (unregister func())
}
```

A provider retains gauge readers and samples them on its own schedule. Weave's
readers only load scalar atomics; updating a count never calls provider code.
Registrations can arrive concurrently. Sum contributions with the same name.
The returned function removes one contribution and should be idempotent; nil is
allowed when nothing was registered. Provider failures are reported by the
provider and do not fail subscriptions. Close Weave components before shutting
down the provider.

For OpenTelemetry, create one adapter and share it across related components:

```go
runtimeMetrics := metricsotel.New(otel.GetMeterProvider())
stack, err := server.New(ctx, cfg, server.Options{Metrics: runtimeMetrics})
```

Here `metricsotel` is `github.com/adiom-data/weave/pkg/metrics/otel`, and `otel` is
`go.opentelemetry.io/otel`. The adapter borrows the supplied provider without
installing or shutting it down. Separate adapters can use independent providers.
The standalone and demo commands configure this adapter after framework telemetry
setup. Framework HTTP/RPC instrumentation is configured by the application.

When composing the lower-level libraries, pass the same adapter in
`queryserver.Options.Metrics` and `storage.PostgresOptions.Metrics`. Other telemetry
systems implement `metrics.Provider` directly; no OpenTelemetry dependency is
required by the core engine or PostgreSQL storage packages.

| Gauge | Meaning |
| --- | --- |
| `weave.query.subscriptions.active` | Active query/entity interests |
| `weave.query.subscriptions.initializing` | Interests initializing |
| `weave.query.cache.queries` | Retained shared query entries, including idle entries |
| `weave.query.cache.entities` | Retained entity-cache entries, including idle entries |
| `weave.postgres.metadata.cached` | Successfully discovered tables |
| `weave.postgres.metadata.loading` | Current shared table-discovery loads |

Counts are operational observations, not an atomic snapshot across components.
There are no per-query, table, entity, user, or subscription labels. Collection
scales with component instances rather than cached rows. A closed component's
contribution is removed; late updates cannot restore it.
