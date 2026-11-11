# Runtime metrics

OpenTelemetry dependencies and instrument definitions live here. The engine and
PostgreSQL cache publish nonnegative counts through `Gauge.Set` and release their
contributions with `Close`. No OTel types enter datastore or subscription contracts.

| Gauge | Meaning |
| --- | --- |
| `weave.query.subscriptions.active` | Active query/entity interests |
| `weave.query.subscriptions.initializing` | Interests initializing |
| `weave.query.cache.queries` | Retained shared query entries, including idle entries |
| `weave.query.cache.entities` | Retained entity-cache entries, including idle entries |
| `weave.postgres.metadata.cached` | Successfully discovered tables |
| `weave.postgres.metadata.loading` | Current shared table-discovery loads |

Values sum contributions from live components in the process. There are no
per-query, table, entity, user, or subscription labels. Counts are operational
observations, not an atomic snapshot of all components. Engine counts are sampled
between coordinator turns; metadata counts update under the cache's existing lock.
Collection reads scalar atomics only: no database IO, actor commands, or scans of
entities/memberships. Its work scales with backend/storage instances, not rows.
Closed components are removed; late workers cannot restore their contributions.

In library usage, the application installs its OpenTelemetry MeterProvider before
constructing Weave components and shuts it down after closing them. Weave does
not install, replace, or shut down a provider. The `cmd/server` executable owns
framework telemetry setup and shutdown and follows its `OTEL_*` configuration.
This package creates no SDK, exporter, collection timers, or listener. It
unregisters a metric callback when its last component handle closes, so a later
component can use a newly installed provider.

Raw WAL positions are deliberately not exported: they are not a meaningful lag
measurement without a comparable source tip, and may exceed numeric precision in
monitoring systems. Framework HTTP/RPC metrics remain independently available.
