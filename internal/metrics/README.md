# Runtime metrics implementation

This package defines provider-neutral metric names, the `Provider` contract, and
atomic `Gauge` handles. The engine and PostgreSQL cache only update local scalar
counts. Gauge updates never call provider code, perform IO, or traverse cached
rows. Metadata counts update under the existing cache lock; engine counts are
sampled between coordinator turns.

The public contract is forwarded through [`pkg/metrics`](../../pkg/metrics/README.md).
OpenTelemetry aggregation and callback registration live separately in
`internal/metrics/otel`, forwarded through `pkg/metrics/otel`. The application
selects that adapter explicitly. Neither this package nor its core consumers
import OpenTelemetry.

Each component owns its handles and closes them on shutdown. A closed handle
reads as zero even if a provider calls its reader after unregistration. The OTel
adapter also makes retired callbacks inert before unregistering them.

Raw WAL positions are deliberately not exported: they are not a meaningful lag
measurement without a comparable source tip, and may exceed numeric precision in
monitoring systems.
