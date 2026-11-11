# Storage boundary

`storage.NewPostgres(ctx, databaseURL, storage.PostgresOptions{...})` constructs
the PostgreSQL storage implementation. It owns a read pool and logical-replication
capture, but does not construct a query server or projection server.
The connection string accepts pgx URI or keyword syntax, including pool options.
Reads, replication, and slot inspection derive from the same parsed settings;
pool-only options are not sent as PostgreSQL session parameters.

`PostgresOptions.Metrics` optionally accepts a
[runtime metrics provider](../metrics/README.md). Nil disables metadata-cache
counts. The storage implementation does not depend on OpenTelemetry.

Adapters expose each change's `BeforeKey` and `AfterKey` separately from `Apply`,
so routing can inspect identity metadata without decoding the row payload.
Delivered changes are immutable and safe for concurrent use by CDC processing
and snapshot preparation. `Apply` retains ownership of native patch semantics.

Pass the returned storage to `queryserver.New`. Declarative visibility policies
gather authorization facts through ordinary query/entity subscriptions managed
by the projection server, not through a separate storage-level fact stream.

## Before connecting

Configure PostgreSQL for logical replication, create tables with primary keys,
and create a publication containing every required table and mutation kind.
For an existing table, for example:

```sql
CREATE PUBLICATION app_publication FOR TABLE app.tasks;
```

Supply its name and a stable slot name:

```go
options := storage.DefaultPostgresOptions()
options.Publication = "app_publication"
options.Slot = "app_weave"
store, err := storage.NewPostgres(ctx, databaseURL, options)
```

The database role needs read and logical-replication permissions. Each running
capture instance needs a different slot; keep its slot name stable on restart.
Default replica identity is sufficient; `REPLICA IDENTITY FULL` is not required.
Query cursors are opaque but unsigned; applications that need tamper protection
at an untrusted client boundary must provide it above the query service.
See the [query service guide](../queryserver/README.md) for composition and
the [engine reference](../../examples/demo/ENGINE.md#postgresql-capture-and-recovery)
for capture behavior and recovery limits.

Adapters may implement `IdentityPreparer` on prepared queries to classify
complete primary-key equality selectors. Its `IdentityQuery` retains every
predicate, exposes the canonical `LookupKey`, and uses a fingerprint distinct
from the same windowed query. Preparation is pure, immutable, and IO-free.
Reference adapters expose `Resolve` for lightweight lookup binding and `Bind`
for actual child-query preparation; prefetch must not require the latter.

## Configuration and ownership

Configuration belongs to storage: publication/slot, allowed tables,
query-preparation limits, capture-side message limits, and logging.
Query execution/cache limits and message fan-out queues belong to the query server.

Successful table metadata and publication validation are discovered once and
retained for the storage instance's lifetime. New tables are discovered on first
request; failures are not cached. Existing tables' schema/publication changes are
picked up after explicit `InvalidateMetadata` or restart. Query validation remains
per-request against immutable cached metadata. Incompatible changes with active
subscriptions require a server restart, not just metadata invalidation.
See [instance management](../management/README.md).

The application must already have created the schema and publication. A running
storage needs an exclusive replication slot; shutdown retains that slot for
restart. Operators must remove permanently unused slots to avoid retaining WAL.
Startup waits for durable capture. Its context also owns the storage lifetime.
Close projections and query servers before closing their storage.

`pkg/storage` forwards shared values, query definitions, and supported adapter
contracts. An application assembling components needs only the constructor,
options, and handle; a new storage provider implements `Storage`, using prepared
queries, whole-record snapshot sessions, committed changes, and ordered progress.
Those supporting types are public because they are part of that implementable
boundary. Connection pools, decoding, native patch handling, and capture state
remain internal. `Close` and `Wait` manage only the source's resources; a source
does not take ownership of its consumers.

For optional mutation correlation, `github.com/adiom-data/weave/pkg/storage/postgres` exposes
`EmitMutationReceipt(ctx, tx, correlation)` over the caller's existing `pgx.Tx`.
See [the correlation helpers](../mutation/README.md); no storage instance or
reactive connection lookup is needed by a mutation server.

## Optional reference loading

Adapters may implement `ReferencePreparer` and return snapshot sessions supporting
`ReferenceWindowReader`. `PreparedReference` binds a known parent image to the same
ordinary selector that `PrepareQuery` would produce, without repeating metadata IO.
`PreparedReferenceFetch` is the prepared loading shape; `ReferenceFetch` is its
query-level, unprepared definition. Neither changes component query fingerprints.

Window reads return separate authoritative base/reference records under the same
snapshot fence. Pagination applies before expansion, and absent targets never
remove base rows. Warm-window reads accept known parent images and fetch only
their targets. The engine owns invalidation capture and fenced installation;
adapters must not write caches or create subscriptions. Without this optional
capability the normal single-table loading path remains available.
