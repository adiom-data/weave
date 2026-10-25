# Engine reference

This is the technical reference for Weave's query, projection, and delivery
model. Start with the [demo guide](README.md) to run an application, or the
[query](../../pkg/queryserver/README.md) and
[projection](../../pkg/projectionserver/README.md) guides to embed the libraries.

- [Queries and windows](#queries-and-windows)
- [Snapshots and live consistency](#snapshots-and-live-consistency)
- [Client model](#client-model)
- [PostgreSQL capture and recovery](#postgresql-capture-and-recovery)
- [Values and schema changes](#values-and-schema-changes)
- [Transaction messages and mutation receipts](#transaction-messages-and-mutation-receipts)
- [Configuration and liveness](#configuration-and-liveness)
- [Internal boundaries](#internal-boundaries)
- [Known limitation: concurrent relation refresh](#known-limitation-concurrent-relation-refresh)

## Queries and windows

Each query targets one relation. It has an immutable filter and ordering, plus
a mutable cursor-bounded window. There are no SQL joins, aggregates, arbitrary
SQL fragments, or mutations in this layer. Projections compose these queries
through nested entity/list subscriptions (N+1 by design).

Supported predicates are `=`, `<>`, `<`, `<=`, `>`, `>=`, `IS NULL`,
`IS NOT NULL`, `NOT`, `AND`, and `OR`. Preparation pushes `NOT` to leaves
before fingerprinting and indexing. Literal equality is type-strict:
text `"123"` and integer `123` differ. The adapter rejects queries it cannot
represent; supported mismatched-type comparisons do not match.

Field paths are arrays of exact segments, so a key containing a dot is not
mistaken for a nested path. Nested object selection/filtering is supported;
array-position queries and nested ordering are not.

Windows use `(left, right]` bounds with `START` and `END` sentinels. Clients can
move, extend, or contract either boundary, including both in one request.
Counts resolve into boundaries: subsequent inserts and deletes can change the
number of rows inside them. Cursors are opaque, unsigned, and tied to the query.
Tamper protection, when required, belongs at the untrusted client boundary.

PostgreSQL supplies ascending primary-key ordering when no order is specified.
For explicit ordering, include a unique tie-breaker when complete, deterministic
membership matters. Ties form an ambiguous cursor bucket: the initial page may
contain only some tied rows, and CDC may add more later. Completeness within
that bucket is intentionally undefined; adjacent keys remain unaffected.

Initialization and count-based boundary changes discover bounds first, then
materialize those exact bounds. Ready shared coverage can avoid redundant reads.
Bounds-only results never become entity-cache payloads or temporary whole-table
coverage. Overlapping windows share query membership rather than copying a full
key set into each window.

## Snapshots and live consistency

For a table snapshot, the engine registers change consumption, reads a
repeatable-read image, captures its source fence, and reconciles committed
changes through that fence before making the image current. Snapshot I/O runs
outside the CDC coordinator. A newer CDC tuple wins over an older snapshot.

Window updates use the same private staging and catch-up pattern. A failed
multi-boundary update does not partially replace the installed window.

Projection nodes initialize independently. The first image is therefore not a
single cross-table PostgreSQL snapshot. Both live `SubscribeProjection` and
unary `GetProjectionSnapshot` accept `wait_for_sync`: they apply ordered
deliveries through a common source cut and repeat if expansion discovers a newer
snapshot baseline. This does not wait for every pending entity payload to hydrate.

The snapshot RPC uses the same projection definitions and visibility policy as
live subscriptions, then closes its temporary subscriptions. It returns nested
selected `data` plus synchronization `extensions`, not normalized model events.
Missing entities are null and lists are arrays; loading/evaluation errors or
unresolved entity payloads fail the whole request. See the
[one-shot response contract](../../pkg/projectionserver/README.md#one-shot-snapshots).
See the [protobuf service](../../proto/reactive/v1/reactive.proto) for both APIs.

Once live, reconstructable effects from a source commit are assembled across a
connection's subscriptions before publication. One committed `ModelBatch`
contains the available entity, membership, and addressed-message effects.
Membership changes and entity changes are applied atomically by the client.
Child expansions or deferred hydration can follow in separate non-atomic batches;
they are not reconstructed historical versions of the original transaction.

Visibility runs above the query boundary against an ordered authorization view.
Denied projection paths are pruned; invalidation can restore their retained raw
subtrees. Diamonds remain occurrence-specific. Unresolved live authorization
withholds the affected scope's publication while private processing continues,
then reconciles to a current authorized image. It does not replay old commits
using newer authorization facts. Other scopes and transaction messages continue.

## Client model

A connection maintains a normalized model:

| Event | Meaning |
| --- | --- |
| `EntityUpsert` | Authoritative replacement of the selected field union; absent fields are removed |
| `EntityPending` | A reference exists but its complete payload is not yet available |
| `EntityReleased` | The connection no longer retains that entity |
| `QueryReset` | Initial authoritative membership |
| `QueryDelta` | Versioned removals and final-index insertions; moves are remove-plus-insert |
| `QueryWindowChanged` | Resolved boundary acknowledgment, even if membership is unchanged |
| `QueryClosed` | The query subscription is no longer retained |
| `ProjectionReference` | An occurrence's entity/query reference and its place in the projection |

Entity IDs encode relation identity and typed primary keys. Clients treat them
as opaque, not interchangeable with application IDs. Multiple projection paths
can reference one entity; a connection batch emits its final entity change once.
Query subscription IDs remain stable when their window changes.

Each committed model batch carries a source checkpoint and monotonic connection
sequence. Invalid or incomplete batches are not partially applied. Slow clients
overflow their own bounded queues and resynchronize; they do not block CDC.

Request IDs on `ReactiveQueries.Open` are scoped to that connection. Repeating an
identical ID and payload is a no-op; reusing the ID for different content is an
error. This does not provide mutation idempotency or cross-connection replay.

## PostgreSQL capture and recovery

Use `wal_level=logical`, a publication containing the required tables and mutation
kinds, and a stable replication slot exclusive to this storage instance. The
demo creates `reactive_queries`; other applications supply their own publication.
Preparation rejects absent, row/column-filtered, incomplete, or otherwise
unsupported publication configurations.

Tables require primary keys. Default replica identity is sufficient;
`REPLICA IDENTITY FULL` is not required. Snapshot and CDC identity encodings
use the same canonical primary-key column order. Key changes become removal of
the old identity and addition of the new one.

Only committed transactions are emitted. Unchanged TOAST cells reuse current
cached values. If an uncached row needs missing payloads, structural decisions
that can already be evaluated may publish its membership and pending reference.
A coalesced primary-key read hydrates it asynchronously. The read waits for its
fence and preserves newer concrete CDC values; newer complete tuples and deletes
supersede it. Filtering or ordering that needs missing data waits for resolution.

`TRUNCATE` is a first-class ordered operation: it clears affected memberships and
cancels obsolete hydrations without querying the now-empty table.

The adapter owns WAL resume state through PostgreSQL's persistent slot. The
engine acknowledges only applied and published source batches, not client receipt
of those batches. A crash can replay unacknowledged work. Connection reconnects
within one process suppress already-delivered duplicate commits.

Client connections have no durable replay. Query-feed loss propagates failure;
browser reconnects take fresh snapshots and reconcile their retained views.
Keep the slot and required WAL available for capture recovery, monitor slot lag,
and remove permanently abandoned slots to prevent unbounded WAL retention.

## Values and schema changes

The canonical values are booleans, signed-64-bit integers, arbitrary-precision
decimals, binary64 floats, text, bytes, UUIDs, UTC datetimes, recursive lists and
string-keyed objects, plus null/missing states. JSON/JSONB becomes recursive
values rather than JSON text.

The PostgreSQL type adapter handles native encodings, query literal conversion,
keys, and cursor operands. The engine performs routing on canonical values;
it does not ask PostgreSQL to probe its in-memory routing indexes.

Dates become midnight UTC; timezone-less timestamps are interpreted as UTC.
Finite internal datetimes use Go `time.Time`; adapter infinity sentinels and
wire-range clamping stay at their respective boundaries. The protobuf transport
emits years 1–9999. Values outside a supported accurate domain may degrade to a
nearby representation so the record remains loadable. Queries, ordering, cursors,
and diffing involving lossy values are intentionally undefined. Unsupported
ordinary columns are omitted; explicitly requested unsupported fields are rejected.

Relation metadata changes update decoding without proactively refreshing every
query. Prepared fingerprints include relation shape, so newly prepared queries
establish their own authoritative coverage. Existing rows can retain an older
shape until touched. Adding an optional column is intended to remain
non-disruptive; incompatible changes can fail queries that can no longer operate.
This is tolerant schema handling, not a promise of live schema migration.

## Transaction messages and mutation receipts

The separate [transaction-message stream](../../proto/transactionmessage/v1/transactionmessage.proto)
carries committed opaque metadata and ordered progress, not row changes.
It shares capture with the change feed but never acknowledges WAL independently.

A projection server attaches to one message source and routes addressed envelopes
to its open connections. Every connection receives a random `connection_context`
in `ConnectionOpened`. It is a routing capability, not authentication; keep it
private and authorize mutations separately.

The standard mutation helpers emit a transactional `weave.client` logical message
in the application's existing transaction. The envelope contains the connection
context, kind `weave.mutation.committed`, and the mutation UUID's raw UTF-8 bytes.
Rollback discards it. See [mutation helpers](../../pkg/mutation/README.md) for the
format and transport-independent header adapter.

Arbitrary transactional logical messages are also supported. Nontransactional
application messages are ignored; the adapter's nontransactional
`weave_barrier` messages carry progress only.

Query and message feeds must independently cover a transaction before it is
joined. Message-only commits can reach a connection with no subscriptions.
Demand-driven `RequestProgress` reports already-observed query progress; it does
not write a new WAL barrier. This is a single-query-source join, not implemented
multi-server federation.

Receipts and available query effects publish together in a committed model batch.
They confirm a commit, not visibility, list inclusion, or completed hydration.
The TypeScript harness retires matching optimism with that publication. Receipt
observers run afterward. Disconnects retire old connection contexts; no durable
receipt replay or offline mutation queue is provided.

Message queues, batches, and retained payloads are bounded. Over-limit metadata
becomes an empty placeholder in its original transaction; count/aggregate overflow
collapses omissions into a bounded tail marker. Placeholders are never receipts.
Slow consumers fail independently. A capture gap retires old feeds rather than
silently resuming them across missing history.

## Configuration and liveness

Libraries accept typed options; they do not read environment variables.
The demo's supported environment settings are in [`.env.example`](.env.example).
Use the public defaults as a base:

| Component | Defaults and configuration |
| --- | --- |
| PostgreSQL | `storage.DefaultPostgresOptions()`: query/message limits, capture buffer, replication retry/status, omission logging |
| Query service | `queryserver.DefaultOptions()`: windows, cache retention, maintenance, message service and transport |
| Projection service | `projectionserver.DefaultOptions()`: expansion, connection queues, shared dependencies, message routing |
| Visibility policy | `projectionserver.DefaultPolicyOptions()`: rule limits, policy cache, fact queries/rows/load concurrency |
| WebSocket bridge | `websocket.DefaultSocketConfig()`: client idle timeout, write timeout, frame size/depth |
| TypeScript | `ReactiveClient` options: reconnect, request, connection/heartbeat timeouts and decoding bounds |

For example, projection definition defaults are 8 levels, 64 nodes, and 1000
active instances; these are configurable server limits, not compiler ceilings.
Inspect the default constructors rather than relying on retired tuning env vars.

The browser sends pings and reconnects if matching pongs stop. The bridge closes
clients after configurable inbound silence, even while forwarding is blocked.
Pongs do not prove database freshness. Configure gRPC keepalive on host-owned
transports and PostgreSQL/TCP failure detection separately. See
[transport liveness](../../pkg/queryserver/README.md#transport-liveness-and-recovery).

Hosts own draining and shutdown: stop admission, allow a grace period if desired,
then cancel remaining streams and close components in dependency order
(projections, queries, storage). HTTP `Shutdown` alone does not close upgraded
WebSockets; cancel their request contexts too.

## Internal boundaries

Public APIs are under `pkg/`; implementations are under `internal/`.

- `storage` / `internal/postgres`: canonical storage contracts, preparation,
  snapshots, CDC adaptation, and native type conversion.
- `internal/engine`: coordinator and fence state machine; `entitycache` owns
  current records/loads, `queryregistry` owns shared coverage/membership,
  `routing` finds candidate queries, `changeprocessor` derives transitions,
  `delivery` fans out batches, and `querywindow` owns interval semantics.
- `querysubscription`: entity/query feed contracts, snapshot installation,
  ordered transaction/progress delivery, cancellation, and teardown. Local and
  gRPC adapters implement the same boundary.
- `internal/projection`: nested expansion and causal publication scheduling.
- `accesscontrol` and projection authorization: ordered policy views,
  fact subscriptions, invalidations, and visibility decisions above query caching.
- `projectionserver`: connection model, visibility-filtered publication, and
  protocol conversion; `projectionserver/websocket` is the optional HTTP edge.

The demo uses queries/messages in-process and exposes only projections over gRPC.
Its schema, claims, policies, mutations, and UI remain application code.

## Known limitation: concurrent relation refresh

Explicit relation-wide adapter invalidation can overlap an older snapshot whose
database read began before it registered entity-cache loads. The refresh may
coalesce with a stale load. Epoch checks prevent stale publication, but affected
subscriptions fail closed and need fresh snapshots. Other relations and CDC
remain healthy.

This does not affect ordinary CDC, asynchronous TOAST hydration, or `TRUNCATE`.
A future fix needs relation-generation tokens on cache loads so older snapshot
lifecycles cannot register or share loads in the new generation.


