# Projection server library

Import `github.com/adiom-data/weave/pkg/projectionserver` and construct `New(ctx, queryClient, Options{...})`.
It returns a `Server` interface and an error. The concrete server, RPC handlers,
connection state, and message router are managed by the library.
Call `Handler(connectOptions...)` to obtain a mount path and HTTP handler for
Connect, gRPC, and gRPC-Web, including live projections and one-shot snapshots.
`Register(grpcServer)` remains available for grpc-go embedders. Both adapters
use the same orchestration implementation; the host owns listeners and their
lifecycle. Bidirectional RPCs require HTTP/2. The library has no environment
configuration, demo schema, JWT claim names, application mutations, or database
bootstrap side effects.

`queryClient` implements `pkg/querysubscription.Client`. A remote query backend
can be supplied using `pkg/querysubscription/grpc.NewClient(connection)`. The
in-process adapter and coordinator remain internal implementation details.
For an embedded backend, construct `storage.NewPostgres`, then pass the storage to
`queryserver.New`. Pass that query backend to `New` and its `Messages()` client through options.
It can alternatively mount query/message services for remote projection hosts.
See [query backend composition](../queryserver/README.md).

## One-shot snapshots

`GetProjectionSnapshot` accepts the same projection tree and parameters as a
live projection, but closes all temporary subscriptions before returning. Its
response is nested data rather than a stream's normalized model:

```json
{
  "data": {
    "projects": [
      {"id": "p1", "title": "Example", "tasks": []}
    ]
  },
  "extensions": {"synchronized": true, "checkpoint": "12345"}
}
```

The root and child `node_id` values are literal response keys (including any
dots); stored field paths retain their names and nested structure. This is a
GraphQL-like data envelope, **not** a GraphQL endpoint or the compiler's aliased
TypeScript result shape. No GraphQL documents, aliases, or partial-error rules
are interpreted here. Generated projection node IDs may contain their full
path, so choose short node IDs in manually constructed snapshot requests when
short response keys are desired.

Each occurrence includes only its own selected fields and expansions, even
when multiple paths reach the same entity. Missing/invisible entity nodes are
`null`; lists are arrays, including empty arrays. A selected field and child
node cannot have the same output key: explicit-selection collisions are rejected
before loading, while select-all collisions require inspecting a returned row.
Both return `InvalidArgument`. Unresolved pending entity payloads fail
the snapshot rather than silently returning incomplete records.

`ProjectionNode.fields` optionally narrows embedded objects and arrays using
stored field names. The projection compiler emits these masks automatically for
nested selections. Without a mask, selected columns retain their whole values;
arbitrary `StructuredValue` scalars also remain whole. Masks apply during output
selection, before serialization: live connections share the union required by
visible references, while snapshots apply each occurrence's mask separately.
Changes confined to unselected nested fields do not emit an entity update.
The query backend and authorization/expansion logic still use full records.
`MaxSelectedFields` bounds each node's recursive mask size and depth as well as
its selected columns. This is output shaping, not field-level authorization.
See the [embedded-output benchmarks](../../internal/projectionserver/BENCHMARKS.md)
for isolated CPU, allocation, and payload-size measurements.

`ProjectionNode.all_fields = true` explicitly selects the whole stored logical
record supplied by the datastore adapter.
It is mutually exclusive with both `query.columns` and `fields`. Empty columns
alone remain invalid; raw `Subscribe` requests still require explicit columns.
The compiler exposes this mode as `@allFields` on table entity/list selections.
It allows a parent selection containing only relationships, while each child
keeps its own output selection. The typed client reader still exposes only the
written definition; this flag does not widen generated types. Snapshots return
the whole record directly, with the same stored-name/child-node-key rules above.
Runtime key collisions with expansions are rejected rather than overwritten.

All-field output includes columns unknown to the read schema and full embedded
values. It uses the whole-record selection shortcut, but still pays for ownership
copies, encoding, and transmission. A whole-record reference widens live output
for shared entities until released; remaining references then narrow it again.
It grants no new row visibility and bypasses no query, binding, or policy checks.

Integers and decimals are strings to preserve precision; bytes are base64,
UUIDs and datetimes are strings, finite floats are numbers, and non-finite
floats are strings. Objects, arrays, booleans, and null remain ordinary JSON
values. Protobuf bindings expose `data` as `google.protobuf.Struct`; Connect's
JSON representation is the plain nested object shown above.

Failures use the RPC's normal error channel, with no partial `data` or `errors`
array. `extensions.synchronized` reflects `wait_for_sync`; a common checkpoint
is included only for synchronized single-source images. As usual with protobuf
JSON, default-valued metadata may be omitted. This does not add a cross-table
database snapshot guarantee.

## Reference fetch hints

`ProjectionList.fetch_reference` can name one selected immediate entity-child
`node_id`. Its parent-column equality bindings describe a one-level identity
lookup. The compiler exposes this as `@fetchReference(child: "alias")` on lists;
see the [fetch-hint example and restrictions](../../examples/demo/PROJECTIONS.md#many-to-many-fetch-hint).
This only optimizes loading. Entity/query subscriptions, authorization, normalized
client results, and the base query cache remain separate and unchanged.

## Visibility and trusted claims

Omitting both policy options uses allow-all. `Options.Policies` accepts either
fixed Go declarations (`Definitions`) or an application-owned `Provider` from
`pkg/accesscontrol/policy`, plus compilation/evaluation bounds and fact
query/row/load-concurrency limits. Rules are CEL strings; facts are private
parameterized projection plans with ordinary query leaves. Incoming and outgoing propagation
edges are explicit. All `Require` rules must pass before access is granted;
a failed requirement skips propagation and direct allow checks. Propagation
skips individual `Allow` rules, and fact discovery stays lazy within expressions.
The library manages fact subscriptions, sharing, pruning, and invalidation.
Fact selectors require complete coverage: `MaxFactRows + 1` must fit the query
backend's window limit. Exceeding it fails the affected consumer, not CDC.
Alternatively, `Options.AccessController` accepts a custom
`pkg/accesscontrol.Controller`; these two options are mutually exclusive.
`Options.ClaimsProvider` supplies trusted claims from each RPC context. No
particular claim or subject identifier is required. `accesscontrol.Claims` is a
map of canonical values; policies alone decide how to interpret it. Hosts can
use `accesscontrol.ContextWithClaims` and `accesscontrol.ClaimsFromContext`, or
provide their own resolver. Authentication, transport security, and long-lived token
refresh are the host's responsibility. The standalone host supports OIDC-verified,
explicitly unverified, and fixed claims. The projection library itself contains
no token verifier and does not require a `sub` claim.

### Loading and live policy changes

Declarative policies wait for required visibility checks before initial
snapshot/Ready, including one-shot snapshots. Consumed updates continue advancing
the private image during that wait; superseded transaction batches are not kept
as an initialization backlog. Initial projection authorization retains its private
image and applies coalesced row/topology deltas, rather than copying and evaluating
the entire projection on every update. Direct queries also retain their private
visibility gate and apply coalesced changes. Pending checks retry when their facts
change or an installed fact snapshot becomes covered, not on every progress
report. Updates after the accepted image cut retain their
normal ordered handoff to live delivery. An unresolved live check freezes
the affected projection/query's published state, withholding new data while
private materialization continues. On resolution it publishes a current
hydration diff (or removal), not historical transactions evaluated with newer
facts. Other scopes and connection-owned mutation confirmations keep flowing.
Already authorized references elsewhere on the connection can still update a
shared entity. This is not permission to use an unrelated projection as a
visibility grant.

Policy providers return immutable `Snapshot{Version, Policy}` values keyed by
relation. A publication pass fetches each required key once, pinning its version.
The shared compiled-policy cache reuses unchanged versions; provider calls still
occur on each new pass. `PolicyOptions.Cache` configures load concurrency and
bounded indexed idle eviction. Optional `Invalidations` broadcasts changed keys
to reevaluate dependent idle occurrences. Without it, changes are adopted on
the next evaluation. The application need not track which keys were requested.
Versions identify content uniquely per key, including a nil (deny-by-default)
policy. See [policy semantics and fact shapes](../../internal/accesscontrol/policy/README.md).

## Configuration

`Options.ProjectionLimits` exposes configuration only. Expansion trees, workers,
publication scheduling, and synchronization remain under `internal/projection`.
No caller must manipulate them to use this service.

Use `projectionserver.DefaultOptions()` as a customizable base:

```go
options := projectionserver.DefaultOptions()
options.Dependencies.IdleTTL = time.Minute
options.Dependencies.EvictionBatchSize = 64
options.Dependencies.CleanupBatchSize = 128
options.ProjectionLimits.MaxConcurrentExpansions = 16
server, err := projectionserver.New(ctx, queries, options)
```

Defaults are returned by value, not mutable globals. Policy definitions, claims
providers, and optional message clients still come from the application. The
WebSocket edge similarly exposes `websocket.DefaultSocketConfig()`, query gRPC
offers `querygrpc.DefaultTransportOptions()`, and the standalone metadata service
offers `transactionmessage.DefaultOptions()`. Runtime tuning belongs to these
typed configs; protocol versions and fixed wire/accounting invariants do not.

## Sharing and lifecycle

The projection server owns a separate dependency cache above the query boundary.
Entity selectors share subscriptions and in-flight loads across projections and
connections; immutable fact-query windows can use that same internal machinery.
Mutable projection list windows keep independent ownership. Materialized entity
payloads are shared internally, with caller-owned copies at the Go feed boundary.
`Options.Dependencies` configures its upstream buffer, subscription limit, idle
retention, sweep interval, eviction batch size, and row-cleanup batch size.
Idle expiry uses an indexed heap rather than scanning active entries. Retirement
detaches row ownership without scanning it; the maintenance worker releases
entity references in bounded batches, with map traversal and upstream handle
closure outside the cache lock. Unreferenced shared subscriptions remain live
during the idle grace period; slow logical feeds fail independently. An upstream
disconnect invalidates its dependent feeds and allows a fresh generation on
reconnect. This cache provides current results, not historical/MVCC reads.

For correlated transaction messages, pass a `pkg/transactionmessage.Client` as
`Options.TransactionMessages`; `Options.MessageOptions` bounds its routing queue
and recovery delay. The server confirms attachment during construction and owns
the shared router, its background reader, and its shutdown. `Server.Wait(ctx)`
reports terminal background failure; `Server.Close()` cancels server work and
closes the owned router. Close the projection server before disposing of its
query/message backends. Query and message feeds must describe the same source.

Close in reverse dependency order: projection service, query backend, then
storage. The host owns listeners, admission/draining, transport security, and
gRPC keepalive. See [transport recovery](../queryserver/README.md#transport-liveness-and-recovery)
for disconnection behavior; reconnect creates new subscriptions, not a durable replay.

## WebSocket transport

`projectionserver/websocket.NewHandler(serve, origin, logger, config)`
provides the binary `/api/reactive` transport. The host-supplied callback
authenticates the supplied HTTP headers, attaches trusted claims to the stream
context, and calls `Server.ServeStream`. Both WebSocket and RPC adapters use this
same projection implementation. There is no internal RPC hop. Host context values
remain available locally; this transport does not interpret credentials itself.

The bridge answers client application pings locally. `SocketConfig.IdleTimeout`
defaults to one minute and is renewed by valid client pings or model messages;
outgoing data does not renew it. Expiry closes the socket and cancels its upstream
service even if request processing is blocked. Choose a timeout comfortably above
the client's heartbeat interval (15 seconds by default). Browser suspension may
expire the lease; reconnect rebuilds subscriptions while retaining the old view.
`WriteTimeout` defaults to ten seconds. Neither timeout measures database freshness.
It registers no database health check or mutation routes. The HTTP host must
cancel WebSocket request contexts on shutdown (HTTP Shutdown alone does not close
hijacked connections). The demo's composition shows this explicitly.

## Policy and transport limits

Operational defaults also apply to partially specified configurations. For
declarative visibility, `Options{Policies: &PolicyOptions{Definitions: definitions}}`
uses default policy limits, cache settings, and fact-read budgets. Use
`DefaultPolicyOptions()` to inspect or override those budgets. A fact read uses
one overflow-detection row: `MaxFactRows + 1` must fit the query backend's maximum
initial window count (999 + 1 with the defaults). Negative policy limits are
invalid, not a way to disable protection. Policies are never enabled by defaults.

The WebSocket bridge accepts `SocketConfig{}` (or individual positive overrides)
and a nil logger, which uses `slog.Default()`. Nonpositive socket limits select
defaults. The permitted browser origin and authentication middleware remain
explicit application choices; never expose an unprotected bridge accidentally.

See [the demo entrypoint](../../examples/demo/cmd/server/main.go) for a complete
composition and `example_test.go` for an embedding example using public imports
only. Running `go test ./pkg/projectionserver/...` exercises the library separately
from the demo.
