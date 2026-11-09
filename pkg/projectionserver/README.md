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
TypeScript result shape. No GraphQL documents or aliases
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

By default, failures use the RPC's normal error channel. With
`allow_partial_errors = true`, recoverable branch failures instead return `null`
and an `errors` entry containing a field/index `path`, `code`, and sanitized
`message`. Failed list rows follow the successful ordered rows as null slots;
an entirely failed expansion is null, not an empty list. Errors contain no row
or private-fact data. `extensions.synchronized` reflects `wait_for_sync`; a common checkpoint
is included only for synchronized single-source images. As usual with protobuf
JSON, default-valued metadata may be omitted. This does not add a cross-table
database snapshot guarantee.

## Partial live failures and recovery

`SubscribeProjection.allow_partial_errors` opts into branch-local evaluation
and subscription failures. The default remains whole-projection failure.
`ModelBatch.branches` identifies an error by projection subscription, occurrence,
and optional entity ID. An empty entity ID covers the whole expansion; otherwise
it covers that row and its descendants. A failed branch relinquishes live entity
and query references, while healthy paths (including other paths to the same
entity) continue. An error is not an authorization denial.

Data, error transitions, and recovery are published in one model batch. Recovery
clears the error and replaces the branch from a complete current image, or
definitively removes it. Evaluation is retried when its row, facts, or relation
definitions change. Failed source subscriptions preserve a list's saved cursor
boundaries and retry with configurable exponential backoff: `RecoveryInitialDelay`
(default 1s), `RecoveryMaxDelay` (30s), and `RecoveryMaxAttempts` (five retries
after the original attempt). Each delay is jittered between half and all of the
exponential delay. These settings live in `Options.ProjectionLimits` and in
standalone YAML under `projection.limits.recovery_initial_delay`,
`recovery_max_delay`, and `recovery_max_attempts`.

After exhaustion the branch stays errored without timer polling. Reopening the
projection starts a fresh budget; a parent rebind to a different selector or a
recreated branch also starts fresh. Fact selectors get fresh budgets when their
last lease is released and a later evaluation requests them anew. A policy
change can cause that lifecycle change but does not reset an unchanged failed
selector. Unrelated CDC activity or reevaluating the same error does not reset
budgets. A successful installation resets the budget for a later failure.
There is no client-side branch retry operation.
Failed placeholders and their in-flight replacements share one logical instance
slot, so recovery works at `MaxActiveInstances` without relaxing the limit for
unrelated expansions. The old error remains addressable until publication.
Asynchronously loaded recovery is hydration, not a fabricated historical commit.

Clients may retain an immutable last-good display separately from the live
entity cache. No fresh data is published through an errored visibility path.
Actual visibility revocations and deletions still remove live branches normally.
Transport, source-ordering, backpressure, and invariant failures remain terminal;
partial errors do not provide unbounded buffering or mask a broken connection.

## Reference fetch hints

`ProjectionList.fetch_reference` can name one selected immediate entity-child
`node_id`. Its parent-column equality bindings describe a one-level identity
lookup. The compiler exposes this as `@fetchReference(child: "alias")` on lists;
see the [fetch-hint example and restrictions](../../examples/demo/PROJECTIONS.md#many-to-many-fetch-hint).
This only optimizes loading. Entity/query subscriptions, authorization, normalized
client results, and the base query cache remain separate and unchanged.

## Computed values, visibility, and trusted claims

Omitting relation/access-control options uses allow-all. `Options.Relations` accepts either
fixed Go declarations (`Definitions`) or an application-owned `Store` from
`pkg/relation`, plus compilation/evaluation bounds and fact
query/row/load-concurrency limits. Expressions are CEL strings; facts are private
parameterized projection plans with ordinary query leaves. Incoming and outgoing propagation
exports are explicit. Visibility is a boolean expression using the same lazy
computed values and facts as selected outputs; there is no separate visibility
propagation or require/allow rule model. Expression short-circuiting skips
unneeded facts and exports. Place cheap local checks before expensive discovery.
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

Relations declare shared `Facts`, named `Computed` definitions, and optional
`Visibility`. A computation has a canonical `Type` and CEL `Expression` over
`item`, `claims`, `facts.name`, and local `computed.name` dependencies. For example:

```go
Relations: &projectionserver.RelationOptions{Definitions: relation.Definitions{
    {Namespace: "app", Name: "tasks"}: {
        Computed: map[string]relation.Computed{
            "_role": {Type: storage.TypeInteger, Expression: `claims.user_id != null && item.owner_id == claims.user_id ? 2 : 0`},
            "canEdit": {Type: storage.TypeBool, Expression: `computed._role >= 2`},
        },
        Visibility: `item.deleted_at == null && computed._role >= 1`,
    },
}},
```

Empty `Visibility` permits access to a configured relation; `"false"` denies.
Missing definitions deny unless
`RelationOptions.AllowAll` bypasses visibility. This does not disable computations.
Values are evaluated only when selected or needed by visibility, another
computation, or a descendant. Types are checked without implicit coercion.
Names beginning with `_` are private backend helpers: they can be referenced
internally and propagated, but both raw projection requests and generated schemas
reject exposing them. Public computations can intentionally derive from them.
This convention does not hide stored database columns with underscore names.

Clients select names using `ProjectionNode.computed`. GraphQL schemas expose
them using `@computed` or `@computed(name:"definitionName")`; normal selection,
aliasing and generated TypeScript decoding apply. Values are separate
occurrence-and-entity keyed events, not shared entity fields. The same entity
can therefore have different results along different paths. One-shot snapshots
combine computed values into row objects; names cannot collide with selected
stored fields or child nodes. Computations are not filters, sort keys, or
relationship bindings. Selecting a structured computation requests its whole
canonical value; typed readers can select a subset of its structure.

For propagation, a parent's `Computed[name].Propagate` declares a target relation
and a parent/item guard, with at most one entry per destination. The field is
exported under its own name; there are no edge IDs or destination renames.
The child can restrict `Relation.Receive` using `Incoming{From, Field}` pairs:
nil accepts all configured exports, an empty non-nil list accepts none. A child
can use `has(inherited._role) ? inherited._role : computed._directRole` without
declaring a corresponding local `_role` field. Any computation or visibility
expression can read the export. Define a local field only to transform, provide
a fallback, or re-export it to the next hop.

Presence tests evaluate export eligibility, not the source value or its facts.
An unknown guard remains unknown; an eligible export is present even when its
value is false, null, or pending. Actual reads retain transitive dependencies and
remain unresolved while the value loads; absent exports read as null. Only the
concrete immediate path edge within the same data source is considered. An
unrelated projection can never supply an inherited value. The parent need not
select the value publicly.

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

For live updates, construct `relation.NewStore(definitions, limits)` and pass it
as `RelationOptions.Store` instead of `Definitions`. Call `store.Replace(next)`
with the complete replacement map. Validation and compilation finish before an
atomic publication; invalid replacements leave the current definitions intact.
Unchanged compiled relations are reused. No version strings or separate
invalidation notifications are required.

Each evaluation pins the entire map once, so parents and children cannot mix
definition generations. Changed, added, and removed relations automatically
wake only their dependents, including idle projections. Existing evaluations
finish using their old map; publication does not wait for all connections to
adopt the replacement. Supplied stores own their compilation/evaluation limits;
`RelationOptions.Limits` applies when constructing a private store from fixed
`Definitions`. There is no provider load queue or compiled-policy idle cache.

Applications may host multiple projection servers with independent stores over
one shared query backend. Routing connections to the appropriate definition
domain remains outside this library. See [policy semantics and fact shapes](../../internal/relation/README.md).

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
declarative evaluation, `Options{Relations: &RelationOptions{Definitions: definitions}}`
uses default policy limits, cache settings, and fact-read budgets. Use
`DefaultRelationOptions()` to inspect or override those budgets. A fact read uses
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
