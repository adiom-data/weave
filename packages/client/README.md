# @adiom-data/weave

A read-only client runtime for Adiom Weave's normalized projection stream. It does
not evaluate datastore predicates, infer list membership, or perform mutations.

The package is a local npm workspace, imported by the React demo. From the
repository root, run `npm install`, `npm test`, and `npm run build`.
Run `npm run generate` (or `make generate`) after editing `.proto` files; Buf
generates both Go bindings and TypeScript into `src/gen`. Generated files are
checked in, so normal client builds do not require a local Go/Buf toolchain.

## Start with typed projections

For application views, prefer generated bindings over handwritten protocol
definitions. The [projection guide](../../examples/demo/PROJECTIONS.md) shows how
a schema and read document produce typed handles and React hooks.

Here is the demo's connection/subscription pattern. The generated hook is
application-specific; `ReactiveClient` and `useConnection` are reusable.

```tsx
import { useMemo } from 'react'
import { ReactiveClient } from '@adiom-data/weave'
import { useConnection } from '@adiom-data/weave/react'
import { useProjectDetails } from '@adiom/weave-demo-projections/react'

function Project({ projectId }: { projectId: string }) {
  const client = useMemo(() => new ReactiveClient({
    url: () => {
      const protocol = location.protocol === 'https:' ? 'wss:' : 'ws:'
      return protocol + '//' + location.host + '/api/reactive'
    },
  }), [])
  useConnection(client)
  const projection = useProjectDetails(client, { projectId })

  if (projection.error) return <p role="alert">{projection.error}</p>
  if (projection.data?.status !== 'available') return <p>Loading…</p>
  return <h1>{projection.data.value.title}</h1>
}
```

This component owns its client. With several views, create one shared client
and mount `useConnection` once above them; each view owns only its projection.

## Guide

- [Framework-independent use](#framework-independent-use)
- [Mutation harness](#optional-mutation-harness)
- [Optimistic entity updates](#optimistic-entity-updates)
- [Temporary entities and list membership](#temporary-entities-and-explicit-membership)
- [Reconnects](#seamless-reconnects) and [connection liveness](#connection-liveness)
- [React hooks and selections](#react)
- [Configuration](#configuration)
- [Tests](#tests)
- [Implementation and wire format](#implementation-and-wire-format)

## Framework-independent use

```ts
import { ReactiveClient, type ProjectionRequest } from '@adiom-data/weave'

const definition: ProjectionRequest = {
  root: {
    nodeId: 'project',
    entity: {
      query: {
        schema: 'tenant_001', table: 'projects',
        columns: [{ segments: ['id'] }, { segments: ['title'] }],
      },
      bindings: [{ column: { segments: ['id'] }, operator: 'EQUAL', parameter: 'id' }],
    },
  },
  parameters: { id: { uuidValue: projectId } },
}

const client = new ReactiveClient({ url: () => 'ws://localhost:8080/api/reactive' })
const project = client.subscribeProjection(definition)
const stop = project.view.subscribe(() => render(project.view.getSnapshot()))
client.connect()

stop()
project.dispose()
client.disconnect()
```

`connect`/`disconnect` are reusable. Projections retained across a disconnect
resubscribe on the next connection. `dispose` is permanent and idempotent for
that handle; it does not release entities still referenced by other projections.
Multiple readers can share the same handle. Separate subscriptions are not
automatically deduplicated, even when their definitions are identical.
Handles are read/control interfaces returned by `subscribeProjection`, not
constructible objects. For list windows, prefer the generated
`handle.window(list)` controls; low-level views also expose occurrence IDs for
`handle.updateWindow`. Do not keep those IDs across reconnects.
Use `wss://` and application authentication for deployed connections.

## Optional mutation harness

Use `MutationHarness` when coordinating a command with optimistic predictions,
reactive confirmation, or application-owned UI state. Ordinary RPCs can still be
called directly. The helper has no ConnectRPC, JWT, retry, or schema dependency.

```ts
import { MutationHarness } from '@adiom-data/weave'

const mutations = new MutationHarness(client, {
  // Standard committed receipts work by default; override decodeReceipts only
  // if the application's backend uses a different receipt format.
  synchronizationTimeoutMs: 15000,
  maxPending: 256,
  // Only classify as rejected when the endpoint establishes no write committed.
  classifyError: error => isDefiniteRejection(error) ? 'rejected' : 'unknown',
  onListenerError: error => report(error),
})

const operation = mutations.run({
  // The application adapter supplies correlation headers and authentication.
  send: context => taskAPI.createTask(input, context),
  optimistic: draft => {
    draft.insert(taskLinks, { task: { title: input.title, completed: false } })
  },
  onResponseSettled: outcome => {
    if (outcome.kind === 'succeeded') selectTask(outcome.response.taskId)
  },
  onApplied: receipt => showSavedIndicator(),
  onOptimisticRetired: event => clearApplicationOwnedPrediction(event.reason),
})

await operation.response // RPC result, independently of the reactive feed
await operation.applied  // committed receipt applied, independently of the RPC
operation.rollbackOptimistic() // local-only; does not cancel sending or tracking
mutations.dispose()
```

`send` receives frozen `{mutationId, connectionContext}`. Registration and any
predictions precede sending; network work starts in a microtask after `run()`
returns. The callback can wrap retries, but must preserve those identifiers and
implement safe retry/idempotency semantics outside the harness. The harness
does not impose an RPC deadline: the transport owns it. Configure synchronization
timeouts to account for any retry budget. Application adapters must own/snapshot
mutable command inputs as appropriate.

The optimistic writer exposes `entity`, `insert`, `remove`, `patch`, and `delete`,
with the same types as the standalone draft. It is valid only during its
synchronous callback; the harness exclusively owns commit. Invalid prediction
setup sends nothing and reports `not-sent`. A missing handshake, disposed harness,
or exhausted capacity throws synchronously before accepting the operation.

`operation.state` is a read-only `Selection` usable with `useSelection` from
`@adiom-data/weave/react`. Its dimensions are independent:

- `rpc`: `pending`, `succeeded`, `rejected`, `unknown`, or `not-sent`.
- `reactive`: `waiting`, `applied`, or `unconfirmed`.
- `optimistic`: `active` or `retired`, including its retirement event.

An RPC response never writes the authoritative store or retires a prediction.
A valid receipt retires the matching entity layer atomically with model
publication, then settles `applied`. It does not guarantee row visibility,
hydration, or inclusion in any projection. Receipts can precede responses;
a later lost response remains RPC-unknown without erasing the known application.

An ambiguous RPC failure rejects `response` with `MutationOutcomeUnknownError`
but continues waiting for a receipt, retaining predictions until that wait ends.
A definite rejection or pre-send setup failure ends the wait immediately. A
timeout, disconnect, or disposal rejects `applied` with `MutationUnconfirmedError`
(a subclass of `MutationOutcomeUnknownError`) without erasing an RPC success.
Unconfirmed is terminal for that wait; late receipts do not restart it, although
normal model updates continue. None of these events undo a database transaction.

The hooks and state subscriptions run asynchronously, outside model publication;
state notifications may coalesce, while lifecycle hooks retain each event. Hook
errors, including rejected promises returned by async hooks/listeners, are
isolated through `onListenerError`. Hooks are not awaited and cannot delay
mutation progress. `onOptimisticRetired` runs exactly
once for each operation's logical scope, even with no entity predictions. Manual
rollback retires that scope without ending response/receipt tracking, so
`onApplied` can still run later without another retirement callback. Reasons are
`applied`, `rejected`, `not-sent`, `unconfirmed`, `rolled-back`, `disconnected`, and
`disposed`. Hooks for an already-started RPC may still run after disposal when
its response eventually settles; application UI effects must respect their own
component lifetimes.

`maxPending` bounds whole operations until both RPC and receipt waits settle.
Ending a receipt wait never releases capacity while its RPC remains in flight.
Neither promise requires observing the other; unused rejections are handled
internally without changing what an awaiting caller receives.

Receipt decoding uses the client's existing pre-publication decoder plus an
optional post-publication callback on the same registration. The callback sees
owned, deduplicated IDs only after successful model application, never after a
rejected batch. It still runs for a transaction whose publication caused a
disconnect; its notification carries the original connection context. Pure
decoding and application lifecycle effects remain separate.
Decoder output is fully copied and validated before affecting any layer.
Registrations have independent unsubscribe ownership, including registrations
sharing the same decoder function. Unsubscribing affects future messages; an
already-decoded in-flight callback still completes after publication. Callback
promises are observed for errors, not awaited.

## Optimistic entity updates

Optimism is an optional presentation layer over authoritative records. It never
changes server query membership, ordering, cursor bounds, or expansion edges.
Use normalized `entityId` values from projection wrappers, not database primary
keys: those identifiers are opaque and not interchangeable.

```ts
const operation = issueYourMutation() // supplies mutationId and an applied promise
const prediction = client.beginOptimistic({
  mutationId: operation.mutationId,
  patches: [{
    entityId: task.entityId,
    set: { title: { textValue: 'Predicted title' } },
    unset: ['obsolete_field'],
  }],
  result: operation.applied,
})
// Optional explicit local rollback; it never undoes a database transaction.
prediction.rollback()
```

`set` replaces whole top-level values, including nested objects/lists. `unset`
removes a field; it is distinct from setting an explicit null. Inputs are
validated with the generated canonical-value codec, copied, and frozen. A
mutation may patch, create, or delete several entities atomically, once per
entity. Unknown/pending patches and deletes stay dormant until backing arrives;
they do not load or subscribe to anything. Duplicate live mutation IDs are rejected.
Use a fresh ID for every logical mutation; do not reuse IDs within a connection,
because receipt correlation cannot distinguish two operations with the same ID.

Layers compose in creation order: the newest pending patch wins for each
field. CDC keeps updating the underlying authoritative record; untouched fields
stay current. Retiring/rolling back one layer leaves all other pending layers
in place. This includes an older still-pending layer when a newer mutation
commits first. No client-side conflict resolution or write-order guarantee is
implied. `store.getEntity()` reads the effective value;
`store.getAuthoritativeEntity()` bypasses optimism with separate dependencies.

The optional `result` promise removes the layer on rejection. Resolving it does
**not** retire the layer: an RPC response is not stream synchronization. Bind
the mutation's synchronization promise to cover both command failures and
missing-receipt timeouts, or manage rollback explicitly.

Application receipt decoding is registered independently of the transport:

```ts
const stop = client.registerOptimisticReceiptDecoder(transaction => {
  // Pure, synchronous application-specific decoding; return [] for other metadata.
  return decodeYourCommittedMutationIDs(transaction.messages)
})
```

The decoder runs only for atomic committed batches with metadata. Returned
IDs are deduplicated; unknown IDs are harmless. The store validates/stages the
whole authoritative batch and retires matching layers **before one atomic
publication**. Existing post-publication transaction observers see the final
effective model. Bad decoder output/errors are reported through the existing
listener-error handler without failing the stream; the offending decoder's
results are ignored. Decoders must not mutate the client/store. Remove the
decoder with `stop()` when its application adapter is disposed.

The harness defaults to `decodeMutationReceipts`, which decodes the standard
`weave.mutation.committed` UTF-8 mutation-ID payload. `mutationHeaders(context, names?)`
builds correlation headers, with configurable names. The demo uses these defaults;
the core has no ConnectRPC dependency. Backends can use the
[Go correlation and PostgreSQL helpers](../../pkg/mutation/README.md), or implement
the same wire format in another language. Custom receipt formats remain optional
through `decodeReceipts`. Predictions that match confirmed
values preserve entity/display references and do not notify stable readers.

Disconnect/reconnect clears all optimistic layers. Retained offline projection
views fall back to their authoritative graph, including membership and slots,
not speculative ones. Pending/unavailable entity events hide predictions until
backing arrives or the layer retires. Rollback handles are idempotent
and identity-scoped: late errors from an older generation cannot remove a new
layer reusing its mutation ID. `clear()` also clears retained views on logout.

### Temporary entities and explicit membership

For generated projections, the draft API infers paths and nested association
records directly from a returned typed list:

```ts
const operation = mutations.createTask({ projectId, title: 'New task' })
const draft = client.optimistic({
  mutationId: operation.mutationId,
  result: operation.applied,
})
draft.insert(taskLinks, { task: { title: 'New task', completed: false } })
const prediction = draft.commit()
```

The list determines the accepted input type. Inputs use decoded generated field
types (for example, `bigint` integers); aliases and stored field paths are encoded
using that projection's existing metadata. Fields may be omitted. Explicit child
`null` clears an entity slot; `[]` creates an empty nested list. Supplied fields
whose stored paths overlap are rejected rather than silently overwriting values.
Each operation snapshots its inputs; nothing is published until `commit()`.
Invalid positions, patches, and conflicting entity operations are rejected
while staging, leaving earlier valid draft operations intact. As with the
low-level API, an entity can be created, patched, or deleted only once in a
mutation; combine its field edits into one patch.

A draft can target lists in several projections on the same client. To share a
new entity between them, create one reference with
`draft.entity({ title: { textValue: 'Shared' } })`, then use that reference as a
child in each insertion. `entity()` and `patch()` take canonical stored fields;
`insert()` takes typed projection fields or an existing entity reference.
`remove(list, reference)` removes only membership; `delete(reference)` hides the
entity everywhere. All operations retire together with the mutation receipt.
This does not infer which other projections should contain an entity: address
each desired list explicitly.

Commit the draft synchronously after starting the mutation, before its receipt
can arrive. A draft cannot be reused after commit or across connection contexts;
disposed/foreign list handles are rejected. It does not execute the mutation,
subscribe to anything, or map temporary identities to real identities.

The lower-level API remains available when explicit targets are needed:

```ts
const task = client.temporaryEntity()
const link = client.temporaryEntity()
const listTarget = project.target(taskLinks) // typed list from this projection
const taskSlot = project.target('project.taskLinks.task', [project.data.getSnapshot()!.entityId, link.entityId])
const operation = mutations.createTask({ projectId, title: 'New task' })
client.beginOptimistic({
  mutationId: operation.mutationId,
  create: [
    { entity: task, values: { title: { textValue: 'New task' } } },
    { entity: link, values: {} },
  ],
  lists: [{ type: 'insert', target: listTarget, entityId: link.entityId, position: 'end' }],
  slots: [{ target: taskSlot, entityId: task.entityId }],
  result: operation.applied,
})
```

Targets identify a definition node and the entity IDs along its ancestor path,
not server occurrence IDs. `handle.target()` addresses its root; a typed list
can be passed directly. A node ID plus ancestor IDs also addresses nested slots
that do not exist on the server yet. Targets and temporary references belong to
one client. The same temporary entity can be referenced by multiple projections.

`lists` supports `insert`, `move`, `remove`, and `clear`; insert/move positions are
`'start'`, `'end'`, `{before: entityId}`, or `{after: entityId}`. Inserting an
existing reference repositions it without duplication. Missing backing/anchors
leave operations dormant. `slots` assigns an entity reference or clears with
`null`. `delete: [entityId]` hides the entity from all lists and makes entity
slots unavailable. Removing a list reference only affects that target list.

Temporary records can be incomplete. Typed readers expose them as
`status: 'optimistic'` with recursively optional stored fields; available
authoritative records remain strictly decoded. Synthetic lists have no server
window to extend until an authoritative occurrence exists. Membership changes
do not rewrite authoritative query versions or cursors.

The matching committed transaction retires **every** operation in its layer
atomically, including temporary records, even if authoritative replacements
are still pending. No temporary-to-server identity mapping is required or
performed. Membership is explicit: there is no automatic predicate evaluation
or inference from field edits. Offline/retry/idempotency semantics remain out
of scope.

## Seamless reconnects

`handle.view` retains the last published root during reconnects and reports
`stale: true`. The new connection builds privately; on that projection's `Ready`,
its authoritative replacement is published in one swap. Initial loading also
waits for `Ready`. This is per projection, not a globally consistent snapshot.
Normal committed updates stage all ready projections before notifying readers.
Publication runs once per model commit, rather than once per affected handle.
Dependency-tracked selectors reuse unaffected projection materializations, and
status-only changes with a retained root do not rebuild display data.

`handle.data` is a data-only selection: it contains node IDs, entity states,
children, and ordered rows, but no occurrence IDs, query versions, or cursors.
Unchanged values retain their references across reconnects, even when the server
assigns new IDs. Reordered lists reuse unchanged rows by entity identity. React
can use `useSelection(handle.data)` to avoid data-driven rerenders on status-only
changes or identical replacements. `useProjection` is a convenience combined
view and still updates for status/control changes.

Window commands use the latest `handle.view` occurrence IDs. They are rejected
while rebuilding and if the ID is absent from the current publication. Do not
retain control IDs across reconnects. Replacement data is authoritative: missing
fields and rows disappear, and pending entities do not retain old payloads.
Terminal subscription failures clear retained data; transport failures retain it.

For logout or a change of identity, call `client.clear()` **before** changing
credentials. It disposes all handles, clears their retained publications, and
disconnects. Create fresh handles for the next subject. `disconnect()` alone is
intentionally non-destructive to published views.
Cleanup is exclusive: synchronous listeners cannot reconnect during `clear()`,
and attempts to create subscriptions during cleanup throw. Ordinary operations
are available again after it returns.

## Connection liveness

Socket close/error events immediately retire that connection generation and,
by default, start bounded-backoff reconnection. A connection that never opens times out.
For silent network failures, the client sends one envelope with a `ping` payload
at a time and expects the bridge to echo its ID in a `pong` payload.
A missed reply retires the connection and rebuilds subscriptions while retaining
the last published view. Replies and timers from old generations are ignored.

These heartbeats do not reach gRPC, change model sequence numbers, or request
database/WAL barriers. The server serializes pong and model writes and cancels
the upstream gRPC stream when the WebSocket closes. The bridge's
`SocketConfig.WriteTimeout` defaults to `10s`; `IdleTimeout` defaults to `60s`.
Valid incoming application pings or model messages renew the idle timeout.
Silence cancels the connection, including requests blocked forwarding upstream.
If client heartbeats are disabled, the client must otherwise send traffic within
the server's idle timeout. A suspended browser may time out and reconnect on resume.
Hosts can cancel bridge request contexts during shutdown, closing upgraded
WebSockets and upstream streams.
Heartbeat detection establishes transport liveness, not database freshness.

### Failure policy and diagnostics

```ts
const client = new ReactiveClient({
  url: () => authenticatedSocketURL(),
  failurePolicy: failure => {
    if (failure.close?.code === 4401) return 'pause' // application-defined code
    return 'retry'
  },
  onDiagnostic: event => metrics.record(event),
})
const counts = client.getDiagnostics()
```

`failurePolicy` is synchronous and returns `retry` (the default), `pause`, or
`clear`. Pause retains authoritative views and handles, drops predictions, and
waits for an explicit `connect()`. Clear also disposes handles and removes
retained data; create new handles afterward. Policy exceptions/invalid results
pause safely and are reported through `onListenerError`. A replacement lifecycle
started by the policy is not overridden by its returned action.

Failures distinguish connection, transport, close, protocol, send, and watchdog
timeouts. Socket close details are supplied when available. Browser connection
errors may reveal neither an HTTP status nor a close code; the runtime does not
guess whether they are authentication failures. Token validation and refresh
remain application concerns.

Diagnostic callbacks run asynchronously and exceptions are isolated through
`onListenerError`. Events cover lifecycle transitions, failure actions, retries,
projection readiness/errors, and window completion. They omit arbitrary error
messages, close reasons, URLs, and record payloads. Failure-policy callbacks
receive the original error/close details, so applications should avoid logging
those indiscriminately.

Lifecycle, transaction, and selection observers may return promises. They are
never awaited by publication; both thrown errors and rejected promises reach
`onListenerError`. An error handler that itself fails falls back to the normal
unhandled-error reporting path. Decision callbacks such as `failurePolicy` and
receipt decoders remain synchronous.

`getDiagnostics()` returns frozen, constant-time counter snapshots: connection
attempts/retries/failures, received frames/bytes, active handles/pending window
commands, authoritative entity states, query/occurrence counts, dependency keys,
and optimistic layers/targets/materializations. These are logical counts, not
heap-size measurements. Counters last for the client lifetime; store counts
reflect only the current connection's model, not retained offline publications.

## React

```tsx
import { useConnection, useProjection } from '@adiom-data/weave/react'

function Project({ client, request }) {
  // client and request must have stable identities (module scope/useMemo).
  const connection = useConnection(client)
  const view = useProjection(client, request)

  if (view.status === 'error') return <Error message={view.error} />
  if (view.root?.type !== 'entity') return <Loading />
  const row = view.root.row
  if (row.entity.status !== 'available') return <Loading />
  return <ProjectDetails values={row.entity.values} children={row.children} />
}
```

In a real application the connection owner should usually live above individual
views, so unmounting one view does not disconnect other projections. The hook
`useConnection(client)` owns connect/disconnect and returns the connection
snapshot (`status` and optional `error`). Mount it once per client; it is not a
reference-counted ownership hook. Other components can read status with
`useSelection` and a stable `client.store.observe(s => s.getConnection())`
selection without owning the connection. Cleanup disconnects without clearing
cached views or disposing projection handles.

`useProjection` owns only its subscription. Network work starts in effects,
never render; SSR returns loading projection state without connecting, and
React StrictMode can safely replay the lifecycle.

An entity view contains a `row`; a list view contains `query` (version/bounds)
and ordered `rows`. Every row contains an entity state and its named child views.
Missing child occurrences are `undefined`; that alone does not distinguish
loading, empty, or hidden data. `Ready` does not promise pending payloads are
hydrated, or that the entire projection shares one database snapshot. A diamond
contains distinct occurrences referring to the same immutable entity snapshot.

For finer-grained subscriptions, use
`client.store.observe(s => s.getEntity(id))` and `useSelection(selection)`.
Selectors must be pure. They track reads of entities, queries, occurrence
buckets, and status. Results are cached; unrelated commits do not invalidate
active selectors. The initial projection materializer walks the affected
projection on a relevant change; it is not an incremental per-node view cache.
`Selection<T>` exposes only `getSnapshot` and `subscribe`; dependency tracking
and cache refresh are private implementation details. Reading a selector never
consumes a pending notification to its subscribers.

Low-level materializers can use `ModelStore.subscribeCommits(listener)`. It
returns an unsubscribe function and runs once after a successful model commit,
with the set of affected active selections, after all have been invalidated and
before ordinary selector delivery. The dependency index finds those selections
without scanning every projection's dependencies. Rejected batches do not
trigger it. The client publishes only affected projections at this boundary,
staging them all before notification. Application components normally use the
handle selections instead.

## Correctness and lifecycle

- Complete batches are staged and validated before touching live state or
  advancing sequence counters. Notifications run after the entire commit.
- Query deltas validate their base/result versions, removals, unique IDs, and
  final insertion positions. Sorted insertions merge with retained order in
  linear traversal rather than repeatedly splicing the list.
- Upserts replace records; absent fields disappear. Pending removes any stale
  payload. Membership removal never implies entity release.
- Projection routing uses explicit subscription and parent-occurrence metadata;
  IDs are opaque. Indexes distinguish diamonds and overlapping projections.
- A bad message, sequence gap, or disconnect drops the model generation and
  resubscribes retained handles for fresh snapshots. Published views survive;
  the low-level `client.store` is connection-local and resets. There is no durable replay.
- Server subscription failures affect that handle. Window-request failures
  reject their promise without failing a healthy projection. A timeout has an
  unknown server outcome: later authoritative window updates still apply.
- Disposal sends Unsubscribe; the server's cleanup batch removes references and
  releases unneeded records. Acknowledgment retires local scope sequence/status
  bookkeeping. Disconnection clears all connection-owned model state.
- Each connection generation owns its own installation/request-routing state.
  Startup and observer-triggered subscriptions share one idempotent installation
  path. A listener may subscribe, dispose, or reconnect synchronously without
  duplicate installation or reviving work from a retired connection.
- Subscriber exceptions are isolated individually, including readers sharing
  one selector. `onListenerError` on `ModelStore` or `ReactiveClient` receives
  those errors; by default they are rethrown asynchronously. They do not undo
  committed state, interrupt healthy listeners, or cause a protocol resync.

## Configuration

The URL callback can supply application-specific connection parameters at
reconnect. Authentication must match your application's WebSocket edge; the
browser's native WebSocket API cannot attach arbitrary authorization headers.
The runtime does not verify JWTs, implement token refresh, persist offline state,
or execute mutations. Optimistic layers only affect presentation. The local demo uses
server-provided fixed claims, with no authentication token.

| Option | Default | Purpose |
| --- | --- | --- |
| `reconnectDelayMs` | 1000 ms | Initial retry delay |
| `maxReconnectDelayMs` | 30000 ms | Retry backoff ceiling |
| `requestTimeoutMs` | 30000 ms | Pending reactive request timeout, not a mutation RPC deadline |
| `connectionTimeoutMs` | 10000 ms | Time allowed for the socket to open |
| `heartbeatIntervalMs` | 15000 ms | Delay before sending the next heartbeat |
| `heartbeatTimeoutMs` | 10000 ms | Time allowed for its matching pong |
| `maxMessageBytes` | 8388608 (8 MiB) | Maximum incoming frame size |
| `maxDepth` | 64 | Decoder nesting bound |
| `maxNodes` | 100000 | Decoder value/node budget |

Set `heartbeatIntervalMs: 0` to disable pings for a transport without that
capability. The bundled WebSocket bridge supports pings and has its own idle
timeout, so disabling them requires another source of regular client traffic.
`createSocket` allows custom transports/tests implementing
the WebSocket-shaped interface. It receives `Uint8Array` sends and must honor
`binaryType = 'arraybuffer'` for incoming message events. The default expects a
browser WebSocket.

## Tests

`npm test` checks the public TypeScript API and runs Node's built-in test runner
after compiling the library. Tests
cover atomic rejection, ordering, pending/hydration, diamonds, overlapping scopes,
selector stability, exclusive cleanup, indexed invalidation, reconnects, timeout
and heartbeat recovery, SSR, and mounted React StrictMode/request lifecycles.
Mounted data-only reader tests verify that identical reconnect replacements do
not rerender, while real changes do. JSDOM is a development-only test dependency.

The demo packages contain opt-in live tests using a running demo service and
Node 22+ WebSocket support. From the repository root:

```sh
WEAVE_TEST_URL=http://localhost:8080 npm run test:demo
```

These tests open multiple viewers, create uniquely named tasks, observe edits,
change windows, reconnect, and soft-delete their own tasks. They do not manage
your server or database lifecycle. Typed application bindings are generated by the separate
Go projection compiler; see [`examples/demo`](../../examples/demo/README.md).
The runtime also continues to accept ordinary projection definitions.

## Implementation and wire format

- `gen/`: Buf/Protobuf-ES message types, schemas, and codecs generated from the
  shared `.proto` definitions. No handwritten copy of the wire schema.
- `protocol.ts`: generated-schema-derived application inputs and a typed adapter
  from generated messages to the immutable runtime model, with invariant and
  resource checks. This is not an application-specific projection compiler.
- `store.ts`: entity, ordered-query, and occurrence stores, plus cached selectors.
- `optimistic.ts`: ordered mutation layers indexed by entity, with immutable
  effective values; it never owns authoritative records or evaluates queries.
- `optimistic_selection.ts`: logical projection targets and explicit ordered
  list/slot overlays, independent of server occurrence IDs and query cursors.
- `projection.ts`: resolves exact occurrence edges into read-only projection views.
- `publication.ts`: retains published views, stages replacements, and reconciles
  immutable display data independently of connection-owned control IDs.
- `client.ts`: multiplexes projections on one socket; owns reconnection and requests.
- `mutations.ts`: optional transport-independent mutation coordination, receipt
  waits, observable lifecycle state, and optimistic-scope cleanup hooks.
- `bindings.ts`: generated-binding support for typed immutable selections,
  scalar decoding, and occurrence-relative window controls. It has no second
  authoritative entity cache and no query evaluator.
- `optimistic_draft.ts`: typed, private optimistic graph drafts using existing
  generated shape metadata. `value_input.ts` owns shared input encoding;
  `projection_metadata.ts` privately associates returned lists with their paths.
- `socket_protocol.ts`: generated binary WebSocket envelopes containing reactive
  messages or heartbeat controls, separate from model events.
- `react.ts`: optional `useSyncExternalStore` integration. Core imports do not load React.

Records remain immutable typed wire-value maps. Integer/decimal values stay as
strings, bytes as base64, and datetimes as timestamp strings; the runtime does
not silently coerce them into lossy JavaScript numbers or dates. `scalar` is a
convenience for application-level decoding, not a universal type conversion.
Projection definitions retain their object-literal shape through generated
JSON-shaped input types; this does not imply JSON on the wire. The generated
codec validates those input objects and produces native protobuf messages.

Each WebSocket binary frame holds one `WebSocketClientFrame` or
`WebSocketServerFrame` from `proto/reactive/v1/websocket.proto`. Reactive payloads
are the same protobuf messages used by gRPC. No JSON text or base64 wrapper is
transmitted. Bytes and timestamps are converted to the runtime's existing
display representation only at the typed adapter boundary; uint64 counters
remain exact `bigint`. Text-frame JSON is not supported.
