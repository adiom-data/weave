# Demo mutations

`@adiom/weave-demo-mutations` is an application-specific, framework-independent client
for the Protobuf/ConnectRPC API in `examples/demo/proto`. It is separate from
the projection compiler and never writes mutation responses into the reactive
cache. Reusable receipt tracking, lifecycle state, hooks, and optimistic cleanup
live in `MutationHarness` in `@adiom-data/weave`, not in this application adapter.
The demo uses the shared correlation headers and standard committed receipt;
its protobuf defines only the application mutation commands and responses.

```ts
import { TaskMutations } from '@adiom/weave-demo-mutations'

const mutations = new TaskMutations(reactiveClient, {
  baseUrl: location.origin,
  timeoutMs: 15000,
  maxPending: 256,
})

// Requires the reactive connection handshake to have completed.
const operation = mutations.createTask({ projectId, title: 'New task' }, {
  optimistic: draft => {
    draft.insert(taskLinks, { task: { title: 'New task', completed: false } })
  },
  onOptimisticRetired: event => clearLocalPrediction(event.reason),
})
const result = await operation.response // taskId returned by the RPC
await operation.applied // independently observed committed stream receipt

mutations.dispose()
```

The receipt may arrive before the RPC response. It confirms a transaction, not
that a task is visible in a projection, fully hydrated, or inside a window.
Only PostgreSQL snapshots/CDC update the authoritative model. The harness
installs the client's pre-publication receipt decoder, so any optimistic layer
with the same mutation ID retires atomically with its authoritative transaction.
Post-publication receipt observers still run after that final view is visible.
To predict changes to a loaded entity, supply `optimistic: draft =>
draft.patch(task, { completed: true })` as the method's second
argument. All three methods accept the harness's optional lifecycle hooks.
`operation.state` can be used directly with the client's React `useSelection`.

`maxPending` bounds whole operations, not just receipt waiters. Capacity is
released only after both the RPC and its receipt wait settle. A receipt or
disconnect cannot free a slot while its RPC is still in flight.

Every call gets a random mutation UUID and carries the current opaque
connection context in `X-Weave-Mutation-Id` and `X-Weave-Connection-Context`
headers. The demo server supplies fixed user/organization/team claims. A context is a
receipt-routing capability, not authorization or an idempotency key. The Go
handlers share transaction ownership, project authorization, and receipt
emission. Creation appends its membership using exact PostgreSQL numeric
arithmetic; completion updates only `completed`, never a stale client title;
deletion soft-deletes the task and all its memberships within the organization.

The explicit **demo write policy** requires an active project in the subject's
organization and a live user/team grant, using the server-provided demo claims.
Update/delete also require the task to belong to that project. A shared task
can be changed through any authorized project containing it; deletion is global,
not an unlink from just the current project.
It is not a generic write-permission system. There is no mutation-engine
dependency on read visibility or shared query-cache internals.

No automatic retries, durable mutation log, idempotency, or offline queue is
provided. A receipt timeout or disconnection rejects `applied` with
`MutationUnconfirmedError`; an ambiguous RPC transport/commit failure rejects
`response` with `MutationOutcomeUnknownError`. Response-decoding failures and `Internal`/`DataLoss`
RPC errors are conservatively unknown too: their codes cannot establish that
the write rolled back. It does not imply rollback. Known command errors
such as permission denial retain their Connect error codes. Dispose invalidates
pending synchronization waits but does not undo or cancel database writes.
RPC and receipt outcomes are independent: a lost RPC response cannot erase an
already-observed commit, and an RPC success remains known if the receipt wait
expires. Ambiguous RPC failures continue awaiting a receipt until the configured
timeout. Callers can await the two independent promises or combine them with
`Promise.all` when both results are needed.

Nonreactive callers can use the generated service directly without either
correlation header; supplying just one header is invalid. Wrong/stale contexts
cannot be checked by a stateless mutation server: the write may succeed while
no live client receives its receipt. Clients then time out as unknown.

The demo uses a typed nested insertion in the harness's optimistic callback to
display a new task immediately. Neither task nor membership needs a database ID
yet. The harness owns installation and cleanup; the receipt decoder retires the
layer alongside authoritative transaction application.
See [`App.tsx`](../../web/src/App.tsx) and the
[client mutation harness](../../../../packages/client/README.md#optional-mutation-harness).

The demo UI uses `CreateTask`, `UpdateTask` (completion only), and `DeleteTask`.
Toggles apply a canonical field patch using the opaque runtime entity ID while
the RPC uses the task's application ID. Deletion predicts both the task
tombstone and removal of loaded membership rows. Mutation responses never
write authoritative data into the reactive store. Live tests also write and
clean up through these RPCs.

Run `npm run generate:mutations` to regenerate Go messages/Connect handlers and
TS bindings. It uses the Go module's pinned Connect generator through `go run`
and the existing local Protobuf generators. No GraphQL types are involved.

From the repository root, run
`npm run test --workspace @adiom/weave-demo-mutations` for response/receipt
ordering, unknown outcomes, reconnects, and binary RPC metadata.
Set `WEAVE_TEST_URL` to opt into the
live tests. They exercise two viewers, optimistic creation, rollback of a denied
update, completion, deletion, and receipt routing without subscriptions. They
create uniquely named tasks and soft-delete only those tasks in cleanup.
