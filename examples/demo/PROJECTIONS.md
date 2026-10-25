# Compiled read projections

[`schema.graphql`](schema.graphql) defines the application's read types and declarative storage
mappings. [`projections.graphql`](projections.graphql) defines selected views. The Go compiler generates
the checked-in `@adiom/weave-demo-projections` workspace package, with separate core
and React entry points. No GraphQL parser, schema, or executor ships to browsers.
Mutation commands and handlers remain completely independent.

```sh
npm run generate:projections
npm run test:demo
npm run build
```

`npm run generate:demo` / `make generate-demo` regenerate both Protobuf and projections.
Normal frontend builds use checked-in generated sources and do not need Go.
The demo tests check generation determinism; the CLI also supports this check
from the repository root:

```sh
go run ./cmd/projectiongen \
  -schema examples/demo/schema.graphql \
  -documents examples/demo/projections.graphql \
  -out examples/demo/packages/demo-projections/src/index.ts \
  -react-out examples/demo/packages/demo-projections/src/react.ts \
  -check
```

## Using the generated library

### Many-to-many fetch hint

The demo's association list opts into one-level reference preloading:

```graphql
taskLinks @window(initialCount: 100, from: END) @fetchReference(child: "task") {
  position
  task { id title }
}
```

`child` names the selected immediate entity-child alias. The query backend can
load a missing association window and its tasks with one PostgreSQL left join.
For covered association windows it reuses cached targets and batches only missing
ones. Extension retains the hint; contraction and covered re-extension do not
refetch the list. The association query's fingerprint and cache are unchanged.

This first version requires parent-column equality bindings covering the target's
complete primary key, with top-level columns of matching PostgreSQL types.
Additional equality guards (such as `organization_id`) are allowed. Static
target filters such as soft-delete checks are supported and do not filter the
association list. Parameter/literal bindings on the entity child, arbitrary joins,
and recursive joined expansion are not supported by the hint. Source values must
be present, as for ordinary projection bindings; dangling identities are allowed.

The projection still opens ordinary child entity subscriptions, including over
gRPC. Preloading removes their database reads, not their subscription messages.
Changes arriving through CDC use normal projection expansion. Visibility and
publication semantics are unchanged: the joined read is not a projection-wide
atomic snapshot. Current tracked cache records take priority over fetched images;
targets invalidated during a fetch fall back to ordinary initialization. Dirty-key
tracking is bounded by the backend's `MaxReferenceInvalidations` option (standalone
YAML: `limits.max_reference_invalidations`, default 100000); overflow disables reuse
of that speculative read rather than stopping CDC.

```tsx
import type { ReactiveClient } from '@adiom-data/weave'
import { useProjectDetails } from '@adiom/weave-demo-projections/react'

function ProjectTasks({ client, projectId }: { client: ReactiveClient; projectId: string }) {
  const projection = useProjectDetails(client, { projectId })
  const links = projection.data?.status === 'available'
    ? projection.data.value.taskLinks : undefined

  return <button disabled={projection.status !== 'ready' || !links} onClick={() => {
    if (links) void projection.window(links).extend('LEFT', 20).catch(reportError)
  }}>Load earlier tasks</button>
}
```

React keys subscriptions by encoded parameter values, not argument object
identity. Changing parameters installs a new scope without exposing old data.
The application still owns the shared connection lifecycle (usually one
`useConnection(client)` above its views) and error presentation. In this sketch,
`reportError` is application-provided. Window changes belong in event handlers,
not in render. See [the running example](web/src/App.tsx).

Framework-independent use:

```ts
import { ProjectDetails } from '@adiom/weave-demo-projections'

const handle = ProjectDetails.subscribe(client, { projectId })
const stop = handle.data.subscribe(() => render(handle.data.getSnapshot()))
// handle.view includes status, error, stale, and typed data.
// handle.window(list).update([...]) changes both boundaries atomically.
stop()
handle.dispose()
```

Window controls expose current boundaries without embedding them in display
data. `control.bounds` is a read-only selection suitable for `useSelection`:

```ts
const control = handle.window(list)
const bounds = control.bounds.getSnapshot()
if (bounds?.left?.cursor) await control.move('LEFT', bounds.left.cursor)
```

Bounds are `undefined` during rebuilding or after that occurrence disappears.
The control resolves replacement IDs after reconnect; unselected entity changes
and unchanged bounds do not trigger typed selection notifications.

Entities are `ProjectedEntity<T>`: `available` with an immutable `value`, or
`pending`/`unavailable` without stale payloads. Missing child occurrences are
`undefined`, even for SDL `!`: field nullability does not promise an expansion
has loaded. We do not invent distinctions between hidden, absent, and loading
occurrences. Nullable stored fields are `T | null | undefined`, preserving the
engine's distinction between explicit null and an absent field.

Relationship lists are immutable `ProjectedList<T>` arrays. Private locators
retain the node and full ancestor entity chain, so diamonds have independent
windows. Controls resolve current server IDs at command time and reject during
rebuilding or after removal. Display data contains no server occurrence IDs
or mutable controls. Weak caches preserve unchanged typed references across
commits and identical reconnects, without another authoritative entity cache.

## Schema mappings

### Whole-record server output

`@allFields` opts a table entity or list node into publishing every stored field,
including complete embedded objects and arrays. It changes server output only;
the generated TS reader still exposes exactly the fields and relationships written
in the definition:

```graphql
query TaskOwners($projectId: UUID!) {
  project(id: $projectId) @allFields {
    taskLinks @window(initialCount: 10) {
      position
      task @allFields {
        title
      }
    }
  }
}
```

Here the project sends all its stored fields, but its typed view exposes only
`taskLinks`. Association rows still send only `position`. Tasks send all stored
fields while their typed views expose only `title`. Child expansions never
inherit `@allFields` automatically. A relationship-only selection is supported
when that node explicitly has `@allFields`; an empty GraphQL selection is still
invalid.

This includes stored fields unknown to the read schema and whole embedded
values even if the client selects only part of them. Snapshots return those
fields directly under stored names, plus explicitly requested expansions;
there is no snapshot-side typed-reader trimming. Neither this directive nor
normal field selection is field-level authorization. Do not opt in on relations
with columns the caller should not receive.

### Mapping declarations

- `@table(schema:, name:, softDelete:)` maps an object to a relation. Optional
  `softDelete` names a nullable schema field and adds `IS NULL` to every query.
- `@column(path: [...])` maps a field to an exact stored path. Omitted mappings
  use the schema name. Dots inside segments are literal, not path separators.
- `@relation(parent: [...], child: [...])` defines typed equality bindings to a
  child relation. Names refer to schema fields. Binding fields need not be
  selected for display: the server expands against whole authoritative records.
  Parent binding fields must currently be non-null concrete scalars; nullable
  foreign-key parents require a future explicit skip-expansion policy.
- Repeatable `@bind(argument:, field:)` maps arguments to equality predicates.
  Arguments may be literals or required scalar variables.
- Object types without `@table` describe embedded values, not query expansions.
  Their selected fields compile to recursive server-side output masks, including
  fields inside embedded arrays. Only selected stored fields are published;
  aliases still apply in the typed TS reader. `StructuredValue` remains an
  explicitly whole, arbitrary value. Backend caches retain complete records.
  Embedded-object and scalar lists are recursively decoded.

The compiler implicitly loads `internal/projectioncompiler/prelude.graphql`.
Editor tooling can load that file alongside the application schema.

## Projection directives

Every relationship list requires `@window(initialCount:, from: START|END)`.
Counts are soft initial requests; live boundaries do not maintain that count.
Each parent's nested list has independent boundaries and controls.

`@orderBy(terms: [{field: "position", descending: false, nullsFirst: false}])`
uses stored schema fields. Include a unique tie-breaker for complete membership;
omitted ordering retains the datastore's default primary-key order.

Constant predicates use the canonical comparison/null/junction vocabulary:

```graphql
taskLinks @window(initialCount: 20) @where(predicate: {
  field: "position", op: GREATER_THAN, value: "10.5"
}) { position }
```

Predicates also support `isNull`, `and`, `or`, and `not`. The compiler validates
literals against the mapped scalar type. Numeric AST tokens retain their
original text and never pass through floating point for decimal conversion.

## Scalar representations

| SDL | Generated TypeScript |
| --- | --- |
| Boolean | boolean |
| String, UUID | string |
| Int | signed-32-bit number |
| Int64 | signed-64-bit bigint |
| Decimal | exact decimal string |
| Float | finite number |
| Float64 | number, including NaN/infinities |
| DateTime | RFC3339 timestamp string; no JS Date precision loss |
| Bytes | base64 string, matching the immutable client representation |
| StructuredValue | recursive tagged Value, not lossy untyped JSON |

These are read contracts, not database schema enforcement or authorization.
Missing non-null fields and mismatched types throw `ProjectionDecodeError` from
the typed selection; they do not disconnect or corrupt other projections.
Subsequent compatible data can recover. Rendering/error boundaries choose how
to surface these errors.

## Deliberate first-version limits

Supported: named queries, aliases, same-object fragments, required scalar
parameters, scalar/embedded selections, nested table expansions, constant
filters/order/windows, and the runtime's dynamic window operations.

Explicitly rejected: multiple operation roots, optional/defaulted operation
parameters, mutation/subscription documents, conditional/polymorphic selections,
application enums, variable reactive directives, and table nodes with no stored
field selected unless `@allFields` is explicit (otherwise select `id`). Empty
wire columns never implicitly request all fields. Direct relationship lists-of-lists are not
supported, but lists expanded for each parent and embedded lists-of-lists are.

Compiler ceilings are 32 table expansion levels / 1024 nodes and 32 embedded
levels. Deployed server limits may be lower and still apply. CLI source files
are limited to 1 MiB. This is a constrained definition language, not a GraphQL
endpoint. Optimistic entity/membership overlays are a separate client-runtime
capability, independent of projection compilation.
