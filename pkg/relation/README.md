# Relation definitions

This package exposes immutable definitions for server-computed values, private
fact projections, a boolean visibility expression, and guarded named exports.
It contains contracts and constructors, not a datastore or public evaluator API.

Pass fixed `Definitions`, or an application-owned `Store`, through
`projectionserver.Options.Relations`. Expressions use canonical values; claims
are supplied by the host. The engine owns compilation, lazy reactive fact loads,
dependency tracking, and ordered publication.

`Relation.Visibility` is a CEL expression over the same inputs as computed
values. It can read private helpers such as `computed._role`; underscore names
cannot be selected by clients. `Computed.Propagate` exports its own name to each
declared destination using `Propagation{To, When}`. Children read
`inherited._role` and can test eligibility with `has(inherited._role)` without
calculating its value. `Relation.Receive` optionally restricts exports using
`Incoming{From, Field}`. There is no separate visibility propagation or rule API.

`NewStore(definitions, limits)` validates and copies a complete definition map.
`Store.Replace(definitions)` validates its replacement before publishing the
whole map atomically. Each evaluation captures one immutable map, including
relations first read later in the pass. Unchanged compiled definitions are
reused, and changed/added/removed relations automatically invalidate only their
dependents. Failed replacements leave the running map untouched. The store has
no workers, load queue, idle cache, or close operation.

Separate projection servers can use separate stores while sharing a query
backend. Tenancy and connection routing remain application concerns; there is
no requirement to load every tenant's definitions into one global map.

See the [projection library guide](../projectionserver/README.md#computed-values-visibility-and-trusted-claims)
for examples and the demo's [relations.yaml](../../examples/demo/relations.yaml)
for a role shared by visibility, propagated task values, and `canEdit` outputs.
