# Relation computation evaluator

This is the compiled-expression portion of the projection runtime. Named
canonical values and visibility share one lazy, occurrence-specific context;
visibility is a reserved boolean computation. Results are not entity fields.

Compilation rejects statically known output types that disagree with a
computation's declared canonical type, without coercion. Computations may return
null; visibility expressions and export guards must return booleans. Dynamic
record/fact/computation reads retain their runtime type checks. CEL loop variables
obey lexical scope, including when they shadow the special input-map names.

Computed expressions may read local `computed.name` dependencies and
`inherited.name`. Local cycles are rejected at compilation, and values
are memoized once per occurrence and immutable view. Propagation explicitly
exports a value under its own name over an immediate, guarded edge.
Selected outputs retain their transitive fact/definition dependencies so
fact-only changes can emit value-only updates. Clients need not select internal
values used by visibility or descendants. Names beginning with `_` are private:
they can be dependencies and exports but cannot be selected by a client.
Literal computation reads permit static validation. `has(inherited.name)` tests
export eligibility without computing the value; dynamic names and enumeration
are unsupported. Presence tests on `computed` and `facts` are unsupported.
For application setup, start with the
[projection service's guide](../../pkg/projectionserver/README.md#computed-values-visibility-and-trusted-claims).
The details below are for policy authors and contributors.

Applications provide a complete relation-definition map through
`pkg/relation.Store`. `Definitions` is the fixed-map convenience;
omitting authorization options still uses allow-all. The application owns
policy storage, tenancy, and authenticated claims. Compilation, lazy dependency
discovery, invalidation, and publication scheduling are library-owned.

## Responsibilities

- Compile CEL expressions into private, immutable programs.
- Validate names, dependencies, guarded exports, and configurable limits.
- Check explicitly configured, directional parent-to-child propagation edges.
- Interpret a nil receive list as unrestricted, an empty non-nil list as none,
  and other lists as explicit `(From relation, Field name)` export allowlists.
- Evaluate visibility as one boolean expression with ordinary short-circuiting.
- Discover only the facts and computations visited by that expression.
- Withhold visibility while its final result remains unresolved.
- Return resolved/visible separately, rather than treating loading as denial.
- Bind fact selectors to the existing canonical query/value representation.

Visibility is an optional CEL boolean expression over `item`, `claims`, named
`facts`, local `computed` values, and named `inherited` exports. Empty visibility
permits a configured relation; `"false"` denies. Missing relations deny unless the
host explicitly enables allow-all. A runtime non-boolean result fails the affected
projection, not the source consumer. Computed values use the same evaluator and
can reference each other; visibility has no separate propagation/rule model.

For example, cheap local checks can short-circuit expensive discovery:

```cel
item.deleted_at == null &&
(item.restricted != true || facts.grants.exists(g, g.item.user_id == claims.user_id)) &&
computed._canView
```

Unrestricted items do not load these facts. Restricted items must satisfy the
explicit check even when `_canView` can inherit access. Unknown inputs do not
serialize discovery: other needed operands can start loading in the same pass.

Each computed field's `Propagate` list contains at most one entry per target
relation, with `To` and a boolean `When` guard. Guards receive immediate `parent`
and `item` records and claims, but cannot discover facts or read computations.
The source field name identifies the export; there are no edge IDs or destination
renames. Any child computation or visibility expression can read the export by
that name, without declaring a corresponding local computation. `Relation.Receive`
optionally restricts the allowed parent relation/name pairs. Only concrete
immediate projection edges within the same source participate.

`has(inherited._role)` is true for an eligible export even if its value is null,
false, or still loading. An unresolved guard makes presence unknown. Reading the
value remains unknown until its computation resolves; an absent export reads as null. Eligibility and
values are independently memoized. Missing-export checks retain parent-definition
dependencies so adding an export invalidates existing consumers.

Missing optional fields are null-like; pending entity images are unknown.
An ordinary record field/index read defaults to null when the field is absent.
Structural equality and membership keep absent object keys distinct from
explicitly null keys. `has(item.field)` distinguishes omission from null.
Policy equality is type-strict: integer `1` differs from float `1.0`, and UUID
differs from text. Cross-type ordering is an error. This holds in both native
execution and CEL, including nested collections and comprehensions. Float NaN
is unequal to itself and has no successful relational comparison.
CEL adapts scalars on demand; canonical records and lists are borrowed through
lazy wrappers, not eagerly copied. UUIDs retain a distinct `weave.UUID` type;
use explicit `uuid(text)` or `string(uuidValue)` conversions when needed.
Decimals have a distinct exact CEL type
whose comparisons reuse the canonical query comparator, including its total
ordering for NaN and infinities. Write decimal literals as `decimal("10.25")`;
do not compare decimals with CEL strings, integers or doubles. Such ordering,
unsupported arithmetic and lossy conversions are errors, not implicit coercions.
Decimal values have a canonical, comparable representation so dynamic CEL map
construction cannot panic. CEL's field/index qualifiers still reject decimal
keys with a normal evaluation error; they are not a supported index type.

A fact is a private parameterized projection using the same plan/binding code
as ordinary projections. Parameters contain `item` and `claims`; child bindings
can reference their immediate parent row. List results have this shape:

```text
[{ item: { ...row fields... }, children: { childName: ... } }]
```

Entity nodes return one wrapper or null. Multiple matches on an entity fact
are an error, not an arbitrary authorization witness. CEL can inspect nested
results, for example `facts.grants.exists(g, g.item.user_id == claims.user_id)`.
Only declared literal fact reads (`facts.grants` or `facts["grants"]`) are
supported, not enumeration or presence tests on the fact map. Referenced facts
are loaded lazily and memoized within the evaluation. Discovery does not itself
grant public visibility to discovered records. Definition depth/nodes, CEL
expression size/runtime cost, fact queries/rows and load concurrency are bounded.

## Runtime boundary

CEL provides parsing and checking, while canonical policy operations define
value semantics independently of CEL's runtime representation. Compilation lowers
an intentionally bounded subset into an immutable execution plan:

- Canonical scalar literals and constant list/string-keyed-object literals.
- Fixed field paths and integer list positions from `item`, `claims` and `parent`.
- Scalar comparisons, null checks, `has()`, and `&&`, `||`, `!`.
- Boolean conditional expressions (`condition ? allowCheck : denyCheck`).
- List/map membership (`in`) using a fixed collection read or constant collection.
- Global/member `size()` on fields and literals; canonical collection sizes avoid element conversion.
- String `startsWith()`, `endsWith()` and `contains()`.

One private `compiledExpression` interface hides
the choice from policy orchestration and the projection layer. The native evaluator
reads only referenced canonical fields and invokes `relation/value` operations
directly. It does not construct CEL scalar, map, or list values. The expression
interface returns canonical values and work usage, with no CEL types.

Explicit conversions/constructors (even constant `decimal("...")`), unsigned
literals, and unsupported syntax (including other functions, comprehensions, arithmetic,
regex, dynamic keys, computed collection construction, and facts) selects CEL
for the entire expression. Value-producing conditionals nested in other operators
also stay on CEL: its conditional-attribute accounting is context-sensitive, so
the native subset only handles conditional boolean control flow. Invalid constant
constructors also stay on CEL so an unused branch does not become a compile-time
failure. Fast plans keep
a CEL fallback for invalid runtime operations. Structured equality and membership
use the same canonical operations in both strategies. This is a whole-expression retry, not mixed
subexpression execution. Fast plans perform no IO or fact discovery, so retries
cannot duplicate external effects. Unknown/loading behavior, short-circuiting,
errors, cancellation, and work accounting are covered by differential tests
and a fuzz target against the CEL compatibility implementation. CEL comparison
and membership calls are decorated to invoke our type-strict operations rather
than its implicit mixed-number comparisons. CEL-only collection types also use
strict recursive equality.

The evaluator deliberately cannot read the dependency cache directly. Its
resolver must expose evidence valid for the evaluation's ordered view, not
whatever record the cache happens to hold when a callback runs. Resolver errors
are errors, not negative existence results.

A resolver returns a private projection value and an explicit completeness
flag. The fact remains unknown until required coverage and descendants are
complete. Pending rows remain unresolved. A size-one window whose selected row
was deleted is not proof that no other matching rows exist.

`internal/projection/evaluation` owns bounded asynchronous loading,
reference-counted selector leases, per-occurrence dependency retention/pruning,
and the explicitly consumed fact-feed position. It consumes the existing query
contract and can use the projection-side shared dependency cache. It does not
own a background read-ahead loop or general historical versions.

New fact snapshots cannot be applied retroactively to transactions preceding
their baselines. The projection runtime integrates dependency effects with its
ordered delivery processing. Initial publication waits for required checks.
An unresolved live check holds the affected scope's last public model while
its private materialization continues. Resolution publishes a current hydration
diff, not a replay of earlier commits against newer facts. Transaction messages
remain on their original connection-owned commit path.

## Atomic definition sets

`Store` owns one complete immutable map. Construction and `Replace` copy and
compile definitions and validate cross-relation references before publication.
Replacement reuses unchanged compiled relations. Invalid replacements leave the
current map untouched; there are no partial updates or per-key load failures.

`Evaluator.Begin` captures the map with one atomic read. All relation lookups
use that snapshot, including a child or previously absent relation first read
after replacement. Evaluations neither block writers nor take a cache lock.
Old maps are reclaimed when their last evaluation reference disappears; current
definitions remain resident. No load pool, cache sweeper, provider callback, or
external invalidation channel is needed.

Replacement notifies watchers only for changed, added, or removed relations.
Dependents reevaluate at valid source boundaries, including idle projections.
Registration compares the captured compiled definition (including absence)
against the current map under the publication lock, closing the registration
race. Compilation never holds that lock. Updates do not synchronously wait for
all projections to reevaluate, and policy changes have no required WAL ordering
relationship. A pass combines one pinned definition set with its explicitly
consumed data view.

Each store is an independent definition domain, not a tenant registry. Hosts
may give projection servers separate stores while sharing a query backend.

## Performance checks

`go test ./internal/relation -run '^$' -bench 'BenchmarkPolicyEvaluation|BenchmarkRecordAccess|BenchmarkNativePolicyOperations' -benchmem`
separates precompiled/prebound CEL execution from canonical-value adaptation and
evaluator bookkeeping, with a handwritten Go predicate as a lower-bound baseline.
The same compiled rule runs through both the optimized evaluator and forced CEL
reference evaluator for a direct comparison.
Compilation, provider IO, fact loading, and projection traversal are excluded.
Small and wide records expose adaptation costs even when rules read few fields.
The record-access benchmark compares the decorator with ordinary CEL under the
same cost tracking; regression tests also require unchanged CEL operation counts
for nested field paths, dynamic indices, presence checks and comprehensions.
