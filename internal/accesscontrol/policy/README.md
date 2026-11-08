# Visibility policy evaluator

This is the internal, compiled-rule portion of the authorization runtime.
For application setup, start with the
[projection service's visibility guide](../../../pkg/projectionserver/README.md#visibility-and-trusted-claims).
The details below are for policy authors and contributors.

Applications provide immutable, versioned relation policies through
`pkg/accesscontrol/policy.Provider`. `Definitions` remains a fixed-map adapter;
omitting authorization options still uses allow-all. The application owns
policy storage, tenancy, and authenticated claims. Compilation, lazy dependency
discovery, invalidation, and publication scheduling are library-owned.

## Responsibilities

- Compile CEL rules into private, immutable programs.
- Validate stable rule/fact/edge identifiers and configurable definition limits.
- Check explicitly configured, directional parent-to-child propagation edges.
- Interpret a nil receive list as unrestricted, an empty non-nil list as none,
  and other lists as explicit incoming edge allowlists.
- Evaluate mandatory requirements before propagation or direct allow rules.
  All requirements must pass; a false requirement stops grant discovery.
- Discover direct allow facts only when propagation has not granted access.
- Withhold visibility while any requirement or the grant remains unresolved.
- Return resolved/visible separately, rather than treating loading as denial.
- Bind fact selectors to the existing canonical query/value representation.

Rules are CEL boolean expressions over `item`, `claims`, and named `facts`.
Propagation expressions additionally receive the immediate `parent`, but cannot
discover facts. Incoming edges are scoped by declaring relation and edge ID.

Visibility requires `ALL(Require) AND (propagated OR ANY(Allow))`. An empty
requirement list adds no constraints, but never grants access on its own. Rules
run in definition order and a false requirement skips the remaining requirements,
propagation, and allow rules. An unknown requirement is not a failure: other
requirements and grants can discover their evidence while it loads, but a grant
cannot publish until every requirement passes. A definitive missing grant or
failed requirement resolves rejection even when other evidence is pending.

Requirements can short-circuit expensive fact discovery. For example:

```cel
item.restricted != true || facts.grants.exists(g, g.item.user_id == claims.user_id)
```

Unrestricted items do not load these facts. Restricted items must satisfy the
explicit grant check even when they receive propagated access. This replaces
negative deny rules: an old `Deny` condition `D` becomes a `Require` condition
`!(D)`; there is no parallel deny-rule API.

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
reads only referenced canonical fields and invokes `policy/value` operations
directly. It does not construct CEL scalar, map, or list values. The expression
interface returns policy truth and work usage, with no CEL types.

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

`internal/projection/authorization` owns bounded asynchronous loading,
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

## Policy versions and cache

Each publication view fetches a relation's policy at most once and retains its
immutable compiled version for the pass. Every new pass calls `GetPolicy` again;
the shared cache reuses compilation when the version is unchanged. Versions
must uniquely identify content per key across all callers. A versioned nil
policy means deny by default. Returned definitions must not be mutated.

Provider calls and compilation happen outside the shared cache lock. A bounded
load pool limits concurrent work. Active dependencies retain compiled entries;
unreferenced entries expire through an indexed heap in bounded batches. Older
passes retain their immutable references safely after replacement.

Optional `Invalidations` broadcasts changed relation keys; the provider need
not remember who requested them. Only dependent occurrences are retried at a
valid source boundary, including idle projections. Registration stamps close
the fetch/register race. Without hints, new evaluations adopt updates, but an
otherwise idle projection may remain unchanged. Policies have no required WAL
ordering relationship: a pass combines its pinned policy versions with its
explicitly consumed data view.

## Performance checks

`go test ./internal/accesscontrol/policy -run '^$' -bench 'BenchmarkPolicyEvaluation|BenchmarkRecordAccess|BenchmarkNativePolicyOperations' -benchmem`
separates precompiled/prebound CEL execution from canonical-value adaptation and
evaluator bookkeeping, with a handwritten Go predicate as a lower-bound baseline.
The same compiled rule runs through both the optimized evaluator and forced CEL
reference evaluator for a direct comparison.
Compilation, provider IO, fact loading, and projection traversal are excluded.
Small and wide records expose adaptation costs even when rules read few fields.
The record-access benchmark compares the decorator with ordinary CEL under the
same cost tracking; regression tests also require unchanged CEL operation counts
for nested field paths, dynamic indices, presence checks and comprehensions.
