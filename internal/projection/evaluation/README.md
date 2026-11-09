# Projection evaluation dependencies

This runtime powers lazy computed values and visibility in the projection server.
Both use the same ordered evidence view, dependency leases, immutable relation
versions, and occurrence-level invalidation. Selected values are retained outside
shared entities and use the same pending/publication gate as visibility.

For application configuration, see the
[projection service guide](../../../pkg/projectionserver/README.md#computed-values-visibility-and-trusted-claims).
This document describes internal evidence ownership and publication scheduling.

## Evidence and ownership

`Facts` owns one consumer's ordered, current fact replica. In the live server it
borrows the projection's ordinary query feed: the owner observes each complete
frame before applying public projection effects. Fixed fact selectors share
upstream work through the process-shared dependency cache, while consumers keep
independent processing positions. There is no dependency on query engine
internals or a storage-specific API, and no second stream to synchronize.

`Scope` materializes private fact projection plans and owns their leaf selector
leases for one projection occurrence. Binding uses the same planner as public
projections. Each evaluation
records the selectors it actually used, retains new ones, and releases unused
ones. Two occurrences or subjects share evidence only when their bound queries
are identical. Decisions and propagation evidence are never shared that way.
Pruned dependencies can be discovered again on a later evaluation.

## Scheduling and synchronization

- `Acquire` registers work without IO. A bounded loader pool creates ordinary
  query subscriptions, coalescing identical selectors within the consumer.
- `Observe` is driven by the projection's single feed consumer. It applies one whole ordered
  envelope before exposing the result for evaluation. It must not run ahead
  concurrently with evaluation of a transaction.
- `Lease.Read(position)` requires the explicitly consumed source position and
  a snapshot baseline no newer than that position. It does not return newer
  facts to satisfy an older read. No historical lookup is provided.
- Snapshot installation wakes `Changed`; it does not invent a transaction or
  advance source progress. `RequestProgress` forwards the existing query-feed
  report operation, not a WAL-write/barrier operation.
- `DrainChanges` returns selector IDs for targeted occurrence invalidation.
  Wakeups and dirty IDs are hints to reevaluate at a valid ordered boundary,
  not permission to evaluate a previous transaction using newly loaded facts.
- Feed ordering/backpressure errors invalidate the consumer. A selector error
  affects only scopes using that selector. Errors never become negative facts.

Selectors use full `START` to `END` coverage. Initialization requests one more
than the configured row bound so truncated coverage can be rejected explicitly.
Growth past that bound also fails that selector. This avoids treating a soft,
count-limited window as proof that no grant exists after its first row leaves.
The configured bound plus one must fit the query backend's window limit.

`Session` indexes selector dependencies back to concrete projection occurrences,
including separate paths through diamonds. `View` memoizes immutable evidence
for one evaluation position and pins one complete definition map per pass; it
never invents historical database reads. Atomic definition replacement generates
targeted relation invalidations, using a separate occurrence index and the same
ordered retry/publication path.

Candidate context uses immutable, parent-linked `accesscontrol.ProjectionPath`
nodes. Each publication pass captures a concrete occurrence's record once;
siblings share captured ancestors, while separate occurrences of the same
entity remain distinct. A transaction overlay supplies final parent images.
Missing or removed ancestors retain unresolved path steps rather than shortening
the required path. New passes capture new nodes, so older asynchronous checks
keep their original images. `ProjectionPath.Steps()` iterates root first without
building a flat path; records exposed to policy implementations are read-only.
The built-in evaluator uses a view-local authorization DAG. A missing parent is
established top down once; each child then checks only its immediate edge and
own requirements and grants. Siblings reuse the same concrete parent's result, while
diamond occurrences remain distinct. Denied or unresolved parents prune child
policy execution. Decisions are never reused across subjects, independent
views, or replacement leaf images supplied by explicit invalidation candidates.
The view pins discovered selector leases while results can be reused. Each
occurrence separately retains the visited ancestor selectors and policy stamps,
so closing a view or another occurrence cannot lose revocation notifications.
Closing the view releases its graph and selector ownership. No historical cache,
cross-view decision cache, public interface, or wire change is introduced.
Async readiness causes an existing-progress report, followed by reevaluation
at a valid current boundary. Unknown evidence is not a negative grant.
`FactChangesAfter` distinguishes new evidence notifications from decisions that
are merely still unresolved. Initial projection admission retains a private
visibility image and applies coalesced changes, retrying affected occurrences
instead of recopying and reevaluating all rows on each ordinary update. Direct
queries retain their gate in the same way. Crossing an installed fact snapshot's
baseline (by a commit or a progress report) emits a targeted readiness edge.
Repeated progress and still-loading selectors do not retry all pending checks.

Initial snapshots wait for required checks. During live loading the publication
component keeps the old public model, consumes updates privately, and later
reconciles with a current authorized image. Slow or failed clients never block
the query feed or CDC. No general MVCC cache or per-client replay log is used.

Opted-in partial failures retain visited fact leases and definition stamps even
when expression evaluation errors. That lets subsequent evidence/definition
updates reevaluate the failed occurrence. Failed leaf subscriptions retry at the
configured recovery cadence with new upstream IDs but stable dependency IDs.
Their replacement snapshots remain fenced, and old views retain their memoized
errors. A dependency-registration recheck also covers errors replaced during an
in-flight evaluation. Already-failed branches can resolve privately without
holding healthy siblings; newly pending checks retain the publication hold.

The publication gate merges changed rows, explicit authorization invalidations,
and affected descendants before evaluating them in one parent-first pass. Each
final occurrence is checked once per delivery, including when several dependency
changes target it. Policy-only candidate images never replace retained row data.
Superseded intermediate images reserve raw insertion positions without changing
visibility or publishing payloads; they are not authorization denials. Real
remove/reinsert operations still produce membership moves at the final rank.
Older invalidations cannot rewind a separately initialized newer snapshot.
Visibility delivery has separate installation and publication phases: create
topology, apply all raw memberships, retain final records, and install all policy
decisions before propagating effective visibility. A depth-ordered work queue
processes changed instances and affected descendants; unchanged windows are not
scanned. Instance reveals/hides visit their covered rows as needed. Removals are
emitted child-first before parent-first additions, and membership deltas use the
completed final state. No deferred membership can trigger a partial reveal.
Admission-mode readiness carries no discarded initial image; admission captures
its own coherent cut. Output assembly borrows immutable visibility images until
the connection model takes ownership, rather than cloning records between those
two synchronous steps. Source capture and mutable-model ownership boundaries
still deep-copy records.

## Performance measurements

Run the stage and initialization microbenchmarks with:

```sh
go test ./internal/relation ./internal/projection/evaluation ./internal/projectionserver \
  -run '^$' -bench 'BenchmarkPolicyPreparation|BenchmarkFactStages|BenchmarkProjectionStages|BenchmarkProjectionInitialization' \
  -benchmem -benchtime=200ms -count=3
```

`BenchmarkFactStages` separates binding, selector fingerprinting, view-memoized
reads, fresh-view copies, cached expansion assembly, complete cached authorization,
and cold discovery through two asynchronously installed queries. Its 1/16 link
rows all resolve the same project, so underlying child queries are shared.
`AuthorizeCachedSharedView` reuses one evaluation view; `FreshView` includes
capturing a new ordered evidence image and pinning policies. The cold case also
includes unresolved evaluation attempts, worker coordination and lease cleanup.

`BenchmarkProjectionStages` measures definition decoding, visibility bookkeeping
with allow-all, normalized model construction, protobuf encoding and single-row
publication updates. Initial visibility/model/encoding stages use flat task
lists; updates reuse an existing materialization, not a new projection.

`BenchmarkProjectionInitialization` runs the production snapshot endpoint with
one project, N association rows and N expanded tasks. Policy variants compare
allow-all, a project grant fact followed by propagation, direct nonempty-ID
checks at every node, a project ID check followed by propagation, and an
unconditional root allow followed by propagation. The latter retains the real
parent/child ID checks on propagation edges; these are performance fixtures,
not application security policies. Warm runs
reuse entity/fact dependencies; the mutable list still creates one upstream
subscription. Cold runs use a fresh service/cache each time; construction and
service shutdown are outside the timed section. Upstream load counts and output
sizes are checked. Definition-snapshot capture and evaluation have a separate
`BenchmarkPolicyPreparation` measurement; compilation is outside the read path.

All upstreams/definitions in these benchmarks are in-memory, with no artificial
delay. They measure local processing and allocation, not PostgreSQL execution,
networking, WAL-fence latency, socket writes or browser work. Stage timings are
independent workloads (some nested), not additive percentages of initialization.
