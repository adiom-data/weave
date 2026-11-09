# Query-engine benchmark

Self-contained, headless comparison of Weave's production query-subscription
engine, Electric's HTTP shape service, and Drasi's continuous-query service. It
does not run the sample app, browser runtime, or authorization policy. The optional topology comparison also
runs the production projection service, with allow-all visibility.

## Run

Requires Docker with Compose v2, Bash, and Git. From the repository root:

```sh
# Quick correctness smoke test, all three backends run sequentially.
./benchmarks/query/run.sh all --clients 4 --projects 4 --rows 10 --rate 20 --warmup 0s --duration 3s

# Many clients sharing a project/query.
./benchmarks/query/run.sh both --scenario shared --clients 100 --projects 1000 --rows 100 --rate 100 --warmup 2s --duration 30s

# Each client subscribes to a distinct project/query.
./benchmarks/query/run.sh both --scenario independent --clients 100 --projects 1000 --rows 100 --rate 100 --warmup 2s --duration 30s
```

Use `weave`, `electric`, or `drasi` for one backend, `both` for Weave/Electric,
or `all` for all three. For example:

```sh
./benchmarks/query/run.sh drasi --scenario shared --clients 100 --projects 1000 --rows 100 --rate 100 --warmup 2s --duration 30s
```

Each invocation builds
local Go binaries, creates a fresh private Compose stack for each backend,
seeds it identically, waits for readiness, runs the load, verifies correctness,
and removes only that invocation's containers and volumes. No host ports are
published and the sample application's database/volumes are not used. The
runner refuses database names other than `weave_query_benchmark`, and seeding
only creates new objects: it never truncates or replaces an existing schema.
Benchmark volumes are disposable; results and images remain after cleanup.

Defaults are 2 CPUs and 2 GiB **per container** (engine, PostgreSQL, generator).
Override before invoking the script:

```sh
BENCH_ENGINE_CPUS=4 BENCH_ENGINE_MEMORY=4g ./benchmarks/query/run.sh both --clients 500
```

Corresponding prefixes `BENCH_POSTGRES_` and `BENCH_GENERATOR_` control the other
containers. PostgreSQL and Electric image manifests are pinned in
`compose.yaml`; `BENCH_POSTGRES_IMAGE` / `BENCH_ELECTRIC_IMAGE` can override them.
Drasi's base image and source/bootstrap/SSE plugins are pinned in `drasi/` and built
only when selected; see the adapter notes below before updating those pins.
Weave and Electric use a query pool size of 20. Run on native architecture, without
emulation, and give Docker enough resources for all three containers. Do not
run concurrent benchmark invocations (they share local image tags).

Useful bounds/options: `--rows` 1–999 (entire project must fit a Weave window),
`--payload-bytes` 1–65536, `--init-concurrency` (default 32), `--init-timeout`
(default 2m), and `--drain-timeout` (default 30s). Independent mode requires at
least as many projects as clients. Total planned warmup + measured writes are
capped at one million to bound measurement memory. Initialization concurrency
limits snapshot requests, not active clients. The wrapper owns endpoints and
backend selection; workload flags cannot redirect it to external servers.

## Embedded versus remote query backend

```sh
./benchmarks/query/run.sh topology --scenario shared --clients 100 --projects 1000 --rows 100 --rate 100 --warmup 2s --duration 30s
```

`weave-embedded` serves a single-table task-list projection with an in-process
query backend. `weave-remote` serves the identical projection with the query
backend in another container, reached over the production gRPC client. Both
clients use the same public projection gRPC stream and select all four columns;
only the internal query boundary changes. Use either backend name individually
to reverse the order across repeated runs. These are separate from the original
`weave` query-only baseline; do not attribute projection overhead to transport.

The remote run samples **both** projection and query containers. Limits remain
per container, so remote has two engine containers versus embedded's one. This
is a hop/serialization and deployment-cost comparison, not an equal-total-CPU
capacity test. It uses Docker networking on one host, not a cross-host network.
The existing every-revision and final-state oracle runs unchanged. This workload
does not yet exercise nested N+1 expansions or access-control facts.

## Many-to-many projection expansion

The PostgreSQL integration benchmark compares an ordinary
`project → links → tasks` projection with the same projection using
`@fetchReference`. Point it at a **disposable** PostgreSQL 15+ database with
logical replication enabled; each sample creates and removes its own schema,
publication, and replication slot:

```sh
WEAVE_TEST_DATABASE_URL='postgres://postgres:password@localhost:5432/benchmark?sslmode=disable' \
  go test ./internal/projectionserver -run '^$' \
  -bench '^BenchmarkReferenceExpansion$' -benchtime=1x -count=3 -timeout=10m
```

Each cold sample starts a fresh backend. Warm samples retain an initial viewer
and time a second viewer, with all entity/query coverage already loaded. Timing
ends at `Ready` with `wait_for_sync=true`; each result checks the expected link
and entity counts. Cases contain 10, 100, or 500 tasks with 256-byte titles and
use allow-all visibility. Setup and warm-up are excluded. Allocation metrics
include concurrent engine work and are allocated bytes, not retained memory.

`embedded` uses the in-process query backend; `grpc` uses the production query
codec over an in-memory gRPC connection. This isolates serialization overhead,
**not** a separate-process/network deployment. The public projection stream is
in-process in both cases. Operation counters distinguish joined loads, batched
target loads, ordinary target/link reads, and individual child preparations;
the latter do not count metadata preparation performed inside `PrepareReference`.
This is an initial-expansion benchmark, not a write-capacity, authorization,
window-extension, or partially warm coverage benchmark.

### Query-layer comparison with Electric

```sh
./benchmarks/query/run.sh both --workload expansion --reference-fetch \
  --clients 10 --projects 100 --rows 100 --payload-bytes 256 --init-concurrency 1

# Ordinary Weave, without the joined loading hint.
./benchmarks/query/run.sh weave --workload expansion \
  --clients 10 --projects 100 --rows 100 --payload-bytes 256 --init-concurrency 1
```

This mode uses the **public query service**, not the projection service, over
real container networking. No projection runtime, authorization, browser, or
Drasi instance runs. It measures initial materialization of `links → tasks`:

- Weave opens a filtered association window, then an entity handle per discovered
  task. With `--reference-fetch`, the parent owns the target interests and returns
  their snapshots together; the subsequent handles are local views, not additional
  subscription RPCs. Without the hint, child RPCs remain sequential.
- Electric reads the association shape, then one batched task shape using
  `id = ANY('{…}'::bigint[])` with the discovered IDs. The typed array avoids a
  large scalar `IN` expression; Electric is not forced into N individual shapes.

Here `--clients` means the number of **sequential cold/warm pairs**, not concurrent
viewers. Every pair uses a distinct project and task IDs; the warm read requests
the same data over a new connection. Weave's first viewer stays open during the
warm read. Electric reuses its existing shape logs. The server and PostgreSQL
remain running between pairs, so "cold" means uncached query/data coverage, not
a freshly restarted database. The first pair also includes first-request startup
costs, which remain visible in the per-pair samples.

The mode requires `--init-concurrency 1` and at least as many projects as pairs.
It always selects distinct projects regardless of `--scenario`; write-rate,
duration, warmup, and drain flags do not drive this read-only workload.
`result.json` contains separate cold/warm distributions plus exact per-pair
latencies and payload-byte counts. All association IDs, target IDs, payloads,
and revisions are validated. Docker caps, image pinning, resource sampling,
and isolated cleanup are the same as the original harness.

Weave samples also report `cold_parent_ms` and `warm_parent_ms`: time to receive
and validate the association subscription, **before any task subscription is
requested**. With `--reference-fetch`, that includes joined prefetch/admission
of the target data, but not delivery of task payloads or child subscription
setup. The harness then opens and validates every task as usual, recording the
full latency separately. This phase measurement is diagnostic, not equivalent
to Electric's complete two-shape materialization time.
`cold_parent_read` and `warm_parent_read` summarize this phase separately on
Weave, including p50/p99 and sample count (zero samples on Electric).

Both sides return the same rows, but not the same subscription topology:
Weave maintains individual child subscriptions while Electric has one batched
target shape. This measures the cost of using the available query APIs for
expansion, not equivalent RPC counts. It does **not** test association churn,
updates, cross-query transaction atomicity, paging, or maximum capacity.

### Window extension and contraction

```sh
./benchmarks/query/run.sh both --workload windows \
  --clients 200 --projects 200 --rows 500 --payload-bytes 256 \
  --init-concurrency 1 --init-timeout 10m
```

This read-only workload measures eight steps per project, using a persistent
connection for all steps. If `N = floor(rows/4)`, the initial range is the first
N rows; steps extend right to 2N, contract right to N, re-extend to 2N, contract
left to `(N,2N]`, re-extend left, atomically shift to `(N,3N]`, and return to
`(START,2N]`. Each project is distinct; repeated ranges within it are warm.
The client verifies exact IDs, payloads, revisions, and counts at every step,
and validates ordered membership deltas on Weave. Timing includes decoding,
applying the response, and validation. Initial timing starts after feed creation;
Weave's lazy connection handshake may still occur in its first request.

**These are different API operations, not equivalent implementations:**

- Weave uses one live query and its native `UpdateWindow` operations. Each step
  waits for the matching window transition and applies its incremental changes.
- Electric requests a bounded shape for each new visible range using its normal
  shape API. Revisiting a range reuses its shape log, but each response supplies
  the full range. It does not emulate native mutable windows, use subset
  snapshots, retain a client-side full-project cache, or measure local-only
  contraction. This is a bounded-shape replacement baseline, not a claim that
  it is Electric's optimal application-level pagination strategy. See the
  [Electric API](https://electric-sql.com/docs/api/http) for subset alternatives.

No writes run during resizing, and this does not benchmark a synchronized switch
between Electric live shapes. Connection cleanup is outside the measured steps.
`result.json` explicitly identifies the mode, and reports each step's p50/p99,
sample count, exact samples, row counts, and received bytes. As in expansion,
`--clients` counts sequential projects, not concurrent viewers; projects must
be at least clients, initialization concurrency must be one, and rows at least
four. Reference-fetch hints are not used for this single-table workload.

Use at least 200 samples for tail comparisons. Ten samples make p99 essentially
the maximum; even 200 samples are exploratory, not a stable tail-latency SLO.
Histogram percentile bounds are within 2% (and capped by the exact maximum).

### Parent-only CPU/allocation diagnostics

`BenchmarkWarmQueryParent` exercises the production query backend directly,
with no projection or task subscriptions. It compares local calls with the
production gRPC codec over `bufconn`, reusing the gRPC connection and opening a
fresh feed per read. The first parent stays live to pin coverage. The fixture
requires a disposable `weave_query_benchmark` database already seeded in
expansion mode with 100 projects, 500 rows/project, and 256-byte payloads. It
creates/removes its own replication slots, not the tables or data.

```sh
WEAVE_TEST_DATABASE_URL='postgres://postgres:password@localhost:5432/weave_query_benchmark?sslmode=disable' \
GOMAXPROCS=2 go test ./benchmarks/query/internal/bench -run '^$' \
  -bench '^BenchmarkWarmQueryParent/grpc/rows=500/hint=true$' \
  -benchtime=1500x -cpuprofile=/tmp/parent.cpu -memprofile=/tmp/parent.allocs
```

The Dockerfile's optional `profile` target builds this test binary for controlled
container runs; profiling does not add any endpoints/hooks to the production
engine. `parent-ns/op` excludes feed cleanup; standard `ns/op`, allocation
metrics, and profiles include cleanup and background work. gRPC profiles include
both client and server in the same process; they do not measure network RTT or
fresh connection setup. `prepare-ns/op`, `reference-prepare-ns/op`, and
`bind-ns/op` measure the corresponding adapter calls within the parent latency.
`resolve-ns/op` / `resolves/op` measure lightweight reference lookup binding.
Warm hinted parents establish one internal entity interest per distinct bound
selector (`binds/op`), resolving one identity per parent row. Child handles reuse
these interests rather than repeating preparation or issuing RPCs. Primary-key
interests do not allocate query window trees.
Every warm operation verifies the ordered parent image; the benchmark fails if
warm reads unexpectedly open a database snapshot. Allocated bytes are not
retained memory, and cumulative profile percentages must not be summed.

`BenchmarkWarmQueryExpansion` adds sequential local child entity handles and
checks their complete records. `parent-ns/op` measures the parent phase;
`children-ns/op` measures the remaining child phase; `read-ns/op` is their total,
excluding feed cleanup. `BenchmarkWarmQueryBatch` is a topology diagnostic:
this fixture's contiguous IDs allow the same records to be returned through
two list subscriptions instead of a parent plus N entity subscriptions. It is
not a generic replacement for many-to-many expansion, and the batched task
selector differs from Electric's ID-array selector. Both diagnostics reuse
the database and transport setup described above and require zero warm data
snapshots. This does not imply zero database IO: ordinary query preparation
still validates relation/publication metadata against PostgreSQL.

### Network tail-latency diagnostics

```sh
BENCH_DIAGNOSTICS=1 BENCH_DIAGNOSTICS_TRACE=1 \
BENCH_DIAGNOSTICS_CPU=1 BENCH_DIAGNOSTICS_HEAP=1 \
./benchmarks/query/run.sh weave --workload expansion --reference-fetch \
  --clients 200 --projects 200 --rows 500 --payload-bytes 256 \
  --init-concurrency 1 --init-timeout 10m
```

This opt-in mode instruments only benchmark binaries, with no production
engine hooks or profiling endpoints. The results directory includes separate
`server`/`runner` `.trace`, `.cpu`, `.heap`, and `.samples.jsonl` files according
to enabled options. CPU/trace/heap capture are independently optional. Runtime
and cgroup CPU counters are sampled every `BENCH_DIAGNOSTICS_PERIOD` (default
100ms). Traces include wall-clock anchors and server subscription tasks;
expansion samples include feed/parent phase timestamps and individual child
request timings. Heap profiles reflect the latest sampled GC state, not a
forced collection. Trace/profile overhead means these runs are diagnostic,
not interchangeable with uninstrumented latency measurements.

Use `go tool trace path/server.trace` for scheduling/GC timelines and
`go tool pprof -top path/server.cpu` or `-sample_index=inuse_space path/server.heap`
for CPU/retained-heap attribution. Concurrent GC phase duration and aggregate
scheduler delay are **not** stop-the-world pause duration or request wall time.
`BENCH_ENGINE_GOGC` changes only the diagnostic server's GC percentage (default
100), allowing controlled experiments without changing production defaults.
The overlay currently supports the query-only `weave` backend.

`BenchmarkLogicalBarrierFlush` separately compares unflushed/flushed logical
messages on a disposable PostgreSQL 17+ `weave_query_benchmark` database. It
writes no tables, creates no slots, and polls the WAL flush frontier after
each message; polling adds approximately millisecond resolution:

```sh
WEAVE_TEST_DATABASE_URL='postgres://postgres:password@localhost:5432/weave_query_benchmark?sslmode=disable' \
go test ./benchmarks/query/internal/bench -run '^$' \
  -bench '^BenchmarkLogicalBarrierFlush$' -benchtime=10x
```

## Workload and correctness

The isolated database contains `public.tasks(id bigint PRIMARY KEY, project_id bigint,
revision bigint, payload text)` with `(project_id, id)` indexing. Each client
subscribes to all columns of `WHERE project_id = X`. Weave uses a real gRPC
connection/feed per client with default primary-key ordering and a full-project
cursor window. Electric uses a separate HTTP connection per client, direct
uncompressed shape requests, and the documented offset/handle/cursor long-poll
protocol. It is a small Go protocol adapter, not Electric's TypeScript SDK.
No CDN, proxy, browser, or client-side subscription sharing is involved.

After every initial client snapshot is validated, a single sequential SQL writer
updates existing rows' revisions round-robin. Each write is its own committed
transaction, with a globally increasing revision used **only by the benchmark
oracle**; the engine does not require version columns. Shared mode writes one
project, so each write should reach every client. Independent mode cycles through
clients' projects, so each write should reach one client. Unsubscribed projects
remain seeded to prevent every predicate from matching the entire database.

Every intermediate update must arrive exactly once at the expected clients,
with strictly increasing per-row revision. Unexpected rows, duplicate receipts,
resynchronizations, failed streams, and missing deliveries fail the run. After
writes stop, clients drain and their complete materialized state is compared
with PostgreSQL. There are no hidden reconnect/retry loops that could conceal
data loss. This measures update delivery, not just eventual convergence.

## Output and interpretation

`results/<timestamp-pid>/<backend>/` contains:

- `result.json`: configuration, status, initialization distribution and wall
  time, write-start-to-client-apply latency (all deliveries, first viewer, last
  viewer), SQL acknowledgment latency, scheduling lag, achieved rate, byte count,
  and correctness counts.
- `resources.jsonl`: periodic Docker CPU, memory, network, and block-I/O samples
  for engine, PostgreSQL, and generator. Very short runs may have few samples.
- `containers.json` / `compose.yaml`: resolved images, architecture and limits.
- `revision.txt`: Git revision, diff summary, and worktree status. Dirty/untracked
  source is reported, not archived; commit source for reproducible comparisons.
- `services.log`, `runner.log`, `resources-errors.log`: diagnostic logs.

Latency uses the load generator's monotonic clock from immediately before SQL
execution through applying the received update locally. It includes database
write/commit and network time: it is **not** pure commit-to-delivery latency.
Warmup writes are verified but excluded from latency and throughput statistics.
Quantiles use bounded histogram buckets (approximately 2% spacing); min/max are
exact. The scheduler targets fixed arrival times; it does not silently reduce
the requested rate when SQL is slow. `writer_limited` flags insufficient achieved
rate or excessive scheduling lag. A writer-limited run cannot establish engine
capacity. Watch generator and PostgreSQL utilization as well as engine usage.

Application bytes are approximate protocol-payload measurements: Weave counts
inbound gRPC message bytes including framing, Electric counts uncompressed HTTP
response bodies; Drasi counts REST response bodies and SSE framing/data, including
query setup. They exclude HTTP headers and TCP/TLS overhead and are not
precisely comparable on-wire traffic. Docker network statistics include traffic
to PostgreSQL. Electric's persisted shape log and Weave's in-memory query cache
also have different persistence costs; resource measurements retain that cost.

Start with correctness smoke runs, then increase one dimension (clients, distinct
queries, write rate, rows, payload) at a time. Repeat each configuration and
alternate backend order using separate invocations to control host noise.
Docker Desktop is useful for exploration; use a dedicated Linux host for capacity
claims. Each backend starts fresh; this is not a warmed-cache resubscription test.

V1 deliberately excludes inserts/deletes, window churn, partial/overlapping
windows, complex predicates, reconnects, projection/auth work, and production
durability equivalence. Smoke tests validate the harness, not a performance
winner or maximum sustainable capacity.

## Drasi adapter

Drasi Server is pinned to **0.2.4** by image digest. Its PostgreSQL source,
bootstrap, and SSE reaction plugins are independently pinned by platform-specific
OCI digests and file hashes in `drasi/plugins-{amd64,arm64}.lock`, taken from the server release.
The Docker build installs them with `--locked` and signature verification;
startup does not resolve newer plugins. Only native amd64/arm64 are supported.

This adapter currently supports the **flat live-update workload**. It creates
one persistent Cypher query per distinct subscribed project using PostgreSQL CDC
and snapshot bootstrap. Shared viewers attach to the same query; independent
viewers use separate queries. A standard SSE reaction uses native per-query routes
and JSON templates to expose each query on its own path. It is configured with
auto-start disabled and started explicitly once. No custom plugin, client-side
query engine, TypeScript process, or broadcast-and-discard filtering is involved.
The server's alternative native `/attach` endpoint was observed delivering the
same query sequence twice in a four-viewer smoke test; it is not used here.

Initialization differs from Weave/Electric: persistent query creation and
bootstrap run before viewers attach. `backend_setup_seconds` reports that phase;
`initial_wall_seconds` includes **both** setup and viewer attachment.
The `initial_sync` distribution measures viewer attachment and snapshot retrieval,
not cold query creation, so do not compare that distribution alone as equivalent
cold-read latency. Query setup is sequential and shares the initialization timeout.
Because Drasi reports its source Running before establishing replication, setup
also waits for its fresh replication slot to be active with a confirmed WAL
position. This happens after subscribing queries (capture starts lazily), and
before allowing writes. No sleep or unmeasured warm-up mutation substitutes for
this capture readiness check. Logging is set to error to avoid a warning for
every normal default-replica-identity update in the pinned PostgreSQL plugin.

The release's separate result-snapshot endpoint and SSE reaction are used only
under this workload's **quiescent initialization**: no writer starts until all
clients are ready. Each client opens its stream, reads the already-materialized
query results, and verifies the entire seed before writes begin. This does not
claim a general race-free snapshot-plus-stream handoff under concurrent writes.
Exact revision-zero seed ADDs still queued from bootstrap are ignored; every
positive-revision duplicate, omission, ordering violation, or stream failure
still fails the original oracle. There are no update polling/reconnect retries.

The PostgreSQL bootstrap provider is table/label-oriented, not an indexed cursor
window read. Distinct-query startup may therefore read substantially more source
data than Weave; that real architecture cost remains in setup time and resource
samples. Expansion and mutable-window workloads explicitly reject Drasi rather
than silently implementing different operations.

All backends now use the same `public` fixture tables because the pinned Drasi
bootstrap plugin only resolves that schema. Earlier recorded runs used `bench`;
the columns, indexes, data distribution, and write workload are unchanged.

Zero's adapter and deployment have been removed. Historical result directories
are retained as records of their original versions.

Pinned protocol references:
[server API implementation](https://github.com/drasi-project/drasi-server/blob/0.2.4/src/api/shared/handlers/query_handlers.rs),
[SSE reaction implementation](https://github.com/drasi-project/drasi-core/blob/22125bf1d66062533b832a166fe4a51079a23d6e/components/reactions/sse/src/sse.rs).

## Tests

```sh
go test -race ./benchmarks/query/...
```

Unit tests cover measurement accounting, validation, latency histogram bounds,
Electric continuation/partial-update/error handling, and Drasi's persistent-query
setup/routing, SSE framing, exact numeric decoding, cancellation, and failures. Docker smoke runs test
the real end-to-end services. Electric protocol references:
[HTTP API](https://electric.ax/docs/sync/api/http) and
[deployment](https://electric.ax/docs/sync/guides/deployment).
