# Query server library

`Backend` owns shared reactive computation. `Register` mounts the query and
transaction-message protocols, but does not expose management RPCs. An application
can optionally mount [`pkg/management`](../management/README.md) behind its own
administrative authentication. The standalone host provides a disabled-by-default
configuration for that service.

Construct storage independently, then pass it to the query server:

```go
store, err := storage.NewPostgres(ctx, databaseURL, storage.PostgresOptions{
    Publication: "app_publication",
    Slot: "app_reactive",
})
if err != nil { return err }
defer store.Close()

queries, err := queryserver.New(ctx, store, queryserver.Options{})
if err != nil { return err }
defer queries.Close()

projections, err := projectionserver.New(ctx, queries, projectionserver.Options{
    TransactionMessages: queries.Messages(),
    AccessController: policy,
    ClaimsProvider: claimsProvider,
})
if err != nil { return err }
defer projections.Close()
projections.Register(grpcServer)
```

Each component accepts only its own configuration. The public packages forward
constructors and intentionally supported contracts to internal implementations.
The query server has no PostgreSQL dependency: it consumes `storage.Storage`.
Its coordinator, shared caches, routing indexes, and subscription delivery remain
private. No loopback transport or alternate embedded fast path is involved.

### Exact entity selectors

`SubscribeEntity` uses a direct identity index when the adapter proves that
positive equality conjuncts fix every primary-key column. Additional predicates
still control membership. Equalities under OR/NOT do not establish identity.
Other selectors retain count-one query/candidate behavior, including ambiguous
matches. The public subscription and transaction-delivery interfaces are the same.

Exact selectors have one candidate slot and direct key routing, with no interval
or ordered-membership trees. A cold read fetches the whole entity in one snapshot;
load ownership is captured before IO and admission waits for its source fence.
Current cached records and known absence can be reused. Idle selector state uses
`QueryIdleTTL` and bounded maintenance, just like abandoned query coverage.

Reference expansions load fenced target records and establish parent-owned
entity interests. Each initial query result is a `QueryRow`: its `Row` is the
source record and its optional `Expanded` snapshot is the zero-or-one target.
An attached snapshot with no target row is a loaded absence; no attached snapshot
means no image is available. Repeated targets have an image per source row on the
wire, while their cached entity and live interest remain shared internally. Live
changes travel as entity effects in the same transaction envelope as the
parent's query effects. Explicit subscriptions and parent-owned interests use
the same standalone `EntityInstallation` and `EntityDelta` payloads; only
`EntityTarget` routing differs. Initial row expansions need no routing wrapper.
Binding resets affect reuse/ownership, not entity data. Matching `FetchFrom`
entity handles reuse these interests locally, without another subscription RPC.
Closing or contracting the parent releases the affected interests. The ordinary
entity cache and selector machinery remain shared with explicit subscriptions;
existing current images take priority over older prefetched snapshots.

Live reference discoveries use a deduplicated, bounded per-feed load queue.
`Options.References.LoadConcurrency` and `LoadQueueLimit` configure it (defaults:
4 workers and 1024 queued interests). Already-loaded interests attach directly.
Overload fails the affected parent and requires resynchronization; CDC never
waits for queue space. Retirements use a separate bounded cleanup worker; if
cleanup itself overflows, the feed is closed and releases all its interests.

## Remote query server

The query host constructs the same storage and query server, then calls
`queries.Register(grpcServer)`. This mounts both query-subscription and
transaction-message services on the application's gRPC server. The application
owns its listener, authentication, and TLS/mTLS configuration.

The projection host supplies `querygrpc.NewClient(connection)` to
`projectionserver.New`, and `messagegrpc.NewClient(connection)` as
`Options.TransactionMessages`. The interfaces are identical to the embedded case.

Multiple projection servers can share one query backend. End-user claims and
visibility policy belong to projections, never the query server.

### Transport liveness and recovery

Configure HTTP/2 keepalive on the host-owned gRPC server and client connections;
query/message protocols do not need their own ping messages. For example, using
`google.golang.org/grpc/keepalive`:

```go
grpcServer := grpc.NewServer(
    grpc.KeepaliveParams(keepalive.ServerParameters{
        Time: 30 * time.Second, Timeout: 10 * time.Second,
    }),
    grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
        MinTime: 30 * time.Second,
    }),
)
connection, err := grpc.NewClient(address,
    grpc.WithTransportCredentials(transportCredentials),
    grpc.WithKeepaliveParams(keepalive.ClientParameters{
        Time: 30 * time.Second, Timeout: 10 * time.Second,
    }),
)
```

Here `address` and `transportCredentials` come from the hosting application.
These timings are deployment choices, not library defaults. The server's
enforcement policy must permit the client's probe frequency. The same
configuration applies to projection gRPC connections.

Keepalive detects an unresponsive transport, not a healthy transport with stalled
application work. Terminal query feeds terminate their RPC; request cancellation
also covers waiting for transport writes. Feed loss invalidates its subscriptions:
there is no automatic remote-feed resume or per-client durable replay. Consumers
must rebuild from fresh snapshots; the browser runtime does this on reconnect.

PostgreSQL capture sends replication status reports periodically and responds to
requested protocol keepalives. Its receive polling timeout is not a dead-peer
timeout: an idle database is valid. Configure PostgreSQL replication timeouts and
TCP failure detection on the deployment for a bounded network-failure policy.
Replication reconnects resume from the slot's acknowledged position while the slot
and required WAL remain available. WAL barriers establish progress, not liveness.

## Cache retention

Start from `queryserver.DefaultOptions()` and override fields before `New`:

```go
options := queryserver.DefaultOptions()
options.Limits.QueryIdleTTL = time.Minute
options.Limits.EvictionBatchSize = 64
options.Transport.MaxPayloadBytes = 32 << 20
backend, err := queryserver.New(ctx, store, options)
```

The returned value is independent of other callers. `Limits` configures cache
maintenance, recovery bounds, result-channel buffers, and snapshot sizing;
`Messages` owns metadata queues/batches; `Transport` controls query gRPC framing
when using `Backend.Register`. Zero fields retain their existing default
semantics; `ProgressBarrierDelay=0` specifically means immediate requests.
Libraries do not read environment variables.

Likewise, `storage.DefaultPostgresOptions()` provides query-planning, capture
buffer, message, and replication retry/status defaults. Supply the application's
publication and slot explicitly. The replication retry maximum
is a strict cap, including when it is not a power-of-two multiple of the minimum.

`Options.Limits.QueryIdleTTL` retains ready abandoned query ranges for 30 seconds
by default. These internal coverage owners have no delivery subscribers, but
continue receiving CDC updates. New windows can reuse overlapping ready ranges;
strict expansion and removal already protected by other active ready windows do
not create redundant retention. Unready initialization ranges never become warm
coverage merely because their loader is canceled.

`QueryMaintenancePeriod` defaults to one second. Expired retention windows lose
their coverage contribution, then memberships are pruned incrementally while
rechecking current coverage. `EvictionBatchSize` bounds retention expiry and
membership positions visited per maintenance turn (default 128); a backlog
continues in separate coordinator turns rather than waiting for another tick.
Normal final-subscriber teardown also drains through maintenance when memberships
remain, including when the subscriber leaves midway through expiry cleanup.
Deleting rows, TRUNCATE, and recovery still see ownership pending cleanup.

Entity payload retention is separate: `EntityIdleTTL` defaults to five minutes
after the final query reference is released. `EvictionPeriod` defaults to one
minute, with indexed, bounded eviction rather than a full-cache sweep.

## Ownership

- The application owns storage. Query construction borrows it; query failure or
  `Close` never closes the source. Failed query startup also leaves storage usable.
- The query server owns its coordinator and message broadcaster. Its `Messages()`
  client shares that broadcaster. Terminal storage failure stops dependent query
  computation, but closing a query server leaves storage capture running.
- Projections borrow query/message clients and own their own routing/subscriptions.
- Close in reverse dependency order: projections, query server, storage.
- Each component's constructor context also owns that component's lifetime.
  `Wait(ctx)` observes termination; canceling a wait does not stop the component.
  `Close` is idempotent and joins owned work. Normal shutdown reports cancellation.
- The host owns mutation pools, policy state, listeners, and gRPC server lifetime.
  Authentication is an outer-layer responsibility.

The examples and PostgreSQL integration test use only public Weave imports.
Run the latter against a disposable logical-replication database with
`WEAVE_TEST_DATABASE_URL=... go test -race ./pkg/queryserver`.
