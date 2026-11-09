# Adiom Weave

A reactive data layer, from PostgreSQL to your application UI.

Define a projection—the entities, related lists, and fields a view needs—and
Weave keeps it current as the database changes. The Go services maintain shared
query results and deliver updates; the TypeScript runtime applies them atomically,
preserves views across reconnects, and provides optional React and optimistic-update
helpers.

Writes stay in your own APIs. Weave observes committed database changes rather
than requiring mutations to pass through its query engine.

## Try the demo

Requirements: Go 1.26.10+, Node.js 22.12+, Docker, and Docker Compose.
Run from the repository root:

```sh
npm install
make db-up
OTEL_SDK_DISABLED=true make server
```

In a second terminal, run `make web`, then open <http://localhost:5173> in two
tabs. Add, complete, or delete a task and watch both views update.

The demo uses fixed claims and has **no authentication**. It binds to loopback
by default; do not expose it publicly. See the [demo guide](examples/demo/README.md)
for configuration, troubleshooting, and database reset instructions.

## How it fits together

```text
PostgreSQL → query service → projection service → client runtime → React
                  ↑                ↑
           shared live queries   relation definitions
```

- **Query service:** single-table filters, ordered cursor windows, shared entity
  and query caches, and committed change delivery. It can run in-process or behind
  the query gRPC API.
- **Projection service:** composes entity/query subscriptions into nested views,
  evaluates lazy computed values and visibility, and serves gRPC plus an optional
  binary WebSocket bridge.
- **Client runtime:** maintains the normalized model, reconnects with fresh
  snapshots, and exposes immutable selections and optional optimistic layers.
- **Definition tooling:** compiles GraphQL-shaped read definitions into typed
  TypeScript bindings and React hooks. This is not a GraphQL server.

PostgreSQL is the implemented datastore. The engine's storage boundary is
database-independent; other adapters are not included.

## Start building

| You want to… | Start here |
| --- | --- |
| Build a React view | [Generated projections](examples/demo/PROJECTIONS.md) |
| Run a backend without writing Go | [Standalone server](cmd/server/README.md) |
| Add custom RPCs to a config-driven backend | [Server hosting library](pkg/server/README.md) |
| Use the TypeScript runtime | [Client guide](packages/client/README.md) |
| Embed or deploy the Go services | [Query service](pkg/queryserver/README.md) and [projection service](pkg/projectionserver/README.md) |
| Connect an existing PostgreSQL database | [Storage setup](pkg/storage/README.md) |
| Correlate your mutations with reactive updates | [Mutation helpers](pkg/mutation/README.md) |
| Understand consistency and recovery | [Engine reference](examples/demo/ENGINE.md) |
| Compare query-engine workloads | [Benchmark guide](benchmarks/query/README.md) |

Go imports start with `github.com/adiom-data/weave`. The TypeScript workspace
package is `@adiom-data/weave`, with React bindings at `@adiom-data/weave/react`.
Packages and generated demo bindings are currently used from this monorepo;
the npm packages are marked private.

## Contracts to know

- Initial snapshots are fenced and reconciled per table. Nested projections do
  not promise one cross-table database snapshot; optional `waitForSync` catches
  their initial images up to a shared source cut.
- Live reconstructable changes preserve source transactions. Newly referenced
  entities can remain pending while missing payloads are hydrated.
- Windows have stable cursor boundaries, not a permanently fixed row count.
  Use a unique ordering tie-breaker when complete membership matters.
- Disconnects rebuild from fresh snapshots. There is no durable per-client replay,
  offline mutation queue, or automatic mutation retry.
- Authentication and mutation authorization belong to your application.
  Read visibility is configurable and defaults to allow-all.

## Develop and test

```sh
make test-core      # Core Go tests and TypeScript runtime tests
make test-demo      # Demo Go/TypeScript tests and frontend build
make build          # Demo Go binary and frontend build
make generate       # Generic protobuf bindings
make generate-demo  # Demo mutation and projection bindings
```

Generated sources are checked in. Regeneration requires Buf, `protoc-gen-go`,
`protoc-gen-go-grpc`, and installed npm dependencies.

Database tests are opt-in with `WEAVE_TEST_DATABASE_URL`; use a disposable
logical-replication database because fixtures create and modify database objects.
Run the demo's live browser-protocol tests with
`WEAVE_TEST_URL=http://localhost:8080 npm run test:demo` while the demo is running.

The reusable Go module lives at the root; the demo has its own Go module with a
local replacement. Frontend packages share an npm workspace. Public embedding
APIs live under `pkg/`; implementations are under `internal/`. Applications and
benchmarks live under `examples/` and `benchmarks/`.

Database commands use the `weave` Compose project. If an older demo stack is
running, stop that project first to release port 5432. Renaming does not migrate
or remove existing database volumes.

## License

Adiom Weave is licensed under the [Apache License 2.0](LICENSE).
