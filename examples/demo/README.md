# Work-tracking demo

A minimal project/task application using PostgreSQL CDC, projections, binary
WebSockets, React, and separate ConnectRPC mutations with optimistic updates.
Application schema, policy, claims, mutations, and UI live here; the reusable
libraries do not depend on the demo.

## Run

Requirements: Go 1.26.10+, Node.js 20.19+ (or 22.12+), Docker, and Docker Compose.
Run from the repository root:

```sh
npm install
make db-up
OTEL_SDK_DISABLED=true make server
```

In another terminal:

```sh
make web
```

Open <http://localhost:5173> in two tabs. Create, complete, or delete tasks in
one tab and watch the other update. No token or custom configuration is needed;
the server reads the bundled [`server.yaml`](server.yaml).

The server uses fixed claims for the seeded user, organization, and team. It
does **not authenticate callers** and binds to loopback by default. This is a
local development example, not a secure deployment. Visibility policies and
database mutation permission checks still run against those claims.

## How it fits together

- [Schema and seed data](migrations/001_init.sql): organizations, users, teams,
  projects, tasks, ordered project/task associations, and visibility grants.
- [Projection definitions](PROJECTIONS.md): GraphQL-shaped build-time documents
  compiled into typed TypeScript readers and React hooks. This is not a GraphQL
  runtime. The demo expands `Project -> ProjectTasks -> Task`.
- [Visibility policy](relations.yaml): user/team grants
  allow a project, with visibility propagated over explicitly approved edges.
- [Fixed claims](server.yaml): trusted local-demo inputs
  supplied to both projections and mutations.
- [Mutations](packages/demo-mutations/README.md): independent ConnectRPC commands.
  The client predicts changes; committed receipts retire predictions alongside
  authoritative CDC updates. RPC responses never overwrite the reactive cache.
- [React app](web/src/App.tsx): owns the connection with `useConnection`, reads
  generated projections, and uses the optional mutation harness.
- [Server](cmd/server/main.go): uses the public config-driven
  [`pkg/server`](../../pkg/server/README.md) stack and mounts it alongside demo mutation RPCs.
  Projection RPCs, mutations, WebSocket, and health share one listener. Database
  pools and write authorization for mutations remain application-owned.

## Configuration

Everything has local-demo defaults. The optional settings are listed in
[`.env.example`](.env.example): database URL, shared listener address, browser origin,
publication and replication slot. The server reads the process
environment; it does **not** automatically load `.env` files. For example:

```sh
WEAVE_ADDRESS=127.0.0.1:8081 OTEL_SDK_DISABLED=true make server
```

Vite proxies requests to port 8080 by default; update
[`web/vite.config.ts`](web/vite.config.ts) if changing the port. There is no
separate gRPC listener or internal RPC hop. A different publication must already exist in PostgreSQL.
Keep the replication slot stable across restarts, and give concurrent server
instances different slots.

Reactive configuration, policy loading, authentication, lifecycle, and limits
are handled by the same public library as `cmd/server`. Use `WEAVE_CONFIG` or
`-config` to select another YAML file. Policy paths are relative to that file.
The demo no longer supports its old `DATABASE_URL`, `HTTP_ADDR`, `GRPC_ADDR`,
`PUBLICATION_NAME` or `REPLICATION_SLOT` aliases; use the
documented `WEAVE_*` settings instead.

Claims start as JSON strings in config/JWTs. `auth.claim_types` converts the
organization/user IDs and team-ID list to canonical UUID values at admission.
Policies compare UUIDs directly, and the user-grant query binds the typed subject
ID rather than loading every user's grants. Mutations receive the same mapped
claims. Invalid configured fixed claims fail startup/config validation.

Telemetry follows the framework's `OTEL_*` settings. The quickstart disables it
because no collector is included. Health uses standard gRPC health checks with
`liveness`/`readiness` service labels; the old `/healthz` route was removed.

To validate the bundled configuration and compile its policy without a database:

```sh
go -C examples/demo run ./cmd/server -check-config
```

## Troubleshooting

- **Port 5432 is busy:** stop the conflicting local PostgreSQL or older demo
  Compose project before `make db-up`. This demo uses the `weave` project name.
- **Configuration seems ignored:** export variables or prefix `make server`
  with them; copying `.env.example` to `.env` does not load it.
- **Tasks do not appear:** check the server log and browser connection status.
  The server must be connected to the database/publication initialized by this demo.
- **Need to stop the database:** `make db-down` stops the stack and retains data.

### Reset an incompatible database

The database initializes only when its Docker volume is first created. If an old
volume has an incompatible schema, the following **deletes all demo data**:

```sh
docker compose -p weave -f examples/demo/docker-compose.yml down -v
make db-up
```

## Development

```sh
make test-demo       # Demo Go/TypeScript tests and frontend build
make test           # Core and demo tests
make build          # Go binary and frontend build
make generate       # Generic protobuf bindings
make generate-demo  # Demo mutation and projection bindings
```

Generated bindings are checked in. Regeneration requires Buf, `protoc-gen-go`,
`protoc-gen-go-grpc`, and installed npm dependencies. Live client tests are
opt-in against a running demo:

```sh
WEAVE_TEST_URL=http://localhost:8080 npm run test:demo
```

For deeper details, see the [engine reference](ENGINE.md),
[projection server](../../pkg/projectionserver/README.md),
[query server](../../pkg/queryserver/README.md),
[client runtime](../../packages/client/README.md), and
[query benchmarks](../../benchmarks/query/README.md).
