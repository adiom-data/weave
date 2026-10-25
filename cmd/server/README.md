# Standalone projection server

`cmd/server` hosts PostgreSQL storage, the embedded query engine, and projection
RPCs through the framework's `httpapp.Service`. One listener serves Connect,
native gRPC, gRPC-Web, binary WebSocket `/api/reactive`, and standard gRPC health.
Bidirectional RPCs require HTTP/2 end to end; browser streaming stays WebSocket.

The command contains flags/signals/startup wiring. Private packages under
`internal/server` own hosting, configuration, identity admission, and policy-file
translation. The query/projection libraries remain framework-independent.

Applications can reuse this complete config-driven host through
[`pkg/server`](../../pkg/server/README.md), adding custom Connect services and
HTTP routes without copying the standalone startup code. This command itself
uses that same public API.

## Run

Requires Go 1.26.10+ to build; a built binary/container does not require Go.

Prepare PostgreSQL with logical replication, primary keys, read/replication
privileges, and an existing publication containing every data and authorization
fact table. See [storage setup](../../pkg/storage/README.md). The server creates
its named replication slot if missing, retains it on shutdown, and resumes it
on restart. Each concurrent capture instance needs its own slot. Permanently
unused slots must be removed externally to avoid retaining WAL.

```sh
export WEAVE_DATABASE_URL='postgres://user:password@localhost/app?sslmode=require'
export OTEL_SDK_DISABLED=true  # For local use without a telemetry collector.
go run ./cmd/server -config cmd/server/server.example.yaml -check-config
go run ./cmd/server -config cmd/server/server.example.yaml
```

The example uses fixed empty claims and allow-all visibility: it is for local or
trusted-upstream use, not public exposure.

`-check-config` validates configuration and compiles policy/CEL without contacting
PostgreSQL or OIDC. It does not validate schema, privileges, discovery documents,
or fact-query compatibility. Normal startup discovers the configured OIDC issuer
and establishes database capture before starting the listener. Signing keys are
fetched/cached by the framework verifier when needed.

Build with `go build -o weave-server ./cmd/server`, or:

```sh
docker build -f cmd/server/Dockerfile -t weave-server .
docker run --rm --env-file /path/to/server.env \
  -e WEAVE_ADDRESS=0.0.0.0:8080 \
  -v /path/to/config:/config:ro -p 127.0.0.1:8080:8080 \
  weave-server -config /config/server.yaml
```

Only the projection service is publicly registered; raw query/message RPCs would
bypass visibility. Unary `GetProjectionSnapshot` and live `Open` use the same
policies and trusted claims. No schema, application mutations, or browser login
flow is installed.

Health uses `grpc.health.v1.Health/Check` with service labels `liveness` and
`readiness`, without credentials. There are no separate HTTP health mirrors.
A terminal component error stops the host. SIGINT/SIGTERM cancel active RPCs and
WebSockets before framework graceful shutdown, then close caches and storage.
The replication slot is not deleted.

## Claims

Explicitly select one `auth.mode`. There is no automatic fallback:

- `oidc`: validate the bearer JWT using issuer discovery, cached signing keys,
  expected issuer/audience, expiration, and not-before. This verifies a supplied
  token; it does not implement an OIDC login or token exchange.
- `unverified_jwt`: parse bearer claims without checking signature, issuer,
  audience, or expiration. Only use behind an authenticating trusted edge that
  prevents direct access to this server.
- `fixed`: ignore bearer headers and use `fixed_claims` for all callers.
  Omitted, `null`, and `{}` mean empty claims. This is not authentication.

```yaml
auth:
  mode: oidc
  issuer: https://identity.example.com/
  audience: weave
  clock_skew: 1m
  http_timeout: 5s
  max_token_bytes: 65536
```

The issuer must publish its OIDC discovery document and `jwks_uri`. Direct
JWKS URL configuration, custom signing-key refresh schedules, and algorithm
selection are no longer exposed; the framework owns discovery and verification.
`clock_skew` must be positive, matching the framework verifier's supported
configuration. HTTPS is required operationally outside trusted local networks.

No application-specific claim names, including `sub`, are required. Strings,
booleans, null, arrays, and objects retain their natural types. Numbers become
int64 when representable, otherwise exact canonical decimals. OIDC validation
uses the framework verifier's precision-preserving raw claims directly.
Only `unverified_jwt` uses a separate unverified decoder, also with `UseNumber`.
Failed verification never falls back to unverified mode.
UUID/date-looking strings are not inferred as native UUIDs/dates.

Optionally declare top-level claim conversions (the same mapping applies in all
three auth modes):

```yaml
auth:
  # mode and credentials as above
  claim_types:
    organization_id: uuid
    user_id: uuid
    team_ids: list<uuid>
```

Currently supported conversions are `uuid` and `list<uuid>`. Only declared claims
are converted; names are literal top-level keys, not dotted paths. UUID strings
normalize into canonical UUID values, so policies and fact-query bindings use
the native type without repeated CEL casts. Undeclared claims retain their parsed
types. Missing claims stay missing; policies decide which are required. Present
nulls, wrong types, malformed UUIDs, and invalid list elements reject admission.
An empty UUID list is valid. Invalid mapping definitions fail startup, and invalid
fixed values also fail `-check-config`. Mapping runs after verification in OIDC
mode; it does not make unverified/fixed modes authenticated.

Claims are captured once at request/stream admission. Existing streams are not
reauthenticated or closed merely because their token expires. Refresh, ongoing
revocation, and continuous authentication belong upstream. New RPCs/streams
authenticate again. Database-driven policy visibility still updates reactively.

The WebSocket edge authenticates Authorization once and calls the same projection
service directly, without an internal RPC hop. It never accepts URL tokens or
claims in message payloads. Invalid credentials close the upgraded socket.
Browser WebSocket APIs cannot set arbitrary Authorization headers; use an
authenticating upstream that injects the header, or a trusted fixed-claims setup.
The React client and binary envelope are unchanged.

Telemetry uses the framework's normal OpenTelemetry configuration. Set
`OTEL_SDK_DISABLED=true` to disable instrumentation and exports, or set
`OTEL_EXPORTER_OTLP_ENDPOINT` to your collector. Without overrides the framework
enables telemetry and targets `http://otel-collector:4318`. Structured application
and HTTP request logs remain enabled independently.

The framework derives HTTP request base contexts from `Run(ctx)`, so cancelling
the host context also cancels active RPCs and WebSockets. WebSocket adapters close
their sockets on cancellation; no extra lifecycle middleware is needed.

The listener defaults to loopback and plaintext HTTP/1 + HTTP/2. Terminate TLS at
a trusted proxy before exposure and preserve HTTP/2 for bidi RPCs. Restrict
`web_origin` for browsers; `*` explicitly permits any origin. Origin checks are
not authentication. `allow_all` exposes every permitted backend field/row.

## Policies

Choose `visibility.mode: allow_all` or `policy`. Policy mode requires
`policy_file`, resolved relative to the server config file (or working directory
with environment-only configuration). Invalid files fail startup, never fall
back to allow-all. Undefined relations deny visibility.

[policy.example.yaml](policy.example.yaml) shows propagation, receiving edges,
explicit deny, and lazy private fact projections. It is an application example,
not an installed schema. The file translates to the existing Go policy model;
CEL evaluation, lazy fact subscriptions, transaction synchronization, and deny
semantics are identical to the library. No additional query evaluator is added.

Relation entries use separate `schema` and `table` fields. Rules have `id` and
`when` (CEL). `propagate` entries also have `to`; `receive` entries name `from`
and its edge `id`. Omitted/null `receive` accepts any configured incoming edge;
`receive: []` accepts none. Propagation only follows matching projection edges,
not unrelated objects elsewhere on a connection.

Facts have `id` and a `projection` with:

- `name`, `kind: list|entity`, and `relation: {schema: ..., table: ...}`.
- Optional `bindings`: `field: [segments]`, `operator: eq|ne|lt|lte|gt|gte`, and
  exactly one of `parameter: [item, field]`, `parameter: [claims, field]`,
  `parent: [field]`, or `literal: {type: text, value: ...}`. Parent bindings read
  the immediate parent of a nested fact projection.
- Optional `filter`: a leaf with `field`, `operator`, and typed `value`, or an
  `and`/`or` list of filters. Operators also include `is_null`/`is_not_null`
  (no value). Query literals support scalar engine types; e.g.
  `{type: uuid, value: "00000000-0000-0000-0000-000000000001"}`.
- Optional `order` entries with `field`, `descending`, and `nulls_first`.
  Omitted ordering uses the adapter's primary-key ordering.
- Optional recursive `children` with the same projection shape.

Paths are arrays of exact field names, not dotted strings or array indexes.
Facts load whole records, are hidden from clients, and must fit the configured
fact limits. A policy cannot silently authorize from truncated facts.
List facts are lists of `{item, children}` wrappers; entity facts are one such
wrapper or null. For example, `facts.grants.exists(g, g.item.active == true)`
tests rows in a list fact.
Bindings retain their input type: a text claim is not implicitly a UUID query
literal. For native UUID columns, bind from a native UUID entity field, or
compare a discovered grant's UUID field against `uuid(claims.user_id)` in CEL.
Predicate/order columns must satisfy the PostgreSQL adapter's supported type
and collation rules (e.g. use `COLLATE "C"` for bytewise text comparisons).


## Configuration

Defaults → YAML → explicitly present environment variables. Unknown YAML keys,
duplicate keys, and additional documents fail validation. Policies are loaded
once at startup; restart to adopt changes. The [example](server.example.yaml)
lists host/stream limits. Unexposed component settings retain library defaults.

| Environment | Setting |
| --- | --- |
| `WEAVE_CONFIG` | Config path, overridden by `-config` |
| `WEAVE_ADDRESS` | Shared Connect/gRPC/WebSocket listener |
| `WEAVE_WEB_ORIGIN` | Browser WebSocket origin |
| `WEAVE_DATABASE_URL` | PostgreSQL pgx URI or keyword configuration |
| `WEAVE_PUBLICATION` / `WEAVE_SLOT` | Storage capture settings |
| `WEAVE_STORAGE_TYPE` | `postgres` (only implemented adapter) |
| `WEAVE_AUTH_MODE` | `oidc`, `unverified_jwt`, or `fixed` |
| `WEAVE_JWT_ISSUER` / `WEAVE_JWT_AUDIENCE` | OIDC verification inputs |
| `WEAVE_FIXED_CLAIMS` | Whole claims JSON object or `null` |
| `WEAVE_VISIBILITY_MODE` / `WEAVE_POLICY_FILE` | Visibility configuration |

Set `websocket.enabled: false` to disable that route while retaining RPCs/health.
The old separate listener settings and direct-JWKS auth settings are removed.

## Tests

`go test -race ./internal/server/... ./internal/projectionserver/...` checks
configuration, OIDC discovery/rotation, exact claims, admission, Connect/gRPC
streaming, and the WebSocket transport. Set `WEAVE_SERVER_TEST_DATABASE_URL` to
an explicitly disposable PostgreSQL database with logical replication to also
test the complete host, lazy visibility facts, live updates, and restart with
the retained replication slot. Its uniquely named table/publication/slot are
removed after the host is stopped.
