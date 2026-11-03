# Config-driven server library

`github.com/adiom-data/weave/pkg/server` is the same host used by `cmd/server`.
It configures PostgreSQL, the query/projection services, policy-file loading,
claims admission, projection RPCs, WebSocket, health, and component supervision.
The implementation stays internal; the public package is a small facade.

```go
cfg, err := server.LoadConfig("server.yaml", os.LookupEnv)
if err != nil {
    return err
}
host, err := server.New(ctx, cfg, server.Options{
    Logger: logger,
    AuthenticatedServices: []httpapp.ConnectService{
        httpapp.Connect(
            demov1connect.TaskMutationServiceName,
            func(opts ...connect.HandlerOption) (string, http.Handler) {
                return demov1connect.NewTaskMutationServiceHandler(mutations, opts...)
            },
        ),
    },
})
if err != nil {
    return err
}
defer host.Close()
return host.Run(ctx)
```

The generated handler factory closure adapts your mutation implementation to
the framework's mounting interface.

- `Services` preserves application-owned admission. No host authentication is
  silently applied. `AuthenticatedServices` explicitly runs configured admission
  first; handlers use `accesscontrol.ClaimsFromContext(ctx)`. Fixed/unverified
  modes retain exactly their configured trust semantics. Mutation authorization
  remains the application's responsibility.
- `Routes` mounts application HTTP routes with their own authentication/CORS
  middleware. Additional services use their own message limits and CORS as well.
  They must not shadow built-in projection/WebSocket/health endpoints.
- `WebSocketAuthentication` optionally supplies application-owned credential
  verification, canonical claims, and renewable deadlines for WebSockets only.
  It replaces configured WebSocket admission; RPC/mutation admission is unchanged.
  Nil uses configured admission, with optional JWT renewal through
  `Config.Socket.Renewal.Enabled` (`websocket.renewal.enabled` in YAML). Defaults
  keep renewal disabled. An application hook and configured renewal are mutually
  exclusive. See the [renewal hooks](../projectionserver/websocket/README.md) and
  [standalone configuration](../../cmd/server/README.md#optional-websocket-renewal).
- The application owns mutation handlers and resources, such as its write pool.
  The host neither initializes an application schema nor closes those resources.
- `New` initializes capture but does not listen. Always call `Close`, including
  when `Run` is never invoked. `Run` is single-use; cancellation of its context,
  the construction context, or `Close` stops it. `Close` is idempotent, waits for
  an active listener to stop, then closes reactive resources in reverse order.
- Use `DefaultConfig()` for programmatic configuration. `LoadConfig` reads YAML
  and environment overrides; `ValidateConfig` additionally compiles the policy
  file without contacting PostgreSQL or OIDC. `New` performs the same validation.
- Telemetry follows framework `OTEL_*` settings, including `OTEL_SDK_DISABLED`.
- `Config.Auth.ClaimTypes` optionally maps top-level claims with `"uuid"` or
  `"list<uuid>"`. The same conversion runs for fixed claims and JWTs, after
  verification when enabled. Missing keys stay missing; invalid present values
  fail admission. The projection/query engines do not perform implicit coercion.

For supported configuration and deployment, see [`cmd/server`](../../cmd/server/README.md).
The [complete YAML example](../../cmd/server/server.example.yaml) lists tuning
for storage, query caches/queues, projections, and policy evaluation. Omitted
fields inherit library defaults; explicit nonpositive tuning fails validation
(except zero progress-barrier delay). The former flat `Config.Limits` settings
now live under `Config.Query`, `Config.Projection`, and `Config.Visibility`.
