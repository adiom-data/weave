# Config-driven server library

`github.com/adiom-data/weave/pkg/server` assembles the reactive stack used by `cmd/server`.
It configures PostgreSQL, the query/projection services, policy-file loading,
claims admission, projection RPCs, WebSocket, and component supervision.
It exposes endpoint descriptors and does not create or run an HTTP server.
The implementation stays internal; the public package is a small facade.

```go
runtime, err := httpapp.Init(ctx)
if err != nil {
    return err
}
defer runtime.Shutdown(context.Background())
ctx, cancel := context.WithCancel(runtime.Context())
defer cancel()
cfg, err := server.LoadConfig("server.yaml", os.LookupEnv)
if err != nil {
    return err
}
if err := cfg.ValidateListener(); err != nil {
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
app := runtime.NewService(
    httpapp.WithServiceAddr(cfg.Address),
    httpapp.WithServiceTimeouts(cfg.ReadHeaderTimeout, cfg.ShutdownTimeout),
    httpapp.WithServiceIdleTimeout(cfg.IdleTimeout),
    httpapp.WithServices(host.Services()...),
    httpapp.WithServiceRoutes(host.Routes()...),
)
done := make(chan error, 1)
go func() {
    err := host.Wait(ctx)
    if ctx.Err() != nil && errors.Is(err, ctx.Err()) {
        err = nil
    }
    done <- err
    cancel()
}()
httpErr := app.Run(ctx)
cancel()
return errors.Join(httpErr, <-done)
```

The generated handler factory closure adapts your mutation implementation to
the framework's mounting interface.

- `Options.AuthenticatedServices` explicitly runs configured admission
  first; handlers use `accesscontrol.ClaimsFromContext(ctx)`. Fixed/unverified
  modes retain exactly their configured trust semantics. Mutation authorization
  remains the application's responsibility.
- Mount `Services()` and `Routes()` on your HTTP server alongside application
  services and routes. Ordinary application endpoints retain their own admission,
  message limits, and CORS. The framework supplies health handlers and HTTP/RPC
  instrumentation. Avoid collisions with the returned service names and paths.
- `WebSocketAuthentication` optionally supplies application-owned credential
  verification, canonical claims, and renewable deadlines for WebSockets only.
  It replaces configured WebSocket admission; RPC/mutation admission is unchanged.
  Nil uses configured admission, with optional JWT renewal through
  `Config.Socket.Renewal.Enabled` (`websocket.renewal.enabled` in YAML). Defaults
  keep renewal disabled. An application hook and configured renewal are mutually
  exclusive. See the [renewal hooks](../projectionserver/websocket/README.md) and
  [standalone configuration](../../cmd/server/README.md#optional-websocket-renewal).
- The application owns mutation handlers and resources, such as its write pool.
  The reactive stack neither initializes an application schema nor closes those resources.
- `New` initializes capture and supervision. `Wait(ctx)` reports a terminal
  component error or cancellation; canceling a wait does not stop the stack.
  The application should cancel HTTP serving when `Wait` reports a failure.
  Stop HTTP serving before calling the idempotent `Close`, which cancels the
  stack and closes projections, queries, and storage in dependency order.
- Use `DefaultConfig()` for programmatic configuration. `LoadConfig` reads YAML
  and environment overrides; `ValidateConfig` additionally compiles the policy
  file without contacting PostgreSQL or OIDC. `New` performs the same reactive
  validation. These do not require a listener address or HTTP timeouts.
- The stack uses the caller's OpenTelemetry provider. Install it before `New`
  and shut it down after `Server.Close`; the server never installs or shuts down a
  provider. `cmd/server` owns framework telemetry setup and follows `OTEL_*`
  settings, including `OTEL_SDK_DISABLED`.
- `Config.Management.Enabled` optionally includes the instance management service
  in `Services()`. It is disabled by default and requires its own
  `Config.Management.BearerToken`, independent of projection authentication.
  See [management](../management/README.md) before exposing this control plane.
- `Config.Auth.ClaimTypes` optionally maps top-level claims with `"uuid"` or
  `"list<uuid>"`. The same conversion runs for fixed claims and JWTs, after
  verification when enabled. Missing keys stay missing; invalid present values
  fail admission. The projection/query engines do not perform implicit coercion.

For supported configuration and deployment, see [`cmd/server`](../../cmd/server/README.md).
Listener address and HTTP timeout fields are consumed by the application when
constructing its HTTP server; the reusable stack does not apply them. Applications
using those fields call `Config.ValidateListener()` separately, as both bundled
commands do before startup and in `-check-config` mode.
The [complete YAML example](../../cmd/server/server.example.yaml) lists tuning
for storage, query caches/queues, projections, and policy evaluation. Omitted
fields inherit library defaults; explicit nonpositive tuning fails validation
(except zero progress-barrier delay). The former flat `Config.Limits` settings
now live under `Config.Query`, `Config.Projection`, and `Config.Visibility`.
