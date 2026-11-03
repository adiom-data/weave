# WebSocket authentication updates

Authentication updates are disabled by default. Without `SocketConfig.Authentication`,
the hosting application's `ServeStream` callback owns admission exactly as before.
The query/projection protocols have no token handling.

The packaged server can enable JWT-backed renewal with
`websocket.renewal.enabled: true`, using its configured OIDC or unverified-JWT
authenticator. No Go hook is needed in that mode. See the
[standalone settings](../../../cmd/server/README.md#optional-websocket-renewal).
The Go hook below remains available for application-specific verification.

For an application-owned verifier, set `SocketConfig.Authentication` (or
`server.Options.WebSocketAuthentication` when using the configured host):

```go
authentication := &websocket.AuthenticationOptions{
    Authenticate: func(ctx context.Context, headers http.Header) (websocket.Authentication, error) {
        // Application code verifies the credential and constructs canonical claims.
        identity, err := verifyCredential(ctx, headers)
        if err != nil {
            return websocket.Authentication{}, err
        }
        return websocket.Authentication{
            Claims: identity.PolicyClaims,
            ExpiresAt: identity.ExpiresAt,
        }, nil
    },
    ValidationTimeout: 5 * time.Second,
    MaxTokenBytes: 64 << 10,
    MaxConcurrentValidations: 32,
}
```

`verifyCredential` is supplied by your application, not this library. The hook
runs once before WebSocket upgrade and when processing a token update. For updates,
it receives the original headers with `Authorization` replaced by `Bearer <token>`.
It must honor context cancellation. Its error details are not sent to the peer.
The returned expiry must be a valid, finite, future timestamp. The verified
claims are copied into the stream context for normal projection authorization.

Claims must include the subject identity, tenant where applicable, and all policy
inputs. Exclude credential-only metadata that changes on renewal (such as JWT
`exp`, `iat`, and token IDs); expiry is provided separately. Claim comparison is
exact over canonical values, including list order. Claims never change in place:
a successful verification changes only the authentication deadline. No success
response is sent, and the wire protocol contains no request ID or expiry field.

- `STATUS_REJECTED` notifies the application that the newest processed credential could
  not be verified. The existing lease is unchanged; token updates can continue
  before expiry. Verification timeout has this outcome too.
- `STATUS_RECONNECT` means verified claims changed; original claims remain in effect,
  the server attempts to send the notice, then closes the connection. A new
  connection must authenticate the new identity. This permits a short grace
  while the notice is written; the original expiry still applies.
- Unspecified/unrecognized status values are exposed as `unknown` by the client.
- These are connection notices, not responses to individual token submissions.
  Older tokens may be skipped, and success is deliberately silent. When the
  feature is disabled, well-formed token updates are ignored without a notice.

Defaults for positive bounds are shown above. Validation runs independently of
the message reader and publication path. Each connection has at most one token
being verified and one latest pending token. New submissions replace pending
tokens rather than queueing history. Saturated verifier capacity causes the
connection's worker to wait asynchronously, then take its newest token; it does
not drop fresh credentials or send `BUSY` replies. Initial admission still rejects
immediately when capacity is exhausted. Concurrent verifier calls across the
handler are limited by `MaxConcurrentValidations`.

Superseded failures are suppressed when a newer token is already pending. A
valid same-claims token can renew the lease even with newer work pending, so
frequent submissions do not starve renewal. Repeated submissions of the same
token may be verified again, allowing application-specific revocation checks.

Expiry cancels the stream and outstanding
validation even if the peer is sending heartbeats; a late result cannot revive an
expired connection. No new frame write starts after the lease expires. Already
transmitted or in-flight data cannot be recalled.

Client applications explicitly obtain and schedule replacement credentials:

```ts
const client = new ReactiveClient({
  url,
  onAuthStatus(status) {
    // Application decides how to react to 'rejected', 'reconnect', or 'unknown'.
  },
})

// Call periodically while connected; no Promise or acknowledgment to wait for.
client.updateAuthToken(await obtainFreshToken())
```

There is no automatic token provider or timer in the client. `updateAuthToken`
sends synchronously and throws if disconnected or sending fails. The client does
not retain/replay tokens, wait for an acknowledgment, or apply a request timeout.
The application owns the token schedule. Notifications from obsolete connections
are ignored, and auth-listener failures do not corrupt model processing.
Keep tokens out of URLs/logs, use WSS in production, and obtain initial browser
credentials through your application's existing cookie/proxy/custom-socket path.
A valid JWT alone does not guarantee immediate revocation detection: that belongs
to the verifier/provider. Use this hook for that check if needed.
