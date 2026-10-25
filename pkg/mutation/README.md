# Optional mutation correlation

Correlation is not authentication, authorization, idempotency, or retry support.
It associates committed database work with an optimistic operation on a reactive
connection. Mutation endpoints can use HTTP, Connect, gRPC, or another transport.

```go
correlation, err := mutation.ReadHeaders(request.Header.Get, mutation.HeaderNames{})
// Handle err, authenticate, authorize, and begin your application's transaction.
// Execute arbitrary application SQL using tx.
err = postgres.EmitMutationReceipt(ctx, tx, correlation)
// Handle err, then commit tx. On any failure, roll back.
```

Imports are `github.com/adiom-data/weave/pkg/mutation` and `github.com/adiom-data/weave/pkg/storage/postgres`. Neither helper
requires a running reactive server. The application owns transaction lifetime.
The emitter executes a transactional logical WAL message on the supplied
`pgx.Tx`; rollback discards it together with application writes.

`HeaderNames{}` selects `X-Weave-Connection-Context` and `X-Weave-Mutation-Id`.
Override either name to match your transport. `ReadHeaders` accepts a getter,
so gRPC callers can adapt incoming metadata without depending on HTTP or Connect:

```go
get := func(name string) string {
    values := md.Get(name) // md is incoming gRPC metadata
    if len(values) == 0 { return "" }
    return values[0]
}
correlation, err := mutation.ReadHeaders(get, mutation.HeaderNames{})
```

Both headers absent means a nonreactive request: emitting its receipt is a no-op.
Otherwise both are required: the context is the canonical base64url encoding of
32 random bytes from the reactive handshake, and the mutation ID is a lowercase
canonical UUID. Validation checks format only, not whether a connection exists.
Keep the context private: it routes receipts but never grants database access.

For implementations in other languages, emit a transactional logical message
with PostgreSQL prefix `weave.client`, containing protobuf
`reactive.v1.RoutedTransactionMessage`. Its `connection_context` is the header
value, `kind` is `weave.mutation.committed`, and `payload` is the mutation ID's
UTF-8 bytes: no protobuf payload wrapper, JSON encoding, or trailing newline.

The TypeScript `MutationHarness` understands this receipt by default.
`mutationHeaders(context, optionalNames)` constructs the request headers.
Applications using another receipt format can still provide `decodeReceipts`.
