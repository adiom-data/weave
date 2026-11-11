# Instance management

This optional control plane is separate from query/projection protocols and
end-user visibility. `NewServer(postgresStorage)` provides a Connect
handler supporting Connect, gRPC, and gRPC-Web. Embedded applications must secure
that handler themselves. It is **not** registered by `queryBackend.Register`.

The underlying Go operations are available without running an RPC service:

```go
table := storage.RelationName{Namespace: "app", Name: "tasks"}
err := postgresStorage.InvalidateMetadata(ctx, &table) // future preparation only
err = postgresStorage.InvalidateMetadata(ctx, nil)    // all metadata, not all data
```

`InvalidateMetadata` forgets cached column/type/identity discovery and publication
validation. Existing immutable plans, subscriptions and entity/query data are not
changed. Unknown/failed discoveries are not cached. A lookup invalidated while
loading cannot repopulate the cache. Concurrent callers share discovery, but one
caller's cancellation cannot cancel it while another caller still needs it.

The RPC uses the same optional-table convention: `{"table":{"namespace":"app",
"name":"tasks"}}` invalidates that table, while `{}` invalidates all metadata.
A present but incomplete table (for example `{"table":{}}`) is a validation error,
not a request to invalidate everything. Storage must implement `MetadataManager`.

Operational counts are exported as metrics, not administrative RPCs.

Operations affect only the addressed instance. Coordinate multiple instances
externally. Successful metadata discovery is retained until invalidation or storage
restart. Incompatible DDL affecting existing plans/subscriptions requires a server
restart; clearing metadata does not migrate or retire live subscriptions. Existing
publication membership is not continuously rechecked.

The [standalone host](../../cmd/server/README.md#management) mounts this service
only when enabled, with a separate administrative bearer token.
