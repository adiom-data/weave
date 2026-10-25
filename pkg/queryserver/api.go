// Package queryserver exposes supported contracts and forwards construction to internal implementations.
package queryserver

import (
	"context"

	impl "github.com/adiom-data/weave/internal/queryserver"
	"github.com/adiom-data/weave/pkg/storage"
)

// Backend owns shared reactive computation and borrows its storage. Pass it directly
// to projectionserver.New or querygrpc.NewServer. Messages returns the matching
// source's metadata client for projections. Register mounts both backend gRPC
// services on a caller-owned server, whose security is the caller's responsibility.
// Consumers must close projection services and feeds before closing the backend.
type Backend = impl.Backend

// Limits configures reactive computation. Zero fields select defaults, except
// ProgressBarrierDelay: zero requests sparse-stream progress immediately.
type Limits = impl.Limits

type Options = impl.Options

// ReferenceOptions bounds asynchronous reference initialization per feed.
type ReferenceOptions = impl.ReferenceOptions

// DefaultOptions returns a fresh base configuration to customize before New.
func DefaultOptions() Options { return impl.DefaultOptions() }

// New borrows storage. Closing this server never closes or takes ownership of
// that storage; the application closes dependent components before their source.
func New(ctx context.Context, source storage.Storage, options Options) (Backend, error) {
	return impl.New(ctx, source, options)
}
