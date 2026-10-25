// Package projectionserver exposes supported contracts and forwards construction to internal implementations.
package projectionserver

import (
	"context"

	impl "github.com/adiom-data/weave/internal/projectionserver"
	"github.com/adiom-data/weave/pkg/querysubscription"
)

// ProjectionLimits exposes only the runtime's configuration, not the expansion
// implementation or its private lifecycle/publication machinery.
type ProjectionLimits = impl.ProjectionLimits

// MessageOptions bounds the optional connection-message routing owned by this server.
type MessageOptions = impl.MessageOptions

// Server is the embedding handle. RPC handlers and connection machinery are
// private; callers mount the service and manage only its lifetime.
type Server = impl.Server
type Stream = impl.Stream

type Options = impl.Options

type DependencyOptions = impl.DependencyOptions
type PolicyOptions = impl.PolicyOptions

// DefaultOptions returns a fresh base configuration to customize before New.
func DefaultOptions() Options { return impl.DefaultOptions() }

// DefaultPolicyOptions returns customizable policy resource defaults. Definitions
// or a provider remain application-owned. MaxFactRows + 1 must fit the query
// backend's maximum initial window count (the defaults fit its default of 1000).
func DefaultPolicyOptions() PolicyOptions { return impl.DefaultPolicyOptions() }

// New constructs the projection service and owns optional message routing. The
// caller owns the query/message clients, policy, listeners, and gRPC server.
func New(ctx context.Context, queries querysubscription.Client, options Options) (Server, error) {
	return impl.New(ctx, queries, options)
}
