// Package queryserver starts a trusted query backend for embedded use or for
// hosting behind the query and transaction-message gRPC transports. It does
// not authenticate callers or enforce end-user visibility.
package queryserver

import (
	"context"

	"google.golang.org/grpc"

	"github.com/adiom-data/weave/internal/querysubscription/model"
	"github.com/adiom-data/weave/internal/transactionmessage"
)

// Backend owns shared reactive computation and borrows its storage. Pass it directly
// to projectionserver.New or querygrpc.NewServer. Messages returns the matching
// source's metadata client for projections. Register mounts both backend gRPC
// services on a caller-owned server, whose security is the caller's responsibility.
// Consumers must close projection services and feeds before closing the backend.
type Backend interface {
	querysubscription.Client
	Messages() transactionmessage.Client
	Register(grpc.ServiceRegistrar)
	Close() error
	Wait(context.Context) error
}
