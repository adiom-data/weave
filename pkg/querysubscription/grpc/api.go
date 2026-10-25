// Package querygrpc exposes supported contracts and forwards construction to internal implementations.
package querygrpc

import (
	"google.golang.org/grpc"

	impl "github.com/adiom-data/weave/internal/querysubscription/grpc"
	"github.com/adiom-data/weave/pkg/querysubscription"
)

func NewClient(connection grpc.ClientConnInterface) querysubscription.Client {
	return impl.NewClient(connection)
}

func NewClientWithOptions(connection grpc.ClientConnInterface, options TransportOptions) querysubscription.Client {
	return impl.NewClientWithOptions(connection, options)
}

const ProtocolVersion = impl.ProtocolVersion

// TransportOptions bound transport framing independently from the logical
// subscription queue. ChunkBytes must fit beneath the configured gRPC message
// limit; MaxPayloadBytes bounds one reconstructed snapshot or delivery.
type TransportOptions = impl.TransportOptions

func DefaultTransportOptions() TransportOptions { return impl.DefaultTransportOptions() }

// server exposes a querysubscription.Client without exposing coordinator or
// datastore implementation details.
type Server = impl.Server

func NewServer(client querysubscription.Client) Server { return impl.NewServer(client) }

func NewServerWithOptions(client querysubscription.Client, options TransportOptions) Server {
	return impl.NewServerWithOptions(client, options)
}
