// Package messagegrpc exposes supported contracts and forwards construction to internal implementations.
package messagegrpc

import (
	"google.golang.org/grpc"

	impl "github.com/adiom-data/weave/internal/transactionmessage/grpc"
	"github.com/adiom-data/weave/pkg/transactionmessage"
)

func NewClient(connection grpc.ClientConnInterface) transactionmessage.Client {
	return impl.NewClient(connection)
}

type Server = impl.Server

func NewServer(service transactionmessage.Service) Server { return impl.NewServer(service) }
