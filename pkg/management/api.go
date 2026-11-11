// Package management exposes optional trusted administration for one instance.
// It is separate from projection/query data protocols and user visibility.
package management

import (
	impl "github.com/adiom-data/weave/internal/management"
	"github.com/adiom-data/weave/pkg/storage"
)

// Server mounts Connect, gRPC and gRPC-Web unary methods. The embedding host
// MUST supply administrative authentication or equivalent network isolation.
type Server = impl.Server

func NewServer(metadata storage.MetadataManager) (Server, error) {
	return impl.NewServer(metadata)
}
