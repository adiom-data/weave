// Package management adapts trusted, instance-local control operations to RPCs.
// Authentication belongs to the host mounting this service.
package management

import (
	"context"
	"errors"
	"net/http"

	"connectrpc.com/connect"
	"github.com/adiom-data/weave/internal/datastore"
	"github.com/adiom-data/weave/internal/storage"
	managementv1 "github.com/adiom-data/weave/pkg/gen/management/v1"
	"github.com/adiom-data/weave/pkg/gen/management/v1/managementv1connect"
)

type Server interface {
	Handler(...connect.HandlerOption) (string, http.Handler)
}
type server struct {
	metadata datastore.MetadataManager
}

func NewServer(metadata datastore.MetadataManager) (Server, error) {
	if metadata == nil {
		return nil, errors.New("metadata management is required")
	}
	return &server{metadata: metadata}, nil
}

func (s *server) Handler(options ...connect.HandlerOption) (string, http.Handler) {
	return managementv1connect.NewManagementHandler(s, options...)
}

func tableName(table *managementv1.Table) (storage.RelationName, error) {
	if table == nil || table.Namespace == "" || table.Name == "" {
		return storage.RelationName{}, connect.NewError(connect.CodeInvalidArgument, errors.New("table namespace and name are required"))
	}
	return storage.RelationName{Namespace: table.Namespace, Name: table.Name}, nil
}

func (s *server) InvalidateMetadata(ctx context.Context, req *connect.Request[managementv1.InvalidateMetadataRequest]) (*connect.Response[managementv1.InvalidateMetadataResponse], error) {
	var relation *storage.RelationName
	if req.Msg.Table != nil {
		name, err := tableName(req.Msg.Table)
		if err != nil {
			return nil, err
		}
		relation = &name
	}
	if err := s.metadata.InvalidateMetadata(ctx, relation); err != nil {
		return nil, operationError(ctx, err)
	}
	return connect.NewResponse(&managementv1.InvalidateMetadataResponse{}), nil
}

func operationError(ctx context.Context, err error) error {
	if errors.Is(err, context.Canceled) || errors.Is(ctx.Err(), context.Canceled) {
		return connect.NewError(connect.CodeCanceled, context.Canceled)
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return connect.NewError(connect.CodeDeadlineExceeded, context.DeadlineExceeded)
	}
	// Avoid exposing native database errors or credentials through administration.
	return connect.NewError(connect.CodeUnavailable, errors.New("management operation could not complete"))
}
