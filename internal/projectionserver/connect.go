package projectionserver

import (
	"context"
	"errors"
	"net/http"

	"connectrpc.com/connect"
	reactivev1 "github.com/adiom-data/weave/pkg/gen/reactive/v1"
	"github.com/adiom-data/weave/pkg/gen/reactive/v1/reactivev1connect"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Handler mounts the projection API using Connect, gRPC, or gRPC-Web. The caller
// owns HTTP hosting and handler options such as authentication and message limits.
func (s *server) Handler(options ...connect.HandlerOption) (string, http.Handler) {
	return reactivev1connect.NewReactiveQueriesHandler(connectService{s}, options...)
}

type connectService struct{ service *server }

func (c connectService) Open(ctx context.Context, stream *connect.BidiStream[reactivev1.ClientMessage, reactivev1.ServerMessage]) error {
	return connectError(c.service.ServeStream(connectProjectionStream{ctx, stream}))
}
func (c connectService) GetProjectionSnapshot(ctx context.Context, request *connect.Request[reactivev1.ProjectionSnapshotRequest]) (*connect.Response[reactivev1.ProjectionSnapshotResponse], error) {
	response, err := c.service.GetProjectionSnapshot(ctx, request.Msg)
	if err != nil {
		return nil, connectError(err)
	}
	return connect.NewResponse(response), nil
}

type connectProjectionStream struct {
	ctx context.Context
	*connect.BidiStream[reactivev1.ClientMessage, reactivev1.ServerMessage]
}

func (s connectProjectionStream) Context() context.Context                 { return s.ctx }
func (s connectProjectionStream) Recv() (*reactivev1.ClientMessage, error) { return s.Receive() }

func connectError(err error) error {
	if err == nil {
		return nil
	}
	var existing *connect.Error
	if errors.As(err, &existing) {
		return err
	}
	if errors.Is(err, context.Canceled) {
		return connect.NewError(connect.CodeCanceled, err)
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return connect.NewError(connect.CodeDeadlineExceeded, err)
	}
	s, ok := status.FromError(err)
	if !ok {
		return connect.NewError(connect.CodeUnknown, err)
	}
	if s.Code() == codes.OK {
		return nil
	}
	return connect.NewError(connect.Code(s.Code()), errors.New(s.Message()))
}
