package projectionserver

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/adiom-data/weave/internal/accesscontrol"
	proj "github.com/adiom-data/weave/internal/projection"
	"github.com/adiom-data/weave/internal/querysubscription/model"
	reactivev1 "github.com/adiom-data/weave/pkg/gen/reactive/v1"
)

// GetProjectionSnapshot materializes a projection without retaining any live
// subscription state after the RPC returns. It deliberately uses the same
// projection, visibility, and query-feed paths as Open.
func (s *server) GetProjectionSnapshot(ctx context.Context, request *reactivev1.ProjectionSnapshotRequest) (*reactivev1.ProjectionSnapshotResponse, error) {
	ctx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(s.lifetime, cancel)
	defer stop()
	defer cancel()
	if s.queries == nil {
		return nil, status.Error(codes.FailedPrecondition, "query subscription client is required")
	}
	if request == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	root, selected, err := projectionNode(request.Root, request.Parameters, s.options.MaxSelectedFields, s.options.MaxPredicateDepth, s.options.MaxPredicateNodes)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid projection: %v", err)
	}
	if err := validateSnapshotLayout(request.Root, selected); err != nil {
		return nil, err
	}
	if s.evaluator == nil {
		for _, fields := range selected {
			if len(computedSelection(fields)) > 0 {
				return nil, status.Error(codes.InvalidArgument, "computed fields require relation definitions")
			}
		}
	}

	claims, err := s.resolveClaims(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Unauthenticated, "resolve access-control claims: %v", err)
	}
	prefix := "snapshot:" + strconv.FormatUint(streamSequence.Add(1), 10)
	feed, err := s.queries.OpenFeed(ctx, querysubscription.FeedOptions{BufferLimit: s.options.SubscriptionQueueLimit})
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "open query feed: %v", err)
	}
	defer feed.Close(context.WithoutCancel(ctx))
	s, prepared, err := s.prepareAccess(ctx, feed, accesscontrol.PreparedID(prefix), claims)
	if err != nil {
		return nil, status.Errorf(codes.PermissionDenied, "prepare access control: %v", err)
	}
	if prepared == nil {
		return nil, status.Error(codes.Internal, "access controller returned nil state")
	}
	defer prepared.Close()
	if prepared.ID() != accesscontrol.PreparedID(prefix) {
		return nil, status.Error(codes.Internal, "access controller returned a different prepared ID")
	}

	projection, err := proj.BeginProjection(ctx, feed, root, proj.StartOptions{
		Prefix: prefix, QueueLimit: s.options.SubscriptionQueueLimit, Limits: proj.Limits(s.options.ProjectionLimits),
		ExpansionLimiter:      s.expansionLimiter,
		WaitForSync:           request.WaitForSync,
		AllowPartialErrors:    request.AllowPartialErrors,
		CurrentImageAdmission: s.relationAccess != nil,
	})
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "start projection: %v", err)
	}
	defer projection.Close()
	if s.relationAccess != nil {
		events, positions, err := s.policySnapshot(ctx, feed, projection, prepared, selected)
		if err != nil {
			return nil, snapshotStatus("materialize authorized projection", err)
		}
		return buildSnapshotResponse(request, prefix, events, positions, selected)
	}

	pumpErr := make(chan error, 1)
	go pumpSnapshotFeed(ctx, feed, projection, pumpErr)
	events, positions, err := projection.TakeInitial(ctx)
	if err != nil {
		select {
		case feedErr := <-pumpErr:
			if feedErr != nil && !errors.Is(feedErr, context.Canceled) && !errors.Is(feedErr, querysubscription.ErrClosed) {
				err = feedErr
			}
		default:
		}
		return nil, snapshotStatus("materialize projection", err)
	}

	visibility := newProjectionVisibility(selected)
	visibility.partial = request.AllowPartialErrors
	events, err = visibility.filterInitial(ctx, s.access, prepared, events)
	if err != nil {
		return nil, snapshotStatus("evaluate access control", err)
	}
	return buildSnapshotResponse(request, prefix, events, positions, selected)
}

func pumpSnapshotFeed(ctx context.Context, feed querysubscription.Feed, projection *proj.Projection, result chan<- error) {
	for {
		event, err := feed.Next(ctx)
		if err == nil {
			_, _, err = projection.ApplyEvent(event)
		}
		if err != nil {
			projection.Close()
			select {
			case result <- err:
			default:
			}
			return
		}
	}
}

func snapshotStatus(operation string, err error) error {
	if err == nil {
		return nil
	}
	code := codes.Internal
	if failure, ok := status.FromError(err); ok {
		code = failure.Code()
	}
	switch {
	case errors.Is(err, context.Canceled):
		code = codes.Canceled
	case errors.Is(err, context.DeadlineExceeded):
		code = codes.DeadlineExceeded
	case errors.Is(err, querysubscription.ErrBackpressure):
		code = codes.ResourceExhausted
	case errors.Is(err, querysubscription.ErrClosed):
		code = codes.Unavailable
	}
	return status.Error(code, fmt.Sprintf("%s: %v", operation, err))
}
