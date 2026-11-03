package querygrpc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/adiom-data/weave/internal/querysubscription/model"
	querysubscriptionv1 "github.com/adiom-data/weave/pkg/gen/querysubscription/v1"
)

// server exposes a querysubscription.Client without exposing coordinator or
// datastore implementation details.
type Server interface{ Register(grpc.ServiceRegistrar) }

type server struct {
	querysubscriptionv1.UnimplementedQuerySubscriptionsServer
	client  querysubscription.Client
	options TransportOptions
}

func NewServer(client querysubscription.Client) Server {
	return NewServerWithOptions(client, TransportOptions{})
}

func NewServerWithOptions(client querysubscription.Client, options TransportOptions) Server {
	return &server{client: client, options: options.withDefaults()}
}

func (s *server) Register(registrar grpc.ServiceRegistrar) {
	querysubscriptionv1.RegisterQuerySubscriptionsServer(registrar, s)
}

func (s *server) Open(stream grpc.BidiStreamingServer[querysubscriptionv1.ClientMessage, querysubscriptionv1.ServerMessage]) error {
	if s.client == nil {
		return errors.New("query subscription client is required")
	}
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	open := first.GetInitialize()
	if first.RequestId == "" || open == nil || open.BufferLimit == 0 {
		return errors.New("first message must initialize the feed with a request ID and positive buffer limit")
	}
	if open.ProtocolVersion != ProtocolVersion {
		return fmt.Errorf("unsupported query subscription protocol version %d", open.ProtocolVersion)
	}
	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()
	feed, err := s.client.OpenFeed(ctx, querysubscription.FeedOptions{BufferLimit: int(open.BufferLimit)})
	if err != nil {
		return err
	}

	state := serverFeed{ctx: ctx, feed: feed, stream: stream, options: s.options, subscriptions: make(map[string]querysubscription.Subscription), operations: make(map[string]context.CancelFunc)}
	var operations sync.WaitGroup
	received := make(chan error, 1)
	forwarded := make(chan error, 1)
	receiverDone := make(chan struct{})
	initialized := make(chan struct{})
	// Returning the RPC is what cancels transport Send/Recv. Never wait for
	// those workers before returning: flow control could otherwise deadlock
	// teardown. Cleanup joins them after gRPC has released the transport.
	defer func() {
		cancel()
		go func() {
			<-receiverDone // no more operation workers can be added
			operations.Wait()
			_ = state.closeFeed(context.WithoutCancel(ctx))
		}()
	}()
	operations.Add(1)
	go func() {
		defer operations.Done()
		select {
		case <-initialized:
		case <-ctx.Done():
			return
		}
		if err := state.forwardFeed(); err != nil {
			forwarded <- err
		}
	}()
	go func() {
		defer close(receiverDone)
		err := state.send(&querysubscriptionv1.ServerMessage{RequestId: first.RequestId, Event: &querysubscriptionv1.ServerMessage_Acknowledged{Acknowledged: &querysubscriptionv1.Acknowledged{ProtocolVersion: ProtocolVersion}}})
		if err == nil {
			close(initialized)
			err = state.receiveRequests(&operations)
		}
		received <- err
	}()
	select {
	case err := <-received:
		return err
	case err := <-forwarded:
		if errors.Is(err, querysubscription.ErrBackpressure) {
			return status.Error(codes.ResourceExhausted, err.Error())
		}
		return status.Errorf(codes.Unavailable, "query feed ended; reconnect and resynchronize: %v", err)
	case <-ctx.Done():
		return status.FromContextError(ctx.Err()).Err()
	}
}

func (state *serverFeed) receiveRequests(operations *sync.WaitGroup) error {
	for {
		message, receiveErr := state.stream.Recv()
		if receiveErr != nil {
			if errors.Is(receiveErr, io.EOF) || errors.Is(receiveErr, context.Canceled) {
				return nil
			}
			return receiveErr
		}
		if cancellation := message.GetCancelRequest(); cancellation != nil {
			if message.RequestId == "" || cancellation.TargetRequestId == "" {
				if sendErr := state.sendError(message.RequestId, "", querysubscriptionv1.Error_CODE_INVALID_REQUEST, "request_id and target_request_id are required"); sendErr != nil {
					return sendErr
				}
				continue
			}
			state.cancelOperation(cancellation.TargetRequestId)
			if sendErr := state.ack(message.RequestId); sendErr != nil {
				return sendErr
			}
			continue
		}
		operationCtx, finish, beginErr := state.beginOperation(message.RequestId, message.GetCloseFeed() != nil)
		if beginErr != nil {
			_ = state.sendError(message.RequestId, operationSubscriptionID(message), querysubscriptionv1.Error_CODE_INVALID_REQUEST, beginErr.Error())
			continue
		}
		operations.Add(1)
		go func() {
			defer operations.Done()
			defer finish()
			state.handle(operationCtx, message)
		}()
	}
}

type serverFeed struct {
	ctx     context.Context
	feed    querysubscription.Feed
	stream  grpc.BidiStreamingServer[querysubscriptionv1.ClientMessage, querysubscriptionv1.ServerMessage]
	options TransportOptions

	sendMu        sync.Mutex
	closeMu       sync.Mutex
	mu            sync.Mutex
	closed        bool
	released      bool
	subscriptions map[string]querysubscription.Subscription
	operations    map[string]context.CancelFunc
	payloadID     atomic.Uint64
}

func (s *serverFeed) send(message *querysubscriptionv1.ServerMessage) error {
	s.sendMu.Lock()
	defer s.sendMu.Unlock()
	return s.stream.Send(message)
}

func (s *serverFeed) beginOperation(requestID string, allowClosed bool) (context.Context, func(), error) {
	if requestID == "" {
		return nil, nil, errors.New("request_id is required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed && !allowClosed {
		return nil, nil, querysubscription.ErrClosed
	}
	if _, exists := s.operations[requestID]; exists {
		return nil, nil, errors.New("request_id is already active")
	}
	ctx, cancel := context.WithCancel(s.ctx)
	s.operations[requestID] = cancel
	return ctx, func() {
		s.mu.Lock()
		delete(s.operations, requestID)
		s.mu.Unlock()
		cancel()
	}, nil
}

func (s *serverFeed) cancelOperation(requestID string) {
	s.mu.Lock()
	cancel := s.operations[requestID]
	s.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

func (s *serverFeed) handle(ctx context.Context, message *querysubscriptionv1.ClientMessage) {
	if message == nil || message.RequestId == "" {
		_ = s.sendError("", "", querysubscriptionv1.Error_CODE_INVALID_REQUEST, "request_id is required")
		return
	}
	var err error
	switch operation := message.Operation.(type) {
	case *querysubscriptionv1.ClientMessage_SubscribeQuery:
		err = s.subscribe(ctx, message.RequestId, operation.SubscribeQuery)
	case *querysubscriptionv1.ClientMessage_SubscribeEntity:
		err = s.subscribeEntity(ctx, message.RequestId, operation.SubscribeEntity)
	case *querysubscriptionv1.ClientMessage_UpdateWindow:
		err = s.updateWindow(ctx, operation.UpdateWindow)
		if err == nil {
			err = s.ack(message.RequestId)
		}
	case *querysubscriptionv1.ClientMessage_Unsubscribe:
		err = s.unsubscribe(ctx, operation.Unsubscribe)
		if err == nil {
			err = s.ack(message.RequestId)
		}
	case *querysubscriptionv1.ClientMessage_RequestProgress:
		if operation.RequestProgress == nil {
			err = errors.New("progress request is required")
		} else {
			err = s.feed.RequestProgress(ctx)
		}
		if err == nil {
			err = s.ack(message.RequestId)
		}
	case *querysubscriptionv1.ClientMessage_CloseFeed:
		err = s.closeFeed(ctx)
		if err == nil {
			err = s.ack(message.RequestId)
		}
	default:
		err = errors.New("operation is required and initialize may only be sent first")
	}
	if err != nil && s.ctx.Err() == nil {
		_ = s.sendError(message.RequestId, operationSubscriptionID(message), errorCode(err), err.Error())
	}
}

func (s *serverFeed) subscribe(ctx context.Context, requestID string, request *querysubscriptionv1.SubscribeQuery) error {
	if request == nil || request.SubscriptionId == "" || request.Window == nil || request.Window.Count == 0 {
		return errors.New("subscription_id and a positive initial window are required")
	}
	query, err := queryFromProto(request.Query)
	if err != nil {
		return err
	}
	reference, err := referenceFromProto(request.Reference)
	if err != nil {
		return err
	}
	err = s.feed.SubscribeQuery(ctx, querysubscription.QuerySubscribeRequest{
		ID: request.SubscriptionId, Query: query, Window: querysubscription.InitialWindow{FromEnd: request.Window.FromEnd, Count: int(request.Window.Count)}, Reference: reference,
	}, func(subscription querysubscription.QuerySubscription, snapshot querysubscription.QuerySnapshot) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		converted, convertErr := snapshotToProto(snapshot)
		if convertErr != nil {
			return convertErr
		}
		s.mu.Lock()
		if err := ctx.Err(); err != nil {
			s.mu.Unlock()
			return err
		}
		if _, exists := s.subscriptions[request.SubscriptionId]; exists {
			s.mu.Unlock()
			return errors.New("subscription already exists")
		}
		s.subscriptions[request.SubscriptionId] = subscription
		s.mu.Unlock()
		if sendErr := s.sendPayload(requestID, request.SubscriptionId, querysubscriptionv1.PayloadChunk_KIND_QUERY_SNAPSHOT, converted); sendErr != nil {
			s.removeSubscription(request.SubscriptionId, subscription)
			return sendErr
		}
		return nil
	})
	if err != nil {
		return err
	}
	return nil
}

func (s *serverFeed) subscribeEntity(ctx context.Context, requestID string, request *querysubscriptionv1.SubscribeEntity) error {
	if request == nil || request.SubscriptionId == "" {
		return errors.New("entity subscription_id is required")
	}
	query, err := queryFromProto(request.Query)
	if err != nil {
		return err
	}
	from, err := referenceSourceFromProto(request.FetchFrom)
	if err != nil {
		return err
	}
	return s.feed.SubscribeEntity(ctx, querysubscription.EntitySubscribeRequest{ID: request.SubscriptionId, Query: query, FetchFrom: from}, func(subscription querysubscription.EntitySubscription, snapshot querysubscription.EntitySnapshot) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		converted, convertErr := entityInstallationToProto(querysubscription.EntityInstallation{Target: querysubscription.EntityTarget{SubscriptionID: request.SubscriptionId}, Snapshot: snapshot})
		if convertErr != nil {
			return convertErr
		}
		s.mu.Lock()
		if err := ctx.Err(); err != nil {
			s.mu.Unlock()
			return err
		}
		if _, exists := s.subscriptions[request.SubscriptionId]; exists {
			s.mu.Unlock()
			return errors.New("subscription already exists")
		}
		s.subscriptions[request.SubscriptionId] = subscription
		s.mu.Unlock()
		if sendErr := s.sendPayload(requestID, request.SubscriptionId, querysubscriptionv1.PayloadChunk_KIND_ENTITY_INSTALLED, converted); sendErr != nil {
			s.removeSubscription(request.SubscriptionId, subscription)
			return sendErr
		}
		return nil
	})
}

func (s *serverFeed) forwardFeed() error {
	for {
		event, err := s.feed.Next(s.ctx)
		if err != nil {
			if errors.Is(err, querysubscription.ErrClosed) {
				s.mu.Lock()
				closing := s.closed
				s.mu.Unlock()
				if closing {
					// The close operation owns cleanup and its acknowledgment.
					// Ending feed output must not cancel that operation's context.
					return nil
				}
			}
			return err
		}
		if err := event.Validate(); err != nil {
			return fmt.Errorf("invalid query feed event: %w", err)
		}
		switch {
		case event.Entity != nil:
			value, err := entityInstallationToProto(*event.Entity)
			if err != nil {
				return err
			}
			if err := s.sendPayload("", "", querysubscriptionv1.PayloadChunk_KIND_ENTITY_INSTALLED, value); err != nil {
				return err
			}
		case event.Delivery != nil && event.Progress == nil && event.Failure == nil:
			converted, convertErr := envelopeToProto(*event.Delivery)
			if convertErr != nil {
				return fmt.Errorf("encode delivery: %w", convertErr)
			}
			if sendErr := s.sendPayload("", "", querysubscriptionv1.PayloadChunk_KIND_DELIVERY, converted); sendErr != nil {
				return sendErr
			}
		case event.Progress != nil && event.Delivery == nil && event.Failure == nil:
			progress := event.Progress
			if sendErr := s.send(&querysubscriptionv1.ServerMessage{Event: &querysubscriptionv1.ServerMessage_Progress{Progress: &querysubscriptionv1.Progress{
				Position: positionToProto(progress.Position), Revision: progress.Revision, PreviousRevision: progress.PreviousRevision,
			}}}); sendErr != nil {
				return sendErr
			}
		case event.Failure != nil && event.Delivery == nil && event.Progress == nil:
			failure := event.Failure
			if subscription := s.subscription(failure.SubscriptionID); subscription != nil {
				s.removeSubscription(failure.SubscriptionID, subscription)
				_ = subscription.Close(context.WithoutCancel(s.ctx))
			}
			if sendErr := s.sendError("", failure.SubscriptionID, errorCode(failure.Cause), failure.Cause.Error()); sendErr != nil {
				return sendErr
			}
		default:
			return errors.New("query feed returned an invalid event")
		}
	}
}

func (s *serverFeed) updateWindow(ctx context.Context, request *querysubscriptionv1.UpdateWindow) error {
	if request == nil || request.SubscriptionId == "" || len(request.Changes) == 0 {
		return errors.New("subscription_id and boundary changes are required")
	}
	subscription := s.subscription(request.SubscriptionId)
	if subscription == nil {
		return errors.New("subscription does not exist")
	}
	changes := make([]querysubscription.BoundaryChange, 0, len(request.Changes))
	for _, change := range request.Changes {
		converted, err := changeFromProto(change)
		if err != nil {
			return err
		}
		changes = append(changes, converted)
	}
	query, ok := subscription.(querysubscription.QuerySubscription)
	if !ok {
		return errors.New("entity subscriptions do not have mutable windows")
	}
	return query.UpdateWindow(ctx, changes, request.Token)
}

func (s *serverFeed) unsubscribe(ctx context.Context, request *querysubscriptionv1.Unsubscribe) error {
	if request == nil || request.SubscriptionId == "" {
		return errors.New("subscription_id is required")
	}
	s.mu.Lock()
	subscription := s.subscriptions[request.SubscriptionId]
	s.mu.Unlock()
	if subscription == nil {
		return nil
	}
	if err := subscription.Close(ctx); err != nil {
		return err
	}
	s.mu.Lock()
	if s.subscriptions[request.SubscriptionId] == subscription {
		delete(s.subscriptions, request.SubscriptionId)
	}
	s.mu.Unlock()
	return nil
}

func (s *serverFeed) closeFeed(ctx context.Context) error {
	s.closeMu.Lock()
	defer s.closeMu.Unlock()
	s.mu.Lock()
	if s.released {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	s.mu.Unlock()
	if err := s.feed.Close(ctx); err != nil {
		return err
	}
	s.mu.Lock()
	s.released = true
	s.mu.Unlock()
	return nil
}

func (s *serverFeed) removeSubscription(id string, subscription querysubscription.Subscription) {
	s.mu.Lock()
	if s.subscriptions[id] == subscription {
		delete(s.subscriptions, id)
	}
	s.mu.Unlock()
}

func (s *serverFeed) sendPayload(requestID, subscriptionID string, kind querysubscriptionv1.PayloadChunk_Kind, message proto.Message) error {
	data, err := marshalPayload(message, s.options.MaxPayloadBytes)
	if err != nil {
		return err
	}
	payloadID := s.payloadID.Add(1)
	index := uint32(0)
	for offset := 0; ; {
		end := offset + s.options.ChunkBytes
		if end > len(data) {
			end = len(data)
		}
		final := end == len(data)
		chunk := &querysubscriptionv1.PayloadChunk{
			Kind: kind, SubscriptionId: subscriptionID, PayloadId: payloadID, Index: index,
			Final: final, TotalSize: uint64(len(data)), Data: append([]byte(nil), data[offset:end]...),
		}
		if err := s.send(&querysubscriptionv1.ServerMessage{RequestId: requestID, Event: &querysubscriptionv1.ServerMessage_PayloadChunk{PayloadChunk: chunk}}); err != nil {
			return err
		}
		if final {
			return nil
		}
		offset = end
		index++
	}
}

func (s *serverFeed) subscription(id string) querysubscription.Subscription {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.subscriptions[id]
}

func (s *serverFeed) ack(requestID string) error {
	return s.send(&querysubscriptionv1.ServerMessage{RequestId: requestID, Event: &querysubscriptionv1.ServerMessage_Acknowledged{Acknowledged: &querysubscriptionv1.Acknowledged{}}})
}

func (s *serverFeed) sendError(requestID, subscriptionID string, code querysubscriptionv1.Error_Code, message string) error {
	return s.send(&querysubscriptionv1.ServerMessage{RequestId: requestID, Event: &querysubscriptionv1.ServerMessage_Error{Error: &querysubscriptionv1.Error{Code: code, SubscriptionId: subscriptionID, Message: message}}})
}

func operationSubscriptionID(message *querysubscriptionv1.ClientMessage) string {
	switch operation := message.Operation.(type) {
	case *querysubscriptionv1.ClientMessage_SubscribeQuery:
		return operation.SubscribeQuery.GetSubscriptionId()
	case *querysubscriptionv1.ClientMessage_SubscribeEntity:
		return operation.SubscribeEntity.GetSubscriptionId()
	case *querysubscriptionv1.ClientMessage_UpdateWindow:
		return operation.UpdateWindow.GetSubscriptionId()
	case *querysubscriptionv1.ClientMessage_Unsubscribe:
		return operation.Unsubscribe.GetSubscriptionId()
	default:
		return ""
	}
}

func errorCode(err error) querysubscriptionv1.Error_Code {
	switch {
	case errors.Is(err, querysubscription.ErrBackpressure):
		return querysubscriptionv1.Error_CODE_BACKPRESSURE
	case errors.Is(err, querysubscription.ErrClosed), errors.Is(err, context.Canceled):
		return querysubscriptionv1.Error_CODE_CLOSED
	default:
		return querysubscriptionv1.Error_CODE_INVALID_REQUEST
	}
}
