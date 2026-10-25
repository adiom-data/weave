// Package messagegrpc exposes the transaction-message broadcast on a trusted
// service-to-service listener. Authentication belongs to that listener.
package messagegrpc

import (
	"context"
	"errors"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/adiom-data/weave/internal/querysubscription/model"
	"github.com/adiom-data/weave/internal/transactionmessage"
	queryv1 "github.com/adiom-data/weave/pkg/gen/querysubscription/v1"
	messagev1 "github.com/adiom-data/weave/pkg/gen/transactionmessage/v1"
)

type Server interface{ Register(grpc.ServiceRegistrar) }

type server struct {
	messagev1.UnimplementedTransactionMessagesServer
	service transactionmessage.Service
}

func NewServer(service transactionmessage.Service) Server { return &server{service: service} }

func (s *server) Register(registrar grpc.ServiceRegistrar) {
	messagev1.RegisterTransactionMessagesServer(registrar, s)
}

func position(p *querysubscription.Position) *queryv1.Position {
	return &queryv1.Position{SourceId: string(p.SourceID), Value: p.Value}
}

func (s *server) Open(_ *emptypb.Empty, stream grpc.ServerStreamingServer[messagev1.MessageBatch]) error {
	if s.service == nil {
		return status.Error(codes.Unavailable, "transaction message service is not configured")
	}
	feed, err := s.service.Open()
	if err != nil {
		return feedError(err)
	}
	defer feed.Close()
	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()
	// Send can block on transport flow control. Keep it off the lifecycle
	// goroutine so a terminal feed signal can return the RPC immediately. gRPC
	// cancels the stream when the handler returns, unblocking the in-flight Send.
	result := make(chan error, 1)
	go func() { result <- forward(ctx, feed, stream) }()
	select {
	case err := <-result:
		return err
	case <-feed.Done():
		return feedError(feed.Err())
	case <-ctx.Done():
		return feedError(ctx.Err())
	}
}

func forward(ctx context.Context, feed transactionmessage.Feed, stream grpc.ServerStreamingServer[messagev1.MessageBatch]) error {
	// Header() lets a caller know it is attached before issuing mutations; no
	// historical delivery is promised for mutations committed before attachment.
	if err := stream.SendHeader(metadata.MD{}); err != nil {
		return err
	}
	for {
		batch, err := feed.Receive(ctx)
		if err != nil {
			return feedError(err)
		}
		wire := &messagev1.MessageBatch{Through: position(batch.Through)}
		for _, transaction := range batch.Transactions {
			item := &messagev1.MessageTransaction{Position: position(transaction.Position)}
			for _, message := range transaction.Messages {
				item.Messages = append(item.Messages, &messagev1.TransactionMessage{Kind: message.Kind, Payload: message.Payload})
			}
			wire.Transactions = append(wire.Transactions, item)
		}
		if err := stream.Send(wire); err != nil {
			return err
		}
	}
}

func feedError(err error) error {
	switch {
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return status.FromContextError(err).Err()
	case errors.Is(err, transactionmessage.ErrSlowConsumer):
		return status.Error(codes.ResourceExhausted, err.Error())
	default:
		return status.Error(codes.Unavailable, "transaction message feed ended; reconnect and resynchronize")
	}
}
