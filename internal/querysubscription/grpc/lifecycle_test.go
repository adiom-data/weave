package querygrpc

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	querysubscription "github.com/adiom-data/weave/internal/querysubscription/model"
	queryv1 "github.com/adiom-data/weave/pkg/gen/querysubscription/v1"
	"google.golang.org/grpc"
)

type blockedConnection struct {
	grpc.ClientConnInterface
	stream *blockedClientStream
}

func (c blockedConnection) NewStream(ctx context.Context, _ *grpc.StreamDesc, _ string, _ ...grpc.CallOption) (grpc.ClientStream, error) {
	c.stream.ctx = ctx
	return c.stream, nil
}

type blockedClientStream struct {
	grpc.ClientStream
	ctx         context.Context
	initialized chan struct{}
	entered     chan struct{}
	exited      chan struct{}
	once        sync.Once
	received    bool
}

func (s *blockedClientStream) SendMsg(value any) error {
	if value.(*queryv1.ClientMessage).GetInitialize() != nil {
		close(s.initialized)
		return nil
	}
	s.once.Do(func() { close(s.entered) })
	<-s.ctx.Done()
	close(s.exited)
	return s.ctx.Err()
}

func (s *blockedClientStream) RecvMsg(value any) error {
	if !s.received {
		<-s.initialized
		s.received = true
		*value.(*queryv1.ServerMessage) = queryv1.ServerMessage{RequestId: "request-1", Event: &queryv1.ServerMessage_Acknowledged{Acknowledged: &queryv1.Acknowledged{ProtocolVersion: ProtocolVersion}}}
		return nil
	}
	<-s.ctx.Done()
	return s.ctx.Err()
}

func TestRequestCancellationDoesNotWaitForTransportWriter(t *testing.T) {
	for _, operation := range []string{"progress", "close feed", "close subscription"} {
		t.Run(operation, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			stream := &blockedClientStream{initialized: make(chan struct{}), entered: make(chan struct{}), exited: make(chan struct{})}
			feed, err := NewClient(blockedConnection{stream: stream}).(*client).openFeed(ctx, querysubscription.FeedOptions{BufferLimit: 16})
			if err != nil {
				t.Fatal(err)
			}
			var call func(context.Context) error
			switch operation {
			case "progress":
				call = feed.RequestProgress
			case "close feed":
				call = feed.Close
			case "close subscription":
				subscription := &remoteSubscription{id: "item", owner: feed.(*remoteFeed)}
				call = subscription.Close
			}
			operationCtx, stop := context.WithCancel(context.Background())
			defer stop()
			result := make(chan error, 1)
			go func() { result <- call(operationCtx) }()
			select {
			case <-stream.entered:
			case <-time.After(time.Second):
				t.Fatal("operation never reached transport")
			}
			// A second call must also respect its deadline while waiting for
			// the writer or the first Close's serialization lock.
			secondCtx, stopSecond := context.WithTimeout(context.Background(), 20*time.Millisecond)
			defer stopSecond()
			second := make(chan error, 1)
			go func() { second <- call(secondCtx) }()
			select {
			case err := <-second:
				if !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("waiting operation: %v", err)
				}
			case <-time.After(time.Second):
				t.Fatal("waiting operation ignored its deadline")
			}
			stop()
			select {
			case err := <-result:
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("in-flight operation: %v", err)
				}
			case <-time.After(time.Second):
				t.Fatal("in-flight operation ignored cancellation")
			}
			cancel()
			select {
			case <-stream.exited:
			case <-time.After(time.Second):
				t.Fatal("feed cancellation did not stop transport writer")
			}
		})
	}
}

type blockedServerStream struct {
	grpc.ServerStream
	ctx      context.Context
	requests chan *queryv1.ClientMessage
	sending  chan struct{}
	exited   chan struct{}
}

func (s *blockedServerStream) Context() context.Context { return s.ctx }
func (s *blockedServerStream) Recv() (*queryv1.ClientMessage, error) {
	select {
	case message := <-s.requests:
		return message, nil
	case <-s.ctx.Done():
		return nil, s.ctx.Err()
	}
}
func (s *blockedServerStream) Send(message *queryv1.ServerMessage) error {
	if message.RequestId == "init" {
		return nil
	}
	close(s.sending)
	<-s.ctx.Done()
	close(s.exited)
	return s.ctx.Err()
}

func TestFeedFailureReturnsRPCBeforeJoiningBlockedOperation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream := &blockedServerStream{ctx: ctx, requests: make(chan *queryv1.ClientMessage, 2), sending: make(chan struct{}), exited: make(chan struct{})}
	stream.requests <- &queryv1.ClientMessage{RequestId: "init", Operation: &queryv1.ClientMessage_Initialize{Initialize: &queryv1.Initialize{ProtocolVersion: ProtocolVersion, BufferLimit: 16}}}
	stream.requests <- &queryv1.ClientMessage{RequestId: "progress", Operation: &queryv1.ClientMessage_RequestProgress{RequestProgress: &queryv1.RequestProgress{}}}
	backend := newTestSession(nil)
	cleaned := make(chan struct{})
	backend.beforeClose = func(context.Context) error { close(cleaned); return nil }
	returned := make(chan error, 1)
	go func() {
		defer cancel() // gRPC cancels transport I/O after its handler returns.
		returned <- NewServer(&testClient{session: backend}).(*server).Open(stream)
	}()
	select {
	case <-stream.sending:
	case <-time.After(time.Second):
		t.Fatal("operation did not block in Send")
	}
	backend.events.Close(querysubscription.ErrClosed)
	select {
	case err := <-returned:
		if err == nil {
			t.Fatal("unexpected feed failure did not fail the RPC")
		}
	case <-time.After(time.Second):
		t.Fatal("RPC waited for blocked Send instead of returning to cancel it")
	}
	for _, done := range []<-chan struct{}{stream.exited, cleaned} {
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("RPC teardown did not release transport and backend workers")
		}
	}
}
