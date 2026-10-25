package projectionserver

import (
	"context"
	"testing"
	"time"

	q "github.com/adiom-data/weave/internal/querysubscription/model"
	reactivev1 "github.com/adiom-data/weave/pkg/gen/reactive/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type stalledSendStream struct {
	ctx     context.Context
	entered chan struct{}
	release chan struct{}
}

func (s *stalledSendStream) Context() context.Context { return s.ctx }
func (s *stalledSendStream) Send(*reactivev1.ServerMessage) error {
	close(s.entered)
	<-s.release
	return s.ctx.Err()
}
func (s *stalledSendStream) Recv() (*reactivev1.ClientMessage, error) {
	<-s.ctx.Done()
	return nil, s.ctx.Err()
}

func TestServeStreamDoesNotReturnWhileSending(t *testing.T) {
	for _, cause := range []string{"client cancellation", "message source failure"} {
		t.Run(cause, func(t *testing.T) {
			_, router := messageRouter(t)
			backend := newPolicyBackend(func(context.Context, q.QuerySubscribeRequest) q.QuerySnapshot {
				return policySnapshot(10, policyRow("projects", "one", "project"))
			})
			service := newServerWithRouter(backend, router)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			stream := &stalledSendStream{ctx: ctx, entered: make(chan struct{}), release: make(chan struct{})}
			finished := make(chan error, 1)
			go func() { finished <- service.ServeStream(stream) }()
			select {
			case <-stream.entered:
			case <-time.After(time.Second):
				close(stream.release)
				t.Fatal("session did not start sending")
			}
			if cause == "client cancellation" {
				cancel()
			} else {
				router.Close()
			}
			select {
			case err := <-finished:
				close(stream.release)
				t.Fatalf("handler returned while Send still owned the transport: %v", err)
			case <-time.After(25 * time.Millisecond):
			}
			close(stream.release)
			select {
			case err := <-finished:
				if err == nil {
					t.Fatal("lost cancellation/source failure")
				}
				if cause == "message source failure" && status.Code(err) != codes.Unavailable {
					t.Fatalf("lost source failure status: %v", err)
				}
			case <-time.After(time.Second):
				t.Fatal("handler did not finish after Send returned")
			}
		})
	}
}
