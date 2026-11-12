package projectionserver

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	q "github.com/adiom-data/weave/internal/querysubscription/model"
	reactivev1 "github.com/adiom-data/weave/pkg/gen/reactive/v1"
)

type scriptedConnectionInput struct {
	ctx      context.Context
	requests []*reactivev1.ClientMessage
	terminal error
	reading  chan struct{}
}

func (s *scriptedConnectionInput) Context() context.Context             { return s.ctx }
func (s *scriptedConnectionInput) Send(*reactivev1.ServerMessage) error { return nil }
func (s *scriptedConnectionInput) Recv() (*reactivev1.ClientMessage, error) {
	if s.reading != nil {
		close(s.reading)
		s.reading = nil
	}
	if len(s.requests) == 0 {
		return nil, s.terminal
	}
	message := s.requests[0]
	s.requests = s.requests[1:]
	return message, nil
}

func TestConnectionInputRetainsRequestsAndTerminalFailureBeforeConsumption(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	failure := errors.New("receive failed")
	requests := []*reactivev1.ClientMessage{{RequestId: "first"}, {RequestId: "second"}}
	input := make(chan connectionInput, len(requests)+1)
	stream := &scriptedConnectionInput{ctx: ctx, requests: requests, terminal: failure}
	// Let the receiver finish while the consumer is busy elsewhere. Failure
	// must remain queued behind the requests, not race a separate close signal.
	receiveConnectionRequests(ctx, stream, input)
	for _, expected := range requests {
		item := <-input
		if item.err != nil || item.message != expected {
			t.Fatalf("request order changed: %+v", item)
		}
	}
	if item := <-input; !errors.Is(item.err, failure) || item.message != nil {
		t.Fatalf("terminal receive failure was lost: %+v", item)
	}
}

func TestConnectionInputCancellationReleasesBlockedDelivery(t *testing.T) {
	for _, terminal := range []bool{false, true} {
		t.Run(map[bool]string{false: "request", true: "terminal error"}[terminal], func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			reading := make(chan struct{})
			stream := &scriptedConnectionInput{ctx: ctx, reading: reading, terminal: io.EOF}
			if !terminal {
				stream.requests = []*reactivev1.ClientMessage{{RequestId: "first"}}
			}
			input, done := make(chan connectionInput), make(chan struct{})
			go func() { defer close(done); receiveConnectionRequests(ctx, stream, input) }()
			<-reading
			cancel()
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("receiver remained blocked after the consumer stopped")
			}
		})
	}
}

func TestConnectionReturnsReceiveFailureAndAcceptsEOF(t *testing.T) {
	failure := errors.New("receive failed")
	for _, terminal := range []error{failure, io.EOF} {
		t.Run(terminal.Error(), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			backend := newPolicyBackend(func(context.Context, q.QuerySubscribeRequest) q.QuerySnapshot { return q.QuerySnapshot{} })
			service := newServer(backend, Options{})
			err := service.ServeStream(&scriptedConnectionInput{ctx: ctx, terminal: terminal})
			want := terminal
			if errors.Is(terminal, io.EOF) {
				want = nil
			}
			if !errors.Is(err, want) {
				t.Fatalf("receive termination = %v, want %v", err, want)
			}
		})
	}
}
