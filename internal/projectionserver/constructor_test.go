package projectionserver

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/adiom-data/weave/internal/querysubscription/model"
	"github.com/adiom-data/weave/internal/transactionmessage"
)

type constructorQueries struct{}

func (constructorQueries) OpenFeed(context.Context, querysubscription.FeedOptions) (querysubscription.Feed, error) {
	return nil, errors.New("unexpected query open during construction")
}

type constructorMessages struct {
	opens, closes atomic.Int32
	err           error
}

func (c *constructorMessages) Open(ctx context.Context) (transactionmessage.Stream, error) {
	c.opens.Add(1)
	if c.err != nil {
		return nil, c.err
	}
	return &constructorStream{ctx: ctx, owner: c}, nil
}

type constructorStream struct {
	ctx   context.Context
	owner *constructorMessages
}

func (f *constructorStream) Receive(ctx context.Context) (transactionmessage.Batch, error) {
	select {
	case <-ctx.Done():
		return transactionmessage.Batch{}, ctx.Err()
	case <-f.ctx.Done():
		return transactionmessage.Batch{}, f.ctx.Err()
	}
}
func (f *constructorStream) Close() error { f.owner.closes.Add(1); return nil }

func TestConstructorOwnsOptionalMessageRoutingLifetime(t *testing.T) {
	messages := &constructorMessages{}
	s, err := New(context.Background(), constructorQueries{}, Options{TransactionMessages: messages})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if messages.opens.Load() != 1 {
		t.Fatal("constructor did not confirm message attachment")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := s.Wait(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Wait after close: %v", err)
	}
	if messages.closes.Load() != 1 {
		t.Fatalf("message stream closed %d times", messages.closes.Load())
	}
}

func TestConstructorRejectsMissingBackendAndFailedMessageAttachment(t *testing.T) {
	if _, err := New(context.Background(), nil, Options{}); err == nil {
		t.Fatal("nil query backend accepted")
	}
	sentinel := errors.New("message service unavailable")
	if _, err := New(context.Background(), constructorQueries{}, Options{TransactionMessages: &constructorMessages{err: sentinel}}); !errors.Is(err, sentinel) {
		t.Fatal(err)
	}
	if _, err := New(context.Background(), constructorQueries{}, Options{TransactionMessages: &constructorMessages{}, MessageOptions: MessageOptions{MaxQueuedBytes: -1}}); err == nil {
		t.Fatal("invalid routing options accepted")
	}
	s, err := New(context.Background(), constructorQueries{}, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	s.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := s.Wait(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("server without messages did not close: %v", err)
	}
}

func TestConstructorRejectsInvalidDependencyConfiguration(t *testing.T) {
	for _, options := range []DependencyOptions{{UpstreamBufferLimit: -1}, {MaxSubscriptions: -1}, {IdleTTL: -time.Second}, {SweepInterval: -time.Second}, {EvictionBatchSize: -1}, {CleanupBatchSize: -1}} {
		if _, err := New(context.Background(), constructorQueries{}, Options{Dependencies: options}); err == nil {
			t.Fatalf("invalid dependency options accepted: %+v", options)
		}
	}
}
