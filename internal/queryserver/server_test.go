package queryserver

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/adiom-data/weave/internal/querysubscription/model"
)

func TestInvalidConfigurationDoesNotStartBackend(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := New(ctx, nil, Options{}); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err := New(context.Background(), nil, Options{}); err == nil {
		t.Fatal("nil storage accepted")
	}
}

func TestSupervisionCancelsAndJoinsAllComponents(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sentinel := errors.New("source failed")
	var stopped atomic.Bool
	err := supervise(ctx, cancel,
		func(context.Context) error { return sentinel },
		func(context.Context) error { <-ctx.Done(); stopped.Store(true); return ctx.Err() },
	)
	if !errors.Is(err, sentinel) || !stopped.Load() {
		t.Fatalf("supervision = %v, joined = %v", err, stopped.Load())
	}
}

func TestCloseIsIdempotentAndWaitTimeoutDoesNotStopBackend(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	b := &backend{cancel: cancel, done: make(chan struct{})}
	go func() {
		b.err = supervise(ctx, cancel, func(context.Context) error { <-ctx.Done(); return ctx.Err() })
		close(b.done)
	}()
	waitCtx, stopWait := context.WithCancel(context.Background())
	stopWait()
	if err := b.Wait(waitCtx); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if ctx.Err() != nil {
		t.Fatal("waiting stopped the backend")
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	if err := b.Wait(context.Background()); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err := b.OpenFeed(context.Background(), querysubscription.FeedOptions{BufferLimit: 1}); !errors.Is(err, querysubscription.ErrClosed) {
		t.Fatalf("feed opened after backend shutdown: %v", err)
	}
}
