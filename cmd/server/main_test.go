package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/adiom-data/framework/httpapp"
	"github.com/adiom-data/framework/telemetry"
	"github.com/adiom-data/weave/pkg/server"
)

type waitServer struct {
	server.Server
	wait func(context.Context) error
}

func (s waitServer) Wait(ctx context.Context) error { return s.wait(ctx) }

func testApplication(address string) httpapp.Service {
	return httpapp.NewService(
		httpapp.WithServiceAddr(address),
		httpapp.WithServiceLogger(slog.New(slog.NewTextHandler(io.Discard, nil))),
		httpapp.WithServiceTelemetry(telemetry.DisabledConfig()),
	)
}

func TestServeStopsHTTPOnReactiveFailure(t *testing.T) {
	for _, failure := range []error{errors.New("reactive failure"), context.Canceled} {
		t.Run(failure.Error(), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
			defer cancel()
			reactive := waitServer{wait: func(context.Context) error { return failure }}
			if err := serve(ctx, testApplication("127.0.0.1:0"), reactive); !errors.Is(err, failure) {
				t.Fatalf("lost reactive failure: %v", err)
			}
		})
	}
}

func TestServeHTTPFailureCancelsReactiveWait(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	stopped := make(chan struct{})
	reactive := waitServer{wait: func(ctx context.Context) error {
		defer close(stopped)
		<-ctx.Done()
		return ctx.Err()
	}}
	err := serve(ctx, testApplication("invalid-address"), reactive)
	if err == nil || errors.Is(err, context.Canceled) {
		t.Fatalf("expected HTTP startup failure: %v", err)
	}
	select {
	case <-stopped:
	default:
		t.Fatal("HTTP startup failure left the reactive waiter running")
	}
}
