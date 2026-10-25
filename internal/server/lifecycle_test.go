package server

import (
	"context"
	"errors"
	"log/slog"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/adiom-data/framework/httpapp"
	"github.com/adiom-data/framework/telemetry"
	"github.com/adiom-data/weave/pkg/projectionserver"
	"github.com/adiom-data/weave/pkg/queryserver"
)

type closeFunc func() error

func (f closeFunc) Close() error { return f() }

type lifecycleQueries struct {
	queryserver.Backend
	close closeFunc
	fail  <-chan error
}

func (q lifecycleQueries) Close() error { return q.close() }
func (q lifecycleQueries) Wait(ctx context.Context) error {
	select {
	case err := <-q.fail:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

type lifecycleProjections struct {
	projectionserver.Server
	close closeFunc
}

func (p lifecycleProjections) Close() error                   { return p.close() }
func (p lifecycleProjections) Wait(ctx context.Context) error { <-ctx.Done(); return ctx.Err() }

func TestHostLifecycle(t *testing.T) {
	for _, cause := range []string{"close before run", "close while running", "run cancellation", "component failure"} {
		t.Run(cause, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			var order []string
			closed := func(name string) closeFunc { return func() error { order = append(order, name); return nil } }
			failure := make(chan error, 1)
			ready := make(chan readyAddresses, 1)
			h := &host{
				ctx: ctx, cancel: cancel, runDone: make(chan struct{}),
				source: closed("storage"), queries: lifecycleQueries{close: closed("queries"), fail: failure},
				projections: lifecycleProjections{close: closed("projections")},
				app:         httpapp.NewService(httpapp.WithServiceAddr("127.0.0.1:0"), httpapp.WithServiceLogger(slog.New(readyLogHandler{ready})), httpapp.WithServiceTelemetry(telemetry.DisabledConfig())),
			}
			defer h.Close()
			sentinel := errors.New("fatal backend error")
			if cause != "close before run" {
				runCtx, stopRun := context.WithCancel(t.Context())
				defer stopRun()
				done := make(chan error, 1)
				go func() { done <- h.Run(runCtx) }()
				select {
				case <-ready:
				case <-time.After(time.Second):
					t.Fatal("host did not listen")
				}
				if err := h.Run(runCtx); err == nil {
					t.Fatal("host allowed a second Run")
				}
				switch cause {
				case "close while running":
					if err := h.Close(); err != nil {
						t.Fatal(err)
					}
				case "run cancellation":
					stopRun()
				case "component failure":
					failure <- sentinel
				}
				select {
				case err := <-done:
					if cause == "component failure" {
						if !errors.Is(err, sentinel) {
							t.Fatal(err)
						}
					} else if err != nil {
						t.Fatal(err)
					}
				case <-time.After(2 * time.Second):
					t.Fatal("Run did not stop")
				}
			}
			var callers sync.WaitGroup
			for i := 0; i < 4; i++ {
				callers.Go(func() {
					if err := h.Close(); err != nil {
						t.Error(err)
					}
				})
			}
			callers.Wait()
			if !reflect.DeepEqual(order, []string{"projections", "queries", "storage"}) {
				t.Fatal(order)
			}
			if err := h.Run(t.Context()); err == nil {
				t.Fatal("closed host allowed Run")
			}
		})
	}
}
