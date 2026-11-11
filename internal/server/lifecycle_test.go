package server

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/adiom-data/framework/httpapp"
	"github.com/adiom-data/weave/internal/metrics"
	"github.com/adiom-data/weave/pkg/projectionserver"
	"github.com/adiom-data/weave/pkg/queryserver"
	"go.opentelemetry.io/otel"
	otelmetric "go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
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

func TestServerLifecycle(t *testing.T) {
	for _, cause := range []string{"partial construction", "close", "parent cancellation", "component failure", "canceled wait"} {
		t.Run(cause, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			var order []string
			closed := func(name string) closeFunc { return func() error { order = append(order, name); return nil } }
			failure := make(chan error, 1)
			s := &server{
				ctx: ctx, cancel: cancel,
				source: closed("storage"), queries: lifecycleQueries{close: closed("queries"), fail: failure},
				projections: lifecycleProjections{close: closed("projections")},
			}
			defer s.Close()
			if cause != "partial construction" {
				s.done = make(chan struct{})
				go s.supervise()
				sentinel := errors.New("fatal backend error")
				switch cause {
				case "close":
					if err := s.Close(); err != nil {
						t.Fatal(err)
					}
				case "parent cancellation":
					cancel()
				case "component failure":
					failure <- sentinel
				case "canceled wait":
					waitCtx, stop := context.WithCancel(ctx)
					stop()
					if err := s.Wait(waitCtx); !errors.Is(err, context.Canceled) {
						t.Fatal(err)
					}
					select {
					case <-s.done:
						t.Fatal("canceling a wait stopped the server")
					default:
					}
					cancel()
				}
				waitCtx, stop := context.WithTimeout(t.Context(), 2*time.Second)
				defer stop()
				err := s.Wait(waitCtx)
				if cause == "component failure" {
					if !errors.Is(err, sentinel) {
						t.Fatal(err)
					}
				} else if !errors.Is(err, context.Canceled) {
					t.Fatal(err)
				}
			}
			var callers sync.WaitGroup
			for range 4 {
				callers.Go(func() {
					if err := s.Close(); err != nil {
						t.Error(err)
					}
				})
			}
			callers.Wait()
			if !reflect.DeepEqual(order, []string{"projections", "queries", "storage"}) {
				t.Fatal(order)
			}
		})
	}
}

// Test applications own HTTP serving and supervise the reactive stack alongside it.
func serveTestServer(ctx context.Context, app httpapp.Service, reactive Server) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		err := reactive.Wait(ctx)
		if ctx.Err() != nil && errors.Is(err, ctx.Err()) {
			err = nil
		}
		done <- err
		cancel()
	}()
	httpErr := app.Run(ctx)
	cancel()
	reactiveErr := <-done
	return errors.Join(httpErr, reactiveErr)
}

func TestServerLeavesCallerHTTPAndTelemetryRunning(t *testing.T) {
	// Isolate OpenTelemetry's irreversible initial provider delegation.
	if os.Getenv("WEAVE_TEST_SERVER_TELEMETRY_CHILD") != "1" {
		binary, err := os.Executable()
		if err != nil {
			t.Fatal(err)
		}
		child := exec.CommandContext(t.Context(), binary, "-test.run=^TestServerLeavesCallerHTTPAndTelemetryRunning$", "-test.count=1")
		child.Env = append(os.Environ(), "WEAVE_TEST_SERVER_TELEMETRY_CHILD=1")
		if output, err := child.CombinedOutput(); err != nil {
			t.Fatalf("isolated telemetry test failed: %v\n%s", err, output)
		}
		return
	}
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	otel.SetMeterProvider(provider)
	t.Cleanup(func() {
		if err := provider.Shutdown(context.Background()); err != nil {
			t.Error(err)
		}
	})
	weaveGauge := metrics.NewGauge(metrics.CachedQueries)
	weaveGauge.Set(7)
	defer weaveGauge.Close()
	meter := provider.Meter("embedding-app")
	instrument, err := meter.Int64ObservableGauge("embedding.app.ready")
	if err != nil {
		t.Fatal(err)
	}
	registration, err := meter.RegisterCallback(func(_ context.Context, observer otelmetric.Observer) error {
		observer.ObserveInt64(instrument, 11)
		return nil
	}, instrument)
	if err != nil {
		t.Fatal(err)
	}
	defer registration.Unregister()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	s := &server{
		ctx: ctx, cancel: cancel, done: make(chan struct{}),
		queries:     lifecycleQueries{close: closeFunc(func() error { return nil })},
		projections: lifecycleProjections{close: closeFunc(func() error { return nil })},
	}
	go s.supervise()
	defer s.Close()
	app := httpapp.NewService(httpapp.WithServices(s.Services()...), httpapp.WithServiceRoutes(append(s.Routes(),
		httpapp.Handle("GET /application", http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })))...))
	httpServer := httptest.NewServer(app.Handler())
	defer httpServer.Close()
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	response, err := http.Get(httpServer.URL + "/application")
	if err != nil {
		t.Fatal("reactive shutdown stopped application HTTP:", err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusNoContent {
		t.Fatal(response.StatusCode)
	}
	if otel.GetMeterProvider() != provider {
		t.Fatal("reactive server changed the caller's provider")
	}
	var collected metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &collected); err != nil {
		t.Fatal("reactive server shut down the caller's provider:", err)
	}
	want := map[string]int64{"embedding.app.ready": 11, metrics.CachedQueries: 7}
	for _, scope := range collected.ScopeMetrics {
		for _, measurement := range scope.Metrics {
			expected, required := want[measurement.Name]
			if !required {
				continue
			}
			value, ok := measurement.Data.(metricdata.Gauge[int64])
			if !ok || len(value.DataPoints) != 1 || value.DataPoints[0].Value != expected {
				t.Fatalf("unexpected measurement: %#v", measurement)
			}
			delete(want, measurement.Name)
		}
	}
	if len(want) != 0 {
		t.Fatalf("metrics missing after reactive shutdown: %v", want)
	}
}
