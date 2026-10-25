package server

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/adiom-data/framework/httpapp"
	"github.com/adiom-data/framework/telemetry"
	"github.com/adiom-data/weave/internal/server/identity"
	"github.com/adiom-data/weave/pkg/accesscontrol"
	reactivev1 "github.com/adiom-data/weave/pkg/gen/reactive/v1"
	"github.com/adiom-data/weave/pkg/projectionserver"
	projectionws "github.com/adiom-data/weave/pkg/projectionserver/websocket"
	"github.com/gorilla/websocket"
)

func TestFrameworkRunContextCancelsActiveRequest(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	arrived := make(chan struct{})
	ready := make(chan readyAddresses, 1)
	app := httpapp.NewService(
		httpapp.WithServiceAddr("127.0.0.1:0"),
		httpapp.WithServiceLogger(slog.New(readyLogHandler{ready: ready})),
		httpapp.WithServiceTelemetry(telemetry.DisabledConfig()),
		httpapp.WithServiceRoutes(httpapp.Handle("GET /waiting", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			close(arrived)
			<-r.Context().Done()
			w.WriteHeader(http.StatusServiceUnavailable)
		}))),
	)
	finished := make(chan error, 1)
	go func() { finished <- app.Run(ctx) }()
	var address string
	select {
	case a := <-ready:
		address = a.address
	case err := <-finished:
		t.Fatalf("host failed startup: %v", err)
	case <-ctx.Done():
		t.Fatal("host startup timed out")
	}
	clientDone := make(chan error, 1)
	go func() {
		client := http.Client{Timeout: 5 * time.Second}
		response, err := client.Get("http://" + address + "/waiting")
		if response != nil {
			response.Body.Close()
		}
		clientDone <- err
	}()
	select {
	case <-arrived:
	case <-ctx.Done():
		t.Fatal("request did not arrive")
	}
	cancel()
	select {
	case err := <-finished:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("active request did not inherit host cancellation")
	}
	select {
	case err := <-clientDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("client remained blocked after shutdown")
	}
}

type socketService struct {
	projectionserver.Server
	serve func(projectionserver.Stream) error
}

func (s socketService) ServeStream(stream projectionserver.Stream) error { return s.serve(stream) }
func TestSocketAuthenticatesOnceAndCallsProjectionDirectly(t *testing.T) {
	var checks atomic.Int32
	authenticate := identity.Authenticator(func(_ context.Context, headers []string) (accesscontrol.Claims, error) {
		checks.Add(1)
		if len(headers) != 1 || headers[0] != "Bearer token" {
			t.Fatal("missing bearer header")
		}
		return accesscontrol.Claims{}, nil
	})
	called := make(chan struct{}, 1)
	service := socketService{serve: func(s projectionserver.Stream) error {
		if _, err := accesscontrol.ClaimsFromContext(s.Context()); err != nil {
			t.Error(err)
		}
		called <- struct{}{}
		if err := s.Send(&reactivev1.ServerMessage{Event: &reactivev1.ServerMessage_ConnectionOpened{ConnectionOpened: &reactivev1.ConnectionOpened{ConnectionContext: "test"}}}); err != nil {
			return err
		}
		_, err := s.Recv()
		return err
	}}
	c := validConfig()
	c.WebOrigin = "*"
	host := httptest.NewServer(newSocketHandler(c, nil, authenticate, service))
	defer host.Close()
	socket, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(host.URL, "http")+"/api/reactive", http.Header{"Authorization": []string{"Bearer token"}})
	if err != nil {
		t.Fatal(err)
	}
	defer socket.Close()
	select {
	case <-called:
	case <-time.After(time.Second):
		t.Fatal("projection service not called")
	}
	if checks.Load() != 1 {
		t.Fatal("claims checked more than once")
	}
	_ = socket.SetReadDeadline(time.Now().Add(time.Second))
	if _, _, err := socket.ReadMessage(); err != nil {
		t.Fatal(err)
	}
}

var _ projectionws.Stream = trustedSocketStream{}
