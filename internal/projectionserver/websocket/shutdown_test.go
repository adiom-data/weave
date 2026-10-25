package websocket

import (
	"context"
	ws "github.com/gorilla/websocket"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestShutdownClosesTransportAndReleasesReader(t *testing.T) {
	for _, cause := range []string{"service failure", "host cancellation", "inactivity"} {
		t.Run(cause, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			reading, received, fail, returned := make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})
			config := SocketConfig{}
			if cause == "inactivity" {
				config.IdleTimeout = 100 * time.Millisecond
			}
			h := NewHandler(func(stream Stream, _ http.Header) error {
				go func() { close(reading); _, _ = stream.Recv(); close(received) }()
				select {
				case <-fail:
					return io.EOF
				case <-stream.Context().Done():
					return stream.Context().Err()
				}
			}, "*", nil, config)
			host := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { defer close(returned); h.ServeHTTP(w, r) }))
			host.Config.BaseContext = func(net.Listener) context.Context { return ctx }
			host.Start()
			defer host.Close()
			socket, _, err := ws.DefaultDialer.Dial("ws"+strings.TrimPrefix(host.URL, "http")+"/api/reactive", nil)
			if err != nil {
				t.Fatal(err)
			}
			defer socket.Close()
			wait := func(c <-chan struct{}, message string) {
				t.Helper()
				select {
				case <-c:
				case <-time.After(3 * time.Second):
					t.Fatal(message)
				}
			}
			wait(reading, "service did not start reading")
			if cause == "service failure" {
				close(fail)
			} else if cause == "host cancellation" {
				cancel()
			}
			wait(returned, "handler did not return")
			wait(received, "transport reader remained blocked")
			_ = socket.SetReadDeadline(time.Now().Add(time.Second))
			if _, _, err := socket.ReadMessage(); err == nil {
				t.Fatal("socket remained open")
			}
		})
	}
}
