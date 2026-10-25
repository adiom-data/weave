package websocket

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestBridgeLeavesAuthenticationToHost(t *testing.T) {
	bridge := NewHandler(nil, "http://localhost:5173", nil, SocketConfig{})
	protected := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer host-accepted" {
			http.Error(w, "host rejected credentials", http.StatusUnauthorized)
			return
		}
		bridge.ServeHTTP(w, r)
	})
	w := httptest.NewRecorder()
	protected.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/reactive", nil))
	if w.Code != http.StatusUnauthorized {
		t.Fatal("host middleware did not reject request")
	}
	w = httptest.NewRecorder()
	bridge.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if w.Code != http.StatusNotFound {
		t.Fatal("bridge registered application route")
	}
}

func TestTransportPreservesHostContext(t *testing.T) {
	type claimsKey struct{}
	seen := make(chan any, 1)
	service := socketService{open: func(stream Stream) error {
		seen <- stream.Context().Value(claimsKey{})
		_, err := stream.Recv()
		return err
	}}
	socket := openTestSocket(t, service, context.Background(), func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			r.Header.Set("Authorization", "Bearer untrusted-client-credential")
			ctx := context.WithValue(r.Context(), claimsKey{}, "host-verified-claims")
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	})
	defer socket.Close()
	select {
	case got := <-seen:
		if got != "host-verified-claims" {
			t.Fatal(got)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("upstream did not open")
	}
}
