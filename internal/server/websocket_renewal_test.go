package server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/adiom-data/weave/internal/server/identity"
	"github.com/adiom-data/weave/pkg/accesscontrol"
	reactivev1 "github.com/adiom-data/weave/pkg/gen/reactive/v1"
	"github.com/adiom-data/weave/pkg/projectionserver"
	projectionws "github.com/adiom-data/weave/pkg/projectionserver/websocket"
	"github.com/adiom-data/weave/pkg/storage"
	"github.com/golang-jwt/jwt/v5"
	"github.com/gorilla/websocket"
	"google.golang.org/protobuf/proto"
)

func renewalToken(t *testing.T, subject string, expires time.Time, tokenID string) string {
	t.Helper()
	encoded, err := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"sub": subject, "exp": expires.Unix(), "iat": time.Now().Unix(), "jti": tokenID,
	}).SignedString([]byte("test-only-unverified-mode"))
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

func TestConfiguredRenewalUsesStableClaimsAndDoesNotChangeRPCAdmission(t *testing.T) {
	c := validConfig()
	c.Auth.Mode = "unverified_jwt"
	c.Socket.Renewal.Enabled = true
	c.Socket.Renewal.ValidationTimeout = 2 * time.Second
	c.Socket.Renewal.MaxConcurrentValidations = 3
	c.Auth.MaxTokenBytes = 12345
	if err := ValidateConfig(c); err != nil {
		t.Fatal(err)
	}
	authenticate, err := identity.New(c.Auth)
	if err != nil {
		t.Fatal(err)
	}
	options, err := socketAuthenticationOptions(c, authenticate, nil)
	if err != nil {
		t.Fatal(err)
	}
	if options.ValidationTimeout != c.Socket.Renewal.ValidationTimeout || options.MaxConcurrentValidations != 3 || options.MaxTokenBytes != c.Auth.MaxTokenBytes {
		t.Fatal("renewal bounds not forwarded")
	}
	firstExpiry := time.Now().Add(time.Hour).Truncate(time.Second)
	secondExpiry := firstExpiry.Add(time.Hour)
	var first projectionws.Authentication
	for index, token := range []string{renewalToken(t, "alice", firstExpiry, "first"), renewalToken(t, "alice", secondExpiry, "second")} {
		headers := http.Header{"Authorization": {"Bearer " + token}}
		result, err := options.Authenticate(t.Context(), headers)
		if err != nil {
			t.Fatal(err)
		}
		if len(result.Claims) != 1 || string(result.Claims["sub"].Data) != "alice" {
			t.Fatal("credential metadata was not separated from policy claims")
		}
		if index == 0 {
			first = result
		} else if !result.ExpiresAt.Equal(secondExpiry) || !storage.EqualValues(first.Claims["sub"], result.Claims["sub"]) {
			t.Fatal("renewal did not preserve claims and advance expiry")
		}
		claims, err := authenticate(t.Context(), headers.Values("Authorization"))
		if err != nil || len(claims) != 4 {
			t.Fatal("configured RPC admission changed", claims, err)
		}
	}
	if _, err := socketAuthenticationOptions(c, authenticate, &projectionws.AuthenticationOptions{}); err == nil {
		t.Fatal("accepted competing renewal implementations")
	}
	c.Socket.Renewal.Enabled = false
	if options, err := socketAuthenticationOptions(c, authenticate, nil); err != nil || options != nil {
		t.Fatal("disabled renewal installed authentication leases")
	}
}

func TestConfiguredRenewalOverWebSocket(t *testing.T) {
	c := validConfig()
	c.Auth.Mode = "unverified_jwt"
	c.Socket.Renewal.Enabled, c.WebOrigin = true, "*"
	authenticate, err := identity.New(c.Auth)
	if err != nil {
		t.Fatal(err)
	}
	options, err := socketAuthenticationOptions(c, authenticate, nil)
	if err != nil {
		t.Fatal(err)
	}
	checked := make(chan struct{}, 4)
	verify := options.Authenticate
	options.Authenticate = func(ctx context.Context, headers http.Header) (projectionws.Authentication, error) {
		result, err := verify(ctx, headers)
		checked <- struct{}{}
		return result, err
	}
	service := socketService{serve: func(stream projectionserver.Stream) error {
		claims, err := accesscontrol.ClaimsFromContext(stream.Context())
		if err != nil || len(claims) != 1 || string(claims["sub"].Data) != "alice" {
			t.Error("incorrect admitted claims", claims, err)
		}
		_, receiveErr := stream.Recv()
		if string(claims["sub"].Data) != "alice" {
			t.Error("claims changed in place")
		}
		return receiveErr
	}}
	host := httptest.NewServer(newSocketHandler(c, nil, authenticate, service, options))
	defer host.Close()
	expires := time.Now().Add(time.Hour)
	conn, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(host.URL, "http")+"/api/reactive", http.Header{"Authorization": {"Bearer " + renewalToken(t, "alice", expires, "initial")}})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	awaitVerification := func() {
		t.Helper()
		select {
		case <-checked:
		case <-time.After(3 * time.Second):
			t.Fatal("token was not verified")
		}
	}
	send := func(token string) {
		t.Helper()
		raw, err := proto.Marshal(&reactivev1.WebSocketClientFrame{Payload: &reactivev1.WebSocketClientFrame_AuthToken{AuthToken: &reactivev1.WebSocketAuthToken{Token: token}}})
		if err != nil {
			t.Fatal(err)
		}
		if err := conn.WriteMessage(websocket.BinaryMessage, raw); err != nil {
			t.Fatal(err)
		}
		awaitVerification()
	}
	awaitVerification() // Admission.
	send(renewalToken(t, "alice", expires.Add(time.Hour), "refreshed"))
	send(renewalToken(t, "bob", expires.Add(time.Hour), "changed-subject"))
	_, raw, err := conn.ReadMessage()
	if err != nil {
		t.Fatal(err)
	}
	var frame reactivev1.WebSocketServerFrame
	if err := proto.Unmarshal(raw, &frame); err != nil {
		t.Fatal(err)
	}
	if frame.GetAuthStatus().GetStatus() != reactivev1.WebSocketAuthStatus_STATUS_RECONNECT {
		t.Fatal("same-claims renewal should be silent; changed claims must request reconnect", &frame)
	}
	if _, _, err := conn.ReadMessage(); err == nil {
		t.Fatal("changed-claims connection was not closed")
	}
}
