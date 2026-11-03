package websocket

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/adiom-data/weave/internal/accesscontrol"
	"github.com/adiom-data/weave/internal/storage"
	reactivev1 "github.com/adiom-data/weave/pkg/gen/reactive/v1"
	"github.com/gorilla/websocket"
)

func authClaims(user string) accesscontrol.Claims {
	return accesscontrol.Claims{"user": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte(user)}}
}
func tokenFrame(token string) *reactivev1.WebSocketClientFrame {
	return &reactivev1.WebSocketClientFrame{Payload: &reactivev1.WebSocketClientFrame_AuthToken{AuthToken: &reactivev1.WebSocketAuthToken{Token: token}}}
}
func pingFrame(id string) *reactivev1.WebSocketClientFrame {
	return &reactivev1.WebSocketClientFrame{Payload: &reactivev1.WebSocketClientFrame_Ping{Ping: &reactivev1.WebSocketHeartbeat{Id: id}}}
}
func readUntilClose(stream Stream) error { _, err := stream.Recv(); return err }

func TestAuthTokenIgnoredWhenDisabled(t *testing.T) {
	if DefaultSocketConfig().Authentication != nil {
		t.Fatal("authentication unexpectedly enabled")
	}
	socket := openTestSocket(t, socketService{open: readUntilClose}, context.Background())
	writeSocketFrame(t, socket, tokenFrame("token"))
	writeSocketFrame(t, socket, pingFrame("still-open"))
	if readSocketFrame(t, socket).GetPong().GetId() != "still-open" {
		t.Fatal("disabled auth produced a notice or closed the connection")
	}
}

func TestAuthTokensCoalesceWithoutBlockingReaderOrPublishingSuccess(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	validated := make(chan string, 4)
	opened := make(chan *socketStream, 1)
	options := &AuthenticationOptions{MaxConcurrentValidations: 1, Authenticate: func(ctx context.Context, headers http.Header) (Authentication, error) {
		token := headers.Get("Authorization")
		if token == "Bearer slow" {
			validated <- token
			close(started)
			select {
			case <-release:
			case <-ctx.Done():
				return Authentication{}, ctx.Err()
			}
			return Authentication{}, errors.New("obsolete rejection")
		}
		expires := time.Now().Add(time.Minute)
		if token != "" {
			validated <- token
			expires = time.Now().Add(time.Hour)
		}
		return Authentication{Claims: authClaims("one"), ExpiresAt: expires}, nil
	}}
	socket := openTestSocketWithConfig(t, socketService{open: func(stream Stream) error {
		claims, err := accesscontrol.ClaimsFromContext(stream.Context())
		if err != nil || !storage.EqualValues(claims["user"], authClaims("one")["user"]) {
			t.Error("missing authenticated claims", err)
		}
		opened <- stream.(*socketStream)
		for {
			message, err := stream.Recv()
			if err != nil {
				return err
			}
			if err := stream.Send(&reactivev1.ServerMessage{SubscriptionId: message.SubscriptionId}); err != nil {
				return err
			}
		}
	}}, context.Background(), SocketConfig{Authentication: options})
	stream := <-opened
	writeSocketFrame(t, socket, tokenFrame("slow"))
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("validation did not start")
	}
	writeSocketFrame(t, socket, tokenFrame("obsolete"))
	writeSocketFrame(t, socket, tokenFrame("latest"))
	writeSocketFrame(t, socket, pingFrame("ping"))
	if readSocketFrame(t, socket).GetPong().GetId() != "ping" {
		t.Fatal("validation blocked heartbeat")
	}
	writeSocketFrame(t, socket, &reactivev1.WebSocketClientFrame{Payload: &reactivev1.WebSocketClientFrame_Message{Message: &reactivev1.ClientMessage{SubscriptionId: "projection"}}})
	if readSocketFrame(t, socket).GetMessage().GetSubscriptionId() != "projection" {
		t.Fatal("validation blocked model traffic")
	}
	close(release)
	for _, expected := range []string{"Bearer slow", "Bearer latest"} {
		select {
		case got := <-validated:
			if got != expected {
				t.Fatal(got, expected)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("pending token lost")
		}
	}
	deadline := time.Now().Add(3 * time.Second)
	for {
		stream.auth.mu.Lock()
		renewed := time.Until(stream.auth.expires) > 30*time.Minute
		stream.auth.mu.Unlock()
		if renewed {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("latest token did not renew lease")
		}
		time.Sleep(time.Millisecond)
	}
	writeSocketFrame(t, socket, pingFrame("silent-success"))
	if readSocketFrame(t, socket).GetPong().GetId() != "silent-success" {
		t.Fatal("success or obsolete rejection emitted an auth response")
	}
	select {
	case extra := <-validated:
		t.Fatal("validated superseded token", extra)
	default:
	}
}

func TestAuthChangedClaimsNotifyReconnectWithoutReplacingClaims(t *testing.T) {
	opened := make(chan *socketStream, 1)
	options := &AuthenticationOptions{Authenticate: func(_ context.Context, headers http.Header) (Authentication, error) {
		user := "one"
		if headers.Get("Authorization") != "" {
			user = "two"
		}
		return Authentication{Claims: authClaims(user), ExpiresAt: time.Now().Add(time.Minute)}, nil
	}}
	socket := openTestSocketWithConfig(t, socketService{open: func(stream Stream) error { opened <- stream.(*socketStream); return readUntilClose(stream) }}, context.Background(), SocketConfig{Authentication: options})
	stream := <-opened
	writeSocketFrame(t, socket, tokenFrame("changed"))
	if readSocketFrame(t, socket).GetAuthStatus().GetStatus() != reactivev1.WebSocketAuthStatus_STATUS_RECONNECT {
		t.Fatal("missing reconnect notice")
	}
	if !storage.EqualValues(stream.auth.claims["user"], authClaims("one")["user"]) {
		t.Fatal("changed claims were installed")
	}
	if _, _, err := socket.ReadMessage(); err == nil {
		t.Fatal("changed identity kept its projection connection")
	}
}

func TestAuthValidationTimeoutNotifiesRejectionWithoutChangingLease(t *testing.T) {
	canceled := make(chan struct{})
	options := &AuthenticationOptions{ValidationTimeout: 50 * time.Millisecond, Authenticate: func(ctx context.Context, headers http.Header) (Authentication, error) {
		if headers.Get("Authorization") != "" {
			<-ctx.Done()
			close(canceled)
			return Authentication{}, ctx.Err()
		}
		return Authentication{Claims: authClaims("one"), ExpiresAt: time.Now().Add(time.Minute)}, nil
	}}
	socket := openTestSocketWithConfig(t, socketService{open: readUntilClose}, context.Background(), SocketConfig{Authentication: options})
	writeSocketFrame(t, socket, tokenFrame("slow"))
	if readSocketFrame(t, socket).GetAuthStatus().GetStatus() != reactivev1.WebSocketAuthStatus_STATUS_REJECTED {
		t.Fatal("missing rejection")
	}
	select {
	case <-canceled:
	default:
		t.Fatal("validation context was not canceled")
	}
	writeSocketFrame(t, socket, pingFrame("alive"))
	if readSocketFrame(t, socket).GetPong().GetId() != "alive" {
		t.Fatal("rejection destroyed an unexpired connection")
	}
}

func TestAuthExpiryCancelsSlowValidationAndUpstream(t *testing.T) {
	closed, verifying, canceled := make(chan struct{}), make(chan struct{}), make(chan struct{})
	options := &AuthenticationOptions{Authenticate: func(ctx context.Context, headers http.Header) (Authentication, error) {
		if headers.Get("Authorization") != "" {
			close(verifying)
			<-ctx.Done()
			close(canceled)
			return Authentication{}, ctx.Err()
		}
		return Authentication{ExpiresAt: time.Now().Add(200 * time.Millisecond)}, nil
	}}
	socket := openTestSocketWithConfig(t, socketService{open: func(stream Stream) error { defer close(closed); return readUntilClose(stream) }}, context.Background(), SocketConfig{Authentication: options})
	writeSocketFrame(t, socket, tokenFrame("slow"))
	select {
	case <-verifying:
	case <-time.After(3 * time.Second):
		t.Fatal("no validation")
	}
	if _, _, err := socket.ReadMessage(); err == nil {
		t.Fatal("expired authentication remained connected")
	}
	for _, signal := range []chan struct{}{closed, canceled} {
		select {
		case <-signal:
		case <-time.After(3 * time.Second):
			t.Fatal("expiry did not cancel work")
		}
	}
}

func TestAuthenticationRejectsAdmissionBeforeServing(t *testing.T) {
	for _, authenticate := range []func(context.Context, http.Header) (Authentication, error){
		nil,
		func(context.Context, http.Header) (Authentication, error) {
			return Authentication{}, errors.New("invalid secret must not be exposed")
		},
		func(context.Context, http.Header) (Authentication, error) {
			return Authentication{ExpiresAt: time.Now().Add(-time.Second)}, nil
		},
	} {
		handler := NewHandler(func(Stream, http.Header) error { t.Error("unauthenticated stream served"); return nil }, "*", nil, SocketConfig{Authentication: &AuthenticationOptions{Authenticate: authenticate}})
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, httptest.NewRequest("GET", "/api/reactive", nil))
		if w.Code != http.StatusUnauthorized || w.Body.String() != "authentication unavailable or rejected\n" {
			t.Fatal(w.Code, w.Body.String())
		}
	}
}

func TestAuthValidationCapacityRetainsLatestWaitingToken(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	validated := make(chan string, 4)
	var calls atomic.Int32
	options := &AuthenticationOptions{MaxConcurrentValidations: 1, Authenticate: func(ctx context.Context, headers http.Header) (Authentication, error) {
		calls.Add(1)
		token := headers.Get("Authorization")
		if token != "" {
			validated <- token
		}
		if token == "Bearer slow" {
			close(started)
			select {
			case <-release:
			case <-ctx.Done():
				return Authentication{}, ctx.Err()
			}
		}
		return Authentication{ExpiresAt: time.Now().Add(time.Minute)}, nil
	}}
	host := httptest.NewServer(NewHandler(func(stream Stream, _ http.Header) error { return readUntilClose(stream) }, "*", nil, SocketConfig{Authentication: options}))
	defer host.Close()
	var peers []*websocket.Conn
	for range 2 {
		socket, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(host.URL, "http")+"/api/reactive", nil)
		if err != nil {
			t.Fatal(err)
		}
		defer socket.Close()
		_ = socket.SetReadDeadline(time.Now().Add(3 * time.Second))
		peers = append(peers, socket)
	}
	writeSocketFrame(t, peers[0], tokenFrame("slow"))
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("no validation")
	}
	writeSocketFrame(t, peers[1], tokenFrame("waiting"))
	writeSocketFrame(t, peers[1], tokenFrame("latest"))
	writeSocketFrame(t, peers[1], pingFrame("not-blocked"))
	if readSocketFrame(t, peers[1]).GetPong().GetId() != "not-blocked" {
		t.Fatal("capacity wait blocked reader")
	}
	if calls.Load() != 3 {
		t.Fatal("capacity limit allowed another verifier invocation")
	}
	close(release)
	for _, expected := range []string{"Bearer slow", "Bearer latest"} {
		select {
		case got := <-validated:
			if got != expected {
				t.Fatal(got, expected)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("capacity wait lost latest token")
		}
	}
}

func TestOversizedAuthTokenNeverInvokesVerifier(t *testing.T) {
	var calls atomic.Int32
	options := &AuthenticationOptions{MaxTokenBytes: 3, Authenticate: func(context.Context, http.Header) (Authentication, error) {
		calls.Add(1)
		return Authentication{ExpiresAt: time.Now().Add(time.Minute)}, nil
	}}
	socket := openTestSocketWithConfig(t, socketService{open: readUntilClose}, context.Background(), SocketConfig{Authentication: options})
	writeSocketFrame(t, socket, tokenFrame("oversized"))
	if _, _, err := socket.ReadMessage(); err == nil {
		t.Fatal("oversized token accepted")
	}
	if calls.Load() != 1 {
		t.Fatal("oversized token reached verifier")
	}
}

func TestAuthLeaseRenewalCannotReviveOrChangeClaims(t *testing.T) {
	lease := &authenticationLease{claims: authClaims("one"), expires: time.Now().Add(time.Hour), changed: make(chan struct{}, 1)}
	later := time.Now().Add(2 * time.Hour)
	if status := lease.renew(Authentication{Claims: authClaims("one"), ExpiresAt: later}); status != renewed || !lease.expires.Equal(later) {
		t.Fatal("renewal failed")
	}
	if status := lease.renew(Authentication{Claims: authClaims("two"), ExpiresAt: later}); status != renewalReconnect {
		t.Fatal("changed claims accepted")
	}
	lease.expires = time.Now().Add(-time.Second)
	if status := lease.renew(Authentication{Claims: authClaims("one"), ExpiresAt: later}); status != renewalRejected {
		t.Fatal("expired connection revived")
	}
}

func TestRenewedAuthLeaseDoesNotExpireAtItsOldDeadline(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	lease := &authenticationLease{expires: time.Now().Add(100 * time.Millisecond), changed: make(chan struct{}, 1)}
	done := make(chan struct{})
	go func() { defer close(done); lease.watch(ctx, cancel) }()
	if status := lease.renew(Authentication{ExpiresAt: time.Now().Add(time.Hour)}); status != renewed {
		t.Fatal(status)
	}
	select {
	case <-ctx.Done():
		t.Fatal("renewed lease expired at old deadline")
	case <-time.After(150 * time.Millisecond):
	}
	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("watcher did not stop")
	}
}

func TestAuthCapacityWaitCancelsWithConnection(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	stream := &socketStream{ctx: ctx, tokens: newTokenUpdates(), handler: &handler{validationSlots: make(chan struct{}, 1)}}
	stream.handler.validationSlots <- struct{}{}
	stream.tokens.put("waiting")
	done := make(chan struct{})
	go func() { defer close(done); stream.validateTokens() }()
	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("capacity wait outlived connection")
	}
}
