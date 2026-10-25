package websocket

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	reactivev1 "github.com/adiom-data/weave/pkg/gen/reactive/v1"
)

type socketService struct{ open func(Stream) error }

func openTestSocket(t *testing.T, service socketService, serverContext context.Context, wrappers ...func(http.Handler) http.Handler) *websocket.Conn {
	t.Helper()
	return openTestSocketWithConfig(t, service, serverContext, SocketConfig{}, wrappers...)
}

func openTestSocketWithConfig(t *testing.T, service socketService, serverContext context.Context, config SocketConfig, wrappers ...func(http.Handler) http.Handler) *websocket.Conn {
	t.Helper()
	handler := NewHandler(func(s Stream, _ http.Header) error { return service.open(s) }, "*", nil, config)
	for _, wrap := range wrappers {
		handler = wrap(handler)
	}
	httpServer := httptest.NewUnstartedServer(handler)
	httpServer.Config.BaseContext = func(net.Listener) context.Context { return serverContext }
	httpServer.Start()
	t.Cleanup(httpServer.Close)
	socket, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(httpServer.URL, "http")+"/api/reactive", nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = socket.Close() })
	if err := socket.SetReadDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatal(err)
	}
	return socket
}

func TestClientHeartbeatsRenewIdleLeaseAndSilenceCancelsUpstream(t *testing.T) {
	closed := make(chan struct{})
	socket := openTestSocketWithConfig(t, socketService{open: func(stream Stream) error {
		defer close(closed)
		_, err := stream.Recv()
		return err
	}}, context.Background(), SocketConfig{IdleTimeout: 250 * time.Millisecond})
	for i := 0; i < 8; i++ {
		writeSocketFrame(t, socket, &reactivev1.WebSocketClientFrame{Payload: &reactivev1.WebSocketClientFrame_Ping{Ping: &reactivev1.WebSocketHeartbeat{Id: "alive"}}})
		if readSocketFrame(t, socket).GetPong().GetId() != "alive" {
			t.Fatal("missing heartbeat reply")
		}
		time.Sleep(50 * time.Millisecond)
	}
	select {
	case <-closed:
		t.Fatal("active client lost its upstream subscription")
	default:
	}
	if _, _, err := socket.ReadMessage(); err == nil {
		t.Fatal("silent client was not disconnected")
	}
	select {
	case <-closed:
	case <-time.After(3 * time.Second):
		t.Fatal("idle expiry did not cancel upstream")
	}
}

func TestSocketHeartbeatsStayAtEdgeAndPeerCloseCancelsUpstream(t *testing.T) {
	requests := make(chan *reactivev1.ClientMessage, 1)
	closed := make(chan struct{})
	socket := openTestSocket(t, socketService{open: func(stream Stream) error {
		defer close(closed)
		message, err := stream.Recv()
		if err != nil {
			return err
		}
		requests <- message
		if err := stream.Send(&reactivev1.ServerMessage{SubscriptionId: message.SubscriptionId, RequestId: message.RequestId, Sequence: 1, Event: &reactivev1.ServerMessage_Ready{Ready: &reactivev1.Ready{}}}); err != nil {
			return err
		}
		_, err = stream.Recv()
		return err
	}}, context.Background())
	writeSocketFrame(t, socket, &reactivev1.WebSocketClientFrame{Payload: &reactivev1.WebSocketClientFrame_Ping{Ping: &reactivev1.WebSocketHeartbeat{Id: "test-heartbeat"}}})
	pong := readSocketFrame(t, socket)
	if pong.GetPong().GetId() != "test-heartbeat" {
		t.Fatalf("unexpected pong: %v", pong)
	}
	select {
	case <-requests:
		t.Fatal("heartbeat leaked into reactive gRPC")
	default:
	}
	writeSocketFrame(t, socket, &reactivev1.WebSocketClientFrame{Payload: &reactivev1.WebSocketClientFrame_Message{Message: &reactivev1.ClientMessage{SubscriptionId: "p", RequestId: "r", Operation: &reactivev1.ClientMessage_Unsubscribe{Unsubscribe: &reactivev1.Unsubscribe{}}}}})
	reply := readSocketFrame(t, socket).GetMessage()
	if reply.GetSubscriptionId() != "p" || reply.GetReady() == nil {
		t.Fatalf("unexpected model reply: %v", reply)
	}
	_ = socket.Close()
	select {
	case <-closed:
	case <-time.After(3 * time.Second):
		t.Fatal("socket disconnect did not cancel upstream gRPC")
	}
}

func writeSocketFrame(t *testing.T, socket *websocket.Conn, frame *reactivev1.WebSocketClientFrame) {
	t.Helper()
	data, err := proto.Marshal(frame)
	if err != nil {
		t.Fatal(err)
	}
	if err := socket.WriteMessage(websocket.BinaryMessage, data); err != nil {
		t.Fatal(err)
	}
}

func readSocketFrame(t *testing.T, socket *websocket.Conn) *reactivev1.WebSocketServerFrame {
	t.Helper()
	kind, data, err := socket.ReadMessage()
	if err != nil {
		t.Fatal(err)
	}
	if kind != websocket.BinaryMessage {
		t.Fatal("expected binary protobuf frame")
	}
	var frame reactivev1.WebSocketServerFrame
	if err := proto.Unmarshal(data, &frame); err != nil {
		t.Fatal(err)
	}
	return &frame
}

func TestUpstreamFailureClosesSocket(t *testing.T) {
	fail := make(chan struct{})
	socket := openTestSocket(t, socketService{open: func(stream Stream) error {
		select {
		case <-fail:
			return status.Error(codes.Unavailable, "upstream stopped")
		case <-stream.Context().Done():
			return stream.Context().Err()
		}
	}}, context.Background())
	close(fail)
	if _, _, err := socket.ReadMessage(); err == nil {
		t.Fatal("upstream failure left WebSocket open")
	}
}

func TestServerCancellationClosesSocketAndUpstream(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	closed := make(chan struct{})
	socket := openTestSocket(t, socketService{open: func(stream Stream) error {
		defer close(closed)
		if err := stream.Send(&reactivev1.ServerMessage{Event: &reactivev1.ServerMessage_Ready{Ready: &reactivev1.Ready{}}}); err != nil {
			return err
		}
		<-stream.Context().Done()
		return stream.Context().Err()
	}}, ctx)
	if _, _, err := socket.ReadMessage(); err != nil {
		t.Fatal(err)
	}
	cancel()
	if _, _, err := socket.ReadMessage(); err == nil {
		t.Fatal("server cancellation left WebSocket open")
	}
	select {
	case <-closed:
	case <-time.After(3 * time.Second):
		t.Fatal("server cancellation did not close upstream")
	}
}
