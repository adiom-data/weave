package websocket

import (
	"context"
	"sync/atomic"
	"testing"

	pb "github.com/adiom-data/weave/pkg/gen/reactive/v1"
)

func TestClientVersionAdmissionAndDeprecation(t *testing.T) {
	for _, test := range []struct {
		name    string
		version *uint64
		want    pb.WebSocketClientVersionStatus_Status
	}{
		{"legacy", nil, pb.WebSocketClientVersionStatus_STATUS_UPGRADE_REQUIRED},
		{"old", new(uint64(2)), pb.WebSocketClientVersionStatus_STATUS_UPGRADE_REQUIRED},
		{"deprecated", new(uint64(4)), pb.WebSocketClientVersionStatus_STATUS_DEPRECATED},
		{"current", new(uint64(5)), pb.WebSocketClientVersionStatus_STATUS_UNSPECIFIED},
		{"future", new(^uint64(0)), pb.WebSocketClientVersionStatus_STATUS_UNSPECIFIED},
	} {
		t.Run(test.name, func(t *testing.T) {
			seen := make(chan struct{}, 1)
			socket := openTestSocketWithConfig(t, socketService{open: func(stream Stream) error {
				_, err := stream.Recv()
				if err != nil {
					return err
				}
				seen <- struct{}{}
				if err := stream.Send(&pb.ServerMessage{Event: &pb.ServerMessage_Ready{Ready: &pb.Ready{}}}); err != nil {
					return err
				}
				_, err = stream.Recv()
				return err
			}}, context.Background(), SocketConfig{ClientVersions: &ClientVersionOptions{Policy: ClientVersionPolicy{MinimumRevision: 3, RecommendedRevision: 5}}})
			if test.version != nil {
				writeSocketFrame(t, socket, &pb.WebSocketClientFrame{Payload: &pb.WebSocketClientFrame_ClientInfo{ClientInfo: &pb.WebSocketClientInfo{Revision: *test.version}}})
			}
			// A ping suffices for legacy admission, without creating a query.
			if test.version == nil {
				writeSocketFrame(t, socket, &pb.WebSocketClientFrame{Payload: &pb.WebSocketClientFrame_Ping{Ping: &pb.WebSocketHeartbeat{Id: "admit"}}})
			}
			if test.want != pb.WebSocketClientVersionStatus_STATUS_UNSPECIFIED {
				notice := readSocketFrame(t, socket).GetClientVersionStatus()
				if notice.GetStatus() != test.want || notice.GetMinimumRevision() != 3 || notice.GetRecommendedRevision() != 5 {
					t.Fatalf("unexpected notice: %v", notice)
				}
			}
			if test.want == pb.WebSocketClientVersionStatus_STATUS_UPGRADE_REQUIRED {
				if _, _, err := socket.ReadMessage(); err == nil {
					t.Fatal("old client remained connected")
				}
				select {
				case <-seen:
					t.Fatal("compatibility frame leaked into reactive service")
				default:
				}
				return
			}
			writeSocketFrame(t, socket, &pb.WebSocketClientFrame{Payload: &pb.WebSocketClientFrame_Message{Message: &pb.ClientMessage{SubscriptionId: "view", RequestId: "request", Operation: &pb.ClientMessage_Unsubscribe{Unsubscribe: &pb.Unsubscribe{}}}}})
			if readSocketFrame(t, socket).GetMessage().GetReady() == nil {
				t.Fatal("admitted client did not reach the service")
			}
		})
	}
}

func TestExistingClientVersionIsRecheckedAndCannotBeReannounced(t *testing.T) {
	for _, duplicate := range []bool{false, true} {
		t.Run(map[bool]string{false: "retire active client", true: "immutable announcement"}[duplicate], func(t *testing.T) {
			var minimum atomic.Uint64
			socket := openTestSocketWithConfig(t, socketService{open: func(stream Stream) error { _, err := stream.Recv(); return err }}, context.Background(), SocketConfig{ClientVersions: &ClientVersionOptions{Current: func() ClientVersionPolicy { return ClientVersionPolicy{MinimumRevision: minimum.Load()} }}})
			info := &pb.WebSocketClientFrame{Payload: &pb.WebSocketClientFrame_ClientInfo{ClientInfo: &pb.WebSocketClientInfo{Revision: 2}}}
			writeSocketFrame(t, socket, info)
			writeSocketFrame(t, socket, &pb.WebSocketClientFrame{Payload: &pb.WebSocketClientFrame_Ping{Ping: &pb.WebSocketHeartbeat{Id: "first"}}})
			if readSocketFrame(t, socket).GetPong().GetId() != "first" {
				t.Fatal("initial connection was rejected")
			}
			if duplicate {
				writeSocketFrame(t, socket, info)
			} else {
				minimum.Store(3)
				writeSocketFrame(t, socket, &pb.WebSocketClientFrame{Payload: &pb.WebSocketClientFrame_Ping{Ping: &pb.WebSocketHeartbeat{Id: "after-update"}}})
				if readSocketFrame(t, socket).GetClientVersionStatus().GetStatus() != pb.WebSocketClientVersionStatus_STATUS_UPGRADE_REQUIRED {
					t.Fatal("existing connection did not observe new floor")
				}
			}
			if _, _, err := socket.ReadMessage(); err == nil {
				t.Fatal("connection was not closed")
			}
		})
	}
}
