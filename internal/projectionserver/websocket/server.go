package websocket

import (
	"context"
	"errors"
	reactivev1 "github.com/adiom-data/weave/pkg/gen/reactive/v1"
	"github.com/gorilla/websocket"
	"google.golang.org/protobuf/proto"
	"log/slog"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

// Stream is the server-side message contract. Both WebSocket and RPC adapters
// drive the same projection service; neither needs to call the other.
type Stream interface {
	Context() context.Context
	Send(*reactivev1.ServerMessage) error
	Recv() (*reactivev1.ClientMessage, error)
}
type ServeStream func(Stream, http.Header) error
type handler struct {
	serve           ServeStream
	origin          string
	logger          *slog.Logger
	upgrader        websocket.Upgrader
	socketConfig    SocketConfig
	validationSlots chan struct{}
}

// SocketConfig belongs to the WebSocket edge, not the reactive query protocol.
// Nonpositive fields select defaults.
type SocketConfig struct {
	WriteTimeout time.Duration
	// IdleTimeout bounds silence from the client, including while forwarding
	// a request upstream. Valid messages and application pings renew the lease.
	IdleTimeout     time.Duration
	MaxMessageBytes int64
	MaxMessageDepth int
	// Authentication is opt-in. Without it, admission remains entirely owned
	// by serve and in-protocol renewal is unavailable.
	Authentication *AuthenticationOptions
}

// DefaultSocketConfig returns conservative edge limits for a browser client.
func DefaultSocketConfig() SocketConfig {
	return SocketConfig{WriteTimeout: 10 * time.Second, IdleTimeout: time.Minute, MaxMessageBytes: 8 << 20, MaxMessageDepth: 64}
}

func (c SocketConfig) withDefaults() SocketConfig {
	d := DefaultSocketConfig()
	if c.WriteTimeout <= 0 {
		c.WriteTimeout = d.WriteTimeout
	}
	if c.IdleTimeout <= 0 {
		c.IdleTimeout = d.IdleTimeout
	}
	if c.MaxMessageBytes <= 0 {
		c.MaxMessageBytes = d.MaxMessageBytes
	}
	if c.MaxMessageDepth <= 0 {
		c.MaxMessageDepth = d.MaxMessageDepth
	}
	return c
}

// NewHandler mounts the binary WebSocket endpoint. With Authentication enabled,
// the hook authenticates admission and installs trusted claims on Stream.Context.
// Otherwise serve owns admission, as before.
func NewHandler(serve ServeStream, origin string, logger *slog.Logger, config SocketConfig) http.Handler {
	if logger == nil {
		logger = slog.Default()
	}
	h := &handler{serve: serve, origin: origin, logger: logger, socketConfig: config.withDefaults()}
	if config.Authentication != nil {
		auth := config.Authentication.withDefaults()
		h.socketConfig.Authentication = &auth
		h.validationSlots = make(chan struct{}, auth.MaxConcurrentValidations)
	}
	h.upgrader = websocket.Upgrader{CheckOrigin: func(r *http.Request) bool {
		return origin == "*" || r.Header.Get("Origin") == "" || r.Header.Get("Origin") == origin
	}}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/reactive", h.reactiveSocket)
	return h.middleware(mux)
}
func (h *handler) middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", h.origin)
		w.Header().Set("Access-Control-Allow-Headers", "Authorization, Content-Type")
		w.Header().Set("Access-Control-Allow-Methods", "GET, OPTIONS")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}
func (h *handler) reactiveSocket(w http.ResponseWriter, r *http.Request) {
	var authentication *authenticationLease
	if options := h.socketConfig.Authentication; options != nil {
		var err error
		authentication, err = h.authenticate(r.Context(), r.Header.Clone())
		if err != nil {
			http.Error(w, "authentication unavailable or rejected", http.StatusUnauthorized)
			return
		}
	}
	socket, err := h.upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer socket.Close()
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()
	if authentication != nil {
		ctx = authentication.context(ctx)
	}
	socket.SetReadLimit(h.socketConfig.MaxMessageBytes)
	var activity atomic.Pointer[time.Time]
	touch := func() { now := time.Now(); activity.Store(&now) }
	touch()
	stream := &socketStream{ctx: ctx, socket: socket, config: h.socketConfig, touch: touch, auth: authentication, handler: h, cancel: cancel, headers: r.Header.Clone()}
	if authentication != nil {
		stream.tokens = newTokenUpdates()
		go stream.validateTokens()
		go authentication.watch(ctx, cancel)
	}
	// Closing is concurrent-safe and releases a blocked Recv or Send, including
	// readers owned by the projection service when that service returns.
	go func() {
		timer := time.NewTimer(h.socketConfig.IdleTimeout)
		defer timer.Stop()
		for {
			select {
			case <-ctx.Done():
				_ = socket.Close()
				return
			case <-timer.C:
				remaining := h.socketConfig.IdleTimeout - time.Since(*activity.Load())
				if remaining <= 0 {
					cancel()
					_ = socket.Close()
					return
				}
				timer.Reset(remaining)
			}
		}
	}()
	if h.serve == nil {
		return
	}
	if err := h.serve(stream, r.Header.Clone()); err != nil {
		h.logger.Debug("reactive WebSocket closed", "error", err)
	}
}

type socketStream struct {
	ctx     context.Context
	socket  *websocket.Conn
	config  SocketConfig
	touch   func()
	writer  sync.Mutex
	auth    *authenticationLease
	handler *handler
	cancel  context.CancelFunc
	tokens  *tokenUpdates
	headers http.Header
}

func (s *socketStream) Context() context.Context { return s.ctx }
func (s *socketStream) write(frame *reactivev1.WebSocketServerFrame) error {
	if err := s.ctx.Err(); err != nil {
		return err
	}
	data, err := proto.Marshal(frame)
	if err != nil {
		return err
	}
	s.writer.Lock()
	defer s.writer.Unlock()
	if err := s.ctx.Err(); err != nil {
		return err
	}
	if s.auth != nil && !s.auth.current() {
		s.cancel()
		return errors.New("authentication expired")
	}
	if err := s.socket.SetWriteDeadline(time.Now().Add(s.config.WriteTimeout)); err != nil {
		return err
	}
	return s.socket.WriteMessage(websocket.BinaryMessage, data)
}
func (s *socketStream) Send(message *reactivev1.ServerMessage) error {
	return s.write(&reactivev1.WebSocketServerFrame{Payload: &reactivev1.WebSocketServerFrame_Message{Message: message}})
}
func (s *socketStream) Recv() (*reactivev1.ClientMessage, error) {
	for {
		if err := s.ctx.Err(); err != nil {
			return nil, err
		}
		kind, data, err := s.socket.ReadMessage()
		if err != nil {
			return nil, err
		}
		if kind != websocket.BinaryMessage {
			return nil, errors.New("binary WebSocket frame required")
		}
		var frame reactivev1.WebSocketClientFrame
		if err := (proto.UnmarshalOptions{RecursionLimit: s.config.MaxMessageDepth, DiscardUnknown: true}).Unmarshal(data, &frame); err != nil {
			return nil, err
		}
		switch payload := frame.Payload.(type) {
		case *reactivev1.WebSocketClientFrame_AuthToken:
			if err := s.updateAuthToken(payload.AuthToken); err != nil {
				return nil, err
			}
		case *reactivev1.WebSocketClientFrame_Ping:
			if payload.Ping == nil || payload.Ping.Id == "" || len(payload.Ping.Id) > 128 {
				return nil, errors.New("invalid WebSocket heartbeat")
			}
			s.touch()
			if err := s.write(&reactivev1.WebSocketServerFrame{Payload: &reactivev1.WebSocketServerFrame_Pong{Pong: payload.Ping}}); err != nil {
				return nil, err
			}
		case *reactivev1.WebSocketClientFrame_Message:
			if payload.Message == nil {
				return nil, errors.New("missing reactive message")
			}
			s.touch()
			return payload.Message, nil
		default:
			return nil, errors.New("missing WebSocket frame payload")
		}
	}
}
