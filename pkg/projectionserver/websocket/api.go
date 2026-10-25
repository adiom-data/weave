// Package websocket exposes supported contracts and forwards construction to internal implementations.
package websocket

import (
	"log/slog"
	"net/http"

	impl "github.com/adiom-data/weave/internal/projectionserver/websocket"
)

// SocketConfig belongs to the WebSocket edge, not the reactive query protocol.
type SocketConfig = impl.SocketConfig
type Stream = impl.Stream
type ServeStream = impl.ServeStream

func DefaultSocketConfig() SocketConfig { return impl.DefaultSocketConfig() }

// NewHandler mounts only the reactive WebSocket endpoint. Database readiness,
// application routes, and their CORS policy belong to the hosting application.
// The host authenticates the supplied headers and drives its projection service
// directly. Returning from serve cancels and closes the WebSocket transport.
func NewHandler(serve ServeStream, origin string, logger *slog.Logger, socketConfig SocketConfig) http.Handler {
	return impl.NewHandler(serve, origin, logger, socketConfig)
}
