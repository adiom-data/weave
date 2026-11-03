package server

import (
	"context"
	"errors"
	"github.com/adiom-data/weave/internal/server/config"
	"github.com/adiom-data/weave/internal/server/identity"
	"github.com/adiom-data/weave/pkg/accesscontrol"
	"github.com/adiom-data/weave/pkg/projectionserver"
	projectionws "github.com/adiom-data/weave/pkg/projectionserver/websocket"
	"log/slog"
	"net/http"
)

// An application hook and configured JWT renewal are alternatives, never stacked
// authenticators. Both use the same transport-owned lease implementation.
func socketAuthenticationOptions(c Config, authenticate identity.Authenticator, custom *projectionws.AuthenticationOptions) (*projectionws.AuthenticationOptions, error) {
	if !c.Socket.Renewal.Enabled {
		return custom, nil
	}
	if custom != nil {
		return nil, errors.New("configured websocket.renewal and WebSocketAuthentication are mutually exclusive")
	}
	r := c.Socket.Renewal
	excluded := append([]string(nil), r.ExcludedClaims...)
	return &projectionws.AuthenticationOptions{
		ValidationTimeout: r.ValidationTimeout, MaxConcurrentValidations: r.MaxConcurrentValidations,
		MaxTokenBytes: c.Auth.MaxTokenBytes,
		Authenticate: func(ctx context.Context, headers http.Header) (projectionws.Authentication, error) {
			claims, err := authenticate(ctx, headers.Values("Authorization"))
			if err != nil {
				return projectionws.Authentication{}, err
			}
			stable, expires, err := identity.Lease(claims, excluded)
			return projectionws.Authentication{Claims: stable, ExpiresAt: expires}, err
		},
	}, nil
}

func newSocketHandler(c config.Config, logger *slog.Logger, authenticate identity.Authenticator, projections projectionserver.Server, authentication *projectionws.AuthenticationOptions) http.Handler {
	serve := func(stream projectionws.Stream, headers http.Header) error {
		if authentication != nil {
			return projections.ServeStream(stream)
		}
		claims, err := authenticate(stream.Context(), headers.Values("Authorization"))
		if err != nil {
			return err
		}
		ctx := accesscontrol.ContextWithClaims(stream.Context(), claims)
		return projections.ServeStream(trustedSocketStream{Stream: stream, ctx: ctx})
	}
	return projectionws.NewHandler(serve, c.WebOrigin, logger, projectionws.SocketConfig{WriteTimeout: c.Socket.WriteTimeout, IdleTimeout: c.Socket.IdleTimeout, MaxMessageBytes: c.Socket.MaxMessageBytes, MaxMessageDepth: c.Socket.MaxMessageDepth, Authentication: authentication})
}

type trustedSocketStream struct {
	projectionws.Stream
	ctx context.Context
}

func (s trustedSocketStream) Context() context.Context { return s.ctx }
