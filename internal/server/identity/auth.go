package identity

import (
	"context"
	"errors"
	"net/http"
	"strings"

	"connectrpc.com/connect"
	"github.com/adiom-data/framework/httpapp/jwtauth"
	"github.com/adiom-data/weave/internal/server/config"
	"github.com/adiom-data/weave/pkg/accesscontrol"
	"github.com/adiom-data/weave/pkg/storage"
	"github.com/golang-jwt/jwt/v5"
)

type Authenticator func(context.Context, []string) (accesscontrol.Claims, error)

func New(c config.AuthConfig) (Authenticator, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}
	mapper := newClaimsMapper(c.ClaimTypes)
	if c.Mode == "fixed" {
		claims, err := mapper.decode(map[string]any(c.FixedClaims))
		if err != nil {
			return nil, err
		}
		return func(context.Context, []string) (accesscontrol.Claims, error) {
			result := make(accesscontrol.Claims, len(claims))
			for k, v := range claims {
				result[k] = storage.CloneValue(v)
			}
			return result, nil
		}, nil
	}
	var verifier *jwtauth.Verifier
	if c.Mode == "oidc" {
		var err error
		verifier, err = jwtauth.NewVerifier(jwtauth.Config{Issuer: c.Issuer, AllowedAudiences: []string{c.Audience}, ClockSkew: c.ClockSkew, HTTPClient: &http.Client{Timeout: c.HTTPTimeout}})
		if err != nil {
			return nil, errors.New("OIDC issuer discovery failed")
		}
	}
	return func(ctx context.Context, headers []string) (accesscontrol.Claims, error) {
		encoded, err := bearer(headers, c.MaxTokenBytes)
		if err != nil {
			return nil, err
		}
		if verifier != nil {
			verified, err := verifier.Verify(ctx, encoded)
			if err != nil {
				return nil, err
			}
			return mapper.decode(verified.Raw)
		}
		// Only explicitly unverified mode decodes without verification. The
		// framework verifier already preserves exact numbers in verified claims.
		raw := jwt.MapClaims{}
		if _, _, err := jwt.NewParser(jwt.WithJSONNumber()).ParseUnverified(encoded, raw); err != nil {
			return nil, err
		}
		return mapper.decode(raw)
	}, nil
}
func bearer(headers []string, maxBytes int) (string, error) {
	if len(headers) != 1 || len(headers[0]) > maxBytes+len("Bearer ") {
		return "", errors.New("one bounded bearer credential is required")
	}
	parts := strings.Fields(headers[0])
	if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") || len(parts[1]) > maxBytes {
		return "", errors.New("invalid bearer credential")
	}
	return parts[1], nil
}
func claimsFromJSON(input map[string]any) (accesscontrol.Claims, error) {
	result := make(accesscontrol.Claims, len(input))
	for k, v := range input {
		value, err := logicalValue(v)
		if err != nil {
			return nil, err
		}
		result[k] = value
	}
	return result, nil
}
func (a Authenticator) context(ctx context.Context, headers http.Header) (context.Context, error) {
	claims, err := a(ctx, headers.Values("Authorization"))
	if err != nil {
		return nil, connect.NewError(connect.CodeUnauthenticated, errors.New("invalid or missing bearer credential"))
	}
	return accesscontrol.ContextWithClaims(ctx, claims), nil
}
func (a Authenticator) WrapUnary(next connect.UnaryFunc) connect.UnaryFunc {
	return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
		ctx, err := a.context(ctx, req.Header())
		if err != nil {
			return nil, err
		}
		return next(ctx, req)
	}
}
func (a Authenticator) WrapStreamingHandler(next connect.StreamingHandlerFunc) connect.StreamingHandlerFunc {
	return func(ctx context.Context, stream connect.StreamingHandlerConn) error {
		ctx, err := a.context(ctx, stream.RequestHeader())
		if err != nil {
			return err
		}
		return next(ctx, stream)
	}
}
func (a Authenticator) WrapStreamingClient(next connect.StreamingClientFunc) connect.StreamingClientFunc {
	return next
}
