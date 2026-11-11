package server

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"strings"

	"connectrpc.com/connect"
)

// Administrative admission is independent of end-user JWT modes and claims.
// Compare fixed-size digests rather than comparing secret strings directly.
func managementAuthentication(token string) connect.UnaryInterceptorFunc {
	want := sha256.Sum256([]byte(token))
	return func(next connect.UnaryFunc) connect.UnaryFunc {
		return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
			values := req.Header().Values("Authorization")
			if len(values) != 1 {
				return nil, connect.NewError(connect.CodeUnauthenticated, errors.New("administrative bearer token required"))
			}
			parts := strings.Fields(values[0])
			if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") {
				return nil, connect.NewError(connect.CodeUnauthenticated, errors.New("administrative bearer token required"))
			}
			got := sha256.Sum256([]byte(parts[1]))
			if subtle.ConstantTimeCompare(want[:], got[:]) != 1 {
				return nil, connect.NewError(connect.CodeUnauthenticated, errors.New("invalid administrative credential"))
			}
			return next(ctx, req)
		}
	}
}
