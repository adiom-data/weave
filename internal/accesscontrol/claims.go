package accesscontrol

import (
	"context"
	"errors"

	"github.com/adiom-data/weave/internal/storage"
)

type claimsContextKey struct{}

// ContextWithClaims attaches application-trusted policy inputs. It performs no
// authentication. Copies isolate both the writer and subsequent readers.
func ContextWithClaims(ctx context.Context, claims Claims) context.Context {
	return context.WithValue(ctx, claimsContextKey{}, Claims(storage.CloneRecord(storage.Record(claims))))
}

// ClaimsFromContext distinguishes absent claims from explicitly empty claims.
func ClaimsFromContext(ctx context.Context) (Claims, error) {
	claims, ok := ctx.Value(claimsContextKey{}).(Claims)
	if !ok {
		return nil, errors.New("policy claims are unavailable")
	}
	return Claims(storage.CloneRecord(storage.Record(claims))), nil
}
