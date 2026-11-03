package identity

import (
	"encoding/json"
	"errors"
	"math"
	"time"

	"github.com/adiom-data/weave/pkg/accesscontrol"
	"github.com/adiom-data/weave/pkg/storage"
	"github.com/golang-jwt/jwt/v5"
)

// Lease separates a JWT's expiration from its stable authorization claims after
// the configured authenticator has verified/decoded and mapped them. It neither
// reparses a token nor changes the trust mode. Input claims are never mutated.
func Lease(claims accesscontrol.Claims, excluded []string) (accesscontrol.Claims, time.Time, error) {
	exp, ok := claims["exp"]
	if !ok || exp.State != storage.ValuePresent || (exp.Type != storage.TypeInteger && exp.Type != storage.TypeDecimal) {
		return nil, time.Time{}, errors.New("renewable authentication requires a numeric JWT exp claim")
	}
	seconds, err := json.Number(exp.Data).Float64()
	if err != nil || math.IsNaN(seconds) || math.IsInf(seconds, 0) {
		return nil, time.Time{}, errors.New("JWT exp is outside the supported numeric range")
	}
	date, err := (jwt.MapClaims{"exp": seconds}).GetExpirationTime()
	if err != nil || date == nil || !date.After(time.Now()) {
		return nil, time.Time{}, errors.New("renewable authentication requires a future JWT exp claim")
	}
	stable := make(accesscontrol.Claims, len(claims))
	for name, value := range claims {
		stable[name] = value
	}
	for _, name := range excluded {
		delete(stable, name)
	}
	return stable, date.Time, nil
}
