package identity

import (
	"encoding/json"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/adiom-data/weave/internal/server/config"
	"github.com/adiom-data/weave/pkg/storage"
)

func TestLeaseSeparatesDeadlineAndStableMappedClaims(t *testing.T) {
	expires := time.Now().Add(time.Hour).Truncate(time.Second)
	raw := map[string]any{"sub": "person", "exp": json.Number(strconv.FormatInt(expires.Unix(), 10)), "iat": json.Number("1"), "nbf": json.Number("1"), "jti": "credential", "organization_id": claimID}
	claims, err := newClaimsMapper(map[string]string{"organization_id": "uuid"}).decode(raw)
	if err != nil {
		t.Fatal(err)
	}
	original := storage.CloneRecord(storage.Record(claims))
	stable, deadline, err := Lease(claims, config.Default().Socket.Renewal.ExcludedClaims)
	if err != nil {
		t.Fatal(err)
	}
	if !deadline.Equal(expires) || len(stable) != 2 || stable["organization_id"].Type != storage.TypeUUID || string(stable["sub"].Data) != "person" {
		t.Fatal("incorrect lease", stable, deadline)
	}
	if !reflect.DeepEqual(storage.Record(claims), original) {
		t.Fatal("lease extraction modified authentication claims")
	}
	delete(stable, "sub")
	if _, exists := claims["sub"]; !exists {
		t.Fatal("stable claims share the source map")
	}
}

func TestLeaseRejectsMissingInvalidAndExpiredExp(t *testing.T) {
	for _, value := range []any{nil, "123", true, json.Number("0"), json.Number("-1"), json.Number("1e1000")} {
		claims, err := claimsFromJSON(map[string]any{"exp": value})
		if err != nil {
			t.Fatal(err)
		}
		if _, _, err := Lease(claims, nil); err == nil {
			t.Fatalf("accepted invalid exp: %v", value)
		}
		delete(claims, "exp")
		if _, _, err := Lease(claims, nil); err == nil {
			t.Fatal("accepted absent exp")
		}
	}
}
