package identity

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/adiom-data/weave/internal/server/config"
	"github.com/adiom-data/weave/pkg/storage"
	"github.com/golang-jwt/jwt/v5"
)

const claimID = "AABBCCDD000040008000000000000001"
const canonicalClaimID = "aabbccdd-0000-4000-8000-000000000001"

func unverifiedToken(t *testing.T, raw map[string]any) string {
	t.Helper()
	token, err := jwt.NewWithClaims(jwt.SigningMethodNone, jwt.MapClaims(raw)).SignedString(jwt.UnsafeAllowNoneSignatureType)
	if err != nil {
		t.Fatal(err)
	}
	return "Bearer " + token
}

func TestClaimMappingsAcrossFixedAndUnverifiedAdmission(t *testing.T) {
	for _, mode := range []string{"fixed", "unverified_jwt"} {
		t.Run(mode, func(t *testing.T) {
			c := config.Default().Auth
			c.Mode = mode
			c.ClaimTypes = map[string]string{"id": "uuid", "ids": "list<uuid>", "missing": "uuid", "urn:subject.id": "uuid"}
			raw := map[string]any{"id": claimID, "ids": []any{claimID, canonicalClaimID}, "unmapped": claimID,
				"urn:subject.id": claimID, "precise": json.Number("9007199254740993"), "nested": map[string]any{"id": claimID}}
			var headers []string
			if mode == "fixed" {
				c.FixedClaims = config.JSONClaims(raw)
			} else {
				headers = []string{unverifiedToken(t, raw)}
			}
			authenticate, err := New(c)
			if err != nil {
				t.Fatal(err)
			}
			// The compiled plan must not retain mutable caller configuration.
			c.ClaimTypes["id"] = "list<uuid>"
			first, err := authenticate(t.Context(), headers)
			if err != nil {
				t.Fatal(err)
			}
			for _, name := range []string{"id", "urn:subject.id"} {
				if first[name].Type != storage.TypeUUID || string(first[name].Data) != canonicalClaimID {
					t.Fatalf("%s was not mapped canonically: %+v", name, first[name])
				}
			}
			if first["ids"].Type != storage.TypeList || len(first["ids"].List) != 2 {
				t.Fatal("list shape changed")
			}
			for _, value := range first["ids"].List {
				if value.Type != storage.TypeUUID || string(value.Data) != canonicalClaimID {
					t.Fatal(value)
				}
			}
			if _, exists := first["missing"]; exists {
				t.Fatal("missing claim was synthesized")
			}
			if first["unmapped"].Type != storage.TypeText || first["nested"].Object["id"].Type != storage.TypeText || string(first["precise"].Data) != "9007199254740993" {
				t.Fatal("unmapped claims changed")
			}
			first["id"].Data[0] = 'X'
			first["ids"].List[0].Data[0] = 'X'
			again, err := authenticate(t.Context(), headers)
			if err != nil || string(again["id"].Data) != canonicalClaimID || string(again["ids"].List[0].Data) != canonicalClaimID {
				t.Fatal("claims were not independent", err)
			}
		})
	}
}

func TestInvalidMappedValuesFailAdmissionWithoutPartialClaims(t *testing.T) {
	cases := []struct {
		kind  string
		value any
	}{
		{"uuid", "very-secret-invalid-id"}, {"uuid", nil}, {"uuid", true}, {"uuid", json.Number("123")}, {"uuid", []any{claimID}},
		{"list<uuid>", claimID}, {"list<uuid>", nil}, {"list<uuid>", []any{claimID, "very-secret-invalid-id"}}, {"list<uuid>", []any{nil}}, {"list<uuid>", map[string]any{}},
	}
	for _, mode := range []string{"fixed", "unverified_jwt"} {
		for _, test := range cases {
			c := config.Default().Auth
			c.Mode, c.ClaimTypes = mode, map[string]string{"subject": test.kind}
			raw := map[string]any{"subject": test.value, "other": "preserved"}
			if mode == "fixed" {
				c.FixedClaims = config.JSONClaims(raw)
				if _, err := New(c); err == nil {
					t.Fatalf("fixed %s accepted %T", test.kind, test.value)
				} else if strings.Contains(err.Error(), "very-secret") {
					t.Fatal("error leaked claim contents")
				}
			} else {
				authenticate, err := New(c)
				if err != nil {
					t.Fatal(err)
				}
				claims, err := authenticate(t.Context(), []string{unverifiedToken(t, raw)})
				if err == nil || claims != nil {
					t.Fatalf("%s returned partial/invalid claims", test.kind)
				}
				if strings.Contains(err.Error(), "very-secret") {
					t.Fatal("error leaked claim contents")
				}
			}
		}
	}
}

func TestMappedEmptyUUIDListRemainsPresent(t *testing.T) {
	c := config.Default().Auth
	c.Mode, c.ClaimTypes, c.FixedClaims = "fixed", map[string]string{"ids": "list<uuid>"}, config.JSONClaims{"ids": []any{}}
	authenticate, err := New(c)
	if err != nil {
		t.Fatal(err)
	}
	claims, err := authenticate(t.Context(), nil)
	if err != nil || claims["ids"].State != storage.ValuePresent || claims["ids"].Type != storage.TypeList || len(claims["ids"].List) != 0 {
		t.Fatal(claims, err)
	}
}
