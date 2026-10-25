package identity

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/adiom-data/weave/internal/server/config"
	"github.com/adiom-data/weave/pkg/accesscontrol"
	reactivev1 "github.com/adiom-data/weave/pkg/gen/reactive/v1"
	"github.com/adiom-data/weave/pkg/storage"
	"github.com/golang-jwt/jwt/v5"
)

func signedToken(t *testing.T, key ed25519.PrivateKey, kid string, claims jwt.MapClaims) string {
	t.Helper()
	token := jwt.NewWithClaims(jwt.SigningMethodEdDSA, claims)
	token.Header["kid"] = kid
	encoded, err := token.SignedString(key)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}
func standardClaims(issuer string) jwt.MapClaims {
	return jwt.MapClaims{"iss": issuer, "aud": "audience", "exp": time.Now().Add(time.Hour).Unix(), "organization": "org", "large": json.Number("9007199254740993"), "teams": []any{"a", "b"},
		"nested":  map[string]any{"values": []any{json.Number("9007199254740993"), json.Number("0.1234567890123456789")}},
		"uuid_id": claimID, "uuid_ids": []any{claimID}}
}
func TestOIDCDiscoveryVerificationAndRotation(t *testing.T) {
	pub, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	var current atomic.Value
	setKey := func(kid string, pub ed25519.PublicKey) {
		current.Store(map[string]any{"keys": []any{map[string]any{"kty": "OKP", "crv": "Ed25519", "alg": "EdDSA", "use": "sig", "kid": kid, "x": base64.RawURLEncoding.EncodeToString(pub)}}})
	}
	setKey("one", pub)
	var discoveries, keyRequests atomic.Int32
	var endpoint *httptest.Server
	endpoint = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var data any
		switch r.URL.Path {
		case "/.well-known/openid-configuration":
			discoveries.Add(1)
			data = map[string]any{"issuer": endpoint.URL, "jwks_uri": endpoint.URL + "/keys", "id_token_signing_alg_values_supported": []string{"EdDSA"}}
		case "/keys":
			keyRequests.Add(1)
			data = current.Load()
		default:
			http.NotFound(w, r)
			return
		}
		if err := json.NewEncoder(w).Encode(data); err != nil {
			t.Error(err)
		}
	}))
	defer endpoint.Close()
	c := config.Default().Auth
	c.Mode = "oidc"
	c.Issuer = endpoint.URL
	c.Audience = "audience"
	c.ClaimTypes = map[string]string{"uuid_id": "uuid", "uuid_ids": "list<uuid>"}
	authenticate, err := New(c)
	if err != nil {
		t.Fatal(err)
	}
	if discoveries.Load() != 1 {
		t.Fatal("startup must discover issuer")
	}
	claims, err := authenticate(t.Context(), []string{"Bearer " + signedToken(t, key, "one", standardClaims(c.Issuer))})
	if err != nil {
		t.Fatal(err)
	}
	if string(claims["large"].Data) != "9007199254740993" || string(claims["organization"].Data) != "org" {
		t.Fatal("claims changed or lost precision")
	}
	values := claims["nested"].Object["values"].List
	expectedDecimal, err := storage.ParseScalar(storage.TypeDecimal, []byte("0.1234567890123456789"))
	if err != nil {
		t.Fatal(err)
	}
	if len(values) != 2 || string(values[0].Data) != "9007199254740993" || !storage.EqualValues(values[1], expectedDecimal) {
		t.Fatal("nested verified claims lost numeric precision")
	}
	if _, ok := claims["sub"]; ok {
		t.Fatal("subject should not be invented")
	}
	if claims["uuid_id"].Type != storage.TypeUUID || string(claims["uuid_id"].Data) != canonicalClaimID || claims["uuid_ids"].List[0].Type != storage.TypeUUID {
		t.Fatal("verified claims were not mapped")
	}
	badMapping := standardClaims(c.Issuer)
	badMapping["uuid_id"] = "invalid-uuid"
	if result, err := authenticate(t.Context(), []string{"Bearer " + signedToken(t, key, "one", badMapping)}); err == nil || result != nil {
		t.Fatal("invalid mapped verified claim was accepted")
	}
	badMapping["iss"] = "wrong-issuer"
	if _, err := authenticate(t.Context(), []string{"Bearer " + signedToken(t, key, "one", badMapping)}); err == nil || strings.Contains(err.Error(), `claim "uuid_id"`) {
		t.Fatal("mapping ran before verification", err)
	}
	for _, change := range []func(jwt.MapClaims){
		func(c jwt.MapClaims) { c["iss"] = "wrong" }, func(c jwt.MapClaims) { c["aud"] = "wrong" }, func(c jwt.MapClaims) { c["exp"] = time.Now().Add(-time.Hour).Unix() }, func(c jwt.MapClaims) { delete(c, "exp") },
	} {
		claims := standardClaims(c.Issuer)
		change(claims)
		if _, err := authenticate(t.Context(), []string{"Bearer " + signedToken(t, key, "one", claims)}); err == nil {
			t.Fatal("accepted invalid claims")
		}
	}
	if keyRequests.Load() != 1 {
		t.Fatal("known keys should be cached")
	}
	bad, err := jwt.NewWithClaims(jwt.SigningMethodHS256, standardClaims(c.Issuer)).SignedString([]byte("not-the-public-key"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := authenticate(t.Context(), []string{"Bearer " + bad}); err == nil {
		t.Fatal("accepted symmetric algorithm")
	}
	newPub, newKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	setKey("two", newPub)
	if _, err := authenticate(t.Context(), []string{"Bearer " + signedToken(t, newKey, "two", standardClaims(c.Issuer))}); err != nil {
		t.Fatal(err)
	}
	if keyRequests.Load() != 2 {
		t.Fatal("unknown key should refresh signing keys")
	}
}
func TestOIDCDiscoveryFailure(t *testing.T) {
	endpoint := httptest.NewServer(http.NotFoundHandler())
	defer endpoint.Close()
	c := config.Default().Auth
	c.Mode = "oidc"
	c.Issuer = endpoint.URL
	c.Audience = "audience"
	if _, err := New(c); err == nil {
		t.Fatal("broken discovery must fail startup")
	}
	for _, change := range []func(*config.AuthConfig){func(c *config.AuthConfig) { c.Issuer = "" }, func(c *config.AuthConfig) { c.Audience = "" }, func(c *config.AuthConfig) { c.Issuer = "file:///keys" }, func(c *config.AuthConfig) { c.ClockSkew = 0 }} {
		x := c
		change(&x)
		if x.Validate() == nil {
			t.Fatal("accepted invalid OIDC config")
		}
	}
}
func TestUnverifiedJWTAndFixedClaims(t *testing.T) {
	c := config.Default().Auth
	c.Mode = "unverified_jwt"
	authenticate, err := New(c)
	if err != nil {
		t.Fatal(err)
	}
	// Deliberately no valid signature, issuer, audience, subject, or current expiry.
	encoded, err := jwt.NewWithClaims(jwt.SigningMethodNone, jwt.MapClaims{"exp": 1, "id": "123"}).SignedString(jwt.UnsafeAllowNoneSignatureType)
	if err != nil {
		t.Fatal(err)
	}
	claims, err := authenticate(t.Context(), []string{"Bearer " + encoded})
	if err != nil {
		t.Fatal(err)
	}
	if claims["id"].Type != storage.TypeText {
		t.Fatal("must not infer UUIDs or numbers from strings")
	}
	for _, headers := range [][]string{nil, {"Bearer bad"}, {"Bearer " + encoded, "Bearer " + encoded}, {"Basic " + encoded}} {
		if _, err := authenticate(t.Context(), headers); err == nil {
			t.Fatal("accepted malformed headers/token")
		}
	}
	c.Mode = "fixed"
	c.FixedClaims = config.JSONClaims{"roles": []any{"read"}}
	authenticate, err = New(c)
	if err != nil {
		t.Fatal(err)
	}
	first, err := authenticate(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	first["roles"].List[0].Data[0] = 'X'
	second, err := authenticate(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if string(second["roles"].List[0].Data) != "read" {
		t.Fatal("fixed claims shared mutable state")
	}
	c.FixedClaims = nil
	authenticate, err = New(c)
	if err != nil {
		t.Fatal(err)
	}
	empty, err := authenticate(t.Context(), nil)
	if err != nil || len(empty) != 0 {
		t.Fatal("empty fixed claims must work")
	}
}

type requestStream struct{ connect.StreamingHandlerConn }

func (requestStream) RequestHeader() http.Header { return http.Header{} }
func TestClaimsCheckedOncePerStream(t *testing.T) {
	var checks atomic.Int32
	authenticate := Authenticator(func(context.Context, []string) (accesscontrol.Claims, error) {
		checks.Add(1)
		return accesscontrol.Claims{"user": {State: storage.ValuePresent, Type: storage.TypeText, Data: []byte("one")}}, nil
	})
	err := authenticate.WrapStreamingHandler(func(ctx context.Context, _ connect.StreamingHandlerConn) error {
		for range 10 {
			claims, err := accesscontrol.ClaimsFromContext(ctx)
			if err != nil || string(claims["user"].Data) != "one" {
				t.Fatal("missing claims")
			}
		}
		return nil
	})(t.Context(), requestStream{})
	if err != nil || checks.Load() != 1 {
		t.Fatal("claims were not captured once")
	}
	bad := Authenticator(func(context.Context, []string) (accesscontrol.Claims, error) { return nil, io.EOF })
	_, err = bad.WrapUnary(func(context.Context, connect.AnyRequest) (connect.AnyResponse, error) {
		t.Fatal("unauthenticated handler called")
		return nil, nil
	})(t.Context(), connect.NewRequest(&reactivev1.ProjectionSnapshotRequest{}))
	if connect.CodeOf(err) != connect.CodeUnauthenticated {
		t.Fatal(err)
	}
}
func TestLogicalValues(t *testing.T) {
	for _, input := range []any{nil, "123", true, json.Number("123"), json.Number("1.25"), json.Number("9223372036854775808"), []any{"a", nil}, map[string]any{"nested": false}} {
		v, err := logicalValue(input)
		if err != nil {
			t.Fatal(err)
		}
		if err := v.ValidateComplete(); err != nil {
			t.Fatal(err)
		}
	}
}
