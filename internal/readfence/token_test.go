package readfence

import (
	"encoding/base64"
	"testing"
)

func TestOpaqueFenceRoundTripAndRejectsMalformedTokens(t *testing.T) {
	value := Encode(^uint64(0))
	if got, err := Decode(value); err != nil || got != ^uint64(0) {
		t.Fatalf("round trip: %d %v", got, err)
	}
	for _, value := range []string{"", "not-a-token", string(make([]byte, 1025)), base64.RawURLEncoding.EncodeToString([]byte(`{"v":2,"p":1}`))} {
		if _, err := Decode(value); err == nil {
			t.Fatal("accepted invalid fence")
		}
	}
}

func TestFenceRequiresAnExplicitNumericPosition(t *testing.T) {
	for _, payload := range []string{
		`{"v":1}`,
		`{"v":1,"p":null}`,
		`{"v":1,"p":"1"}`,
		`{"v":1,"p":-1}`,
		`{"v":1,"p":18446744073709551616}`,
	} {
		if _, err := Decode(base64.RawURLEncoding.EncodeToString([]byte(payload))); err == nil {
			t.Fatalf("accepted invalid token %s", payload)
		}
	}
	if value, err := Decode(Encode(0)); err != nil || value != 0 {
		t.Fatalf("explicit zero must remain valid: %d %v", value, err)
	}
}
