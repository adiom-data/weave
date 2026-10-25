package accesscontrol

import (
	"context"
	"github.com/adiom-data/weave/internal/storage"
	"testing"
)

func TestClaimsContextNeedsNoIdentityAndOwnsCopies(t *testing.T) {
	if _, err := ClaimsFromContext(context.Background()); err == nil {
		t.Fatal("missing claims accepted")
	}
	if _, err := ClaimsFromContext(ContextWithClaims(context.Background(), nil)); err != nil {
		t.Fatal("explicit empty claims rejected", err)
	}
	input := Claims{"role": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("reader")}}
	ctx := ContextWithClaims(context.Background(), input)
	input["role"].Data[0] = 'X'
	first, err := ClaimsFromContext(ctx)
	if err != nil || string(first["role"].Data) != "reader" {
		t.Fatal(first, err)
	}
	first["role"].Data[0] = 'Y'
	second, _ := ClaimsFromContext(ctx)
	if string(second["role"].Data) != "reader" {
		t.Fatal("context claims changed through reader")
	}
}
