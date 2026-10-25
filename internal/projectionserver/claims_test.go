package projectionserver

import (
	"context"
	"errors"
	"github.com/adiom-data/weave/internal/accesscontrol"
	"github.com/adiom-data/weave/internal/storage"
	"testing"
)

func TestResolveClaimsCopiesProviderValuesAndRequiresNoIdentity(t *testing.T) {
	input := accesscontrol.Claims{"roles": {Type: storage.TypeList, State: storage.ValuePresent, List: []storage.Value{{Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("reader")}}}}
	s := &server{options: Options{ClaimsProvider: func(context.Context) (accesscontrol.Claims, error) { return input, nil }}}
	got, err := s.resolveClaims(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	input["roles"].List[0].Data[0] = 'X'
	if string(got["roles"].List[0].Data) != "reader" {
		t.Fatal("provider retained mutable claims alias")
	}
	s.options.ClaimsProvider = nil
	if _, err := s.resolveClaims(context.Background()); err != nil {
		t.Fatal("omitted claims require identity", err)
	}
	failure := errors.New("host rejected credentials")
	s.options.ClaimsProvider = func(context.Context) (accesscontrol.Claims, error) { return nil, failure }
	if _, err := s.resolveClaims(context.Background()); !errors.Is(err, failure) {
		t.Fatal("host error swallowed", err)
	}
}
