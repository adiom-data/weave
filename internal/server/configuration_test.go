package server

import (
	"context"
	"net/http"
	"testing"

	"connectrpc.com/connect"
	"github.com/adiom-data/framework/httpapp"
)

func TestConfigValidationDoesNotConnectAndNewRejectsInvalidPolicy(t *testing.T) {
	c := validConfig()
	c.Storage.Postgres.URL = "postgres://127.0.0.1:1/not-a-database"
	if err := ValidateConfig(c); err != nil {
		t.Fatal(err)
	}
	c.Limits.MaxFactRows = 1000
	c.Visibility.Mode = "policy"
	c.Visibility.PolicyFile = writeTestFile(t, "invalid-policy.yaml", "relations: [{schema: public, table: things, allow: [{id: bad, when: '!! invalid cel'}]}]")
	if _, err := New(context.Background(), c, Options{}); err == nil {
		t.Fatal("invalid policy was not rejected before connecting")
	}
}

func TestExtensionServiceNamesMustBeUniqueAndUnreserved(t *testing.T) {
	factory := func(...connect.HandlerOption) (string, http.Handler) { return "/test/", http.NotFoundHandler() }
	custom := httpapp.Connect("app.v1.Commands", factory)
	for _, options := range []Options{
		{Services: []httpapp.ConnectService{{Name: "missing handler"}}},
		{Services: []httpapp.ConnectService{httpapp.Connect("reactive.v1.ReactiveQueries", factory)}},
		{Services: []httpapp.ConnectService{custom}, AuthenticatedServices: []httpapp.ConnectService{custom}},
		{Routes: []httpapp.Route{{Pattern: "/missing-handler"}}},
	} {
		if err := validateEndpoints(options); err == nil {
			t.Fatal("invalid endpoint accepted")
		}
	}
	if err := validateEndpoints(Options{Services: []httpapp.ConnectService{custom}}); err != nil {
		t.Fatal(err)
	}
}

func TestValidateConfigRejectsInvalidFixedClaimMappingOffline(t *testing.T) {
	c := validConfig()
	c.Storage.Postgres.URL = "postgres://127.0.0.1:1/not-a-database"
	c.Auth.ClaimTypes = map[string]string{"subject": "uuid"}
	c.Auth.FixedClaims = JSONClaims{"subject": "invalid-uuid"}
	if err := ValidateConfig(c); err == nil {
		t.Fatal("check-config accepted invalid fixed UUID claim")
	}
	if _, err := New(t.Context(), c, Options{}); err == nil {
		t.Fatal("New accepted invalid fixed UUID claim")
	}
	delete(c.Auth.FixedClaims, "subject")
	if err := ValidateConfig(c); err != nil {
		t.Fatal("missing optional mapped claim should be valid", err)
	}
}
