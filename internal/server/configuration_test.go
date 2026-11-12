package server

import (
	"context"
	"net/http"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/adiom-data/framework/httpapp"
)

func TestQueryFenceTimeoutConfigurationIsForwarded(t *testing.T) {
	c := validConfig()
	c.Query.FenceWaitTimeout = 45 * time.Second
	if got := queryOptions(c).FenceWaitTimeout; got != c.Query.FenceWaitTimeout {
		t.Fatalf("query fence wait timeout = %v, want %v", got, c.Query.FenceWaitTimeout)
	}
}

func TestConfigValidationDoesNotConnectAndNewRejectsInvalidPolicy(t *testing.T) {
	c := validConfig()
	c.Storage.Postgres.URL = "postgres://127.0.0.1:1/not-a-database"
	if err := ValidateConfig(c); err != nil {
		t.Fatal(err)
	}
	c.Relations.MaxFactRows = 1000
	c.Visibility.Mode = "definitions"
	c.Relations.File = writeTestFile(t, "invalid-relations.yaml", "relations: [{schema: public, table: things, visibility: '!! invalid cel'}]\n")
	if _, err := New(context.Background(), c, Options{}); err == nil {
		t.Fatal("invalid policy was not rejected before connecting")
	}
}

func TestLibraryValidationIgnoresCallerOwnedListener(t *testing.T) {
	c := validConfig()
	c.Address = ""
	c.ShutdownTimeout, c.ReadHeaderTimeout, c.IdleTimeout = 0, 0, 0
	if err := ValidateConfig(c); err != nil {
		t.Fatal("library required unused listener settings:", err)
	}
	c.MaxMessageBytes = 0
	if err := ValidateConfig(c); err == nil {
		t.Fatal("library accepted an invalid reactive message limit")
	}
}

func TestExtensionServiceNamesMustBeUniqueAndUnreserved(t *testing.T) {
	factory := func(...connect.HandlerOption) (string, http.Handler) { return "/test/", http.NotFoundHandler() }
	custom := httpapp.Connect("app.v1.Commands", factory)
	for _, options := range []Options{
		{AuthenticatedServices: []httpapp.ConnectService{{Name: "missing handler"}}},
		{AuthenticatedServices: []httpapp.ConnectService{httpapp.Connect("reactive.v1.ReactiveQueries", factory)}},
		{AuthenticatedServices: []httpapp.ConnectService{custom, custom}},
	} {
		if err := validateEndpoints(options); err == nil {
			t.Fatal("invalid endpoint accepted")
		}
	}
	if err := validateEndpoints(Options{AuthenticatedServices: []httpapp.ConnectService{custom}}); err != nil {
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

func TestPolicyFileUsesConfiguredCompilationLimits(t *testing.T) {
	c := validConfig()
	c.Visibility.Mode = "definitions"
	c.Relations.File = writeTestFile(t, "relations.yaml", `
relations:
  - schema: app
    table: projects
    visibility: 'true'
`)
	if err := ValidateConfig(c); err != nil {
		t.Fatal(err)
	}
	c.Relations.Limits.MaxExpressionBytes = 3
	if err := ValidateConfig(c); err == nil {
		t.Fatal("policy-file compilation ignored configured expression budget")
	}
}
