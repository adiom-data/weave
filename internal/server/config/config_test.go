package config

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func validConfig() Config {
	c := Default()
	c.Storage.Postgres.URL = "postgres://localhost/app"
	c.Storage.Postgres.Publication, c.Storage.Postgres.Slot = "publication", "slot"
	c.Auth.Mode, c.Visibility.Mode = "fixed", "allow_all"
	return c
}
func writeTestFile(t *testing.T, name, contents string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, []byte(contents), 0600); err != nil {
		t.Fatal(err)
	}
	return path
}
func noEnv(string) (string, bool) { return "", false }

func TestListenerSettingsAreValidatedSeparately(t *testing.T) {
	c := validConfig()
	if err := c.ValidateListener(); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*Config){
		"empty address":    func(c *Config) { c.Address = "" },
		"invalid address":  func(c *Config) { c.Address = "invalid" },
		"shutdown timeout": func(c *Config) { c.ShutdownTimeout = 0 },
		"header timeout":   func(c *Config) { c.ReadHeaderTimeout = 0 },
		"idle timeout":     func(c *Config) { c.IdleTimeout = 0 },
	} {
		t.Run(name, func(t *testing.T) {
			invalid := c
			mutate(&invalid)
			if err := invalid.Validate(); err != nil {
				t.Fatal("listener settings gated the reactive stack:", err)
			}
			if err := invalid.ValidateListener(); err == nil {
				t.Fatal("invalid listener settings accepted")
			}
		})
	}
}

func TestLoadDoesNotRequireListenerSettings(t *testing.T) {
	path := writeTestFile(t, "server.yaml", `
address: ""
shutdown_timeout: 0s
read_header_timeout: 0s
idle_timeout: 0s
storage:
  postgres:
    url: postgres://localhost/app
    publication: pub
    slot: slot
auth:
  mode: fixed
visibility:
  mode: allow_all
`)
	c, err := Load(path, noEnv)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.ValidateListener(); err == nil {
		t.Fatal("load erased the invalid listener settings")
	}
}

func TestClientRevisionConfiguration(t *testing.T) {
	path := writeTestFile(t, "server.yaml", `
storage:
  postgres:
    url: postgres://localhost/app
    publication: publication
    slot: slot
auth:
  mode: fixed
visibility:
  mode: allow_all
websocket:
  minimum_client_revision: 9007199254740993
  recommended_client_revision: 18446744073709551615
`)
	c, err := Load(path, noEnv)
	if err != nil {
		t.Fatal(err)
	}
	if c.Socket.MinimumClientRevision != 9007199254740993 || c.Socket.RecommendedClientRevision != ^uint64(0) {
		t.Fatal("client revisions lost integer precision")
	}
	c = validConfig()
	c.Socket.MinimumClientRevision = 3
	c.Socket.RecommendedClientRevision = 2
	if c.Validate() == nil {
		t.Fatal("accepted recommended revision below minimum")
	}
	c.Socket.RecommendedClientRevision = 0
	if err := c.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestConfigDefaultsAndEnvironment(t *testing.T) {
	path := writeTestFile(t, "server.yaml", `
storage:
  type: postgres
  postgres:
    url: postgres://file/app
    publication: pub
    slot: slot
auth:
  mode: fixed
  fixed_claims:
    organization: one
    teams: [red, blue]
    precise: 9007199254740993
    active: TRUE
    date_label: 2026-10-29
visibility:
  mode: definitions
shutdown_timeout: 3s
query:
  limits:
    subscription_batch_size: 23
  references:
    load_concurrency: 2
    load_queue_limit: 32
relations:
  file: relations.yaml
`)
	c, err := Load(path, func(name string) (string, bool) {
		if name == "WEAVE_DATABASE_URL" {
			return "postgres://env/app", true
		}
		return "", false
	})
	if err != nil {
		t.Fatal(err)
	}
	if c.Storage.Postgres.URL != "postgres://env/app" || c.Address != "127.0.0.1:8080" || c.Relations.File != filepath.Join(filepath.Dir(path), "relations.yaml") {
		t.Fatalf("wrong config: %+v", c)
	}
	if c.Auth.FixedClaims["precise"].(json.Number).String() != "9007199254740993" || c.Auth.FixedClaims["active"] != true || c.Auth.FixedClaims["date_label"] != "2026-10-29" {
		t.Fatal("claim types changed")
	}
	if c.Socket.IdleTimeout != Default().Socket.IdleTimeout {
		t.Fatal("partial config erased defaults")
	}
	if c.Query.References.LoadConcurrency != 2 || c.Query.References.LoadQueueLimit != 32 {
		t.Fatal("reference work limits were not loaded")
	}
	if c.Query.Limits.SubscriptionBatchSize != 23 {
		t.Fatal("subscription registration batch size was not loaded")
	}
}

func TestSubscriptionBatchSizeRejectsNegativeValues(t *testing.T) {
	c := validConfig()
	c.Query.Limits.SubscriptionBatchSize = -1
	if err := c.Validate(); err == nil {
		t.Fatal("accepted negative subscription batch size")
	}
}

func TestReferenceWorkLimitsRejectNegativeValues(t *testing.T) {
	for _, concurrency := range []bool{false, true} {
		c := validConfig()
		if concurrency {
			c.Query.References.LoadConcurrency = -1
		} else {
			c.Query.References.LoadQueueLimit = -1
		}
		if err := c.Validate(); err == nil {
			t.Fatal("accepted negative reference limit")
		}
	}
}

func TestStrictConfig(t *testing.T) {
	for _, raw := range []string{"typo: true", "auth: {mode: fixed, mode: jwks}", "{}\n---\n{}", "auth: {fixed_claims: {x: 1, x: 2}}", "auth: {fixed_claims: [not, object]}", "auth: {fixed_claims: {bad: .nan}}"} {
		t.Run(raw, func(t *testing.T) {
			if _, err := Load(writeTestFile(t, "bad.yaml", raw), noEnv); err == nil {
				t.Fatal("accepted malformed config")
			}
		})
	}
	for _, mode := range []string{"", "postgresql", "mongo"} {
		c := validConfig()
		c.Storage.Type = mode
		if err := c.Validate(); err == nil {
			t.Fatal("invalid storage type")
		}
	}
	c := validConfig()
	c.Auth.Mode = ""
	if c.Validate() == nil {
		t.Fatal("auth mode must be explicit")
	}
	c = validConfig()
	c.Visibility.Mode = ""
	if c.Validate() == nil {
		t.Fatal("visibility mode must be explicit")
	}
}

func TestFixedClaimsEnvironment(t *testing.T) {
	for _, raw := range []string{`null`, `{}`, `{"user":"123","roles":["read"],"n":1.25}`} {
		c, err := Load("", func(name string) (string, bool) {
			values := map[string]string{"WEAVE_DATABASE_URL": "postgres://localhost/app", "WEAVE_PUBLICATION": "pub", "WEAVE_SLOT": "slot", "WEAVE_AUTH_MODE": "fixed", "WEAVE_VISIBILITY_MODE": "allow_all", "WEAVE_FIXED_CLAIMS": raw}
			v, ok := values[name]
			return v, ok
		})
		if err != nil {
			t.Fatal(err)
		}
		if raw == "null" && c.Auth.FixedClaims != nil {
			t.Fatal("null claims must remain empty")
		}
	}
}

func TestClaimTypesConfiguration(t *testing.T) {
	c := validConfig()
	if err := DecodeYAML([]byte("auth:\n  claim_types:\n    arbitrary_subject: uuid\n    teams: list<uuid>\n"), &c); err != nil {
		t.Fatal(err)
	}
	if err := c.Validate(); err != nil {
		t.Fatal(err)
	}
	if c.Auth.ClaimTypes["teams"] != ClaimTypeUUIDList {
		t.Fatal("claim_types was not decoded")
	}
	for _, kind := range []string{"", "UUID", "list", "list<list<uuid>>", "made-up"} {
		c.Auth.ClaimTypes = map[string]string{"subject": kind}
		if err := c.Validate(); err == nil {
			t.Fatalf("accepted unsupported claim mapping %q", kind)
		}
	}
	c.Auth.ClaimTypes = map[string]string{"": "uuid"}
	if err := c.Validate(); err == nil {
		t.Fatal("empty claim mapping name accepted")
	}
}
