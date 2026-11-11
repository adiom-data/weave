package config

import "testing"

func TestManagementDefaultsAndEnvironment(t *testing.T) {
	c := validConfig()
	if c.Management.Enabled {
		t.Fatal("management enabled by default")
	}
	c.Management.Enabled = true
	if c.Validate() == nil {
		t.Fatal("missing admin credential accepted")
	}
	c.Management.BearerToken = "admin token"
	if c.Validate() == nil {
		t.Fatal("whitespace token accepted")
	}
	c.Management.BearerToken = "admin\u00a0token"
	if c.Validate() == nil {
		t.Fatal("unicode whitespace token accepted")
	}
	c.Management.BearerToken = "separate-admin-token"
	if err := c.Validate(); err != nil {
		t.Fatal(err)
	}
	path := writeTestFile(t, "server.yaml", `storage:
  postgres:
    url: postgres://localhost/app
    publication: app
    slot: app
auth:
  mode: fixed
visibility:
  mode: allow_all
`)
	lookup := func(key string) (string, bool) {
		switch key {
		case "WEAVE_MANAGEMENT_ENABLED":
			return "true", true
		case "WEAVE_MANAGEMENT_TOKEN":
			return "admin-token", true
		}
		return "", false
	}
	loaded, err := Load(path, lookup)
	if err != nil || !loaded.Management.Enabled || loaded.Management.BearerToken != "admin-token" {
		t.Fatal(loaded.Management, err)
	}
	if _, err := Load(path, func(key string) (string, bool) {
		if key == "WEAVE_MANAGEMENT_ENABLED" {
			return "yes", true
		}
		return "", false
	}); err == nil {
		t.Fatal("invalid boolean accepted")
	}
}
