package config

import (
	"testing"
	"time"
)

func TestRenewalConfiguration(t *testing.T) {
	c := validConfig()
	if c.Socket.Renewal.Enabled {
		t.Fatal("renewal enabled by default")
	}
	if err := DecodeYAML([]byte(`auth:
  mode: unverified_jwt
websocket:
  renewal:
    enabled: true
    validation_timeout: 2s
    max_concurrent_validations: 4
    excluded_claims: [exp, iat]
`), &c); err != nil {
		t.Fatal(err)
	}
	if err := c.Validate(); err != nil {
		t.Fatal(err)
	}
	if c.Socket.Renewal.ValidationTimeout != 2*time.Second || c.Socket.Renewal.MaxConcurrentValidations != 4 || len(c.Socket.Renewal.ExcludedClaims) != 2 {
		t.Fatal("renewal configuration was not loaded")
	}
	for name, change := range map[string]func(*Config){
		"fixed":               func(c *Config) { c.Auth.Mode = "fixed" },
		"socket disabled":     func(c *Config) { c.Socket.Enabled = false },
		"timeout":             func(c *Config) { c.Socket.Renewal.ValidationTimeout = 0 },
		"concurrency":         func(c *Config) { c.Socket.Renewal.MaxConcurrentValidations = 0 },
		"empty exclusion":     func(c *Config) { c.Socket.Renewal.ExcludedClaims = []string{""} },
		"duplicate exclusion": func(c *Config) { c.Socket.Renewal.ExcludedClaims = []string{"exp", "exp"} },
		"mapped exp":          func(c *Config) { c.Auth.ClaimTypes = map[string]string{"exp": "uuid"} },
	} {
		t.Run(name, func(t *testing.T) {
			invalid := c
			change(&invalid)
			if invalid.Validate() == nil {
				t.Fatal("invalid renewal configuration accepted")
			}
		})
	}
}
