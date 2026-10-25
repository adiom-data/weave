package server

import (
	"fmt"

	"github.com/adiom-data/weave/internal/server/config"
	"github.com/adiom-data/weave/internal/server/identity"
	"github.com/adiom-data/weave/internal/server/policyfile"
	"github.com/adiom-data/weave/pkg/accesscontrol/policy"
	"github.com/adiom-data/weave/pkg/projectionserver"
)

type Config = config.Config
type AuthConfig = config.AuthConfig
type JSONClaims = config.JSONClaims

func DefaultConfig() Config { return config.Default() }

// LoadConfig reads YAML and environment overrides. Relative policy paths resolve
// against the configuration file. Policy compilation happens in New/ValidateConfig.
func LoadConfig(path string, lookup func(string) (string, bool)) (Config, error) {
	return config.Load(path, lookup)
}

// ValidateConfig compiles fixed policies and checks configuration without
// connecting to the datastore or OIDC issuer.
func ValidateConfig(c Config) error {
	_, err := prepareConfig(c)
	return err
}

func prepareConfig(c Config) (projectionserver.Options, error) {
	var empty projectionserver.Options
	if err := c.Validate(); err != nil {
		return empty, err
	}
	if err := identity.ValidateFixedClaims(c.Auth); err != nil {
		return empty, fmt.Errorf("fixed claims: %w", err)
	}
	var provider policy.Provider
	if c.Visibility.Mode == "policy" {
		var err error
		provider, err = policyfile.Load(c.Visibility.PolicyFile, policy.DefaultLimits())
		if err != nil {
			return empty, fmt.Errorf("load visibility policies: %w", err)
		}
	}
	return projectionOptions(c, provider)
}
