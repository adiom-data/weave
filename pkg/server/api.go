// Package server hosts a config-driven reactive stack alongside application RPCs.
package server

import (
	"context"

	impl "github.com/adiom-data/weave/internal/server"
)

// Config describes the reactive stack, policy file, authentication, and listener.
type Config = impl.Config

// AuthConfig selects fixed, unverified JWT, or OIDC-verified claims admission.
type AuthConfig = impl.AuthConfig

// JSONClaims preserves JSON-compatible fixed claims, including exact numbers.
type JSONClaims = impl.JSONClaims

// Options adds a logger and application-owned RPC services or HTTP routes.
type Options = impl.Options

// Host owns one listener and the reactive stack. Its only operations are Run and Close.
type Host = impl.Host

// DefaultConfig returns operational defaults; storage, auth, and visibility must be supplied.
func DefaultConfig() Config { return impl.DefaultConfig() }

// LoadConfig reads YAML and environment overrides, resolving relative policy paths.
func LoadConfig(path string, lookup func(string) (string, bool)) (Config, error) {
	return impl.LoadConfig(path, lookup)
}

// ValidateConfig checks configuration and compiles policy without contacting PostgreSQL or OIDC.
func ValidateConfig(c Config) error { return impl.ValidateConfig(c) }

// New initializes the reactive stack without listening. The caller must Close the host.
func New(ctx context.Context, config Config, options Options) (Host, error) {
	return impl.New(ctx, config, options)
}
