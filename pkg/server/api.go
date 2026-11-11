// Package server assembles a config-driven reactive stack for caller-owned HTTP servers.
package server

import (
	"context"

	impl "github.com/adiom-data/weave/internal/server"
)

// Config describes the reactive stack and optional application listener settings.
// ValidateListener checks the latter when an application chooses to use them.
type Config = impl.Config

// AuthConfig selects fixed, unverified JWT, or OIDC-verified claims admission.
type AuthConfig = impl.AuthConfig

// JSONClaims preserves JSON-compatible fixed claims, including exact numbers.
type JSONClaims = impl.JSONClaims

// Options adds a logger, authenticated application RPC services, and optional
// WebSocket authentication renewal hooks.
type Options = impl.Options

// Server supplies HTTP endpoints and owns the reactive stack. The application
// mounts Services and Routes, supervises Wait, and stops serving before Close.
// The caller owns OpenTelemetry setup and shuts its provider down after Close.
type Server = impl.Server

// DefaultConfig returns operational defaults; storage, auth, and visibility must be supplied.
func DefaultConfig() Config { return impl.DefaultConfig() }

// LoadConfig reads YAML and environment overrides, resolves relative policy paths,
// and validates reactive settings. Listener settings are not required.
func LoadConfig(path string, lookup func(string) (string, bool)) (Config, error) {
	return impl.LoadConfig(path, lookup)
}

// ValidateConfig checks reactive settings and compiles policy without contacting
// PostgreSQL or OIDC. It does not validate application listener settings.
func ValidateConfig(c Config) error { return impl.ValidateConfig(c) }

// New initializes the reactive stack without listening. The caller must Close the server.
func New(ctx context.Context, config Config, options Options) (Server, error) {
	return impl.New(ctx, config, options)
}
