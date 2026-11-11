package main

import (
	"io"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"

	"github.com/adiom-data/weave/pkg/server"
)

func TestBundledConfigurationCompilesWithPublicHost(t *testing.T) {
	cfg, err := server.LoadConfig(filepath.Join("..", "..", "server.yaml"), func(string) (string, bool) { return "", false })
	if err != nil {
		t.Fatal(err)
	}
	if err := server.ValidateConfig(cfg); err != nil {
		t.Fatal(err)
	}
	if cfg.Address != "127.0.0.1:8080" || cfg.Auth.FixedClaims["user_id"] != "20000000-0000-4000-8000-000000000001" {
		t.Fatal("demo config does not match the seeded identity/listener")
	}
	if cfg.Relations.File != filepath.Join("..", "..", "relations.yaml") {
		t.Fatal(cfg.Relations.File)
	}
}

func TestCheckConfigDoesNotConnectToDatabase(t *testing.T) {
	t.Setenv("WEAVE_DATABASE_URL", "postgres://127.0.0.1:1/not-a-database")
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	if err := execute(logger, []string{"-config", filepath.Join("..", "..", "server.yaml"), "-check-config"}); err != nil {
		t.Fatal(err)
	}
}

func TestCheckConfigRejectsInvalidListener(t *testing.T) {
	t.Setenv("WEAVE_ADDRESS", "")
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	err := execute(logger, []string{"-config", filepath.Join("..", "..", "server.yaml"), "-check-config"})
	if err == nil || !strings.Contains(err.Error(), "address is required") {
		t.Fatalf("expected listener validation before startup: %v", err)
	}
}
