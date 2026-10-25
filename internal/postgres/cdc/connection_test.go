package cdc

import (
	"log/slog"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestReplicationConnectionsDeriveFromParsedPoolConfiguration(t *testing.T) {
	for name, dsn := range map[string]string{
		"URI":               "postgres://example:p%40ss%3Fword@localhost:5433/app?sslmode=disable&pool_max_conns=7&pool_min_conns=2&application_name=weave&search_path=public",
		"keyword DSN":       "host=localhost port=5433 user=example password='secret with spaces' dbname=app sslmode=disable pool_max_conns=7 pool_min_conns=2 application_name=weave search_path=public",
		"TLS and fallbacks": "postgres://example@localhost:5433,localhost:5434/app?sslmode=prefer&pool_max_conns=7&pool_min_conns=2&application_name=weave&search_path=public",
	} {
		t.Run(name, func(t *testing.T) {
			pool, err := pgxpool.ParseConfig(dsn)
			if err != nil {
				t.Fatal(err)
			}
			if pool.MaxConns != 7 || pool.MinConns != 2 {
				t.Fatal("pool options were not consumed")
			}
			base := &pool.ConnConfig.Config
			base.RuntimeParams["bytea_output"] = "escape"
			r, err := NewReplicator(base, "publication", "slot", NewBroker(), slog.Default(), ReplicatorOptions{})
			if err != nil {
				t.Fatal(err)
			}
			for _, config := range []struct {
				value       *pgconn.Config
				replication bool
			}{{r.connectionConfig, false}, {r.replicationConfig(), true}} {
				got := config.value
				if got.Host != base.Host || got.Port != base.Port || got.User != base.User || got.Password != base.Password || got.Database != base.Database {
					t.Fatal("connection identity changed")
				}
				if len(got.Fallbacks) != len(base.Fallbacks) || (got.TLSConfig == nil) != (base.TLSConfig == nil) {
					t.Fatal("TLS/fallback configuration lost")
				}
				for key := range got.RuntimeParams {
					if key == "pool_max_conns" || key == "pool_min_conns" {
						t.Fatalf("pool-only option %s leaked into connection startup", key)
					}
				}
				if got.RuntimeParams["application_name"] != "weave" || got.RuntimeParams["search_path"] != "public" {
					t.Fatal("session parameters lost")
				}
				if got.RuntimeParams["TimeZone"] != "UTC" || got.RuntimeParams["DateStyle"] != "ISO, YMD" || got.RuntimeParams["bytea_output"] != "hex" {
					t.Fatal("canonical output formatting lost")
				}
				if config.replication != (got.RuntimeParams["replication"] == "database") {
					t.Fatal("replication mode leaked into normal SQL connection")
				}
			}
			if _, exists := base.RuntimeParams["replication"]; exists {
				t.Fatal("caller configuration was mutated")
			}
			if base.RuntimeParams["bytea_output"] != "escape" {
				t.Fatal("caller output configuration was mutated")
			}
			base.RuntimeParams["search_path"] = "caller mutation"
			first := r.replicationConfig()
			first.RuntimeParams["search_path"] = "connection mutation"
			first.Host = "changed"
			if next := r.replicationConfig(); next.RuntimeParams["search_path"] != "public" || next.Host != r.connectionConfig.Host {
				t.Fatal("mutable connection settings leaked across copies")
			}
		})
	}
}

func TestReplicatorRequiresParsedConfiguration(t *testing.T) {
	if _, err := NewReplicator(nil, "publication", "slot", NewBroker(), slog.Default(), ReplicatorOptions{}); err == nil {
		t.Fatal("nil configuration accepted")
	}
}
