package server

import (
	"fmt"

	"github.com/adiom-data/weave/internal/server/config"
	"github.com/adiom-data/weave/internal/server/identity"
	"github.com/adiom-data/weave/internal/server/relationfile"
	"github.com/adiom-data/weave/pkg/accesscontrol"
	"github.com/adiom-data/weave/pkg/projectionserver"
	"github.com/adiom-data/weave/pkg/queryserver"
	relationdef "github.com/adiom-data/weave/pkg/relation"
	"github.com/adiom-data/weave/pkg/storage"
	"github.com/adiom-data/weave/pkg/transactionmessage"
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

func postgresOptions(c Config) storage.PostgresOptions {
	p := c.Storage.Postgres
	return storage.PostgresOptions{
		Publication: p.Publication, Slot: p.Slot, AllowedTables: p.AllowedTables,
		QueryLimits: storage.QueryLimits(p.QueryLimits), MessageLimits: storage.MessageLimits(p.MessageLimits),
		MessageLogInterval: p.MessageLogInterval, Replication: storage.ReplicationOptions(p.Replication),
		CaptureBufferLimit: p.CaptureBufferLimit,
	}
}

func queryOptions(c Config) queryserver.Options {
	q := c.Query
	m := q.Messages
	return queryserver.Options{
		Limits: queryserver.Limits(q.Limits), References: queryserver.ReferenceOptions(q.References),
		Messages: transactionmessage.Options{
			MessageLimits: transactionmessage.MessageLimits(m.MessageLimits), MaxQueuedTransactions: m.MaxQueuedTransactions,
			MaxQueuedBytes: m.MaxQueuedBytes, MaxBatchTransactions: m.MaxBatchTransactions, MaxBatchBytes: m.MaxBatchBytes,
		},
		// This host does not register raw query RPCs; transport framing is unused.
	}
}

func configuredProjectionOptions(c Config) projectionserver.Options {
	p := c.Projection
	return projectionserver.Options{
		MessageOptions: projectionserver.MessageOptions(p.MessageOptions), MessageProgressInterval: p.MessageProgressInterval,
		MaxRequestsPerConnection: p.MaxRequestsPerConnection, MaxSubscriptionsPerConnection: p.MaxSubscriptionsPerConnection,
		SubscriptionQueueLimit: p.SubscriptionQueueLimit, OutboundQueueLimit: p.OutboundQueueLimit,
		MaxSelectedFields: p.MaxSelectedFields, MaxPredicateNodes: p.MaxPredicateNodes, MaxPredicateDepth: p.MaxPredicateDepth,
		ProjectionLimits: projectionserver.ProjectionLimits(p.ProjectionLimits), MaxConcurrentProjectionExpansions: p.MaxConcurrentProjectionExpansions,
		Dependencies: projectionserver.DependencyOptions(p.Dependencies),
	}
}

func configuredRelationOptions(c Config) projectionserver.RelationOptions {
	p := c.Relations
	return projectionserver.RelationOptions{
		AllowAll:    c.Visibility.Mode == "allow_all",
		Limits:      relationdef.Limits(p.Limits),
		MaxFactRows: p.MaxFactRows, MaxFactQueries: p.MaxFactQueries, FactLoadConcurrency: p.FactLoadConcurrency,
	}
}

func projectionOptions(c Config, store *relationdef.Store) projectionserver.Options {
	o := configuredProjectionOptions(c)
	o.ClaimsProvider = accesscontrol.ClaimsFromContext
	if store != nil {
		p := configuredRelationOptions(c)
		p.Store = store
		o.Relations = &p
	}
	return o
}

// ValidateConfig compiles fixed policies and checks reactive configuration without
// connecting to the datastore or OIDC issuer. Listener settings are caller-owned.
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
	var store *relationdef.Store
	if c.Relations.File != "" {
		var err error
		store, err = relationfile.Load(c.Relations.File, relationdef.Limits(c.Relations.Limits))
		if err != nil {
			return empty, fmt.Errorf("load relation definitions: %w", err)
		}
	}
	return projectionOptions(c, store), nil
}
