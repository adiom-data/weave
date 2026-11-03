package server

import (
	"errors"
	"fmt"

	"github.com/adiom-data/weave/internal/server/config"
	"github.com/adiom-data/weave/internal/server/identity"
	"github.com/adiom-data/weave/internal/server/policyfile"
	"github.com/adiom-data/weave/pkg/accesscontrol"
	"github.com/adiom-data/weave/pkg/accesscontrol/policy"
	"github.com/adiom-data/weave/pkg/projectionserver"
	"github.com/adiom-data/weave/pkg/queryserver"
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

func configuredPolicyOptions(c Config) projectionserver.PolicyOptions {
	p := c.Visibility
	return projectionserver.PolicyOptions{
		Limits: policy.Limits(p.Limits), Cache: policy.CacheOptions(p.Cache),
		MaxFactRows: p.MaxFactRows, MaxFactQueries: p.MaxFactQueries, FactLoadConcurrency: p.FactLoadConcurrency,
	}
}

func projectionOptions(c Config, provider policy.Provider) (projectionserver.Options, error) {
	o := configuredProjectionOptions(c)
	o.ClaimsProvider = accesscontrol.ClaimsFromContext
	if c.Visibility.Mode == "policy" {
		if provider == nil {
			return o, errors.New("policy mode requires a compiled provider")
		}
		p := configuredPolicyOptions(c)
		p.Provider = provider
		o.Policies = &p
	}
	return o, nil
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
		provider, err = policyfile.Load(c.Visibility.PolicyFile, policy.Limits(c.Visibility.Limits))
		if err != nil {
			return empty, fmt.Errorf("load visibility policies: %w", err)
		}
	}
	return projectionOptions(c, provider)
}
