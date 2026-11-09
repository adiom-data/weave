package config

import (
	"fmt"
	"reflect"
	"time"

	"github.com/adiom-data/weave/internal/messagebudget"
	messages "github.com/adiom-data/weave/internal/transactionmessage"
	"github.com/adiom-data/weave/pkg/projectionserver"
	"github.com/adiom-data/weave/pkg/queryserver"
	"github.com/adiom-data/weave/pkg/storage"
)

// These data-only types define the operator-facing YAML vocabulary. Structural
// conversions to library configuration keep every scalar field accounted for
// without exposing providers, callbacks, or other implementation choices.
type QueryConfig struct {
	Limits     QueryLimits          `yaml:"limits"`
	References ReferenceConfig      `yaml:"references"`
	Messages   MessageServiceConfig `yaml:"messages"`
}

type QueryLimits struct {
	EntityIdleTTL             time.Duration `yaml:"entity_idle_ttl"`
	EvictionPeriod            time.Duration `yaml:"eviction_period"`
	EvictionBatchSize         int           `yaml:"eviction_batch_size"`
	QueryIdleTTL              time.Duration `yaml:"query_idle_ttl"`
	QueryMaintenancePeriod    time.Duration `yaml:"query_maintenance_period"`
	MaxResyncTransactions     int           `yaml:"max_resync_transactions"`
	MaxResyncChanges          int           `yaml:"max_resync_changes"`
	MaxHydrations             int           `yaml:"max_hydrations"`
	ResyncResultBuffer        int           `yaml:"resync_result_buffer"`
	HydrationResultBuffer     int           `yaml:"hydration_result_buffer"`
	MaxWindowRows             int           `yaml:"max_window_rows"`
	SnapshotPageRows          int           `yaml:"snapshot_page_rows"`
	SubscriptionBatchSize     int           `yaml:"subscription_batch_size"`
	MaxReferenceInvalidations int           `yaml:"max_reference_invalidations"`
	ProgressBarrierDelay      time.Duration `yaml:"progress_barrier_delay"`
}

type ReferenceConfig struct {
	LoadConcurrency int `yaml:"load_concurrency"`
	LoadQueueLimit  int `yaml:"load_queue_limit"`
}

type MessageLimits struct {
	MaxMessageBytes     int `yaml:"max_message_bytes"`
	MaxTransactionBytes int `yaml:"max_transaction_bytes"`
	MaxMessages         int `yaml:"max_messages"`
}

type MessageServiceConfig struct {
	MessageLimits         MessageLimits `yaml:"message_limits"`
	MaxQueuedTransactions int           `yaml:"max_queued_transactions"`
	MaxQueuedBytes        int           `yaml:"max_queued_bytes"`
	MaxBatchTransactions  int           `yaml:"max_batch_transactions"`
	MaxBatchBytes         int           `yaml:"max_batch_bytes"`
}

type PostgresQueryLimits struct {
	MaxOrderTerms          int `yaml:"max_order_terms"`
	MaxPredicateNodes      int `yaml:"max_predicate_nodes"`
	MaxPredicateDepth      int `yaml:"max_predicate_depth"`
	MaxPredicateValueBytes int `yaml:"max_predicate_value_bytes"`
}

type ReplicationConfig struct {
	RetryMinDelay  time.Duration `yaml:"retry_min_delay"`
	RetryMaxDelay  time.Duration `yaml:"retry_max_delay"`
	StatusInterval time.Duration `yaml:"status_interval"`
}

type ProjectionConfig struct {
	MessageOptions                    ProjectionMessageConfig `yaml:"messages"`
	MessageProgressInterval           time.Duration           `yaml:"message_progress_interval"`
	MaxRequestsPerConnection          int                     `yaml:"max_requests_per_connection"`
	MaxSubscriptionsPerConnection     int                     `yaml:"max_subscriptions_per_connection"`
	SubscriptionQueueLimit            int                     `yaml:"subscription_queue_limit"`
	OutboundQueueLimit                int                     `yaml:"outbound_queue_limit"`
	MaxSelectedFields                 int                     `yaml:"max_selected_fields"`
	MaxPredicateNodes                 int                     `yaml:"max_predicate_nodes"`
	MaxPredicateDepth                 int                     `yaml:"max_predicate_depth"`
	ProjectionLimits                  ProjectionLimits        `yaml:"limits"`
	MaxConcurrentProjectionExpansions int                     `yaml:"max_concurrent_projection_expansions"`
	Dependencies                      DependencyConfig        `yaml:"dependencies"`
}

type ProjectionMessageConfig struct {
	MaxQueuedTransactions int           `yaml:"max_queued_transactions"`
	MaxQueuedBytes        int           `yaml:"max_queued_bytes"`
	ReconnectDelay        time.Duration `yaml:"reconnect_delay"`
}

type ProjectionLimits struct {
	MaxDefinitionDepth      int `yaml:"max_definition_depth"`
	MaxDefinitionNodes      int `yaml:"max_definition_nodes"`
	MaxActiveInstances      int `yaml:"max_active_instances"`
	MaxConcurrentExpansions int `yaml:"max_concurrent_expansions"`
	MaxPendingExpansions    int `yaml:"max_pending_expansions"`
}

type DependencyConfig struct {
	UpstreamBufferLimit int           `yaml:"upstream_buffer_limit"`
	MaxSubscriptions    int           `yaml:"max_subscriptions"`
	IdleTTL             time.Duration `yaml:"idle_ttl"`
	SweepInterval       time.Duration `yaml:"sweep_interval"`
	EvictionBatchSize   int           `yaml:"eviction_batch_size"`
	CleanupBatchSize    int           `yaml:"cleanup_batch_size"`
}

type RelationLimits struct {
	MaxNodes           int `yaml:"max_nodes"`
	MaxDepth           int `yaml:"max_depth"`
	MaxEvaluationSteps int `yaml:"max_evaluation_steps"`
	MaxExpressionBytes int `yaml:"max_expression_bytes"`
}

func (c *Config) setTuningDefaults() {
	pg := storage.DefaultPostgresOptions()
	c.Storage.Postgres.QueryLimits = PostgresQueryLimits(pg.QueryLimits)
	c.Storage.Postgres.MessageLimits = MessageLimits(pg.MessageLimits)
	c.Storage.Postgres.Replication = ReplicationConfig(pg.Replication)
	c.Storage.Postgres.CaptureBufferLimit = pg.CaptureBufferLimit
	c.Storage.Postgres.MessageLogInterval = pg.MessageLogInterval
	q := queryserver.DefaultOptions()
	c.Query.Limits = QueryLimits(q.Limits)
	c.Query.References = ReferenceConfig(q.References)
	c.Query.Messages = MessageServiceConfig{
		MessageLimits: MessageLimits(q.Messages.MessageLimits), MaxQueuedTransactions: q.Messages.MaxQueuedTransactions,
		MaxQueuedBytes: q.Messages.MaxQueuedBytes, MaxBatchTransactions: q.Messages.MaxBatchTransactions, MaxBatchBytes: q.Messages.MaxBatchBytes,
	}
	p := projectionserver.DefaultOptions()
	c.Projection = ProjectionConfig{
		MessageOptions: ProjectionMessageConfig(p.MessageOptions), MessageProgressInterval: p.MessageProgressInterval,
		MaxRequestsPerConnection: p.MaxRequestsPerConnection, MaxSubscriptionsPerConnection: p.MaxSubscriptionsPerConnection,
		SubscriptionQueueLimit: p.SubscriptionQueueLimit, OutboundQueueLimit: p.OutboundQueueLimit,
		MaxSelectedFields: p.MaxSelectedFields, MaxPredicateNodes: p.MaxPredicateNodes, MaxPredicateDepth: p.MaxPredicateDepth,
		ProjectionLimits: ProjectionLimits(p.ProjectionLimits), MaxConcurrentProjectionExpansions: p.MaxConcurrentProjectionExpansions,
		Dependencies: DependencyConfig(p.Dependencies),
	}
	a := projectionserver.DefaultRelationOptions()
	c.Relations.Limits = RelationLimits(a.Limits)
	c.Relations.MaxFactRows, c.Relations.MaxFactQueries = a.MaxFactRows, a.MaxFactQueries
	c.Relations.FactLoadConcurrency = a.FactLoadConcurrency
}

// Validate numeric tuning consistently at the config boundary, including fields
// which some library constructors otherwise silently default. Zero is meaningful
// only for progress_barrier_delay; omitted YAML fields retain Default values.
func positiveTuning(path string, value any) error {
	v := reflect.ValueOf(value)
	for i := 0; i < v.NumField(); i++ {
		field := v.Type().Field(i)
		name := path + "." + field.Tag.Get("yaml")
		item := v.Field(i)
		if item.Kind() == reflect.Struct {
			if err := positiveTuning(name, item.Interface()); err != nil {
				return err
			}
			continue
		}
		if item.Kind() != reflect.Int && item.Kind() != reflect.Int64 {
			return fmt.Errorf("unsupported tuning field %s", name)
		}
		if name == "query.limits.progress_barrier_delay" {
			if item.Int() < 0 {
				return fmt.Errorf("%s must be nonnegative", name)
			}
		} else if item.Int() <= 0 {
			return fmt.Errorf("%s must be positive", name)
		}
	}
	return nil
}

func (c Config) validateTuning() error {
	pg := c.Storage.Postgres
	for _, section := range []struct {
		name  string
		value any
	}{
		{"query", c.Query}, {"projection", c.Projection},
		{"relations.limits", c.Relations.Limits},
		{"storage.postgres.query_limits", pg.QueryLimits}, {"storage.postgres.message_limits", pg.MessageLimits},
		{"storage.postgres.replication", pg.Replication},
	} {
		if err := positiveTuning(section.name, section.value); err != nil {
			return err
		}
	}
	if pg.CaptureBufferLimit <= 0 || pg.MessageLogInterval <= 0 {
		return fmt.Errorf("storage.postgres capture_buffer_limit and message_log_interval must be positive")
	}
	if pg.Replication.RetryMaxDelay < pg.Replication.RetryMinDelay {
		return fmt.Errorf("storage.postgres.replication.retry_max_delay must be at least retry_min_delay")
	}
	if err := messagebudget.Limits(pg.MessageLimits).Validate(); err != nil {
		return fmt.Errorf("storage.postgres.message_limits: %w", err)
	}
	m := c.Query.Messages
	if err := messages.ValidateOptions(messages.Options{
		MessageLimits: messages.MessageLimits(m.MessageLimits), MaxQueuedTransactions: m.MaxQueuedTransactions,
		MaxQueuedBytes: m.MaxQueuedBytes, MaxBatchTransactions: m.MaxBatchTransactions, MaxBatchBytes: m.MaxBatchBytes,
	}); err != nil {
		return fmt.Errorf("query.messages: %w", err)
	}
	if c.Relations.MaxFactRows <= 0 || c.Relations.MaxFactQueries <= 0 || c.Relations.FactLoadConcurrency <= 0 {
		return fmt.Errorf("relations fact limits and concurrency must be positive")
	}
	if c.Relations.File != "" && c.Relations.MaxFactRows >= c.Query.Limits.MaxWindowRows {
		return fmt.Errorf("relations.max_fact_rows + 1 must fit within query.limits.max_window_rows")
	}
	return nil
}
