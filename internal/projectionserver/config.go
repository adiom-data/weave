package projectionserver

import (
	"time"

	"github.com/adiom-data/weave/internal/accesscontrol/policy"
	proj "github.com/adiom-data/weave/internal/projection"
	"github.com/adiom-data/weave/internal/projection/dependency"
)

// DefaultPolicyOptions returns operational defaults without selecting policies.
// Fact reads request one extra row to detect overflow, so MaxFactRows must be
// smaller than the query backend's maximum initial window count.
func DefaultPolicyOptions() PolicyOptions {
	return PolicyOptions{
		Limits: policy.DefaultLimits(), Cache: policy.DefaultCacheOptions(),
		MaxFactQueries: 1000, MaxFactRows: 999, FactLoadConcurrency: 8,
	}
}

func (o PolicyOptions) withDefaults() PolicyOptions {
	d := DefaultPolicyOptions()
	if o.MaxFactQueries == 0 {
		o.MaxFactQueries = d.MaxFactQueries
	}
	if o.MaxFactRows == 0 {
		o.MaxFactRows = d.MaxFactRows
	}
	if o.FactLoadConcurrency == 0 {
		o.FactLoadConcurrency = d.FactLoadConcurrency
	}
	return o
}

// DefaultOptions returns a fresh, application-customizable base configuration.
// It does not enable policy evaluation or transaction messages: their providers
// remain explicit application choices.
func DefaultOptions() Options {
	return Options{
		MaxRequestsPerConnection: 65536, MaxSubscriptionsPerConnection: 100,
		SubscriptionQueueLimit: 256, OutboundQueueLimit: 256,
		MaxSelectedFields: 100, MaxPredicateNodes: 64, MaxPredicateDepth: 8,
		ProjectionLimits: ProjectionLimits(proj.DefaultLimits()), MaxConcurrentProjectionExpansions: 32,
		Dependencies:            DependencyOptions(dependency.DefaultOptions()),
		MessageOptions:          MessageOptions{MaxQueuedTransactions: 256, MaxQueuedBytes: 8 << 20, ReconnectDelay: 100 * time.Millisecond},
		MessageProgressInterval: 10 * time.Millisecond,
	}
}

func (o Options) withDefaults() Options {
	d := DefaultOptions()
	if o.Policies != nil {
		configuration := o.Policies.withDefaults()
		o.Policies = &configuration
	}
	if o.MessageProgressInterval <= 0 {
		o.MessageProgressInterval = d.MessageProgressInterval
	}
	if o.MaxRequestsPerConnection <= 0 {
		o.MaxRequestsPerConnection = d.MaxRequestsPerConnection
	}
	if o.MaxSubscriptionsPerConnection <= 0 {
		o.MaxSubscriptionsPerConnection = d.MaxSubscriptionsPerConnection
	}
	if o.SubscriptionQueueLimit <= 0 {
		o.SubscriptionQueueLimit = d.SubscriptionQueueLimit
	}
	if o.OutboundQueueLimit <= 0 {
		o.OutboundQueueLimit = d.OutboundQueueLimit
	}
	if o.MaxSelectedFields <= 0 {
		o.MaxSelectedFields = d.MaxSelectedFields
	}
	if o.MaxPredicateNodes <= 0 {
		o.MaxPredicateNodes = d.MaxPredicateNodes
	}
	if o.MaxPredicateDepth <= 0 {
		o.MaxPredicateDepth = d.MaxPredicateDepth
	}
	o.ProjectionLimits = ProjectionLimits(proj.Limits(o.ProjectionLimits).WithDefaults())
	if o.MaxConcurrentProjectionExpansions <= 0 {
		o.MaxConcurrentProjectionExpansions = d.MaxConcurrentProjectionExpansions
	}
	if o.Dependencies.UpstreamBufferLimit == 0 {
		o.Dependencies.UpstreamBufferLimit = d.Dependencies.UpstreamBufferLimit
	}
	if o.Dependencies.MaxSubscriptions == 0 {
		o.Dependencies.MaxSubscriptions = d.Dependencies.MaxSubscriptions
	}
	if o.Dependencies.IdleTTL == 0 {
		o.Dependencies.IdleTTL = d.Dependencies.IdleTTL
	}
	if o.Dependencies.SweepInterval == 0 {
		o.Dependencies.SweepInterval = d.Dependencies.SweepInterval
	}
	o.Dependencies = DependencyOptions(dependency.Options(o.Dependencies).WithDefaults())
	if o.MessageOptions.MaxQueuedTransactions == 0 {
		o.MessageOptions.MaxQueuedTransactions = d.MessageOptions.MaxQueuedTransactions
	}
	if o.MessageOptions.MaxQueuedBytes == 0 {
		o.MessageOptions.MaxQueuedBytes = d.MessageOptions.MaxQueuedBytes
	}
	if o.MessageOptions.ReconnectDelay == 0 {
		o.MessageOptions.ReconnectDelay = d.MessageOptions.ReconnectDelay
	}
	return o
}
