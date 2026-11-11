package engine

import "time"

// DefaultCoordinatorLimits returns a fresh base configuration. Progress delay
// is intentionally zero: callers may request sparse-stream progress immediately.
func DefaultCoordinatorLimits() CoordinatorLimits {
	return CoordinatorLimits{
		EntityIdleTTL: 5 * time.Minute, EvictionPeriod: time.Minute,
		EvictionBatchSize: 128, QueryIdleTTL: 30 * time.Second, QueryMaintenancePeriod: time.Second,
		MaxResyncTransactions: 4096, MaxResyncChanges: 100000, MaxHydrations: 1024,
		ResyncResultBuffer: 16, HydrationResultBuffer: 64,
		MaxWindowRows: 1000, SnapshotPageRows: 1000,
		SubscriptionBatchSize:     64,
		MaxReferenceInvalidations: 100000,
	}
}

func (o CoordinatorLimits) withDefaults() CoordinatorLimits {
	d := DefaultCoordinatorLimits()
	if o.SubscriptionBatchSize == 0 {
		o.SubscriptionBatchSize = d.SubscriptionBatchSize
	}
	if o.MaxReferenceInvalidations == 0 {
		o.MaxReferenceInvalidations = d.MaxReferenceInvalidations
	}
	if o.EntityIdleTTL == 0 {
		o.EntityIdleTTL = d.EntityIdleTTL
	}
	if o.EvictionPeriod == 0 {
		o.EvictionPeriod = d.EvictionPeriod
	}
	if o.EvictionBatchSize == 0 {
		o.EvictionBatchSize = d.EvictionBatchSize
	}
	if o.QueryIdleTTL == 0 {
		o.QueryIdleTTL = d.QueryIdleTTL
	}
	if o.QueryMaintenancePeriod == 0 {
		o.QueryMaintenancePeriod = d.QueryMaintenancePeriod
	}
	if o.MaxResyncTransactions == 0 {
		o.MaxResyncTransactions = d.MaxResyncTransactions
	}
	if o.MaxResyncChanges == 0 {
		o.MaxResyncChanges = d.MaxResyncChanges
	}
	if o.MaxHydrations == 0 {
		o.MaxHydrations = d.MaxHydrations
	}
	if o.ResyncResultBuffer == 0 {
		o.ResyncResultBuffer = d.ResyncResultBuffer
	}
	if o.HydrationResultBuffer == 0 {
		o.HydrationResultBuffer = d.HydrationResultBuffer
	}
	if o.MaxWindowRows == 0 {
		o.MaxWindowRows = d.MaxWindowRows
	}
	if o.SnapshotPageRows == 0 {
		o.SnapshotPageRows = d.SnapshotPageRows
	}
	return o
}
