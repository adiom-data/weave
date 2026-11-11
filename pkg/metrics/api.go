// Package metrics exposes provider-neutral runtime instrumentation contracts.
package metrics

import impl "github.com/adiom-data/weave/internal/metrics"

// Provider registers scalar gauge readers and owns metric export. Contributions
// with the same name are summed. Weave calls it during construction and cleanup,
// never while updating a count. Nil disables runtime metrics.
type Provider = impl.Provider

const (
	ActiveSubscriptions       = impl.ActiveSubscriptions
	InitializingSubscriptions = impl.InitializingSubscriptions
	CachedQueries             = impl.CachedQueries
	CachedEntities            = impl.CachedEntities
	CachedMetadata            = impl.CachedMetadata
	LoadingMetadata           = impl.LoadingMetadata
)
