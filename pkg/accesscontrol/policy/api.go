// Package policy exposes CEL visibility policies, private fact projection
// definitions, and application-owned immutable policy providers.
package policy

import (
	impl "github.com/adiom-data/weave/internal/accesscontrol/policy"
	"github.com/adiom-data/weave/internal/projection/plan"
)

type Definition = impl.Map
type Relation = impl.Relation
type Rule = impl.Rule
type Fact = impl.Fact
type Edge = impl.Edge
type EdgeReference = impl.EdgeReference
type Limits = impl.Limits
type Key = impl.Key
type Snapshot = impl.Snapshot
type Provider = impl.Provider
type ProviderFunc = impl.ProviderFunc
type CacheOptions = impl.CacheOptions
type Projection = plan.Node
type Binding = plan.Binding

func DefaultLimits() Limits             { return impl.DefaultLimits() }
func DefaultCacheOptions() CacheOptions { return impl.DefaultCacheOptions() }
func NewStaticProvider(definitions Definition, limits Limits) (Provider, error) {
	return impl.NewStaticProvider(definitions, limits)
}
