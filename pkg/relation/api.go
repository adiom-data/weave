// Package relation exposes server-computed projection values, visibility expressions,
// private fact projections, and atomically replaceable definition stores.
package relation

import (
	"github.com/adiom-data/weave/internal/projection/plan"
	impl "github.com/adiom-data/weave/internal/relation"
)

type Definitions = impl.Definitions
type Relation = impl.Relation
type Computed = impl.Computed
type Propagation = impl.Propagation
type Fact = impl.Fact
type Incoming = impl.Incoming
type Limits = impl.Limits
type Store = impl.Store
type Projection = plan.Node
type Binding = plan.Binding

func DefaultLimits() Limits { return impl.DefaultLimits() }
func NewStore(definitions Definitions, limits Limits) (*Store, error) {
	return impl.NewStore(definitions, limits)
}
