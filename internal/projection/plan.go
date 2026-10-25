package projection

import (
	"fmt"

	"github.com/adiom-data/weave/internal/projection/plan"
	q "github.com/adiom-data/weave/internal/querysubscription/model"
	"github.com/adiom-data/weave/internal/storage"
)

// FromPlan lowers the same data-only definitions used by private policy facts
// into the public projection runtime. The caller supplies an immutable plan.
func FromPlan(root *plan.Node, parameters storage.Record, windows map[string]q.InitialWindow) *ProjectionNode {
	node := &ProjectionNode{Name: root.Name, Entity: root.Entity, Window: windows[root.Name]}
	// Definitions are validated before lowering; retain the data-only fetch
	// hint on the list rather than introducing another projection runtime path.
	node.Reference, _ = plan.ReferenceHint(root)
	node.ReferenceChild = root.FetchReference
	node.BuildQuery = func(parent storage.Record) (storage.QuerySpec, error) {
		query, matchable, err := plan.Bind(root, parameters, parent)
		if err != nil {
			return storage.QuerySpec{}, err
		}
		if !matchable {
			return storage.QuerySpec{}, fmt.Errorf("projection %s has an absent or null binding", root.Name)
		}
		return query, nil
	}
	for _, child := range root.Children {
		node.Children = append(node.Children, FromPlan(child, parameters, windows))
	}
	return node
}
