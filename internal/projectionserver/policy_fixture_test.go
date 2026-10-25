package projectionserver

import (
	"github.com/adiom-data/weave/internal/accesscontrol/policy"
	"github.com/adiom-data/weave/internal/projection/plan"
	"github.com/adiom-data/weave/internal/storage"
)

func testPolicyFact(id string, relation storage.RelationName, bind bool) policy.Fact {
	root := &plan.Node{Name: id, Query: storage.QuerySpec{Relation: relation}}
	if bind {
		root.Bindings = []plan.Binding{{Field: "project_id", Operator: storage.ComparisonEqual, Parameter: storage.MustFieldPath("item", "id")}}
	}
	return policy.Fact{ID: id, Projection: root}
}
