package projectionserver

import (
	"github.com/adiom-data/weave/internal/projection/plan"
	relationdef "github.com/adiom-data/weave/internal/relation"
	"github.com/adiom-data/weave/internal/storage"
)

func testPolicyFact(id string, relation storage.RelationName, bind bool) relationdef.Fact {
	root := &plan.Node{Name: id, Query: storage.QuerySpec{Relation: relation}}
	if bind {
		root.Bindings = []plan.Binding{{Field: "project_id", Operator: storage.ComparisonEqual, Parameter: storage.MustFieldPath("item", "id")}}
	}
	return relationdef.Fact{ID: id, Projection: root}
}
