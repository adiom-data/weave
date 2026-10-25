package plan

import (
	"github.com/adiom-data/weave/internal/storage"
	"testing"
)

func TestReferenceHintRequiresOneImmediateIdentityExpansion(t *testing.T) {
	child := &Node{Name: "task", Entity: true, Query: storage.QuerySpec{Relation: storage.RelationName{Name: "tasks"}},
		Bindings: []Binding{{Field: "id", Operator: storage.ComparisonEqual, Parent: "task_id"}}}
	root := &Node{Name: "links", Query: storage.QuerySpec{Relation: storage.RelationName{Name: "links"}}, FetchReference: "task", Children: []*Node{child}}
	hint, err := ReferenceHint(root)
	if err != nil || len(hint.Bindings) != 1 || hint.Bindings[0].Source != "task_id" || hint.Bindings[0].Target != "id" {
		t.Fatalf("hint=%+v err=%v", hint, err)
	}
	if _, err := Clone(root, 10, 10); err != nil {
		t.Fatal(err)
	}
	for _, modify := range []func(){
		func() { root.Entity = true },
		func() { root.FetchReference = "missing" },
		func() { child.Entity = false },
		func() { child.Bindings[0].Operator = storage.ComparisonLess },
		func() { child.Bindings[0].Parent = ""; child.Bindings[0].Parameter = "id" },
	} {
		copy, _ := Clone(root, 10, 10)
		modify()
		if _, err := ReferenceHint(root); err == nil {
			t.Fatal("unsupported hint accepted")
		}
		root = copy
		child = root.Children[0]
	}
}

func TestBindingsPreserveTypesAndDoNotBroadenMissingInputs(t *testing.T) {
	uuid := storage.Value{Type: storage.TypeUUID, State: storage.ValuePresent, Data: []byte("00000000-0000-0000-0000-000000000001")}
	n := &Node{Name: "grants", Query: storage.QuerySpec{Relation: storage.RelationName{Name: "grants"}}, Bindings: []Binding{{Field: "project_id", Operator: storage.ComparisonEqual, Parent: "id"}}}
	copy, err := Clone(n, 10, 10)
	if err != nil {
		t.Fatal(err)
	}
	query, match, err := Bind(copy, nil, storage.Record{"id": uuid})
	if err != nil || !match || query.Filter.Comparison.Value.Type != storage.TypeUUID {
		t.Fatalf("lost canonical type: %+v %v", query, err)
	}
	if _, match, err := Bind(copy, nil, nil); err != nil || match {
		t.Fatal("missing input broadened selector")
	}
	n.Bindings[0].Field = "other"
	if copy.Bindings[0].Field != "project_id" {
		t.Fatal("definition was not privately copied")
	}
	copy.Bindings[0].Parameter = "extra"
	if _, err := Clone(copy, 10, 10); err == nil {
		t.Fatal("ambiguous binding accepted")
	}
	n.Children = []*Node{n}
	if _, err := Clone(n, 10, 10); err == nil {
		t.Fatal("cyclic projection accepted")
	}
}
