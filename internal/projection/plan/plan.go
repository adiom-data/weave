// Package plan owns transport-independent, immutable projection definitions and
// parameter binding. Both public projections and private policy facts use it.
package plan

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/adiom-data/weave/internal/storage"
)

type Node struct {
	Name           string
	Entity         bool
	Query          storage.QuerySpec
	Bindings       []Binding
	Children       []*Node
	FetchReference string
}

// ReferenceHint recognizes only an immediate to-one child whose parent
// equality bindings resolve a complete identity (validated by the datastore).
// Deeper projection expansion and authorization remain above the query layer.
func ReferenceHint(node *Node) (*storage.ReferenceFetch, error) {
	if node.FetchReference == "" {
		return nil, nil
	}
	if node.Entity {
		return nil, errors.New("fetch_reference requires a list node")
	}
	for _, child := range node.Children {
		if child.Name != node.FetchReference {
			continue
		}
		if !child.Entity {
			return nil, errors.New("fetch_reference must name an immediate entity child")
		}
		hint := &storage.ReferenceFetch{Query: child.Query}
		for _, binding := range child.Bindings {
			if binding.Parent == "" || binding.Operator != storage.ComparisonEqual || binding.Parameter != "" || binding.Literal != nil {
				return nil, errors.New("fetch_reference requires only parent-column equality bindings")
			}
			hint.Bindings = append(hint.Bindings, storage.ReferenceBinding{Source: binding.Parent, Target: binding.Field})
		}
		return hint, hint.Validate()
	}
	return nil, errors.New("fetch_reference does not name an immediate entity child")
}

// Exactly one of Literal, Parameter, and Parent supplies the bound value.
// Structured Parameter paths support private facts' item/claims inputs without
// adding application-specific concepts to the projection planner.
type Binding struct {
	Field     storage.FieldPath
	Operator  storage.ComparisonOperator
	Literal   *storage.Value
	Parameter storage.FieldPath
	Parent    storage.FieldPath
}

func Clone(root *Node, maxNodes, maxDepth int) (*Node, error) {
	nodes := 0
	stack := map[*Node]bool{}
	var validate func(*Node, int) error
	validate = func(n *Node, depth int) error {
		nodes++
		if nodes > maxNodes || depth > maxDepth {
			return errors.New("projection definition limit exceeded")
		}
		if n == nil || n.Name == "" || n.Query.Relation.Name == "" || stack[n] {
			return errors.New("invalid or cyclic projection definition")
		}
		stack[n] = true
		defer delete(stack, n)
		for _, b := range n.Bindings {
			if err := b.Field.Validate(); err != nil {
				return err
			}
			if b.Operator < storage.ComparisonEqual || b.Operator > storage.ComparisonGreaterOrEqual {
				return errors.New("invalid binding operator")
			}
			count := 0
			if b.Literal != nil {
				count++
				if _, err := storage.LiteralFromValue(*b.Literal); err != nil {
					return err
				}
			}
			for _, path := range []storage.FieldPath{b.Parameter, b.Parent} {
				if path != "" {
					count++
					if err := path.Validate(); err != nil {
						return err
					}
				}
			}
			if count != 1 {
				return errors.New("binding requires exactly one value source")
			}
		}
		names := map[string]bool{}
		for _, child := range n.Children {
			if child == nil || names[child.Name] {
				return errors.New("duplicate or absent projection child")
			}
			names[child.Name] = true
			if err := validate(child, depth+1); err != nil {
				return err
			}
		}
		if _, err := ReferenceHint(n); err != nil {
			return err
		}
		return nil
	}
	if maxNodes < 1 || maxDepth < 1 {
		return nil, errors.New("positive projection definition limits are required")
	}
	if err := validate(root, 1); err != nil {
		return nil, err
	}
	encoded, err := json.Marshal(root)
	if err != nil {
		return nil, err
	}
	var copy Node
	if err := json.Unmarshal(encoded, &copy); err != nil {
		return nil, err
	}
	return &copy, nil
}

// Bind returns matchable=false for an absent/null input. Missing evidence is
// handled by the caller before binding; it must never broaden a selector.
func Bind(node *Node, parameters, parent storage.Record) (query storage.QuerySpec, matchable bool, err error) {
	query = node.Query
	for _, b := range node.Bindings {
		var value storage.Value
		if b.Literal != nil {
			value = *b.Literal
		} else {
			record, path := parameters, b.Parameter
			if b.Parent != "" {
				record, path = parent, b.Parent
			}
			var found bool
			value, found = storage.Lookup(record, path)
			if !found || value.State == storage.ValueMissing || value.State == storage.ValueNull {
				return query, false, nil
			}
			if value.State != storage.ValuePresent {
				return query, false, errors.New("projection binding has unresolved evidence")
			}
		}
		literal, err := storage.LiteralFromValue(value)
		if err != nil {
			return query, false, fmt.Errorf("binding %s: %w", b.Field.Display(), err)
		}
		predicate := &storage.Predicate{Comparison: &storage.Comparison{Field: b.Field, Operator: b.Operator, Value: literal}}
		if query.Filter == nil {
			query.Filter = predicate
		} else {
			query.Filter = &storage.Predicate{Junction: &storage.Junction{Operator: storage.JunctionAnd, Operands: []*storage.Predicate{query.Filter, predicate}}}
		}
	}
	return query, true, nil
}
