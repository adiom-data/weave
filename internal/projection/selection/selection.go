// Package selection implements stored-name output masks, independent of query
// evaluation and transport. Selections are immutable; Union counts references
// so overlapping selections can be added and removed without losing coverage.
package selection

import (
	"maps"

	"github.com/adiom-data/weave/internal/storage"
)

type node struct {
	whole    int
	fields   map[string]*node
	elements *node
}

// A nil Selection selects the whole value. Constructors own their trees.
type Selection struct{ root *node }
type Field struct {
	Path  storage.FieldPath
	Value *Selection
}

func Paths(paths []storage.FieldPath) *Selection {
	if len(paths) == 0 {
		return nil
	}
	fields := make([]Field, len(paths))
	for i, path := range paths {
		fields[i].Path = path
	}
	return Object(fields)
}

func Object(fields []Field) *Selection {
	root := &node{fields: make(map[string]*node)}
	for _, field := range fields {
		current := root
		for _, name := range field.Path.Segments() {
			if current.fields == nil {
				current.fields = make(map[string]*node)
			}
			if current.fields[name] == nil {
				current.fields[name] = &node{}
			}
			current = current.fields[name]
		}
		merge(current, tree(field.Value), 1)
	}
	return &Selection{root: root}
}

func Elements(element *Selection) *Selection {
	root := &node{elements: &node{}}
	merge(root.elements, tree(element), 1)
	return &Selection{root: root}
}

func tree(s *Selection) *node {
	if s == nil {
		return &node{whole: 1}
	}
	return s.root
}

// Names returns the top-level output names. Nil means all names are selected.
func (s *Selection) Names() map[string]struct{} {
	if s == nil || s.root.whole > 0 {
		return nil
	}
	names := make(map[string]struct{}, len(s.root.fields))
	for name := range s.root.fields {
		names[name] = struct{}{}
	}
	return names
}

// View creates private container scaffolding while borrowing selected leaves.
// Callers must not mutate borrowed values; publication owns them by cloning.
func (s *Selection) View(record storage.Record) storage.Record {
	return objectView(record, tree(s))
}

type Union struct{ root node }

func (u *Union) Add(s *Selection, delta int)               { merge(&u.root, tree(s), delta) }
func (u *Union) View(record storage.Record) storage.Record { return objectView(record, &u.root) }

func merge(dst, src *node, delta int) {
	dst.whole += src.whole * delta
	if dst.whole < 0 {
		panic("selection reference underflow")
	}
	for name, child := range src.fields {
		if dst.fields == nil {
			dst.fields = make(map[string]*node)
		}
		if dst.fields[name] == nil {
			dst.fields[name] = &node{}
		}
		merge(dst.fields[name], child, delta)
		if empty(dst.fields[name]) {
			delete(dst.fields, name)
		}
	}
	if src.elements != nil {
		if dst.elements == nil {
			dst.elements = &node{}
		}
		merge(dst.elements, src.elements, delta)
		if empty(dst.elements) {
			dst.elements = nil
		}
	}
}

func empty(n *node) bool { return n.whole == 0 && len(n.fields) == 0 && n.elements == nil }

func objectView(record storage.Record, n *node) storage.Record {
	if n.whole > 0 {
		return maps.Clone(record)
	}
	result := make(storage.Record, len(n.fields))
	for name, selected := range n.fields {
		if value, ok := record[name]; ok {
			result[name] = valueView(value, selected)
		}
	}
	return result
}

func valueView(value storage.Value, n *node) storage.Value {
	if n.whole > 0 || value.State != storage.ValuePresent {
		return value
	}
	switch value.Type {
	case storage.TypeObject:
		value.Object = objectView(value.Object, n)
	case storage.TypeList:
		if len(value.List) == 0 {
			return value
		}
		items := make([]storage.Value, len(value.List))
		for i, item := range value.List {
			if n.elements != nil {
				items[i] = valueView(item, n.elements)
			} else {
				// An object-only selection cannot traverse a list. Preserve
				// cardinality but never expose its unselected contents.
				items[i] = storage.Value{State: storage.ValueNull}
			}
		}
		value.List = items
	}
	return value
}
