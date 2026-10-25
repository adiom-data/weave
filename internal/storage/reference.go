package storage

import "errors"

// ReferenceFetch expands each row of one query window to zero or one target,
// using bindings that cover the complete target primary key. This is not a
// cross-parent batching or fan-out operation. Repeated targets are allowed.
// It does not alter base membership, ordering, cursors, or fingerprint. Query
// is the referenced entity selector before applying parent-field bindings.
type ReferenceFetch struct {
	Query    QuerySpec
	Bindings []ReferenceBinding
}

type ReferenceBinding struct {
	Source FieldPath
	Target FieldPath
}

func (r *ReferenceFetch) Validate() error {
	if r == nil {
		return nil
	}
	if r.Query.Relation.Name == "" || len(r.Bindings) == 0 {
		return errors.New("reference fetch requires a target query and identity bindings")
	}
	seen := map[FieldPath]bool{}
	for _, binding := range r.Bindings {
		if err := binding.Source.Validate(); err != nil {
			return err
		}
		if err := binding.Target.Validate(); err != nil {
			return err
		}
		if seen[binding.Target] {
			return errors.New("reference target binding is duplicated")
		}
		seen[binding.Target] = true
	}
	return nil
}

// BindReference has the same conjunction shape as ordinary projection binding.
// Keeping this shared prevents fetch hints from generating different selectors
// (and therefore different query-cache entries) than subsequent subscriptions.
func BindReference(fetch ReferenceFetch, parent Record) (QuerySpec, Record, bool, error) {
	query := fetch.Query
	identity := Record{}
	for _, binding := range fetch.Bindings {
		value, found := Lookup(parent, binding.Source)
		if !found || value.State == ValueNull || value.State == ValueMissing {
			return query, nil, false, nil
		}
		literal, err := LiteralFromValue(value)
		if err != nil {
			return QuerySpec{}, nil, false, err
		}
		predicate := &Predicate{Comparison: &Comparison{Field: binding.Target, Operator: ComparisonEqual, Value: literal}}
		if query.Filter == nil {
			query.Filter = predicate
		} else {
			query.Filter = &Predicate{Junction: &Junction{Operator: JunctionAnd, Operands: []*Predicate{query.Filter, predicate}}}
		}
		identity[binding.Target.Root()] = value
	}
	return query, identity, true, nil
}
