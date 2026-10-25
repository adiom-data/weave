package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/adiom-data/weave/internal/datastore"
	"github.com/adiom-data/weave/internal/postgres/cdc"
	"github.com/adiom-data/weave/internal/storage"
)

type preparedReference struct {
	target *postgresPrepared
	shape  storage.ReferenceFetch
	limits QueryLimits
}

type referenceLookup struct {
	target      *postgresPrepared
	key         storage.EntityKey
	predicates  []storage.Predicate
	comparisons []postgresComparison
	unfiltered  bool
}

func (l *referenceLookup) Key() storage.EntityKey   { return l.key }
func (l *referenceLookup) UnfilteredIdentity() bool { return l.unfiltered }
func (l *referenceLookup) Matches(record storage.Record) (bool, error) {
	matched, err := l.target.Matches(record)
	if err != nil || !matched {
		return matched, err
	}
	for i := range l.predicates {
		matched, err = storage.EvaluatePredicate(&l.predicates[i], record)
		if err != nil || !matched {
			return matched, err
		}
	}
	return true, nil
}

func (p *preparedReference) Resolve(parent storage.Record) (datastore.ReferenceLookup, bool, error) {
	lookup := &referenceLookup{target: p.target, predicates: make([]storage.Predicate, 0, len(p.shape.Bindings)), unfiltered: p.shape.Query.Filter == nil && len(p.shape.Bindings) == len(p.target.plan.primary)}
	identity := storage.Record{}
	for _, binding := range p.shape.Bindings {
		value, found := storage.Lookup(parent, binding.Source)
		if !found || value.State == storage.ValueNull || value.State == storage.ValueMissing {
			return nil, false, nil
		}
		literal, err := storage.LiteralFromValue(value)
		if err != nil {
			return nil, false, err
		}
		if len(literal.Data) > p.limits.withDefaults().MaxPredicateValueBytes {
			return nil, false, errors.New("reference binding exceeds predicate value limit")
		}
		comparison := &storage.Comparison{Field: binding.Target, Operator: storage.ComparisonEqual, Value: literal}
		predicate := storage.Predicate{Comparison: comparison}
		if err := p.target.plan.canonicalizePredicateValues(&predicate); err != nil {
			return nil, false, err
		}
		compiled, err := p.target.plan.adapter.prepareComparison(p.target.plan.columns[binding.Target.Root()], comparison)
		if err != nil {
			return nil, false, err
		}
		identity[binding.Target.Root()] = comparison.Value.Value()
		column := p.target.plan.columns[binding.Target.Root()]
		lookup.unfiltered = lookup.unfiltered && column.primary && comparison.Value.Type == column.valueType
		lookup.predicates = append(lookup.predicates, predicate)
		lookup.comparisons = append(lookup.comparisons, compiled)
	}
	key, err := p.target.EntityKey(identity)
	if err != nil {
		return nil, false, err
	}
	lookup.key = key
	return lookup, true, nil
}

func (s *PostgresDataSource) PrepareReference(ctx context.Context, base datastore.PreparedQuery, shape storage.ReferenceFetch) (datastore.PreparedReference, error) {
	if err := shape.Validate(); err != nil {
		return nil, err
	}
	parent, ok := base.(*postgresPrepared)
	if !ok || parent == nil {
		return nil, errors.New("reference base requires a PostgreSQL prepared query")
	}
	target, err := s.PrepareQuery(ctx, shape.Query)
	if err != nil {
		return nil, err
	}
	prepared := target.(*postgresPrepared)
	// Validate the bound predicate's structure once, without inventing values
	// or compiling one query per parent. Actual literal sizes are checked by
	// Resolve and the normal adapter conversion validates their scalar values.
	validation := shape.Query.Filter
	for _, binding := range shape.Bindings {
		leaf := &storage.Predicate{Comparison: &storage.Comparison{Field: binding.Target, Operator: storage.ComparisonEqual}}
		if validation == nil {
			validation = leaf
		} else {
			validation = &storage.Predicate{Junction: &storage.Junction{Operator: storage.JunctionAnd, Operands: []*storage.Predicate{validation, leaf}}}
		}
	}
	if err := prepared.plan.validatePredicate(validation, map[string]struct{}{}, s.queryLimits.withDefaults()); err != nil {
		return nil, err
	}
	_, _, err = referenceWindowSQL(parent.plan, prepared.plan, datastore.WindowRequest{
		Limit: 1, Bounds: datastore.Bounds{Left: datastore.Boundary{Kind: datastore.BoundaryStart}, Right: datastore.Boundary{Kind: datastore.BoundaryEnd}},
	}, shape.Bindings)
	if err != nil {
		return nil, err
	}
	shape.Bindings = append([]storage.ReferenceBinding(nil), shape.Bindings...)
	shape.Query = cloneQuerySpec(shape.Query)
	return &preparedReference{target: prepared, shape: shape, limits: s.queryLimits}, nil
}

func (p *preparedReference) Fetch() datastore.ReferenceFetch {
	return datastore.ReferenceFetch{Target: p.target, Bindings: append([]datastore.ReferenceBinding(nil), p.shape.Bindings...)}
}

func (p *preparedReference) Bind(parent storage.Record) (datastore.PreparedQuery, storage.QuerySpec, storage.EntityKey, bool, error) {
	spec, identity, matchable, err := storage.BindReference(p.shape, parent)
	if err != nil || !matchable {
		return nil, spec, storage.EntityKey{}, matchable, err
	}
	key, err := p.target.EntityKey(identity)
	if err != nil {
		return nil, spec, storage.EntityKey{}, false, err
	}
	bound, err := p.target.plan.bindSelector(spec, p.limits)
	return &postgresPrepared{plan: bound}, cloneQuerySpec(spec), key, true, err
}

// bindSelector reuses immutable relation metadata, not prepared comparison
// pointers. Its literal normalization and fingerprinting are the same as an
// ordinary PrepareQuery, without one catalog query per referenced entity.
func (p *plan) bindSelector(spec storage.QuerySpec, limits QueryLimits) (*plan, error) {
	bound := *p
	bound.query = cloneQuerySpec(spec)
	bound.order = resolveQueryOrder(&bound.query, bound.primary)
	bound.comparisons = make(map[*storage.Comparison]postgresComparison)
	if err := bound.validatePredicate(bound.query.Filter, map[string]struct{}{}, limits.withDefaults()); err != nil {
		return nil, err
	}
	if err := bound.validateLocalComparisons(bound.query.Filter); err != nil {
		return nil, err
	}
	if err := bound.canonicalizePredicateValues(bound.query.Filter); err != nil {
		return nil, err
	}
	if err := bound.preparePredicateComparisons(bound.query.Filter); err != nil {
		return nil, err
	}
	// Relation metadata is immutable for the lifetime of a prepared plan. Only
	// the selector changes when binding another referenced entity.
	var err error
	bound.fingerprint, err = queryFingerprint(bound.query, bound.relationID, bound.schemaFingerprint)
	return &bound, err
}

// referenceWindowSQL joins one bounded association window to a complete target
// identity. Neither target absence nor a target predicate changes list coverage.
func referenceWindowSQL(base, target *plan, request datastore.WindowRequest, bindings []datastore.ReferenceBinding) (string, []any, error) {
	if base == nil || target == nil || request.Limit < 1 {
		return "", nil, errors.New("reference fetch requires prepared queries and a positive window limit")
	}
	if len(target.primary) == 0 || len(bindings) < len(target.primary) {
		return "", nil, errors.New("reference bindings must cover the complete target primary key")
	}
	primary := make(map[string]bool, len(target.primary))
	for _, field := range target.primary {
		primary[field] = true
	}
	joins := make([]string, 0, len(bindings))
	seen := make(map[string]bool, len(bindings))
	for _, binding := range bindings {
		if err := binding.Source.Validate(); err != nil {
			return "", nil, err
		}
		if err := binding.Target.Validate(); err != nil {
			return "", nil, err
		}
		if len(binding.Source.Segments()) != 1 || len(binding.Target.Segments()) != 1 {
			return "", nil, errors.New("PostgreSQL reference bindings require top-level columns")
		}
		from, to := binding.Source.Root(), binding.Target.Root()
		if _, ok := base.columns[from]; !ok {
			return "", nil, fmt.Errorf("unknown reference source column %q", from)
		}
		if err := target.requireColumn(to); err != nil {
			return "", nil, err
		}
		if err := target.validateLocalComparisons(&storage.Predicate{Comparison: &storage.Comparison{Field: binding.Target, Operator: storage.ComparisonEqual}}); err != nil {
			return "", nil, err
		}
		if seen[to] {
			return "", nil, fmt.Errorf("reference target column %q is bound more than once", to)
		}
		seen[to] = true
		delete(primary, to)
		// Do not introduce implicit SQL casts with semantics different from the
		// canonical type adapter. More general conversions can be added later.
		if base.columns[from].typeOID != target.columns[to].typeOID {
			return "", nil, errors.New("reference columns must have the same PostgreSQL type")
		}
		joins = append(joins, `"weave_base".`+quote(from)+` = "weave_target".`+quote(to))
	}
	if len(primary) != 0 {
		return "", nil, errors.New("reference bindings must cover the complete target primary key")
	}
	left, err := postgresBoundary(request.Bounds.Left)
	if err != nil {
		return "", nil, err
	}
	right, err := postgresBoundary(request.Bounds.Right)
	if err != nil {
		return "", nil, err
	}
	window, args, err := base.windowSQLProjection(left, right, request.Reverse, request.Limit, "*")
	if err != nil {
		return "", nil, err
	}
	filter, err := target.predicateSQL(target.query.Filter, &args)
	if err != nil {
		return "", nil, err
	}
	selection := make([]string, 0, len(base.needed)+len(target.needed))
	for _, component := range []struct {
		alias  string
		fields []string
	}{{"weave_base", base.needed}, {"weave_target", target.needed}} {
		for _, name := range component.fields {
			selection = append(selection, quote(component.alias)+"."+quote(name)+"::text")
		}
	}
	orders := make([]string, 0, len(base.order))
	for _, term := range base.order {
		direction, nulls := "ASC", "NULLS LAST"
		if term.Descending {
			direction = "DESC"
		}
		if term.NullsFirst {
			nulls = "NULLS FIRST"
		}
		orders = append(orders, `"weave_base".`+quote(term.Field.Root())+" "+direction+" "+nulls)
	}
	return fmt.Sprintf("SELECT %s FROM (%s) AS weave_base LEFT JOIN (SELECT * FROM %s.%s WHERE %s) AS weave_target ON %s ORDER BY %s",
		strings.Join(selection, ", "), window, quote(target.schema), quote(target.table), filter, strings.Join(joins, " AND "), strings.Join(orders, ", ")), args, nil
}

func (s *postgresSnapshot) LoadWindowWithReference(ctx context.Context, query datastore.PreparedQuery, request datastore.WindowRequest, fetch datastore.ReferenceFetch) ([]datastore.ReferenceWindowRow, error) {
	base, ok := query.(*postgresPrepared)
	if !ok || base == nil || base.plan == nil || base.Relation() != s.relation {
		return nil, errors.New("query is outside this relation snapshot")
	}
	target, ok := fetch.Target.(*postgresPrepared)
	if !ok || target == nil || target.plan == nil || target.Relation().Source != s.source.ID() {
		return nil, errors.New("reference target requires a same-source PostgreSQL prepared query")
	}
	if s.done {
		return nil, errors.New("snapshot is already closed")
	}
	sql, args, err := referenceWindowSQL(base.plan, target.plan, request, fetch.Bindings)
	if err != nil {
		return nil, err
	}
	rows, err := s.tx.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]datastore.ReferenceWindowRow, 0)
	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			return nil, err
		}
		baseRow, err := referenceRecord(base.plan, values[:len(base.plan.needed)])
		if err != nil {
			return nil, err
		}
		row := datastore.ReferenceWindowRow{Row: baseRow}
		targetValues := values[len(base.plan.needed):]
		// A real target has a non-null complete primary key. All-null values
		// from the left join denote no matching target, not a null entity.
		present := false
		for _, value := range targetValues {
			present = present || value != nil
		}
		if present {
			record, err := referenceRecord(target.plan, targetValues)
			if err != nil {
				return nil, err
			}
			row.Reference = &record
		}
		result = append(result, row)
	}
	return result, rows.Err()
}

func referenceRecord(p *plan, values []any) (datastore.AuthoritativeRecord, error) {
	if len(values) != len(p.needed) {
		return datastore.AuthoritativeRecord{}, errors.New("reference row has the wrong column count")
	}
	row := make(cdc.Row, len(values))
	for index, name := range p.needed {
		cell := cdc.Cell{TypeOID: p.columns[name].typeOID, Null: values[index] == nil}
		if !cell.Null {
			text, ok := values[index].(string)
			if !ok {
				return datastore.AuthoritativeRecord{}, errors.New("reference column did not decode as text")
			}
			cell.Text = []byte(text)
		}
		row[name] = cell
	}
	if _, err := p.materialize(row); err != nil {
		return datastore.AuthoritativeRecord{}, err
	}
	record, err := postgresRecord(p.adapter, row)
	return datastore.AuthoritativeRecord{Record: record}, err
}

func (s *postgresSnapshot) LoadReferences(ctx context.Context, fetch datastore.ReferenceFetch, parents []storage.Record) ([]datastore.AuthoritativeRecord, error) {
	target, ok := fetch.Target.(*postgresPrepared)
	if !ok || target == nil || s.done {
		return nil, errors.New("reference read requires an open PostgreSQL snapshot and target")
	}
	if _, _, err := referenceWindowSQL(s.plan, target.plan, datastore.WindowRequest{Limit: 1,
		Bounds: datastore.Bounds{Left: datastore.Boundary{Kind: datastore.BoundaryStart}, Right: datastore.Boundary{Kind: datastore.BoundaryEnd}},
	}, fetch.Bindings); err != nil {
		return nil, err
	}
	prepared := &preparedReference{target: target, shape: storage.ReferenceFetch{Query: target.Query(), Bindings: fetch.Bindings}, limits: s.source.queryLimits}
	var predicates []string
	var args []any
	static, err := target.plan.predicateSQL(target.plan.query.Filter, &args)
	if err != nil {
		return nil, err
	}
	// Only parameterized equality leaves vary per parent. Reuse the target's
	// metadata and static predicate; do not fingerprint or compile N queries.
	sqlPlan := *target.plan
	sqlPlan.comparisons = make(map[*storage.Comparison]postgresComparison, len(fetch.Bindings))
	for _, parent := range parents {
		resolved, matchable, err := prepared.Resolve(parent)
		if err != nil {
			return nil, err
		}
		if !matchable {
			continue
		}
		lookup := resolved.(*referenceLookup)
		parts := make([]string, 0, len(lookup.predicates))
		clear(sqlPlan.comparisons)
		for i := range lookup.predicates {
			predicate := &lookup.predicates[i]
			comparison := predicate.Comparison
			sqlPlan.comparisons[comparison] = lookup.comparisons[i]
			part, err := sqlPlan.predicateSQL(predicate, &args)
			if err != nil {
				return nil, err
			}
			parts = append(parts, part)
		}
		predicates = append(predicates, "("+strings.Join(parts, " AND ")+")")
	}
	if len(predicates) == 0 {
		return nil, nil
	}
	fields := make([]string, 0, len(target.plan.needed))
	for _, field := range target.plan.needed {
		fields = append(fields, quote(field)+"::text")
	}
	sql := fmt.Sprintf("SELECT %s FROM %s.%s WHERE (%s) AND (%s)", strings.Join(fields, ", "), quote(target.plan.schema), quote(target.plan.table), static, strings.Join(predicates, " OR "))
	rows, err := s.tx.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []datastore.AuthoritativeRecord
	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			return nil, err
		}
		record, err := referenceRecord(target.plan, values)
		if err != nil {
			return nil, err
		}
		result = append(result, record)
	}
	return result, rows.Err()
}

var _ datastore.ReferenceWindowReader = (*postgresSnapshot)(nil)
var _ datastore.ReferencePreparer = (*PostgresDataSource)(nil)
