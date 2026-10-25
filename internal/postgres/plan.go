package postgres

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/adiom-data/weave/internal/postgres/cdc"
	"github.com/adiom-data/weave/internal/storage"
)

type column struct {
	name      string
	typeOID   uint32
	valueType storage.ValueType
	supported bool
	primary   bool
	collation string
}

type primaryColumn struct {
	name     string
	position int
}

func orderedPrimaryNames(columns []primaryColumn) ([]string, error) {
	ordered := append([]primaryColumn(nil), columns...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].position < ordered[j].position })
	result := make([]string, len(ordered))
	for index, column := range ordered {
		if column.position != index+1 {
			return nil, errors.New("invalid primary-key metadata")
		}
		result[index] = column.name
	}
	return result, nil
}

type boundaryKind uint8

const (
	boundaryStart boundaryKind = iota
	boundaryCursor
	boundaryEnd
)

type boundary struct {
	kind   boundaryKind
	values []*string
}

type plan struct {
	adapter           *postgresTypeAdapter
	query             storage.QuerySpec
	schema            string
	table             string
	relationID        uint32
	columns           map[string]column
	needed            []string
	primary           []string
	order             []storage.OrderTerm
	fingerprint       string
	schemaFingerprint string
	replicaIdentity   byte
	comparisons       map[*storage.Comparison]postgresComparison
}

type materializedRow struct {
	values cdc.Row
	keys   []*string
}

func cloneQuerySpec(query storage.QuerySpec) storage.QuerySpec {
	cloned := query
	cloned.Order = append([]storage.OrderTerm(nil), query.Order...)
	cloned.Filter = clonePredicate(query.Filter)
	return cloned
}

func clonePredicate(predicate *storage.Predicate) *storage.Predicate {
	if predicate == nil {
		return nil
	}
	cloned := &storage.Predicate{}
	if predicate.Comparison != nil {
		comparison := *predicate.Comparison
		comparison.Value.Data = append([]byte(nil), predicate.Comparison.Value.Data...)
		cloned.Comparison = &comparison
	}
	if predicate.Null != nil {
		nullTest := *predicate.Null
		cloned.Null = &nullTest
	}
	if predicate.Junction != nil {
		junction := &storage.Junction{Operator: predicate.Junction.Operator}
		for _, operand := range predicate.Junction.Operands {
			junction.Operands = append(junction.Operands, clonePredicate(operand))
		}
		cloned.Junction = junction
	}
	return cloned
}

// resolveQueryOrder gives an unordered request the relation's stable identity
// order. The resolved form is stored on the prepared query so fingerprinting,
// routing, SQL, cursors, and CDC evaluation all use exactly the same order.
func resolveQueryOrder(query *storage.QuerySpec, primary []string) []storage.OrderTerm {
	order := append([]storage.OrderTerm(nil), query.Order...)
	if len(order) == 0 {
		order = make([]storage.OrderTerm, 0, len(primary))
		for _, name := range primary {
			order = append(order, storage.OrderTerm{Field: storage.FieldPath(name)})
		}
	}
	query.Order = append([]storage.OrderTerm(nil), order...)
	return order
}

// compileCompletePlan compiles the storage-neutral query model into native
// PostgreSQL comparison, cursor, and SQL semantics. Cached entities always
// contain every relation column, independently of the client's selection.
func compileCompletePlan(ctx context.Context, db *pgxpool.Pool, adapter *postgresTypeAdapter, query storage.QuerySpec, allow map[string]struct{}, limits QueryLimits) (*plan, error) {
	query = cloneQuerySpec(query)
	if query.Relation.Namespace == "" || query.Relation.Name == "" {
		return nil, errors.New("schema and table are required")
	}
	qualified := query.Relation.Namespace + "." + query.Relation.Name
	if _, ok := allow[qualified]; len(allow) > 0 && !ok {
		return nil, fmt.Errorf("table %s is not permitted", qualified)
	}
	rows, err := db.Query(ctx, `
		SELECT c.oid, c.relreplident::text, a.attname, a.atttypid,
		       COALESCE((
		           SELECT key.ordinality::integer
		           FROM pg_index i
		           CROSS JOIN LATERAL unnest(i.indkey) WITH ORDINALITY AS key(attnum, ordinality)
		           WHERE i.indrelid = c.oid AND i.indisprimary
		             AND key.ordinality <= i.indnkeyatts::bigint
		             AND key.attnum = a.attnum
		       ), 0) AS primary_position,
		       COALESCE(coll.collname, '') AS collation
		FROM pg_class c
		JOIN pg_namespace n ON n.oid = c.relnamespace
		JOIN pg_attribute a ON a.attrelid = c.oid
		LEFT JOIN pg_collation coll ON coll.oid = a.attcollation
		WHERE n.nspname = $1 AND c.relname = $2
		  AND c.relkind IN ('r', 'p') AND a.attnum > 0 AND NOT a.attisdropped
		ORDER BY a.attnum`, query.Relation.Namespace, query.Relation.Name)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	p := &plan{adapter: adapter, query: query, schema: query.Relation.Namespace, table: query.Relation.Name, columns: make(map[string]column), comparisons: make(map[*storage.Comparison]postgresComparison)}
	var allColumns []string
	var primaryColumns []primaryColumn
	for rows.Next() {
		var c column
		var replicaIdentity string
		var primaryPosition int
		if err := rows.Scan(&p.relationID, &replicaIdentity, &c.name, &c.typeOID, &primaryPosition, &c.collation); err != nil {
			return nil, err
		}
		c.primary = primaryPosition > 0
		if len(replicaIdentity) == 1 {
			p.replicaIdentity = replicaIdentity[0]
		}
		c.valueType, c.supported = p.adapter.logicalType(c.typeOID)
		p.columns[c.name] = c
		if c.supported {
			allColumns = append(allColumns, c.name)
		}
		if c.primary {
			mapping, _ := postgresMapping(c.typeOID)
			if !c.supported || mapping.dynamic {
				return nil, fmt.Errorf("primary-key column %q uses unsupported PostgreSQL type %d", c.name, c.typeOID)
			}
			primaryColumns = append(primaryColumns, primaryColumn{name: c.name, position: primaryPosition})
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(p.columns) == 0 {
		return nil, errors.New("table does not exist")
	}
	p.primary, err = orderedPrimaryNames(primaryColumns)
	if err != nil {
		return nil, err
	}
	if len(p.primary) == 0 {
		return nil, errors.New("table must have a primary key")
	}
	if p.replicaIdentity != 'd' {
		return nil, errors.New("table must use the primary key as REPLICA IDENTITY DEFAULT")
	}
	p.order = resolveQueryOrder(&p.query, p.primary)
	if len(p.order) > limits.MaxOrderTerms {
		return nil, fmt.Errorf("resolved ordering must contain at most %d terms", limits.MaxOrderTerms)
	}
	ordered := make(map[string]struct{}, len(p.order))
	for _, term := range p.order {
		name := term.Field.Root()
		if err := p.requirePath(term.Field); err != nil {
			return nil, err
		}
		if len(term.Field.Segments()) != 1 {
			return nil, fmt.Errorf("nested ordering path %s is not supported yet", term.Field.Display())
		}
		if _, duplicate := ordered[name]; duplicate {
			return nil, fmt.Errorf("ordering column %q is duplicated", name)
		}
		ordered[name] = struct{}{}
	}
	predicateColumns := make(map[string]struct{})
	if err := p.validatePredicate(p.query.Filter, predicateColumns, limits); err != nil {
		return nil, err
	}
	if err := p.validateLocalComparisons(p.query.Filter); err != nil {
		return nil, err
	}
	if err := p.canonicalizePredicateValues(p.query.Filter); err != nil {
		return nil, err
	}
	if err := p.preparePredicateComparisons(p.query.Filter); err != nil {
		return nil, err
	}
	for _, term := range p.order {
		name := term.Field.Root()
		if !locallyComparable(p.columns[name]) {
			return nil, fmt.Errorf("ordering column %q uses PostgreSQL type %d or collation %q whose ordering cannot be reproduced safely", name, p.columns[name].typeOID, p.columns[name].collation)
		}
	}
	// Every source read materializes the complete row. Output shaping belongs to
	// the projection/client layer and cannot affect shared query state.
	p.needed = append([]string(nil), allColumns...)
	sort.Strings(p.needed)
	p.schemaFingerprint, err = relationSchemaFingerprint(p.columns)
	if err != nil {
		return nil, fmt.Errorf("fingerprint relation schema: %w", err)
	}
	p.fingerprint, err = queryFingerprint(p.query, p.relationID, p.schemaFingerprint)
	if err != nil {
		return nil, fmt.Errorf("fingerprint query: %w", err)
	}
	return p, nil
}
