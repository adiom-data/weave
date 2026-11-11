package postgres

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/adiom-data/weave/internal/postgres/cdc"
)

func (p *plan) windowSQL(left, right boundary, reverse bool, limit int) (string, []any, error) {
	return p.windowSQLSelecting(left, right, reverse, limit, p.needed)
}

func (p *plan) windowSQLSelecting(left, right boundary, reverse bool, limit int, fields []string) (string, []any, error) {
	selects := make([]string, 0, len(fields))
	for _, name := range fields {
		selects = append(selects, quote(name)+"::text AS "+quote(name))
	}
	return p.windowSQLProjection(left, right, reverse, limit, strings.Join(selects, ", "))
}

// windowSQLProjection keeps native window selection separate from decoding.
// Reference loads select native values here and cast only after the join, so
// native identity equality and ordering are not replaced by text comparisons.
func (p *plan) windowSQLProjection(left, right boundary, reverse bool, limit int, selection string) (string, []any, error) {
	args := []any{}
	filter, err := p.predicateSQL(p.query.Filter, &args)
	if err != nil {
		return "", nil, err
	}
	clauses := []string{"(" + filter + ")"}
	if left.kind == boundaryEnd || right.kind == boundaryStart {
		clauses = append(clauses, "FALSE")
	} else {
		if left.kind == boundaryCursor {
			after, err := p.afterSQL(left.values, &args)
			if err != nil {
				return "", nil, err
			}
			clauses = append(clauses, "("+after+")")
		}
		if right.kind == boundaryCursor {
			after, err := p.afterSQL(right.values, &args)
			if err != nil {
				return "", nil, err
			}
			clauses = append(clauses, "NOT ("+after+")")
		}
	}
	orders := make([]string, 0, len(p.order))
	for _, term := range p.order {
		desc, nullsFirst := term.Descending, term.NullsFirst
		if reverse {
			desc, nullsFirst = !desc, !nullsFirst
		}
		direction := "ASC"
		if desc {
			direction = "DESC"
		}
		nulls := "NULLS LAST"
		if nullsFirst {
			nulls = "NULLS FIRST"
		}
		// ORDER BY otherwise resolves the selected ::text alias rather than the
		// native column, changing both pagination and ordering (e.g. 1, 10, 2).
		orders = append(orders, quote(p.table)+"."+quote(term.Field.Root())+" "+direction+" "+nulls)
	}
	return fmt.Sprintf("SELECT %s FROM %s.%s WHERE %s ORDER BY %s LIMIT %d", selection, quote(p.schema), quote(p.table), strings.Join(clauses, " AND "), strings.Join(orders, ", "), limit), args, nil
}

func (p *plan) afterSQL(values []*string, args *[]any) (string, error) {
	if len(values) != len(p.order) {
		return "", errors.New("cursor key count does not match ordering")
	}
	prefix := make([]string, 0, len(values))
	terms := make([]string, 0, len(values))
	for index, term := range p.order {
		columnName := quote(term.Field.Root())
		value := values[index]
		var after, equal string
		if value == nil {
			equal = columnName + " IS NULL"
			if term.NullsFirst {
				after = columnName + " IS NOT NULL"
			} else {
				after = "FALSE"
			}
		} else {
			*args = append(*args, *value)
			placeholder := fmt.Sprintf("$%d", len(*args))
			op := ">"
			if term.Descending {
				op = "<"
			}
			equal = fmt.Sprintf("%s IS NOT NULL AND %s = %s", columnName, columnName, placeholder)
			comparison := fmt.Sprintf("%s IS NOT NULL AND %s %s %s", columnName, columnName, op, placeholder)
			if term.NullsFirst {
				after = comparison
			} else {
				after = "(" + columnName + " IS NULL OR (" + comparison + "))"
			}
		}
		parts := append(append([]string(nil), prefix...), after)
		terms = append(terms, "("+strings.Join(parts, " AND ")+")")
		prefix = append(prefix, "("+equal+")")
	}
	return strings.Join(terms, " OR "), nil
}

type rowQuerier interface {
	Query(context.Context, string, ...any) (pgx.Rows, error)
}

func (p *plan) loadWindow(ctx context.Context, db rowQuerier, left, right boundary, reverse bool, limit int) ([]*materializedRow, error) {
	sql, args, err := p.windowSQL(left, right, reverse, limit)
	if err != nil {
		return nil, err
	}
	rows, err := db.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	result, err := p.collect(rows)
	if err != nil {
		return nil, err
	}
	if reverse {
		slices.Reverse(result)
	}
	return result, nil
}

func (p *plan) loadWindowKeys(ctx context.Context, db rowQuerier, left, right boundary, reverse bool, limit int) ([]*postgresSortKey, error) {
	fields := make([]string, 0, len(p.order))
	for _, term := range p.order {
		fields = append(fields, term.Field.Root())
	}
	sql, args, err := p.windowSQLSelecting(left, right, reverse, limit, fields)
	if err != nil {
		return nil, err
	}
	rows, err := db.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]*postgresSortKey, 0, limit)
	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			return nil, err
		}
		key := &postgresSortKey{values: make([]*string, len(values))}
		for index, value := range values {
			if value == nil {
				continue
			}
			text, ok := value.(string)
			if !ok {
				return nil, fmt.Errorf("ordering field %q did not decode as text", fields[index])
			}
			key.values[index] = &text
		}
		result = append(result, key)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if reverse {
		slices.Reverse(result)
	}
	return result, nil
}

func (p *plan) collect(rows pgx.Rows) ([]*materializedRow, error) {
	defer rows.Close()
	var result []*materializedRow
	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			return nil, err
		}
		row := make(cdc.Row, len(p.needed))
		for i, name := range p.needed {
			cell := cdc.Cell{TypeOID: p.columns[name].typeOID}
			if values[i] == nil {
				cell.Null = true
			} else {
				cell.Text = []byte(values[i].(string))
			}
			row[name] = cell
		}
		materialized, err := p.materialize(row)
		if err != nil {
			return nil, err
		}
		result = append(result, materialized)
	}
	return result, rows.Err()
}

func (p *plan) materialize(row cdc.Row) (*materializedRow, error) {
	for _, name := range p.primary {
		cell, ok := row[name]
		if !ok || cell.Null || cell.Missing {
			return nil, errors.New("row lacks primary key")
		}
	}
	keys := make([]*string, 0, len(p.order))
	for _, term := range p.order {
		cell := row[term.Field.Root()]
		if cell.Null {
			keys = append(keys, nil)
		} else {
			value := string(cell.Text)
			keys = append(keys, &value)
		}
	}
	return &materializedRow{values: row, keys: keys}, nil
}
