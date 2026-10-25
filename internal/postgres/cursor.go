package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/adiom-data/weave/internal/entitykey"
	"github.com/adiom-data/weave/internal/postgres/cdc"
	"github.com/adiom-data/weave/internal/storage"
	"github.com/adiom-data/weave/internal/valueencoding"
)

type cursorPayload struct {
	Query  string    `json:"q"`
	Values []*string `json:"v"`
}

func (p *plan) encodeCursor(values []*string) ([]byte, error) {
	return json.Marshal(cursorPayload{Query: p.fingerprint, Values: values})
}

func (p *plan) decodeCursor(cursor []byte) ([]*string, error) {
	var decoded cursorPayload
	if err := json.Unmarshal(cursor, &decoded); err != nil || decoded.Query != p.fingerprint || len(decoded.Values) != len(p.order) {
		return nil, errors.New("cursor does not belong to query")
	}
	return decoded.Values, nil
}

func (p *plan) compareKeys(a, b []*string) (int, error) {
	if len(a) != len(p.order) || len(b) != len(p.order) {
		return 0, errors.New("sort key count does not match ordering")
	}
	for i, term := range p.order {
		cmp, err := p.compareValue(p.columns[term.Field.Root()].typeOID, a[i], b[i], term.NullsFirst)
		if err != nil {
			return 0, fmt.Errorf("compare ordering field %q: %w", term.Field, err)
		}
		// NULLS FIRST/LAST is independent of ASC/DESC in PostgreSQL. Reverse
		// only comparisons between actual values.
		if term.Descending && a[i] != nil && b[i] != nil {
			cmp = -cmp
		}
		if cmp != 0 {
			return cmp, nil
		}
	}
	return 0, nil
}

func (p *plan) compareValue(oid uint32, a, b *string, nullsFirst bool) (int, error) {
	if a == nil || b == nil {
		if a == nil && b == nil {
			return 0, nil
		}
		if a == nil {
			if nullsFirst {
				return -1, nil
			}
			return 1, nil
		}
		if nullsFirst {
			return 1, nil
		}
		return -1, nil
	}
	left, leftSupported, err := p.adapter.decodeCell(cdc.Cell{TypeOID: oid, Text: []byte(*a)})
	if err != nil {
		return 0, err
	}
	right, rightSupported, err := p.adapter.decodeCell(cdc.Cell{TypeOID: oid, Text: []byte(*b)})
	if err != nil {
		return 0, err
	}
	if !leftSupported || !rightSupported {
		return 0, fmt.Errorf("PostgreSQL type %d has no logical ordering", oid)
	}
	compared, comparable := storage.CompareValues(left, right)
	if !comparable {
		return 0, fmt.Errorf("logical type %q has no ordering", left.Type)
	}
	return compared, nil
}

func (p *plan) refetch(ctx context.Context, db rowQuerier, identity string) (*materializedRow, error) {
	parts, err := p.decodeIdentity(identity)
	if err != nil {
		return nil, err
	}
	selects, wheres, args := []string{}, []string{}, []any{}
	for _, name := range p.needed {
		selects = append(selects, quote(name)+"::text AS "+quote(name))
	}
	for _, name := range p.primary {
		part := parts[name]
		logical, err := valueencoding.ParseScalarBytes(p.columns[name].valueType, part.Value)
		if err != nil {
			return nil, fmt.Errorf("decode primary-key field %q: %w", name, err)
		}
		native, err := p.adapter.encodeScalar(p.columns[name].typeOID, logical)
		if err != nil {
			return nil, fmt.Errorf("encode primary-key field %q: %w", name, err)
		}
		args = append(args, native)
		// The parameter's type is inferred from the primary-key column. Avoid
		// casting the indexed column to text, which would defeat normal PK lookup.
		wheres = append(wheres, fmt.Sprintf("%s = $%d", quote(name), len(args)))
	}
	rows, err := db.Query(ctx, fmt.Sprintf("SELECT %s FROM %s.%s WHERE %s", strings.Join(selects, ", "), quote(p.schema), quote(p.table), strings.Join(wheres, " AND ")), args...)
	if err != nil {
		return nil, err
	}
	result, err := p.collect(rows)
	if err != nil {
		return nil, err
	}
	if len(result) == 0 {
		return nil, nil
	}
	return result[0], nil
}

// decodeIdentity validates named primary-key components without coupling
// refetching to either PostgreSQL's physical column order or its index order.
func (p *plan) decodeIdentity(identity string) (map[string]entitykey.Part, error) {
	parts, err := entitykey.Decode(identity)
	if err != nil || len(parts) != len(p.primary) {
		return nil, errors.New("invalid row identity")
	}
	byName := make(map[string]entitykey.Part, len(parts))
	for _, part := range parts {
		column, exists := p.columns[part.Name]
		if !exists || !column.primary || part.TypeOID != column.typeOID {
			return nil, errors.New("invalid row identity")
		}
		if _, duplicate := byName[part.Name]; duplicate {
			return nil, errors.New("invalid row identity")
		}
		byName[part.Name] = part
	}
	for _, name := range p.primary {
		if _, exists := byName[name]; !exists {
			return nil, errors.New("invalid row identity")
		}
	}
	return byName, nil
}
