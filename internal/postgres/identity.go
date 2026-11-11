package postgres

import (
	"errors"
	"github.com/adiom-data/weave/internal/datastore"
	"github.com/adiom-data/weave/internal/storage"
)

// Only positive equality conjuncts can establish an identity. We deliberately
// do not infer constraints from OR/NOT, even if a more powerful prover could.
func (p *postgresPrepared) PrepareIdentity() (datastore.IdentityQuery, bool, error) {
	values := storage.Record{}
	var prepareErr error
	var visit func(*storage.Predicate)
	visit = func(predicate *storage.Predicate) {
		if predicate == nil || prepareErr != nil {
			return
		}
		if c := predicate.Comparison; c != nil && c.Operator == storage.ComparisonEqual {
			column, exists := p.plan.columns[c.Field.Root()]
			if exists && column.primary && len(c.Field.Segments()) == 1 && c.Value.Type == column.valueType {
				value, exact, err := p.identityLiteral(column, c)
				prepareErr = err
				if exact {
					values[column.name] = value
				}
			}
		}
		if j := predicate.Junction; j != nil && j.Operator == storage.JunctionAnd {
			for _, operand := range j.Operands {
				visit(operand)
			}
		}
	}
	visit(p.plan.query.Filter)
	if prepareErr != nil {
		return nil, false, prepareErr
	}
	for _, name := range p.plan.primary {
		if _, exists := values[name]; !exists {
			return nil, false, nil
		}
	}
	if len(p.plan.primary) == 0 {
		return nil, false, nil
	}
	key, err := p.EntityKey(values)
	if err != nil {
		return nil, false, err
	}
	copy := *p.plan
	copy.fingerprint = "entity:" + copy.fingerprint
	return &identityPrepared{postgresPrepared: postgresPrepared{plan: &copy}, key: key}, true, nil
}

// An equality can establish identity only when the compiled comparison names
// an actual native key. Empty native-domain comparisons remain ordinary queries.
func (p *postgresPrepared) identityLiteral(column column, comparison *storage.Comparison) (storage.Value, bool, error) {
	prepared, ok := p.plan.comparisons[comparison]
	if !ok {
		return storage.Value{}, false, errors.New("identity comparison was not prepared")
	}
	switch prepared.kind {
	case postgresComparisonScalar:
		return comparison.Value.Value(), true, nil
	case postgresComparisonDate:
		// Date comparisons use timestamp parameters so that non-midnight
		// literals can correctly produce an empty result. Such a literal is
		// not an identity unless encoding it as the actual key preserves it.
		copy := *comparison
		if err := p.plan.adapter.normalizeComparisonLiteral(column.typeOID, &copy); err != nil {
			return storage.Value{}, false, nil
		}
		value := copy.Value.Value()
		return value, storage.EqualValues(value, comparison.Value.Value()), nil
	default:
		return storage.Value{}, false, nil
	}
}

type identityPrepared struct {
	postgresPrepared
	key storage.EntityKey
}

func (p *identityPrepared) LookupKey() storage.EntityKey { return p.key }

// Snapshot methods unwrap this specialization; the native plan itself is
// unchanged apart from its cache-domain fingerprint.
func nativePrepared(query datastore.PreparedQuery) (*postgresPrepared, bool) {
	switch p := query.(type) {
	case *postgresPrepared:
		return p, p != nil
	case *identityPrepared:
		if p == nil {
			return nil, false
		}
		return &p.postgresPrepared, true
	default:
		return nil, false
	}
}
