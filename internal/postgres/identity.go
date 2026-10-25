package postgres

import (
	"github.com/adiom-data/weave/internal/datastore"
	"github.com/adiom-data/weave/internal/storage"
)

// Only positive equality conjuncts can establish an identity. We deliberately
// do not infer constraints from OR/NOT, even if a more powerful prover could.
func (p *postgresPrepared) PrepareIdentity() (datastore.IdentityQuery, bool, error) {
	values := storage.Record{}
	var visit func(*storage.Predicate)
	visit = func(predicate *storage.Predicate) {
		if predicate == nil {
			return
		}
		if c := predicate.Comparison; c != nil && c.Operator == storage.ComparisonEqual {
			column, exists := p.plan.columns[c.Field.Root()]
			if exists && column.primary && len(c.Field.Segments()) == 1 && c.Value.Type == column.valueType {
				values[column.name] = c.Value.Value()
			}
		}
		if j := predicate.Junction; j != nil && j.Operator == storage.JunctionAnd {
			for _, operand := range j.Operands {
				visit(operand)
			}
		}
	}
	visit(p.plan.query.Filter)
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
