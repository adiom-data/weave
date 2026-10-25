package queryregistry

import (
	"github.com/adiom-data/weave/internal/datastore"
	"github.com/adiom-data/weave/internal/engine/routing"
	"github.com/adiom-data/weave/internal/storage"
)

func (r *QueryRegistry) HasRelation(relation storage.RelationKey) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.byRelation[relation]) > 0
}

func (r *QueryRegistry) RoutingFields(relation storage.RelationKey) []RoutingInterest {
	r.mu.RLock()
	defer r.mu.RUnlock()
	fields := r.byField[relation]
	result := make([]RoutingInterest, 0, len(fields))
	for field := range fields {
		result = append(result, RoutingInterest{Field: field, Values: len(r.valueField[routingFieldKey{relation: relation, field: field}]) > 0})
	}
	return result
}

// Candidates returns a correctness-preserving superset. Existing membership
// catches payload changes and rows leaving a query. Old/new probes for changed
// fields catch rows entering or leaving predicate ranges, while incomplete
// probes conservatively visit every query registered on only that field.
func (r *QueryRegistry) Candidates(relation storage.RelationKey, key storage.EntityKey, probes []routing.RoutingProbe, includeFallback bool) []QueryDescriptor {
	r.mu.RLock()
	defer r.mu.RUnlock()
	selected := make(map[datastore.QueryFingerprint]struct{})
	copySet(selected, r.byEntity[key])
	if includeFallback {
		copySet(selected, r.fallback[relation])
	}
	for _, probe := range probes {
		fieldKey := routingFieldKey{relation: relation, field: probe.Field}
		copySet(selected, r.watchField[fieldKey])
		if probe.Ordering {
			copySet(selected, r.orderField[fieldKey])
		}
		if probe.Complete {
			for _, route := range probe.Tokens {
				copySet(selected, r.byRoute[route])
			}
			for _, point := range probe.Points {
				if index := r.byRange[point.Domain]; index != nil {
					matches, ok := index.candidates(point.Value)
					if !ok {
						copySet(selected, r.byRelation[relation])
						break
					}
					copySet(selected, matches)
				}
			}
		} else {
			copySet(selected, r.byField[relation][probe.Field])
		}
	}
	result := make([]QueryDescriptor, 0, len(selected))
	for fingerprint := range selected {
		if query := r.queries[fingerprint]; query != nil && query.plan.Relation() == relation {
			result = append(result, QueryDescriptor{Plan: query.plan})
		}
	}
	return result
}

func (r *QueryRegistry) addField(relation storage.RelationKey, field storage.FieldPath, fingerprint datastore.QueryFingerprint) {
	fields := r.byField[relation]
	if fields == nil {
		fields = make(map[storage.FieldPath]map[datastore.QueryFingerprint]struct{})
		r.byField[relation] = fields
	}
	addIndex(fields, field, fingerprint)
}

func (r *QueryRegistry) removeField(relation storage.RelationKey, field storage.FieldPath, fingerprint datastore.QueryFingerprint) {
	fields := r.byField[relation]
	removeIndex(fields, field, fingerprint)
	if len(fields) == 0 {
		delete(r.byField, relation)
	}
}

func (r *QueryRegistry) RelationCandidates(relation storage.RelationKey) []QueryDescriptor {
	r.mu.RLock()
	defer r.mu.RUnlock()
	set := r.byRelation[relation]
	result := make([]QueryDescriptor, 0, len(set))
	for fingerprint := range set {
		if query := r.queries[fingerprint]; query != nil {
			result = append(result, QueryDescriptor{Plan: query.plan})
		}
	}
	return result
}

func addIndex[K comparable](index map[K]map[datastore.QueryFingerprint]struct{}, key K, fingerprint datastore.QueryFingerprint) {
	set := index[key]
	if set == nil {
		set = make(map[datastore.QueryFingerprint]struct{})
		index[key] = set
	}
	set[fingerprint] = struct{}{}
}

func removeIndex[K comparable](index map[K]map[datastore.QueryFingerprint]struct{}, key K, fingerprint datastore.QueryFingerprint) {
	set := index[key]
	delete(set, fingerprint)
	if len(set) == 0 {
		delete(index, key)
	}
}

func copySet(destination, source map[datastore.QueryFingerprint]struct{}) {
	for value := range source {
		destination[value] = struct{}{}
	}
}
