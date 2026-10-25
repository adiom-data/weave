// Package routing compiles canonical predicates into conservative index
// registrations and converts canonical record values into index probes.
package routing

import (
	"crypto/sha256"
	"fmt"

	"github.com/adiom-data/weave/internal/storage"
	"github.com/adiom-data/weave/internal/valueencoding"
)

type RoutingToken string

type RoutingPoint struct {
	// Domain identifies one comparable field domain, not a database index or
	// join key. Points are derived from changed normalized records so the query
	// registry can perform an in-memory interval lookup.
	Domain RoutingToken
	Value  storage.Value
}

type RoutingExact struct {
	Field storage.FieldPath
	Token RoutingToken
}

// RoutingProbe is the canonical form of one old or new field value. The
// processor associates probes with a field known to have changed.
type RoutingProbe struct {
	Field    storage.FieldPath
	Tokens   []RoutingToken
	Points   []RoutingPoint
	Complete bool
	// Ordering enables coarse ordering dependencies for an existing row. It is
	// false for inserts/unknown rows, which are discovered by predicate leaves
	// or relation fallback and must not fan out through common sort columns.
	Ordering bool
}

type RoutingRange struct {
	Field          storage.FieldPath
	Domain         RoutingToken
	Lower          *storage.Value // nil means unbounded
	LowerInclusive bool
	Upper          *storage.Value // nil means unbounded
	UpperInclusive bool
}

// RoutingPlan contains independent, conservative leaf registrations extracted
// from a canonical predicate. Finding any exact value, interval, or watcher
// selects the query for exact evaluation.
type RoutingPlan struct {
	Exact       []RoutingExact
	Ranges      []RoutingRange
	WatchFields []storage.FieldPath
	OrderFields []storage.FieldPath
	Fallback    bool
}

func Compile(relation storage.RelationKey, query storage.QuerySpec) RoutingPlan {
	plan := predicatePlan(relation, query.Filter)
	seen := make(map[storage.FieldPath]struct{}, len(query.Order))
	for _, term := range query.Order {
		if _, exists := seen[term.Field]; exists {
			continue
		}
		seen[term.Field] = struct{}{}
		plan.OrderFields = append(plan.OrderFields, term.Field)
	}
	return plan
}

func Probe(relation storage.RelationKey, field storage.FieldPath, value storage.Value) (tokens []RoutingToken, points []RoutingPoint, complete bool) {
	if value.State == storage.ValueMissing || value.State == storage.ValueUnavailable {
		return nil, nil, false
	}
	var canonical []byte
	if value.State == storage.ValuePresent {
		canonical = valueencoding.CanonicalBytes(value)
	}
	tokens = append(tokens, exactToken(relation, field, value.State, canonical))
	if value.State == storage.ValuePresent {
		tokens = append(tokens, exactToken(relation, field, storage.ValuePresent, nil))
		if _, comparable := storage.CompareValues(value, value); comparable {
			points = append(points, RoutingPoint{Domain: rangeDomain(relation, field), Value: value})
		}
	}
	return tokens, points, true
}

func Compare(left, right storage.Value) (int, bool) {
	return storage.CompareValues(left, right)
}

func predicatePlan(relation storage.RelationKey, predicate *storage.Predicate) RoutingPlan {
	if predicate == nil {
		return RoutingPlan{Fallback: true}
	}
	if comparison := predicate.Comparison; comparison != nil {
		value := comparison.Value.Value()
		switch comparison.Operator {
		case storage.ComparisonEqual:
			return RoutingPlan{Exact: []RoutingExact{{Field: comparison.Field, Token: exactToken(relation, comparison.Field, storage.ValuePresent, valueencoding.CanonicalBytes(value))}}}
		case storage.ComparisonNotEqual:
			return RoutingPlan{WatchFields: []storage.FieldPath{comparison.Field}}
		}
		interval := RoutingRange{Field: comparison.Field, Domain: rangeDomain(relation, comparison.Field)}
		switch comparison.Operator {
		case storage.ComparisonLess:
			interval.Upper = &value
		case storage.ComparisonLessOrEqual:
			interval.Upper, interval.UpperInclusive = &value, true
		case storage.ComparisonGreater:
			interval.Lower = &value
		case storage.ComparisonGreaterOrEqual:
			interval.Lower, interval.LowerInclusive = &value, true
		default:
			return RoutingPlan{Fallback: true}
		}
		return RoutingPlan{Ranges: []RoutingRange{interval}}
	}
	if nullTest := predicate.Null; nullTest != nil {
		state := storage.ValueNull
		if !nullTest.IsNull {
			state = storage.ValuePresent
		}
		return RoutingPlan{Exact: []RoutingExact{{Field: nullTest.Field, Token: exactToken(relation, nullTest.Field, state, nil)}}}
	}
	if junction := predicate.Junction; junction != nil {
		var combined RoutingPlan
		for _, operand := range junction.Operands {
			child := predicatePlan(relation, operand)
			combined.Exact = append(combined.Exact, child.Exact...)
			combined.Ranges = append(combined.Ranges, child.Ranges...)
			combined.WatchFields = append(combined.WatchFields, child.WatchFields...)
			combined.Fallback = combined.Fallback || child.Fallback
		}
		return combined
	}
	return RoutingPlan{Fallback: true}
}

func exactToken(relation storage.RelationKey, field storage.FieldPath, state storage.ValueState, canonical []byte) RoutingToken {
	prefix := fmt.Sprintf("exact\x00%s\x00%s\x00%s\x00%s\x00%d\x00", relation.Source, relation.OpaqueID, relation.Namespace, field, state)
	sum := sha256.Sum256(append([]byte(prefix), canonical...))
	return RoutingToken(fmt.Sprintf("%x", sum[:]))
}

func rangeDomain(relation storage.RelationKey, field storage.FieldPath) RoutingToken {
	payload := fmt.Sprintf("range\x00%s\x00%s\x00%s\x00%s", relation.Source, relation.OpaqueID, relation.Namespace, field)
	sum := sha256.Sum256([]byte(payload))
	return RoutingToken(fmt.Sprintf("%x", sum[:]))
}
