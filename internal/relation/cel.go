package relation

import (
	"errors"
	"fmt"
	"strconv"

	"cel.dev/cel-go/cel"
	"cel.dev/cel-go/common/types"
	"cel.dev/cel-go/common/types/ref"
	"cel.dev/cel-go/common/types/traits"
	"github.com/adiom-data/weave/internal/storage"
)

type celExpression struct{ program cel.Program }

func (e *celExpression) evaluate(s *evaluation) (storage.Value, uint64, error) {
	item := recordValue(s.input.Candidate.Record)
	if !s.input.Candidate.Complete {
		item = types.NewUnknown(1, nil)
	}
	parent := ref.Val(types.NullValue)
	if s.input.Parent != nil {
		parent = recordValue(s.input.Parent.Candidate.Record)
		if !s.input.Parent.Candidate.Complete {
			parent = types.NewUnknown(2, nil)
		}
	}
	value, details, err := e.program.ContextEval(s.ctx, map[string]any{"item": item, "parent": parent, "claims": recordValue(storage.Record(s.input.Claims)), "facts": &factMap{Mapper: types.NewRefValMap(types.DefaultTypeAdapter, map[ref.Val]ref.Val{}), state: s}, "computed": newComputationMap(s, false), "inherited": newComputationMap(s, true)})
	if err != nil {
		return storage.Value{}, 0, fmt.Errorf("CEL evaluation: %w", err)
	}
	var cost uint64
	if details != nil && details.ActualCost() != nil {
		cost = *details.ActualCost()
	}
	remaining := *s.remaining
	result, err := canonicalOutput(value, &remaining)
	// Collection conversion is bounded too; direct canonical wrappers borrow
	// their immutable values and never copy every element for a scalar check.
	cost += uint64(*s.remaining - remaining)
	return result, cost, err
}

// factMap resolves only the fact keys actually read by CEL short-circuiting.
// Incomplete projection evidence is an unknown value, never an empty list.
type factMap struct {
	traits.Mapper
	state *evaluation
}

func (m *factMap) Find(key ref.Val) (ref.Val, bool) {
	id, ok := key.(types.String)
	if !ok {
		return types.NewErr("fact key must be a string"), false
	}
	s := m.state
	if err := s.step(); err != nil {
		s.failure = err
		return types.NewErr("%v", err), false
	}
	if cached, ok := s.facts[string(id)]; ok {
		mergeDependencies(s.dependencies, cached.dependencies)
		if cached.err != nil {
			return types.NewErr("%v", cached.err), false
		}
		if !cached.complete {
			return types.NewUnknown(3, nil), true
		}
		return valueToCEL(cached.value), true
	}
	var definition *Fact
	for i := range s.policy.definition.Facts {
		if s.policy.definition.Facts[i].ID == string(id) {
			definition = &s.policy.definition.Facts[i]
			break
		}
	}
	if definition == nil {
		return types.NewErr("unknown fact %q", id), false
	}
	if s.resolver == nil {
		s.failure = errors.New("fact resolver is required")
		return types.NewErr("fact resolver is required"), false
	}
	// A pending candidate cannot bind its record into an authoritative fact
	// selector. Constant/claim-only rules can still authorize the reference.
	if !s.input.Candidate.Complete {
		return types.NewUnknown(3, nil), true
	}
	parameters := storage.Record{"item": {Type: storage.TypeObject, State: storage.ValuePresent, Object: map[string]storage.Value(s.input.Candidate.Record)}, "claims": {Type: storage.TypeObject, State: storage.ValuePresent, Object: s.input.Claims}}
	result, err := s.resolver.Resolve(s.ctx, FactRequest{ID: string(id), Projection: definition.Projection, Parameters: parameters, WorkLimit: *s.remaining})
	if result.Work < 0 || result.Work > *s.remaining {
		err = errors.New("authorization projection work limit exceeded")
	} else {
		*s.remaining -= result.Work
	}
	s.facts[string(id)] = factValue{result.Value, result.Complete, err, result.Dependencies}
	mergeDependencies(s.dependencies, result.Dependencies)
	if err != nil {
		s.failure = err
		return types.NewErr("%v", err), false
	}
	if !result.Complete {
		return types.NewUnknown(3, nil), true
	}
	if err := result.Value.ValidateComplete(); err != nil {
		s.failure = err
		return types.NewErr("%v", err), false
	}
	return valueToCEL(result.Value), true
}
func (m *factMap) Get(key ref.Val) ref.Val { v, _ := m.Find(key); return v }

// Record maps retain CEL's ordinary structural semantics. The record-access
// decorator supplies null defaults only for field reads, never for Find,
// equality, membership, or presence checks.
type recordMap struct {
	traits.Mapper
	record storage.Record
}

// Collection wrappers borrow immutable canonical images. CEL adapts only the
// elements it actually reads; no eager record/list materialization is needed.
type canonicalList struct {
	traits.Lister
	values []storage.Value
}

func (l *canonicalList) Get(index ref.Val) ref.Val {
	i, ok := index.(types.Int)
	if !ok {
		return types.NewErr("list index must be an integer")
	}
	if i < 0 || int64(i) >= int64(len(l.values)) {
		return types.NewErr("index out of bounds: %d", i)
	}
	return valueToCEL(l.values[i])
}

type canonicalAdapter struct{}

func (canonicalAdapter) NativeToValue(v any) ref.Val {
	if value, ok := v.(storage.Value); ok {
		return valueToCEL(value)
	}
	return types.DefaultTypeAdapter.NativeToValue(v)
}

func recordValue(record storage.Record) ref.Val {
	return &recordMap{Mapper: types.NewDynamicMap(canonicalAdapter{}, record), record: record}
}
func valueToCEL(v storage.Value) ref.Val {
	if v.State == storage.ValueUnavailable {
		return types.NewUnknown(4, nil)
	}
	if v.State != storage.ValuePresent {
		return types.NullValue
	}
	switch v.Type {
	case storage.TypeText:
		return types.String(v.Data)
	case storage.TypeUUID:
		return celUUID(v.Data)
	case storage.TypeDecimal:
		return decimalValue(string(v.Data))
	case storage.TypeBytes:
		return types.Bytes(v.Data)
	case storage.TypeBool:
		b, err := strconv.ParseBool(string(v.Data))
		if err != nil {
			return types.NewErr("invalid bool")
		}
		return types.Bool(b)
	case storage.TypeInteger:
		n, err := strconv.ParseInt(string(v.Data), 10, 64)
		if err != nil {
			return types.NewErr("invalid integer")
		}
		return types.Int(n)
	case storage.TypeFloat:
		n, err := strconv.ParseFloat(string(v.Data), 64)
		if err != nil {
			return types.NewErr("invalid float")
		}
		return types.Double(n)
	case storage.TypeDateTime:
		return types.Timestamp{Time: v.DateTime}
	case storage.TypeObject:
		return recordValue(storage.Record(v.Object))
	case storage.TypeList:
		return &canonicalList{Lister: types.NewDynamicList(canonicalAdapter{}, v.List), values: v.List}
	}
	return types.NewErr("unsupported logical type %v", v.Type)
}
