package policy

import (
	"fmt"
	"reflect"
	"strconv"

	"cel.dev/cel-go/cel"
	"cel.dev/cel-go/common/operators"
	"cel.dev/cel-go/common/types"
	"cel.dev/cel-go/common/types/ref"
	"cel.dev/cel-go/common/types/traits"
	"cel.dev/cel-go/interpreter"
	policyvalue "github.com/adiom-data/weave/internal/accesscontrol/policy/value"
	"github.com/adiom-data/weave/internal/storage"
)

// UUID is a distinct policy type. String conversion is explicit and is only
// performed by CEL; normal reads and comparisons never erase its identity.
var uuidType = types.NewObjectType("weave.UUID", traits.ComparerType)

type celUUID string

func (u celUUID) Type() ref.Type { return uuidType }
func (u celUUID) Value() any {
	return storage.Value{Type: storage.TypeUUID, State: storage.ValuePresent, Data: []byte(u)}
}
func (u celUUID) Equal(other ref.Val) ref.Val {
	v, ok := other.(celUUID)
	return types.Bool(ok && u == v)
}
func (u celUUID) Compare(other ref.Val) ref.Val {
	right, ok := other.(celUUID)
	if !ok {
		return types.MaybeNoSuchOverloadErr(other)
	}
	order, err := policyvalue.Compare(u.Value().(storage.Value), right.Value().(storage.Value))
	if err != nil {
		return types.WrapErr(err)
	}
	return types.Int(order)
}
func (u celUUID) ConvertToNative(t reflect.Type) (any, error) {
	if t == reflect.TypeFor[storage.Value]() {
		return u.Value(), nil
	}
	return nil, fmt.Errorf("cannot convert UUID to %v", t)
}
func (u celUUID) ConvertToType(t ref.Type) ref.Val {
	switch t {
	case uuidType:
		return u
	case types.TypeType:
		return uuidType
	case types.StringType:
		return types.String(u)
	}
	return types.NewErr("cannot convert UUID to %v", t)
}
func uuidOptions() []cel.EnvOption {
	options := []cel.EnvOption{
		cel.Types(uuidType),
		cel.Function("string", cel.Overload("weave_uuid_to_string", []*cel.Type{uuidType}, cel.StringType, cel.UnaryBinding(func(v ref.Val) ref.Val { return v.ConvertToType(types.StringType) }))),
		cel.Function("uuid", cel.Overload("weave_uuid_string", []*cel.Type{cel.StringType}, uuidType, cel.UnaryBinding(func(v ref.Val) ref.Val {
			x, err := storage.ParseScalar(storage.TypeUUID, []byte(v.(types.String)))
			if err != nil {
				return types.WrapErr(err)
			}
			return celUUID(x.Data)
		}))),
	}
	return append(options, scalarComparisonOptions(uuidType, "uuid")...)
}

// Policy comparisons replace CEL's implicit mixed-number comparisons, including
// within comprehensions and over CEL-constructed collections. Keep the call's
// identity/arguments intact so CEL's cost and cancellation observers still work.
func canonicalOperationsDecorator() interpreter.InterpretableDecoratorV2 {
	return func(node interpreter.InterpretableV2) (interpreter.InterpretableV2, error) {
		if constructor, ok := node.(interpreter.InterpretableConstructor); ok {
			return &canonicalConstructor{constructor}, nil
		}
		call, ok := node.(interpreter.InterpretableCall)
		if !ok {
			return node, nil
		}
		switch call.Function() {
		case operators.Equals, operators.NotEquals, operators.Less, operators.LessEquals, operators.Greater, operators.GreaterEquals, operators.In:
			return &canonicalCall{call}, nil
		}
		return node, nil
	}
}

type canonicalCall struct{ interpreter.InterpretableCall }

func (c *canonicalCall) Eval(a interpreter.Activation) ref.Val {
	f, err := interpreter.NewExecutionFrame(a)
	if err != nil {
		return types.WrapErr(err)
	}
	defer f.Close()
	return c.Exec(f)
}
func (c *canonicalCall) Exec(f *interpreter.ExecutionFrame) ref.Val {
	args := c.Args()
	a := args[0].Exec(f)
	if types.IsError(a) {
		return a
	}
	b := args[1].Exec(f)
	if types.IsError(b) {
		return b
	}
	var pending *types.Unknown
	pending, _ = types.MaybeMergeUnknowns(a, pending)
	pending, _ = types.MaybeMergeUnknowns(b, pending)
	if pending != nil {
		return pending
	}
	return celPolicyOperation(c.Function(), a, b)
}
func celPolicyOperation(op string, a, b ref.Val) ref.Val {
	// CEL-only scalar types (uint, duration, type values) stay distinct. They
	// retain their own operations, but cannot coerce a canonical operand.
	av, aCanonical := fromCEL(a)
	bv, bCanonical := fromCEL(b)
	if !aCanonical || !bCanonical {
		if op == operators.In {
			return celCollectionContains(b, a)
		}
		if op == operators.Equals || op == operators.NotEquals {
			eq := celStructuralEqual(a, b)
			if op == operators.NotEquals {
				if boolean, ok := eq.(types.Bool); ok {
					return boolean.Negate()
				}
			}
			return eq
		}
		if a.Type() != b.Type() {
			return types.NewErr("incompatible policy comparison: %s and %s", a.Type(), b.Type())
		}
		if cmp, ok := a.(traits.Comparer); ok {
			order := cmp.Compare(b)
			if i, ok := order.(types.Int); ok {
				return relationResult(op, int(i))
			}
			return order
		}
		return types.NewErr("unsupported policy comparison")
	}
	var result storage.Value
	var err error
	switch op {
	case operators.Equals, operators.NotEquals:
		result, err = policyvalue.Equal(av, bv)
		if err == nil && op == operators.NotEquals && result.State != storage.ValueUnavailable {
			result = policyvalue.Bool(!policyvalue.IsTrue(result))
		}
	case operators.In:
		result, err = policyvalue.Contains(bv, av)
	default:
		spelling := map[string]string{operators.Less: "<", operators.LessEquals: "<=", operators.Greater: ">", operators.GreaterEquals: ">="}[op]
		result, err = policyvalue.Relational(spelling, av, bv)
	}
	if err != nil {
		return types.WrapErr(err)
	}
	return valueToCEL(result)
}

// CEL-only collections can contain uints, durations or non-string map keys.
// Never delegate their structural comparison to CEL's coercing equality: walk
// them lazily and use the same policy operation for every child/key.
func celStructuralEqual(a, b ref.Val) ref.Val {
	if a.Type() != b.Type() {
		return types.False
	}
	var pending ref.Val
	if left, ok := a.(traits.Lister); ok {
		right, ok := b.(traits.Lister)
		if !ok || left.Size() != right.Size() {
			return types.False
		}
		for i := types.Int(0); i < left.Size().(types.Int); i++ {
			eq := celPolicyOperation(operators.Equals, left.Get(i), right.Get(i))
			if eq == types.False {
				return eq
			}
			if types.IsUnknownOrError(eq) {
				pending = eq
			}
		}
	} else if left, ok := a.(traits.Mapper); ok {
		right, ok := b.(traits.Mapper)
		if !ok || left.Size() != right.Size() {
			return types.False
		}
		it := left.Iterator()
		for it.HasNext() == types.True {
			key := it.Next()
			other, found := strictMapFind(right, key)
			if !found {
				return types.False
			}
			eq := celPolicyOperation(operators.Equals, left.Get(key), other)
			if eq == types.False {
				return eq
			}
			if types.IsUnknownOrError(eq) {
				pending = eq
			}
		}
	} else {
		return types.Equal(a, b)
	}
	if pending != nil {
		return pending
	}
	return types.True
}
func relationResult(op string, n int) ref.Val {
	switch op {
	case operators.Less:
		return types.Bool(n < 0)
	case operators.LessEquals:
		return types.Bool(n <= 0)
	case operators.Greater:
		return types.Bool(n > 0)
	default:
		return types.Bool(n >= 0)
	}
}
func celCollectionContains(collection, needle ref.Val) ref.Val {
	if list, ok := collection.(traits.Lister); ok {
		pending := ref.Val(nil)
		for i := types.Int(0); i < list.Size().(types.Int); i++ {
			v := celPolicyOperation(operators.Equals, needle, list.Get(i))
			if v == types.True {
				return v
			}
			if types.IsUnknownOrError(v) {
				pending = v
			}
		}
		if pending != nil {
			return pending
		}
		return types.False
	}
	if object, ok := collection.(traits.Mapper); ok {
		_, found := strictMapFind(object, needle)
		return types.Bool(found)
	}
	return types.NewErr("membership requires a collection")
}

// Canonical objects and ordinary CEL literal maps both have O(1), type-strict
// lookup. Do not use CEL's generic Find for numeric keys: it coerces them.
func strictMapFind(object traits.Mapper, key ref.Val) (ref.Val, bool) {
	if record, ok := object.(*recordMap); ok {
		name, ok := key.(types.String)
		if !ok {
			return nil, false
		}
		value, found := record.record[string(name)]
		if !found {
			return nil, false
		}
		return valueToCEL(value), true
	}
	if values, ok := object.Value().(map[ref.Val]ref.Val); ok {
		switch key.(type) {
		case types.String, types.Int, types.Uint, types.Bool, celUUID, celDecimal:
			value, found := values[key]
			return value, found
		default:
			return nil, false
		}
	}
	// Other CEL-produced mapper implementations are uncommon and may not expose
	// hashable storage. Preserve strict semantics without assuming their layout.
	it := object.Iterator()
	for it.HasNext() == types.True {
		other := it.Next()
		if celPolicyOperation(operators.Equals, key, other) == types.True {
			return object.Get(other), true
		}
	}
	return nil, false
}

// Conversion here is exclusively a CEL boundary operation. Native execution
// never calls it. Canonical collection wrappers unwrap in O(1).
func fromCEL(v ref.Val) (storage.Value, bool) {
	switch x := v.(type) {
	case *recordMap:
		return storage.Value{Type: storage.TypeObject, State: storage.ValuePresent, Object: x.record}, true
	case *canonicalList:
		return storage.Value{Type: storage.TypeList, State: storage.ValuePresent, List: x.values}, true
	case celUUID:
		return x.Value().(storage.Value), true
	case celDecimal:
		return x.logicalValue(), true
	case types.Null:
		return policyvalue.Null, true
	case types.Bool:
		return policyvalue.Bool(bool(x)), true
	case types.Int:
		return policyvalue.Int(int64(x)), true
	case types.Double:
		return storage.Value{Type: storage.TypeFloat, State: storage.ValuePresent, Data: strconv.AppendFloat(nil, float64(x), 'g', -1, 64)}, true
	case types.String:
		return storage.Value{Type: storage.TypeText, State: storage.ValuePresent, Data: []byte(x)}, true
	case types.Bytes:
		return storage.Value{Type: storage.TypeBytes, State: storage.ValuePresent, Data: []byte(x)}, true
	case types.Timestamp:
		return storage.NewDateTime(x.Time), true
	case *types.Unknown:
		return policyvalue.Unknown, true
	}
	return policyvalue.Null, false
}
