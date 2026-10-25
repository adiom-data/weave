package policy

import (
	"fmt"
	"reflect"

	"cel.dev/cel-go/cel"
	"cel.dev/cel-go/common/operators"
	"cel.dev/cel-go/common/types"
	"cel.dev/cel-go/common/types/ref"
	"cel.dev/cel-go/common/types/traits"
	policyvalue "github.com/adiom-data/weave/internal/accesscontrol/policy/value"
	"github.com/adiom-data/weave/internal/storage"
)

var decimalType = types.NewObjectType("weave.Decimal", traits.ComparerType)

// celDecimal is an exact canonical value, not a string or double. Comparisons
// reuse the same implementation as query predicates and routing. Other numeric
// types remain distinct; unsupported arithmetic/conversions fail explicitly.
// A canonical string also gives CEL a comparable map-key representation. Keeping
// storage.Value here would embed slices/maps and panic in CEL's map literals.
type celDecimal string

func decimalValue(text string) ref.Val {
	value, err := storage.ParseScalar(storage.TypeDecimal, []byte(text))
	if err != nil {
		return types.NewErr("invalid decimal: %v", err)
	}
	return celDecimal(value.Data)
}

func (d celDecimal) Type() ref.Type { return decimalType }
func (d celDecimal) logicalValue() storage.Value {
	return storage.Value{Type: storage.TypeDecimal, State: storage.ValuePresent, Data: []byte(d)}
}
func (d celDecimal) Value() any { return d.logicalValue() }
func (d celDecimal) ConvertToNative(target reflect.Type) (any, error) {
	if target == reflect.TypeFor[storage.Value]() {
		return d.Value(), nil
	}
	return nil, fmt.Errorf("cannot convert decimal to %v", target)
}
func (d celDecimal) ConvertToType(target ref.Type) ref.Val {
	switch target {
	case decimalType:
		return d
	case types.TypeType:
		return decimalType
	default:
		return types.NewErr("cannot convert decimal to %v", target)
	}
}
func (d celDecimal) Equal(other ref.Val) ref.Val {
	right, ok := other.(celDecimal)
	if !ok {
		return types.False
	}
	result, err := policyvalue.Equal(d.logicalValue(), right.logicalValue())
	if err != nil {
		return types.WrapErr(err)
	}
	return types.Bool(policyvalue.IsTrue(result))
}
func (d celDecimal) Compare(other ref.Val) ref.Val {
	right, ok := other.(celDecimal)
	if !ok {
		return types.MaybeNoSuchOverloadErr(other)
	}
	compared, err := policyvalue.Compare(d.logicalValue(), right.logicalValue())
	if err != nil {
		return types.WrapErr(err)
	}
	return types.Int(compared)
}

func decimalOptions() []cel.EnvOption {
	options := []cel.EnvOption{
		cel.Types(decimalType),
		cel.Function("decimal", cel.Overload("weave_decimal_string", []*cel.Type{cel.StringType}, decimalType,
			cel.UnaryBinding(func(value ref.Val) ref.Val { return decimalValue(string(value.(types.String))) }))),
	}
	return append(options, scalarComparisonOptions(decimalType, "decimal")...)
}

func scalarComparisonOptions(kind *cel.Type, id string) []cel.EnvOption {
	var options []cel.EnvOption
	for _, operator := range []struct{ name, id string }{
		{operators.Less, "less"}, {operators.LessEquals, "less_equal"},
		{operators.Greater, "greater"}, {operators.GreaterEquals, "greater_equal"},
	} {
		// Standard CEL comparison bindings dispatch through traits.Comparer.
		options = append(options, cel.Function(operator.name, cel.Overload("weave_"+id+"_"+operator.id,
			[]*cel.Type{kind, kind}, cel.BoolType)))
	}
	return options
}
