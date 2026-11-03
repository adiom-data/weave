package projectioncompiler

import (
	"encoding/base64"
	"fmt"
	"math"
	"strconv"

	"github.com/vektah/gqlparser/v2/ast"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/adiom-data/weave/internal/storage"
	reactivev1 "github.com/adiom-data/weave/pkg/gen/reactive/v1"
)

// GraphQL has its own enum vocabulary; do not couple it to protobuf JSON names.
func graphqlComparisonOperator(value string) (reactivev1.Comparison_Operator, bool) {
	switch value {
	case "EQUAL":
		return reactivev1.Comparison_OPERATOR_EQUAL, true
	case "NOT_EQUAL":
		return reactivev1.Comparison_OPERATOR_NOT_EQUAL, true
	case "LESS_THAN":
		return reactivev1.Comparison_OPERATOR_LESS_THAN, true
	case "LESS_THAN_OR_EQUAL":
		return reactivev1.Comparison_OPERATOR_LESS_THAN_OR_EQUAL, true
	case "GREATER_THAN":
		return reactivev1.Comparison_OPERATOR_GREATER_THAN, true
	case "GREATER_THAN_OR_EQUAL":
		return reactivev1.Comparison_OPERATOR_GREATER_THAN_OR_EQUAL, true
	default:
		return reactivev1.Comparison_OPERATOR_UNSPECIFIED, false
	}
}

func literal(typ *ast.Type, value *ast.Value) (*reactivev1.Value, error) {
	if value == nil || value.Kind == ast.Variable || value.Kind == ast.NullValue || typ.Elem != nil {
		return nil, fmt.Errorf("binding/filter literals must be concrete non-null scalars")
	}
	kind := scalarKinds[typ.Name()]
	raw := []byte(value.Raw)
	if kind == "bool" && value.Kind != ast.BooleanValue {
		return nil, fmt.Errorf("expected Boolean literal")
	}
	if (kind == "text" || kind == "uuid" || kind == "datetime" || kind == "bytes") && value.Kind != ast.StringValue && value.Kind != ast.BlockValue {
		return nil, fmt.Errorf("expected %s string literal", typ.Name())
	}
	if kind == "integer" || kind == "int32" || kind == "decimal" || kind == "float" || kind == "finiteFloat" {
		if value.Kind != ast.IntValue && value.Kind != ast.FloatValue && value.Kind != ast.StringValue {
			return nil, fmt.Errorf("expected numeric literal")
		}
	}
	logical := storage.ValueType(kind)
	if kind == "int32" {
		logical = storage.TypeInteger
		if _, err := strconv.ParseInt(value.Raw, 10, 32); err != nil {
			return nil, err
		}
	}
	if kind == "finiteFloat" {
		logical = storage.TypeFloat
		number, err := strconv.ParseFloat(value.Raw, 64)
		if err != nil || math.IsInf(number, 0) || math.IsNaN(number) {
			return nil, fmt.Errorf("Float requires a finite binary64 value")
		}
	}
	if kind == "bytes" {
		bytes, err := base64.StdEncoding.DecodeString(value.Raw)
		if err != nil {
			return nil, err
		}
		raw = bytes
	}
	parsed, err := storage.ParseScalar(logical, raw)
	if err != nil {
		return nil, err
	}
	out := &reactivev1.Value{}
	switch logical {
	case storage.TypeBool:
		out.Kind = &reactivev1.Value_BoolValue{BoolValue: string(parsed.Data) == "true"}
	case storage.TypeText:
		out.Kind = &reactivev1.Value_TextValue{TextValue: string(parsed.Data)}
	case storage.TypeInteger:
		out.Kind = &reactivev1.Value_IntegerValue{IntegerValue: string(parsed.Data)}
	case storage.TypeDecimal:
		out.Kind = &reactivev1.Value_DecimalValue{DecimalValue: string(parsed.Data)}
	case storage.TypeFloat:
		out.Kind = &reactivev1.Value_FloatValue{FloatValue: string(parsed.Data)}
	case storage.TypeUUID:
		out.Kind = &reactivev1.Value_UuidValue{UuidValue: string(parsed.Data)}
	case storage.TypeBytes:
		out.Kind = &reactivev1.Value_BytesValue{BytesValue: parsed.Data}
	case storage.TypeDateTime:
		timestamp := timestamppb.New(parsed.DateTime)
		if err := timestamp.CheckValid(); err != nil {
			return nil, err
		}
		out.Kind = &reactivev1.Value_DatetimeValue{DatetimeValue: timestamp}
	default:
		return nil, fmt.Errorf("%s is not supported as a comparison/binding literal", typ.Name())
	}
	return out, nil
}

func (c *Compiler) predicate(def *ast.Definition, value *ast.Value, depth int) (*reactivev1.Predicate, error) {
	if depth > 16 || value == nil || value.Kind != ast.ObjectValue {
		return nil, fmt.Errorf("@where requires a constant predicate object, nesting at most 16")
	}
	values := value.Children
	branches := 0
	for _, key := range []string{"field", "and", "or", "not"} {
		if values.ForName(key) != nil {
			branches++
		}
	}
	if branches != 1 {
		return nil, fmt.Errorf("predicate requires exactly one field/and/or/not branch")
	}
	if fieldName := values.ForName("field"); fieldName != nil {
		if fieldName.Kind != ast.StringValue {
			return nil, fmt.Errorf("predicate field must be a constant name")
		}
		field, err := c.mappedField(def, fieldName.Raw)
		if err != nil {
			return nil, err
		}
		if null := values.ForName("isNull"); null != nil {
			if null.Kind != ast.BooleanValue || values.ForName("op") != nil || values.ForName("value") != nil {
				return nil, fmt.Errorf("null predicate requires only field and isNull")
			}
			return &reactivev1.Predicate{Expression: &reactivev1.Predicate_NullTest{NullTest: &reactivev1.NullTest{Column: path(column(field)), IsNull: null.Raw == "true"}}}, nil
		}
		opValue := values.ForName("op")
		if opValue == nil || opValue.Kind != ast.EnumValue {
			return nil, fmt.Errorf("comparison predicate requires constant op")
		}
		op, ok := graphqlComparisonOperator(opValue.Raw)
		if !ok {
			return nil, fmt.Errorf("unknown comparison operator")
		}
		encoded, err := literal(field.Type, values.ForName("value"))
		if err != nil {
			return nil, err
		}
		return &reactivev1.Predicate{Expression: &reactivev1.Predicate_Comparison{Comparison: &reactivev1.Comparison{Column: path(column(field)), Operator: op, Value: encoded}}}, nil
	}
	if values.ForName("op") != nil || values.ForName("value") != nil || values.ForName("isNull") != nil {
		return nil, fmt.Errorf("junction predicate cannot contain comparison fields")
	}
	if negated := values.ForName("not"); negated != nil {
		child, err := c.predicate(def, negated, depth+1)
		if err != nil {
			return nil, err
		}
		return &reactivev1.Predicate{Expression: &reactivev1.Predicate_Negation{Negation: &reactivev1.Negation{Operand: child}}}, nil
	}
	for _, key := range []string{"and", "or"} {
		if children := values.ForName(key); children != nil {
			if children.Kind != ast.ListValue || len(children.Children) == 0 {
				return nil, fmt.Errorf("predicate %s must be a nonempty constant list", key)
			}
			junction := &reactivev1.Junction{Operator: reactivev1.Junction_OPERATOR_AND}
			if key == "or" {
				junction.Operator = reactivev1.Junction_OPERATOR_OR
			}
			for _, child := range children.Children {
				lowered, err := c.predicate(def, child.Value, depth+1)
				if err != nil {
					return nil, err
				}
				junction.Operands = append(junction.Operands, lowered)
			}
			return &reactivev1.Predicate{Expression: &reactivev1.Predicate_Junction{Junction: junction}}, nil
		}
	}
	return nil, fmt.Errorf("invalid predicate")
}
