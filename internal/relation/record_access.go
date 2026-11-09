package relation

import (
	"cel.dev/cel-go/common/types"
	"cel.dev/cel-go/common/types/ref"
	"cel.dev/cel-go/interpreter"
)

// CEL deliberately separates attribute reads from structural map operations.
// Extend its attribute planning hook rather than making missing map keys appear
// present, which would also change equality, membership and presence tests.
func recordAccessDecorator() interpreter.InterpretableDecoratorV2 {
	// CEL extends the same underlying attribute as it plans a.b.c, and places
	// its own observer outside our wrapper. Deduplicate by attribute identity,
	// not by the outer wrapper type: wrapping the observer again would both
	// repeat qualification and defeat CEL's observer deduplication.
	// This state belongs to one program construction, never an evaluation.
	seen := make(map[interpreter.Attribute]struct{})
	return func(node interpreter.InterpretableV2) (interpreter.InterpretableV2, error) {
		attribute, ok := node.(interpreter.InterpretableAttribute)
		if !ok {
			return node, nil
		}
		if _, wrapped := seen[attribute.Attr()]; wrapped {
			return node, nil
		}
		seen[attribute.Attr()] = struct{}{}
		return &recordAttribute{attribute}, nil
	}
}

type recordAttribute struct {
	interpreter.InterpretableAttribute
}

func (a *recordAttribute) AddQualifier(q interpreter.Qualifier) (interpreter.Attribute, error) {
	wrapped := recordQualifier{q}
	if constant, ok := q.(interpreter.ConstantQualifier); ok {
		return a.InterpretableAttribute.AddQualifier(recordConstantQualifier{wrapped, constant.Value()})
	}
	return a.InterpretableAttribute.AddQualifier(wrapped)
}

type recordQualifier struct{ interpreter.Qualifier }

func (q recordQualifier) Qualify(vars interpreter.Activation, object any) (any, error) {
	value, err := q.Qualifier.Qualify(vars, object)
	if err != nil {
		if _, record := object.(*recordMap); record {
			// Preserve successful qualification verbatim (including has()'s
			// boolean result). Only an absent record field gets a default;
			// use CEL's presence API rather than inspecting error strings.
			_, found, presenceErr := q.Qualifier.QualifyIfPresent(vars, object, false)
			if presenceErr == nil && !found {
				return types.NullValue, nil
			}
		}
	}
	return value, err
}

// Preserve the qualifier's constant identity for CEL's planning/cost machinery.
type recordConstantQualifier struct {
	recordQualifier
	value ref.Val
}

func (q recordConstantQualifier) Value() ref.Val { return q.value }
