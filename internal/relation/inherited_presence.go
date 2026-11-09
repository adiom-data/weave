package relation

import (
	"cel.dev/cel-go/common/types"
	"cel.dev/cel-go/common/types/ref"
	"cel.dev/cel-go/interpreter"
)

// CEL's ordinary map presence test calls Find, which would compute the value.
// Use its planning extension for statically identified inherited has() nodes so
// eligibility stays lazy and unresolved guards remain unknown, not "present".
func inheritedPresenceDecorator(reads map[int64]string) interpreter.InterpretableDecoratorV2 {
	return func(node interpreter.InterpretableV2) (interpreter.InterpretableV2, error) {
		name, found := reads[node.ID()]
		attribute, ok := node.(interpreter.InterpretableAttribute)
		if !found || !ok {
			return node, nil
		}
		return &inheritedPresence{InterpretableAttribute: attribute, name: name}, nil
	}
}

type inheritedPresence struct {
	interpreter.InterpretableAttribute
	name string
}

func (p *inheritedPresence) Exec(frame *interpreter.ExecutionFrame) ref.Val {
	value, found := frame.ResolveName("inherited")
	values, ok := value.(*computationMap)
	if !found || !ok {
		// Only global reads are decorated. Preserve ordinary CEL behavior for
		// a missing or differently typed activation supplied outside the runtime.
		return p.InterpretableAttribute.Exec(frame)
	}
	if values.state.owner == nil {
		return types.NewErr("inherited presence requires an evaluation context")
	}
	value, present, err := values.presence(p.name)
	if err != nil {
		return types.WrapErr(err)
	}
	if unknown, ok := value.(*types.Unknown); ok {
		return unknown
	}
	return types.Bool(present)
}

func (p *inheritedPresence) Eval(activation interpreter.Activation) ref.Val {
	return p.Exec(interpreter.AsFrame(activation))
}
