package relation

import (
	"cel.dev/cel-go/common/types"
	"cel.dev/cel-go/common/types/ref"
	"cel.dev/cel-go/common/types/traits"
	"cel.dev/cel-go/interpreter"
)

// CEL-created collections must use the same access rules as canonical ones.
// In particular, CEL normally accepts an integral float/uint as a list index
// and coerces numeric map keys. These wrappers deliberately do neither.
type strictCELList struct{ traits.Lister }

func (l *strictCELList) Get(key ref.Val) ref.Val {
	if _, ok := key.(types.Int); !ok {
		return types.NewErr("list index must be an integer")
	}
	return l.Lister.Get(key)
}
func (l *strictCELList) Add(other ref.Val) ref.Val { return strictCELCollection(l.Lister.Add(other)) }

type strictCELMap struct{ traits.Mapper }

func (m *strictCELMap) Find(key ref.Val) (ref.Val, bool) { return strictMapFind(m.Mapper, key) }
func (m *strictCELMap) Get(key ref.Val) ref.Val {
	value, found := m.Find(key)
	if !found {
		return types.NewErr("no such key: %v", key)
	}
	return value
}
func (m *strictCELMap) Contains(key ref.Val) ref.Val {
	_, found := m.Find(key)
	return types.Bool(found)
}
func strictCELCollection(value ref.Val) ref.Val {
	switch v := value.(type) {
	case *canonicalList, *recordMap, *strictCELList, *strictCELMap:
		return value
	case traits.Lister:
		return &strictCELList{v}
	case traits.Mapper:
		return &strictCELMap{v}
	default:
		return value
	}
}

type canonicalConstructor struct {
	interpreter.InterpretableConstructor
}

func (c *canonicalConstructor) Exec(frame *interpreter.ExecutionFrame) ref.Val {
	return strictCELCollection(c.InterpretableConstructor.Exec(frame))
}
func (c *canonicalConstructor) Eval(a interpreter.Activation) ref.Val {
	frame, err := interpreter.NewExecutionFrame(a)
	if err != nil {
		return types.WrapErr(err)
	}
	defer frame.Close()
	return c.Exec(frame)
}
