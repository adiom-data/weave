package relation

import (
	"cel.dev/cel-go/common"
	"cel.dev/cel-go/common/cost"
	"cel.dev/cel-go/common/operators"
	"cel.dev/cel-go/common/overloads"
	policyvalue "github.com/adiom-data/weave/internal/relation/value"
	"github.com/adiom-data/weave/internal/storage"
)

func (n *fastNode) conditional(f *fastFrame) storage.Value {
	v := n.children[0].eval(f)
	if f.fatal != nil || f.deopt {
		return policyvalue.False
	}
	if v.State == storage.ValueUnavailable {
		return v
	}
	if v.State != storage.ValuePresent || v.Type != storage.TypeBool {
		f.deopt = true
		return policyvalue.False
	}
	i := 2
	if policyvalue.IsTrue(v) {
		i = 1
	}
	child := n.children[i]
	switch child.op {
	case "item", "claims", "parent":
		return child.readAttribute(f, false)
	}
	return child.eval(f)
}
func (n *fastNode) operation(f *fastFrame) storage.Value {
	a := n.children[0].eval(f)
	if f.fatal != nil || f.deopt {
		return policyvalue.False
	}
	var b storage.Value
	if len(n.children) > 1 {
		b = n.children[1].eval(f)
	}
	if f.fatal != nil || f.deopt {
		return policyvalue.False
	}
	units := uint64(1)
	switch n.overload {
	case overloads.InList:
		units = scalarSize(b)
	case overloads.StartsWithString, overloads.EndsWithString:
		units = cost.SafeMultiplyByFactor(scalarSize(b), common.StringTraversalCostFactor)
	case overloads.ContainsString:
		units = cost.SafeMultiply(cost.SafeMultiplyByFactor(scalarSize(a), common.StringTraversalCostFactor), cost.SafeMultiplyByFactor(scalarSize(b), common.StringTraversalCostFactor))
	}
	f.charge(units)
	if f.fatal != nil {
		return policyvalue.False
	}
	if a.State == storage.ValueUnavailable {
		return a
	}
	if len(n.children) > 1 && b.State == storage.ValueUnavailable {
		return b
	}
	var result storage.Value
	var err error
	switch n.op {
	case operators.In:
		result, err = policyvalue.Contains(b, a)
	case "size":
		var count int64
		count, err = policyvalue.Size(a)
		result = policyvalue.Int(count)
	default:
		result, err = policyvalue.StringPredicate(n.op, a, b)
	}
	if err != nil {
		f.deopt = true
	}
	return result
}
