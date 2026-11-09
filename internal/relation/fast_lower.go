package relation

import (
	"cel.dev/cel-go/common"
	"cel.dev/cel-go/common/cost"
	"cel.dev/cel-go/common/operators"
	"cel.dev/cel-go/common/overloads"
	policyvalue "github.com/adiom-data/weave/internal/relation/value"
	"github.com/adiom-data/weave/internal/storage"
	exprpb "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
)

// Recognition is all-or-nothing; unsupported syntax never enters native plans.
func lowerExpression(e *exprpb.Expr, references map[int64]*exprpb.Reference) *fastNode {
	if e == nil {
		return nil
	}
	if n := lowerConstant(e); n != nil {
		return n
	}
	if selectExpr := e.GetSelectExpr(); selectExpr != nil && selectExpr.TestOnly {
		root, path, ok := readBase(selectExpr.Operand)
		if !ok {
			return nil
		}
		return &fastNode{op: root, path: append(path, readStep{key: selectExpr.Field}), presence: true}
	}
	if root, path, ok := lowerRead(e); ok {
		return &fastNode{op: root, path: path}
	}
	call := e.GetCallExpr()
	if call == nil {
		return nil
	}
	args := call.Args
	if call.Target != nil {
		switch call.Function {
		case "size", "startsWith", "endsWith", "contains":
		default:
			return nil
		}
		args = append([]*exprpb.Expr{call.Target}, args...)
	}
	switch call.Function {
	case operators.LogicalAnd, operators.LogicalOr:
		if len(args) < 2 {
			return nil
		}
	case operators.LogicalNot, "size":
		if len(args) != 1 {
			return nil
		}
	case operators.Conditional:
		if len(args) != 3 {
			return nil
		}
	case operators.Equals, operators.NotEquals, operators.Less, operators.LessEquals, operators.Greater, operators.GreaterEquals, operators.In, "startsWith", "endsWith", "contains":
		if len(args) != 2 {
			return nil
		}
	default:
		return nil
	}
	n := &fastNode{op: call.Function}
	for _, arg := range args {
		child := lowerExpression(arg, references)
		if child == nil {
			return nil
		}
		n.children = append(n.children, child)
	}
	// Conditionals are native boolean control flow, not arbitrary value
	// producers inside other operators. CEL's conditional-attribute accounting
	// is context-sensitive; keep those composite expressions on its evaluator.
	switch n.op {
	case operators.LogicalAnd, operators.LogicalOr, operators.LogicalNot, operators.Conditional:
	default:
		for _, child := range n.children {
			if containsConditional(child) {
				return nil
			}
		}
	}
	if n.op == operators.In {
		if !permitCollection(n.children[1]) {
			return nil
		}
	}
	if n.op == "size" {
		if !permitCollection(n.children[0]) {
			return nil
		}
	}
	n.traversal = n.op == operators.Equals || n.op == operators.NotEquals
	if r := references[e.Id]; r != nil && len(r.OverloadId) == 1 {
		n.overload = r.OverloadId[0]
		switch n.overload {
		case overloads.LessString, overloads.LessEqualsString, overloads.GreaterString, overloads.GreaterEqualsString,
			overloads.LessBytes, overloads.LessEqualsBytes, overloads.GreaterBytes, overloads.GreaterEqualsBytes:
			n.traversal = true
		}
	}
	return n
}

func containsConditional(n *fastNode) bool {
	if n.op == operators.Conditional {
		return true
	}
	for _, child := range n.children {
		if containsConditional(child) {
			return true
		}
	}
	return false
}

// Aggregate operands are literal values or fixed reads, not computed objects.
func permitCollection(n *fastNode) bool {
	switch n.op {
	case "item", "claims", "parent":
		return true
	case "":
		return true
	}
	return false
}

// Precompute immutable literals, retaining their original CEL runtime cost.
// Invalid constructors or unsupported entries select CEL rather than changing
// when errors happen (an invalid constructor may be in an unused branch).
func lowerConstant(e *exprpb.Expr) *fastNode {
	if e == nil {
		return nil
	}
	if c := e.GetConstExpr(); c != nil {
		var value storage.Value
		switch v := c.ConstantKind.(type) {
		case *exprpb.Constant_NullValue:
			value = policyvalue.Null
		case *exprpb.Constant_BoolValue:
			value = policyvalue.Bool(v.BoolValue)
		case *exprpb.Constant_Int64Value:
			value = policyvalue.Int(v.Int64Value)
		case *exprpb.Constant_Uint64Value:
			// Unsigned values are CEL-only; the canonical model is int64.
			return nil
		case *exprpb.Constant_DoubleValue:
			value = storage.NewFloat(v.DoubleValue)
		case *exprpb.Constant_StringValue:
			value = storage.Value{Type: storage.TypeText, State: storage.ValuePresent, Data: []byte(v.StringValue)}
		case *exprpb.Constant_BytesValue:
			value = storage.Value{Type: storage.TypeBytes, State: storage.ValuePresent, Data: append([]byte(nil), v.BytesValue...)}
		default:
			return nil
		}
		return &fastNode{value: value}
	}
	// Constructors and conversions, even with constant arguments, stay in CEL.
	if list := e.GetListExpr(); list != nil && len(list.OptionalIndices) == 0 {
		values := make([]storage.Value, 0, len(list.Elements))
		units := uint64(common.ListCreateBaseCost)
		for _, element := range list.Elements {
			child := lowerConstant(element)
			if child == nil {
				return nil
			}
			values = append(values, child.value)
			units = cost.SafeAdd(units, child.constantCost)
		}
		return &fastNode{value: storage.Value{Type: storage.TypeList, State: storage.ValuePresent, List: values}, constantCost: units}
	}
	if object := e.GetStructExpr(); object != nil && object.MessageName == "" {
		values := map[string]storage.Value{}
		units := uint64(common.MapCreateBaseCost)
		for _, entry := range object.Entries {
			if entry.OptionalEntry {
				return nil
			}
			key, value := lowerConstant(entry.GetMapKey()), lowerConstant(entry.Value)
			if key == nil || value == nil {
				return nil
			}
			if key.value.Type != storage.TypeText || key.value.State != storage.ValuePresent {
				return nil
			}
			name := string(key.value.Data)
			if _, duplicate := values[name]; duplicate {
				return nil
			}
			values[name] = value.value
			units = cost.SafeAdd(units, key.constantCost, value.constantCost)
		}
		return &fastNode{value: storage.Value{Type: storage.TypeObject, State: storage.ValuePresent, Object: values}, constantCost: units}
	}
	return nil
}

func lowerRead(e *exprpb.Expr) (string, []readStep, bool) {
	if e == nil {
		return "", nil, false
	}
	if selectExpr := e.GetSelectExpr(); selectExpr != nil && !selectExpr.TestOnly {
		root, path, ok := readBase(selectExpr.Operand)
		return root, append(path, readStep{key: selectExpr.Field}), ok
	}
	if call := e.GetCallExpr(); call != nil && call.Function == operators.Index && len(call.Args) == 2 {
		var step readStep
		switch key := call.Args[1].GetConstExpr().GetConstantKind().(type) {
		case *exprpb.Constant_StringValue:
			step.key = key.StringValue
		case *exprpb.Constant_Int64Value:
			step.list = true
			step.index = key.Int64Value
		default:
			return "", nil, false
		}
		root, path, ok := readBase(call.Args[0])
		return root, append(path, step), ok
	}
	return "", nil, false
}
func readBase(e *exprpb.Expr) (string, []readStep, bool) {
	if id := e.GetIdentExpr(); id != nil {
		switch id.Name {
		case "item", "claims", "parent":
			return id.Name, nil, true
		}
	}
	return lowerRead(e)
}
