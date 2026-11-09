package relation

import (
	"errors"
	"strconv"

	"cel.dev/cel-go/common"
	"cel.dev/cel-go/common/cost"
	"cel.dev/cel-go/common/operators"
	policyvalue "github.com/adiom-data/weave/internal/relation/value"
	"github.com/adiom-data/weave/internal/storage"
)

// Native plans operate exclusively on canonical values. Unsupported syntax
// selects CEL at compilation; runtime guards may retry the whole expression
// there to retain its error/short-circuit behavior. Native plans perform no IO.
type fastExpression struct {
	root     *fastNode
	fallback *celExpression
	limit    uint64
}

func (e *fastExpression) evaluate(s *evaluation) (storage.Value, uint64, error) {
	f := fastFrame{state: s, limit: e.limit}
	v := e.root.eval(&f)
	if f.fatal != nil {
		return storage.Value{}, f.cost, f.fatal
	}
	if f.deopt {
		return e.fallback.evaluate(s)
	}
	return v, f.cost, nil
}

type fastFrame struct {
	state       *evaluation
	cost, limit uint64
	deopt       bool
	fatal       error
}

func (f *fastFrame) charge(n uint64) {
	if f.fatal != nil {
		return
	}
	if err := f.state.ctx.Err(); err != nil {
		f.fatal = err
		return
	}
	f.cost = cost.SafeAdd(f.cost, n)
	if f.cost > f.limit {
		f.fatal = errors.New("authorization evaluation work limit exceeded")
	}
}

type fastNode struct {
	op           string
	value        storage.Value
	path         []readStep
	children     []*fastNode
	traversal    bool
	overload     string
	constantCost uint64
	presence     bool
}
type readStep struct {
	key   string
	index int64
	list  bool
}

func (n *fastNode) eval(f *fastFrame) storage.Value {
	if f.fatal != nil || f.deopt {
		return policyvalue.False
	}
	if n.op == "" {
		if n.constantCost != 0 {
			f.charge(n.constantCost)
		}
		return n.value
	}
	switch n.op {
	case "item", "claims", "parent":
		return n.readAttribute(f, true)
	case operators.LogicalAnd, operators.LogicalOr:
		return n.logical(f)
	case operators.Conditional:
		return n.conditional(f)
	case operators.In, "size", "startsWith", "endsWith", "contains":
		return n.operation(f)
	case operators.LogicalNot:
		v := n.children[0].eval(f)
		f.charge(1)
		if v.State == storage.ValueUnavailable {
			return v
		}
		if v.Type != storage.TypeBool || v.State != storage.ValuePresent {
			f.deopt = true
			return policyvalue.False
		}
		return policyvalue.Bool(!policyvalue.IsTrue(v))
	}
	a := n.children[0].eval(f)
	if f.deopt || f.fatal != nil {
		return policyvalue.False
	}
	b := n.children[1].eval(f)
	if f.deopt || f.fatal != nil {
		return policyvalue.False
	}
	units := uint64(1)
	if n.traversal {
		units = cost.SafeMultiplyByFactor(min(scalarSize(a), scalarSize(b)), common.StringTraversalCostFactor)
	}
	f.charge(units)
	if f.fatal != nil {
		return policyvalue.False
	}
	var result storage.Value
	var err error
	switch n.op {
	case operators.Equals, operators.NotEquals:
		result, err = policyvalue.Equal(a, b)
		if err == nil && n.op == operators.NotEquals && result.State != storage.ValueUnavailable {
			result = policyvalue.Bool(!policyvalue.IsTrue(result))
		}
	default:
		op := map[string]string{operators.Less: "<", operators.LessEquals: "<=", operators.Greater: ">", operators.GreaterEquals: ">="}[n.op]
		result, err = policyvalue.Relational(op, a, b)
	}
	if err != nil {
		f.deopt = true
	}
	return result
}

func (n *fastNode) readAttribute(f *fastFrame, includeRoot bool) storage.Value {
	units := uint64(len(n.path))
	if includeRoot {
		units++
	}
	var record storage.Record
	switch n.op {
	case "item":
		if !f.state.input.Candidate.Complete {
			f.charge(units)
			return policyvalue.Unknown
		}
		record = f.state.input.Candidate.Record
	case "claims":
		record = storage.Record(f.state.input.Claims)
	case "parent":
		parent := f.state.input.Parent
		if parent == nil {
			f.deopt = true
			return policyvalue.False
		}
		if !parent.Candidate.Complete {
			f.charge(units)
			return policyvalue.Unknown
		}
		record = parent.Candidate.Record
	}
	container := storage.Value{Type: storage.TypeObject, State: storage.ValuePresent, Object: record}
	for i, step := range n.path {
		var v storage.Value
		var found bool
		if !step.list && container.State == storage.ValuePresent && container.Type == storage.TypeObject {
			v, found = container.Object[step.key]
		} else if step.list && container.State == storage.ValuePresent && container.Type == storage.TypeList && step.index >= 0 && step.index < int64(len(container.List)) {
			v, found = container.List[step.index], true
		} else {
			f.deopt = true
			return policyvalue.False
		}
		if i < len(n.path)-1 {
			if !found || v.State != storage.ValuePresent || (v.Type != storage.TypeObject && v.Type != storage.TypeList) {
				f.deopt = true
				return policyvalue.False
			}
			container = v
			continue
		}
		if !found {
			f.charge(units)
			if step.list {
				f.deopt = true
			}
			if n.presence {
				return policyvalue.False
			}
			return policyvalue.Null
		}
		// Scalar adaptation can fail even for a presence test. Resolve that
		// before charging successful qualification, just as the CEL boundary does.
		if v.State == storage.ValuePresent {
			if err := checkNativeScalar(v); err != nil {
				f.deopt = true
				return policyvalue.False
			}
		}
		f.charge(units)
		if n.presence {
			if v.State == storage.ValueUnavailable {
				return policyvalue.Unknown
			}
			return policyvalue.True
		}
		return v
	}
	f.deopt = true
	return policyvalue.False
}

func (n *fastNode) logical(f *fastFrame) storage.Value {
	pending := false
	for _, child := range n.children {
		v := child.eval(f)
		if f.deopt || f.fatal != nil {
			return policyvalue.False
		}
		if v.State == storage.ValueUnavailable {
			pending = true
			continue
		}
		if v.State != storage.ValuePresent || v.Type != storage.TypeBool {
			f.deopt = true
			return policyvalue.False
		}
		b := policyvalue.IsTrue(v)
		if n.op == operators.LogicalAnd && !b || n.op == operators.LogicalOr && b {
			return v
		}
	}
	if pending {
		return policyvalue.Unknown
	}
	return policyvalue.Bool(n.op == operators.LogicalAnd)
}
func scalarSize(v storage.Value) uint64 {
	switch v.Type {
	case storage.TypeText, storage.TypeBytes, storage.TypeList, storage.TypeObject:
		if v.State == storage.ValuePresent {
			n, _ := policyvalue.Size(v)
			return uint64(n)
		}
	}
	return 1
}

// Authoritative records are canonical already. Check only scalar encodings
// whose CEL adaptation can fail; do not canonicalize/copy every field read.
func checkNativeScalar(v storage.Value) error {
	var err error
	switch v.Type {
	case storage.TypeInteger:
		_, err = strconv.ParseInt(string(v.Data), 10, 64)
	case storage.TypeFloat:
		_, err = strconv.ParseFloat(string(v.Data), 64)
	case storage.TypeBool:
		if string(v.Data) != "true" && string(v.Data) != "false" {
			err = errors.New("non-canonical boolean")
		}
	case storage.TypeDecimal:
		if _, ok := storage.CompareValues(v, v); !ok {
			return errors.New("invalid decimal")
		}
	}
	return err
}
