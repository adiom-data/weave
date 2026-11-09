package relation

import (
	"fmt"

	"cel.dev/cel-go/cel"
	"github.com/adiom-data/weave/internal/storage"
	exprpb "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
)

// Check known result types without coercion or evaluating the expression.
// Dynamic results still use the runtime's canonical conversion/type checks.
// Computations may return null; visibility and propagation guards may not.
type expressionOutput struct {
	kind     storage.ValueType
	nullable bool
}

func (o expressionOutput) validate(actual *cel.Type) error {
	switch actual.Kind() {
	case cel.DynKind, cel.TypeParamKind:
		return nil
	case cel.NullTypeKind:
		if o.nullable {
			return nil
		}
	}
	var compatible bool
	switch o.kind {
	case storage.TypeBool:
		compatible = actual.Kind() == cel.BoolKind
	case storage.TypeInteger:
		compatible = actual.Kind() == cel.IntKind
	case storage.TypeFloat:
		compatible = actual.Kind() == cel.DoubleKind
	case storage.TypeDecimal:
		compatible = actual.IsExactType(decimalType)
	case storage.TypeText:
		compatible = actual.Kind() == cel.StringKind
	case storage.TypeBytes:
		compatible = actual.Kind() == cel.BytesKind
	case storage.TypeUUID:
		compatible = actual.IsExactType(uuidType)
	case storage.TypeDateTime:
		compatible = actual.Kind() == cel.TimestampKind
	case storage.TypeList:
		compatible = actual.Kind() == cel.ListKind
	case storage.TypeObject:
		compatible = actual.Kind() == cel.MapKind
	}
	if !compatible {
		return fmt.Errorf("expression returns %s, expected %s", actual, o.kind)
	}
	return nil
}

// Scopes mirror CEL comprehensions: range/initializer use the outer scope,
// condition/step see the accumulator and iterators, and result sees only the
// accumulator. Linked frames preserve shadowing without copying a map per node.
type expressionScope struct {
	parent *expressionScope
	names  []string
}

func (s *expressionScope) global(e *exprpb.Expr, name string) bool {
	if e.GetIdentExpr().GetName() != name {
		return false
	}
	for scope := s; scope != nil; scope = scope.parent {
		for _, local := range scope.names {
			if local == name {
				return false
			}
		}
	}
	return true
}

func walkExpr(e *exprpb.Expr, depth int, scope *expressionScope, visit func(*exprpb.Expr, int, *expressionScope) error) error {
	if e == nil {
		return nil
	}
	if err := visit(e, depth, scope); err != nil {
		return err
	}
	var children []*exprpb.Expr
	switch n := e.ExprKind.(type) {
	case *exprpb.Expr_SelectExpr:
		children = append(children, n.SelectExpr.Operand)
	case *exprpb.Expr_CallExpr:
		children = append(children, n.CallExpr.Target)
		children = append(children, n.CallExpr.Args...)
	case *exprpb.Expr_ListExpr:
		children = append(children, n.ListExpr.Elements...)
	case *exprpb.Expr_StructExpr:
		for _, entry := range n.StructExpr.Entries {
			children = append(children, entry.GetMapKey(), entry.Value)
		}
	case *exprpb.Expr_ComprehensionExpr:
		c := n.ComprehensionExpr
		for _, outer := range []*exprpb.Expr{c.IterRange, c.AccuInit} {
			if err := walkExpr(outer, depth+1, scope, visit); err != nil {
				return err
			}
		}
		accumulator := &expressionScope{parent: scope, names: []string{c.AccuVar}}
		loop := &expressionScope{parent: accumulator, names: []string{c.IterVar, c.IterVar2}}
		for _, body := range []*exprpb.Expr{c.LoopCondition, c.LoopStep} {
			if err := walkExpr(body, depth+1, loop, visit); err != nil {
				return err
			}
		}
		return walkExpr(c.Result, depth+1, accumulator, visit)
	}
	for _, child := range children {
		if err := walkExpr(child, depth+1, scope, visit); err != nil {
			return err
		}
	}
	return nil
}
