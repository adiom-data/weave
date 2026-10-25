package policy

import (
	"errors"
	"fmt"

	"cel.dev/cel-go/cel"
	"github.com/adiom-data/weave/internal/projection/plan"
	"github.com/adiom-data/weave/internal/storage"
	exprpb "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
)

type Limits struct {
	MaxNodes, MaxDepth, MaxEvaluationSteps int
	MaxExpressionBytes                     int
}

func DefaultLimits() Limits {
	return Limits{MaxNodes: 10000, MaxDepth: 64, MaxEvaluationSteps: 100000, MaxExpressionBytes: 1 << 20}
}
func (l Limits) normalized() (Limits, error) {
	d := DefaultLimits()
	if l.MaxNodes == 0 {
		l.MaxNodes = d.MaxNodes
	}
	if l.MaxDepth == 0 {
		l.MaxDepth = d.MaxDepth
	}
	if l.MaxEvaluationSteps == 0 {
		l.MaxEvaluationSteps = d.MaxEvaluationSteps
	}
	if l.MaxExpressionBytes == 0 {
		l.MaxExpressionBytes = d.MaxExpressionBytes
	}
	if l.MaxNodes < 1 || l.MaxDepth < 1 || l.MaxEvaluationSteps < 1 || l.MaxExpressionBytes < 1 {
		return l, errors.New("positive policy limits are required")
	}
	return l, nil
}

type compiledRule struct {
	id      string
	program compiledExpression
}
type compiledEdge struct {
	id      string
	to      storage.RelationName
	program compiledExpression
}
type compiledRelation struct {
	definition  Relation
	allow, deny []compiledRule
	propagate   []compiledEdge
}

func Compile(definitions Map, limits Limits) (*Evaluator, error) {
	limits, err := limits.normalized()
	if err != nil {
		return nil, err
	}
	e := &Evaluator{relations: map[storage.RelationName]*compiledRelation{}, maxSteps: limits.MaxEvaluationSteps}
	for key, definition := range definitions {
		compiled, err := compileRelation(key, definition, limits)
		if err != nil {
			return nil, err
		}
		e.relations[key] = compiled
	}
	// Fixed definitions can additionally validate all cross-relation edges.
	for key, relation := range e.relations {
		for _, edge := range relation.propagate {
			if e.relations[edge.to] == nil {
				return nil, fmt.Errorf("edge %s targets an unconfigured relation", edge.id)
			}
		}
		for _, ref := range relation.definition.Receive {
			parent := e.relations[ref.From]
			found := false
			if parent != nil {
				for _, edge := range parent.propagate {
					found = found || edge.id == ref.ID && edge.to == key
				}
			}
			if !found {
				return nil, fmt.Errorf("unknown received edge %s", ref.ID)
			}
		}
	}
	return e, nil
}
func compileRelation(key storage.RelationName, definition Relation, limits Limits) (*compiledRelation, error) {
	if key.Name == "" {
		return nil, errors.New("policy relation name is required")
	}
	copy := Relation{Propagate: append([]Edge(nil), definition.Propagate...), Allow: append([]Rule(nil), definition.Allow...), Deny: append([]Rule(nil), definition.Deny...)}
	if definition.Receive != nil {
		copy.Receive = append([]EdgeReference{}, definition.Receive...)
	}
	facts := map[string]bool{}
	nodes := 0
	for _, fact := range definition.Facts {
		if fact.ID == "" || facts[fact.ID] {
			return nil, fmt.Errorf("invalid or duplicate fact %q", fact.ID)
		}
		facts[fact.ID] = true
		root, err := plan.Clone(fact.Projection, limits.MaxNodes, limits.MaxDepth)
		if err != nil {
			return nil, fmt.Errorf("fact %s: %w", fact.ID, err)
		}
		var count func(*plan.Node)
		count = func(n *plan.Node) {
			nodes++
			for _, child := range n.Children {
				count(child)
			}
		}
		count(root)
		if nodes > limits.MaxNodes {
			return nil, errors.New("policy definition limit exceeded")
		}
		copy.Facts = append(copy.Facts, Fact{ID: fact.ID, Projection: root})
	}
	out := &compiledRelation{definition: copy}
	seen := map[string]bool{}
	for _, group := range []struct {
		rules  []Rule
		target *[]compiledRule
	}{{copy.Allow, &out.allow}, {copy.Deny, &out.deny}} {
		for _, rule := range group.rules {
			if rule.ID == "" || seen[rule.ID] {
				return nil, fmt.Errorf("invalid or duplicate rule %q", rule.ID)
			}
			seen[rule.ID] = true
			program, err := compileCEL(rule.When, false, facts, limits, &nodes)
			if err != nil {
				return nil, fmt.Errorf("rule %s: %w", rule.ID, err)
			}
			*group.target = append(*group.target, compiledRule{rule.ID, program})
		}
	}
	seen = map[string]bool{}
	for _, edge := range copy.Propagate {
		if edge.ID == "" || edge.To.Name == "" || seen[edge.ID] {
			return nil, errors.New("invalid or duplicate propagation edge")
		}
		seen[edge.ID] = true
		program, err := compileCEL(edge.When, true, nil, limits, &nodes)
		if err != nil {
			return nil, fmt.Errorf("edge %s: %w", edge.ID, err)
		}
		out.propagate = append(out.propagate, compiledEdge{edge.ID, edge.To, program})
	}
	refs := map[EdgeReference]bool{}
	for _, ref := range copy.Receive {
		if ref.From.Name == "" || ref.ID == "" || refs[ref] {
			return nil, errors.New("invalid or duplicate received edge")
		}
		refs[ref] = true
	}
	return out, nil
}
func compileCEL(source string, parent bool, facts map[string]bool, limits Limits, nodes *int) (compiledExpression, error) {
	if source == "" {
		return nil, errors.New("CEL expression is required")
	}
	if len(source) > limits.MaxExpressionBytes {
		return nil, errors.New("policy expression size limit exceeded")
	}
	opts := []cel.EnvOption{cel.Variable("item", cel.DynType), cel.Variable("claims", cel.DynType), cel.ParserExpressionSizeLimit(limits.MaxExpressionBytes), cel.ParserRecursionLimit(limits.MaxDepth)}
	opts = append(opts, decimalOptions()...)
	opts = append(opts, uuidOptions()...)
	if parent {
		opts = append(opts, cel.Variable("parent", cel.DynType))
	} else {
		opts = append(opts, cel.Variable("facts", cel.MapType(cel.StringType, cel.DynType)))
	}
	env, err := cel.NewEnv(opts...)
	if err != nil {
		return nil, err
	}
	ast, issues := env.Compile(source)
	if issues.Err() != nil {
		return nil, issues.Err()
	}
	if ast.OutputType() != cel.BoolType {
		return nil, errors.New("policy CEL expression must return bool")
	}
	// The lazy map supports named reads, not enumeration or presence tests.
	// Validate the parent use of every facts identifier so unsupported map
	// operations cannot silently observe the empty backing map.
	factReads := map[int64]bool{}
	err = walkExpr(ast.Expr(), 1, func(e *exprpb.Expr, depth int) error {
		*nodes++
		if *nodes > limits.MaxNodes || depth > limits.MaxDepth {
			return errors.New("policy expression limit exceeded")
		}
		if selectExpr := e.GetSelectExpr(); selectExpr != nil && selectExpr.Operand.GetIdentExpr().GetName() == "facts" {
			if selectExpr.TestOnly {
				return errors.New("fact presence tests are unsupported; read the declared fact directly")
			}
			factReads[selectExpr.Operand.Id] = true
			if !facts[selectExpr.Field] {
				return fmt.Errorf("unknown fact %q", selectExpr.Field)
			}
		}
		if call := e.GetCallExpr(); call != nil && call.Function == "_[_]" && len(call.Args) == 2 && call.Args[0].GetIdentExpr().GetName() == "facts" {
			factReads[call.Args[0].Id] = true
			id := call.Args[1].GetConstExpr().GetStringValue()
			if !facts[id] {
				return fmt.Errorf("unknown or non-literal fact %q", id)
			}
		}
		if e.GetIdentExpr().GetName() == "facts" && !factReads[e.Id] {
			return errors.New("facts must be accessed by a declared literal name")
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	program, err := env.Program(ast, cel.CustomDecoratorV2(recordAccessDecorator()), cel.CustomDecoratorV2(canonicalOperationsDecorator()), cel.CostLimit(uint64(limits.MaxEvaluationSteps)), cel.InterruptCheckFrequency(1))
	if err != nil {
		return nil, err
	}
	fallback := &celExpression{program: program}
	checked, err := cel.AstToCheckedExpr(ast)
	if err != nil {
		return nil, err
	}
	if root := lowerExpression(checked.Expr, checked.ReferenceMap); root != nil {
		return &fastExpression{root: root, fallback: fallback, limit: uint64(limits.MaxEvaluationSteps)}, nil
	}
	return fallback, nil
}
func walkExpr(e *exprpb.Expr, depth int, visit func(*exprpb.Expr, int) error) error {
	if e == nil {
		return nil
	}
	if err := visit(e, depth); err != nil {
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
		children = append(children, c.IterRange, c.AccuInit, c.LoopCondition, c.LoopStep, c.Result)
	}
	for _, child := range children {
		if err := walkExpr(child, depth+1, visit); err != nil {
			return err
		}
	}
	return nil
}
