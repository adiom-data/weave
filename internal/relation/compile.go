package relation

import (
	"errors"
	"fmt"
	"reflect"
	"slices"
	"sort"

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

type compiledValue struct {
	kind         storage.ValueType
	program      compiledExpression
	dependencies []string
}
type exportTarget struct {
	to    storage.RelationName
	field string
}
type compiledRelation struct {
	definition Relation
	visibility *compiledValue
	computed   map[string]*compiledValue
	exports    map[exportTarget]compiledExpression
}

func Compile(definitions Definitions, limits Limits) (*Evaluator, error) {
	store, err := NewStore(definitions, limits)
	if err != nil {
		return nil, err
	}
	return NewEvaluator(store)
}

func compileDefinitions(definitions Definitions, limits Limits, previous *definitionSet) (*definitionSet, error) {
	next := &definitionSet{relations: make(map[Key]*compiledRelation, len(definitions))}
	for key, definition := range definitions {
		if previous != nil {
			if compiled := previous.relations[key]; compiled != nil && reflect.DeepEqual(compiled.definition, definition) {
				next.relations[key] = compiled
				continue
			}
		}
		compiled, err := compileRelation(key, definition, limits)
		if err != nil {
			return nil, err
		}
		next.relations[key] = compiled
	}
	for key, definition := range next.relations {
		for route := range definition.exports {
			target := next.relations[route.to]
			if target == nil {
				return nil, fmt.Errorf("propagation targets an unconfigured relation %s.%s", route.to.Namespace, route.to.Name)
			}
		}
		for _, incoming := range definition.definition.Receive {
			parent := next.relations[incoming.From]
			if parent == nil {
				return nil, fmt.Errorf("unknown received relation %s.%s", incoming.From.Namespace, incoming.From.Name)
			}
			if _, found := parent.exports[exportTarget{key, incoming.Field}]; !found {
				return nil, fmt.Errorf("unknown received export %s.%s.%s", incoming.From.Namespace, incoming.From.Name, incoming.Field)
			}
		}
	}
	return next, nil
}
func compileRelation(key storage.RelationName, definition Relation, limits Limits) (*compiledRelation, error) {
	if key.Name == "" {
		return nil, errors.New("relation name is required")
	}
	copy := Relation{Facts: slices.Clone(definition.Facts), Visibility: definition.Visibility, Receive: slices.Clone(definition.Receive)}
	if definition.Computed != nil {
		copy.Computed = make(map[string]Computed, len(definition.Computed))
	}
	received := map[Incoming]bool{}
	for _, source := range copy.Receive {
		if source.From.Name == "" || source.Field == "" || source.Field == visibilityField || received[source] {
			return nil, errors.New("invalid or duplicate incoming export")
		}
		received[source] = true
	}
	facts := map[string]bool{}
	nodes := 0
	for i, fact := range definition.Facts {
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
			return nil, errors.New("relation definition limit exceeded")
		}
		copy.Facts[i].Projection = root
	}
	names := map[string]bool{}
	for name, value := range definition.Computed {
		if name == "" || name == visibilityField {
			return nil, fmt.Errorf("invalid computed name %q", name)
		}
		switch value.Type {
		case storage.TypeBool, storage.TypeInteger, storage.TypeFloat, storage.TypeDecimal, storage.TypeText, storage.TypeBytes, storage.TypeUUID, storage.TypeDateTime, storage.TypeList, storage.TypeObject:
		default:
			return nil, fmt.Errorf("computed field %q needs a canonical type", name)
		}
		names[name] = true
		value.Propagate = slices.Clone(value.Propagate)
		copy.Computed[name] = value
	}
	out := &compiledRelation{definition: copy, computed: map[string]*compiledValue{}, exports: map[exportTarget]compiledExpression{}}
	addEdges := func(source string, edges []Propagation) error {
		for _, edge := range edges {
			route := exportTarget{edge.To, source}
			if _, exists := out.exports[route]; edge.To.Name == "" || exists {
				return fmt.Errorf("invalid or duplicate export %q to %s.%s", source, edge.To.Namespace, edge.To.Name)
			}
			program, err := compileCEL(edge.When, true, facts, limits, &nodes)
			if err != nil {
				return fmt.Errorf("export %s to %s.%s: %w", source, edge.To.Namespace, edge.To.Name, err)
			}
			out.exports[route] = program
		}
		return nil
	}
	ordered := make([]string, 0, len(copy.Computed))
	for name := range copy.Computed {
		ordered = append(ordered, name)
	}
	sort.Strings(ordered)
	for _, name := range ordered {
		value := copy.Computed[name]
		field := &compiledValue{kind: value.Type}
		program, err := compileRelationExpression(value.Expression, false, facts, names, expressionOutput{kind: value.Type, nullable: true}, limits, &nodes, &field.dependencies)
		if err != nil {
			return nil, fmt.Errorf("computed %s: %w", name, err)
		}
		field.program = program
		out.computed[name] = field
		if err := addEdges(name, value.Propagate); err != nil {
			return nil, err
		}
	}
	if copy.Visibility != "" {
		program, err := compileRelationExpression(copy.Visibility, false, facts, names, expressionOutput{kind: storage.TypeBool}, limits, &nodes, nil)
		if err != nil {
			return nil, fmt.Errorf("visibility: %w", err)
		}
		out.visibility = &compiledValue{kind: storage.TypeBool, program: program}
	}
	visiting, done := map[string]bool{}, map[string]bool{}
	var visit func(string) error
	visit = func(name string) error {
		if visiting[name] {
			return fmt.Errorf("computed dependency cycle at %q", name)
		}
		if done[name] {
			return nil
		}
		visiting[name] = true
		for _, dep := range out.computed[name].dependencies {
			if err := visit(dep); err != nil {
				return err
			}
		}
		delete(visiting, name)
		done[name] = true
		return nil
	}
	for name := range out.computed {
		if err := visit(name); err != nil {
			return nil, err
		}
	}
	return out, nil
}
func compileCEL(source string, parent bool, facts map[string]bool, limits Limits, nodes *int) (compiledExpression, error) {
	return compileRelationExpression(source, parent, facts, nil, expressionOutput{kind: storage.TypeBool}, limits, nodes, nil)
}
func compileRelationExpression(source string, parent bool, facts, computed map[string]bool, output expressionOutput, limits Limits, nodes *int, dependencies *[]string) (compiledExpression, error) {
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
		opts = append(opts, cel.Variable("facts", cel.MapType(cel.StringType, cel.DynType)), cel.Variable("computed", cel.MapType(cel.StringType, cel.DynType)), cel.Variable("inherited", cel.MapType(cel.StringType, cel.DynType)))
	}
	env, err := cel.NewEnv(opts...)
	if err != nil {
		return nil, err
	}
	ast, issues := env.Compile(source)
	if issues.Err() != nil {
		return nil, issues.Err()
	}
	if err := output.validate(ast.OutputType()); err != nil {
		return nil, err
	}
	// Lazy maps support named reads, not enumeration. Only inherited exports
	// support presence tests, which inspect eligibility without computing values.
	// Validate the parent use of each unshadowed lazy-map identifier so
	// unsupported operations cannot silently observe the empty backing map.
	factReads := map[int64]bool{}
	computedReads := map[int64]bool{}
	inheritedReads := map[int64]bool{}
	inheritedPresenceReads := map[int64]string{}
	err = walkExpr(ast.Expr(), 1, nil, func(e *exprpb.Expr, depth int, scope *expressionScope) error {
		*nodes++
		if *nodes > limits.MaxNodes || depth > limits.MaxDepth {
			return errors.New("policy expression limit exceeded")
		}
		if selectExpr := e.GetSelectExpr(); selectExpr != nil && scope.global(selectExpr.Operand, "facts") {
			if selectExpr.TestOnly {
				return errors.New("fact presence tests are unsupported; read the declared fact directly")
			}
			factReads[selectExpr.Operand.Id] = true
			if !facts[selectExpr.Field] {
				return fmt.Errorf("unknown fact %q", selectExpr.Field)
			}
		}
		if call := e.GetCallExpr(); call != nil && call.Function == "_[_]" && len(call.Args) == 2 && scope.global(call.Args[0], "facts") {
			factReads[call.Args[0].Id] = true
			id := call.Args[1].GetConstExpr().GetStringValue()
			if !facts[id] {
				return fmt.Errorf("unknown or non-literal fact %q", id)
			}
		}
		if x := e.GetSelectExpr(); x != nil {
			if scope.global(x.Operand, "computed") {
				if x.TestOnly || !computed[x.Field] {
					return fmt.Errorf("unknown or unsupported computed read %q", x.Field)
				}
				computedReads[x.Operand.Id] = true
				if dependencies != nil {
					*dependencies = append(*dependencies, x.Field)
				}
			}
			if scope.global(x.Operand, "inherited") {
				if x.Field == "" || x.Field == visibilityField {
					return errors.New("inherited requires a named computed export")
				}
				inheritedReads[x.Operand.Id] = true
				if x.TestOnly {
					inheritedPresenceReads[e.Id] = x.Field
				}
			}
		}
		if x := e.GetCallExpr(); x != nil && x.Function == "_[_]" && len(x.Args) == 2 && scope.global(x.Args[0], "computed") {
			name := x.Args[1].GetConstExpr().GetStringValue()
			if !computed[name] {
				return errors.New("computed fields require declared literal names")
			}
			computedReads[x.Args[0].Id] = true
			if dependencies != nil {
				*dependencies = append(*dependencies, name)
			}
		}
		if x := e.GetCallExpr(); x != nil && x.Function == "_[_]" && len(x.Args) == 2 && scope.global(x.Args[0], "inherited") {
			name := x.Args[1].GetConstExpr().GetStringValue()
			if name == "" || name == visibilityField {
				return errors.New("inherited exports require literal names")
			}
			inheritedReads[x.Args[0].Id] = true
		}
		if scope.global(e, "computed") && !computedReads[e.Id] {
			return errors.New("computed fields require declared literal names")
		}
		if scope.global(e, "inherited") && !inheritedReads[e.Id] {
			return errors.New("inherited exports require literal names")
		}
		if scope.global(e, "facts") && !factReads[e.Id] {
			return errors.New("facts must be accessed by a declared literal name")
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	program, err := env.Program(ast, cel.CustomDecoratorV2(recordAccessDecorator()), cel.CustomDecoratorV2(inheritedPresenceDecorator(inheritedPresenceReads)), cel.CustomDecoratorV2(canonicalOperationsDecorator()), cel.CostLimit(uint64(limits.MaxEvaluationSteps)), cel.InterruptCheckFrequency(1))
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
