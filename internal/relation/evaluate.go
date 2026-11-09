package relation

import (
	"context"
	"errors"
	"fmt"

	"cel.dev/cel-go/common/types"
	"cel.dev/cel-go/common/types/ref"
	"cel.dev/cel-go/common/types/traits"
	"github.com/adiom-data/weave/internal/storage"
)

const visibilityField = "$visibility"

// Result is immutable evidence for one computation in one ordered view.
// Dependencies are opaque materialization keys supplied by the fact resolver.
type Result struct {
	Value        storage.Value
	Complete     bool
	Dependencies map[string]struct{}
	Definitions  map[Key]struct{}
}

// Context memoizes one occurrence. Parents must belong to the same evaluation
// pass. It is owned by a serialized view, not shared across mutable clocks.
type Context struct {
	pass         *Evaluation
	input        Input
	parent       *Context
	resolver     Resolver
	allowAll     bool
	remaining    int
	values       map[string]Result
	active       map[string]bool
	facts        map[string]factValue
	exports      map[string]Result // eligibility only; has() never computes an export
	dependencies map[string]struct{}
	definitions  map[Key]struct{}
}

// Evidence includes inputs read before an evaluation failed. Retaining them is
// necessary for a later fact/definition update to recover the occurrence.
func (c *Context) Evidence() (map[string]struct{}, map[Key]struct{}) {
	return c.dependencies, c.definitions
}

func (p *Evaluation) NewContext(input Input, resolver Resolver, parent *Context, allowAll bool) *Context {
	if parent == nil && input.Parent != nil {
		parent = p.NewContext(Input{Candidate: input.Parent.Candidate, Claims: input.Claims}, resolver, nil, allowAll)
	}
	return &Context{pass: p, input: input, parent: parent, resolver: resolver, allowAll: allowAll, remaining: p.owner.maxSteps, values: map[string]Result{}, active: map[string]bool{}, facts: map[string]factValue{}}
}

func boolean(value bool) storage.Value {
	text := "false"
	if value {
		text = "true"
	}
	return storage.Value{Type: storage.TypeBool, State: storage.ValuePresent, Data: []byte(text)}
}
func literalResult(value storage.Value) Result {
	return Result{Value: value, Complete: value.State != storage.ValueUnavailable}
}
func decision(result Result) Decision {
	return Decision{Visible: result.Complete && result.Value.Type == storage.TypeBool && string(result.Value.Data) == "true", Resolved: result.Complete}
}
func mergeDependencies(dst, src map[string]struct{}) {
	for key := range src {
		if dst != nil {
			dst[key] = struct{}{}
		}
	}
}
func (s *evaluation) absorb(result Result) {
	mergeDependencies(s.dependencies, result.Dependencies)
	for key := range result.Definitions {
		if s.definitions != nil {
			s.definitions[key] = struct{}{}
		}
	}
}
func (c *Context) Visibility(ctx context.Context) (Result, error) {
	return c.compute(ctx, visibilityField)
}
func (c *Context) Compute(ctx context.Context, name string) (Result, error) {
	if name == visibilityField {
		return Result{}, errors.New("visibility is not a public computed field")
	}
	return c.compute(ctx, name)
}
func (c *Context) compute(ctx context.Context, name string) (Result, error) {
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}
	if result, ok := c.values[name]; ok {
		return result, nil
	}
	if c.active[name] {
		return Result{}, fmt.Errorf("computed dependency cycle at %q", name)
	}
	if c.remaining <= 0 {
		return Result{}, errors.New("relation evaluation work limit exceeded")
	}
	c.remaining--
	c.active[name] = true
	defer delete(c.active, name)
	if name == visibilityField && c.allowAll {
		r := literalResult(boolean(true))
		c.values[name] = r
		return r, nil
	}
	definition, err := c.pass.load(ctx, relationName(c.input.Candidate.Key.Relation))
	if err != nil {
		return Result{}, err
	}
	s := &evaluation{ctx: ctx, input: c.input, resolver: c.resolver, policy: definition, remaining: &c.remaining, facts: c.facts, owner: c, dependencies: map[string]struct{}{}, definitions: map[Key]struct{}{relationName(c.input.Candidate.Key.Relation): {}}}
	defer func() {
		if c.dependencies == nil {
			c.dependencies = map[string]struct{}{}
			c.definitions = map[Key]struct{}{}
		}
		mergeDependencies(c.dependencies, s.dependencies)
		for key := range s.definitions {
			c.definitions[key] = struct{}{}
		}
	}()
	var value storage.Value
	if name == visibilityField {
		value, err = c.visible(s, definition)
	} else {
		if definition == nil || definition.computed[name] == nil {
			return Result{}, fmt.Errorf("unknown computed field %q on %s.%s", name, c.input.Candidate.Key.Relation.Namespace, c.input.Candidate.Key.Relation.Name)
		}
		field := definition.computed[name]
		value, err = s.value(field.program)
		if err == nil && value.State == storage.ValuePresent && value.Type != field.kind {
			err = fmt.Errorf("computed field %q returned %s, expected %s", name, value.Type, field.kind)
		}
	}
	if err != nil {
		return Result{Dependencies: s.dependencies, Definitions: s.definitions}, fmt.Errorf("%s: %w", name, err)
	}
	result := Result{Value: value, Complete: value.State != storage.ValueUnavailable, Dependencies: s.dependencies, Definitions: s.definitions}
	c.values[name] = result
	return result, nil
}

func (c *Context) visible(s *evaluation, definition *compiledRelation) (storage.Value, error) {
	if definition == nil {
		return boolean(false), nil
	}
	if definition.visibility == nil {
		return boolean(true), nil
	}
	value, err := s.eval(definition.visibility.program)
	if err != nil {
		return storage.Value{}, err
	}
	if value == unknown {
		return storage.Value{State: storage.ValueUnavailable}, nil
	}
	return boolean(value == yes), nil
}

// export checks one named export on the concrete immediate parent. Eligibility
// is memoized independently of its value, including negative/pending results and
// the parent-definition dependency needed to observe a subsequently added export.
func (c *Context) export(s *evaluation, name string) (Result, error) {
	if result, ok := c.exports[name]; ok {
		return result, nil
	}
	if c.exports == nil {
		c.exports = make(map[string]Result)
	}
	result := literalResult(boolean(false))
	if c.parent == nil || c.parent.input.Candidate.Key.Relation.Source != c.input.Candidate.Key.Relation.Source || c.input.Parent != nil && !c.input.Parent.Visible {
		c.exports[name] = result
		return result, nil
	}
	key := relationName(c.parent.input.Candidate.Key.Relation)
	if !receives(s.policy.definition.Receive, key, name) {
		c.exports[name] = result
		return result, nil
	}
	result.Definitions = map[Key]struct{}{key: {}}
	definition, err := c.pass.load(s.ctx, key)
	if err != nil {
		return Result{}, err
	}
	if definition != nil {
		if guardExpression, exists := definition.exports[exportTarget{relationName(c.input.Candidate.Key.Relation), name}]; exists {
			guard, err := s.eval(guardExpression)
			if err != nil {
				return Result{}, fmt.Errorf("export %s.%s.%s: %w", key.Namespace, key.Name, name, err)
			}
			result.Value = boolean(guard == yes)
			if guard == unknown {
				result.Value = storage.Value{State: storage.ValueUnavailable}
				result.Complete = false
			}
		}
	}
	c.exports[name] = result
	return result, nil
}

type computationMap struct {
	traits.Mapper
	state     *evaluation
	inherited bool
}

func newComputationMap(s *evaluation, inherited bool) *computationMap {
	return &computationMap{Mapper: types.NewRefValMap(types.DefaultTypeAdapter, map[ref.Val]ref.Val{}), state: s, inherited: inherited}
}
func (m *computationMap) Find(key ref.Val) (ref.Val, bool) {
	name, ok := key.(types.String)
	if !ok {
		return types.NewErr("computed keys must be strings"), false
	}
	s := m.state
	if s.owner == nil {
		s.failure = errors.New("computed access requires an evaluation context")
		return types.WrapErr(s.failure), false
	}
	var result Result
	var err error
	if !m.inherited {
		result, err = s.owner.Compute(s.ctx, string(name))
	} else {
		result, err = s.owner.export(s, string(name))
		s.absorb(result)
		if err == nil && result.Complete {
			if decision(result).Visible {
				result, err = s.owner.parent.Compute(s.ctx, string(name))
			} else {
				result = literalResult(storage.Value{State: storage.ValueNull})
			}
		}
	}
	s.absorb(result)
	if err != nil {
		s.failure = err
		return types.WrapErr(err), false
	}
	if !result.Complete {
		return types.NewUnknown(5, nil), true
	}
	return valueToCEL(result.Value), true
}
func (m *computationMap) Get(key ref.Val) ref.Val { value, _ := m.Find(key); return value }

func (m *computationMap) presence(name string) (any, bool, error) {
	s := m.state
	result, err := s.owner.export(s, name)
	if err != nil {
		s.failure = err
		return nil, false, err
	}
	s.absorb(result)
	if !result.Complete {
		return types.NewUnknown(5, nil), true, nil
	}
	return nil, decision(result).Visible, nil
}
