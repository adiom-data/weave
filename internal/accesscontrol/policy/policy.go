// Package policy compiles CEL visibility rules and immutable, parameterized
// fact projections. It owns no datastore or query subscription machinery.
package policy

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/adiom-data/weave/internal/accesscontrol"
	"github.com/adiom-data/weave/internal/projection/plan"
	"github.com/adiom-data/weave/internal/storage"
)

type Rule struct {
	ID   string
	When string
}
type Fact struct {
	ID         string
	Projection *plan.Node
}
type Edge struct {
	ID   string
	To   storage.RelationName
	When string
}

// EdgeReference scopes an incoming edge to its declaring relation. Separately
// loaded policies cannot accidentally grant access through a colliding edge ID.
type EdgeReference struct {
	From storage.RelationName
	ID   string
}
type Relation struct {
	Propagate []Edge
	// nil accepts any configured incoming edge; non-nil empty accepts none.
	Receive []EdgeReference
	Facts   []Fact
	// Require rules are mandatory constraints, evaluated before any grants.
	// All must pass; an empty list adds no constraints and does not grant access.
	Require []Rule
	// Allow rules are alternative grants, skipped when propagation grants access.
	Allow []Rule
}
type Map map[storage.RelationName]Relation

type Evaluator struct {
	relations map[storage.RelationName]*compiledRelation
	maxSteps  int
	store     *policyStore
}
type ParentState struct {
	Candidate accesscontrol.Candidate
	Visible   bool
}
type Input struct {
	Claims    accesscontrol.Claims
	Candidate accesscontrol.Candidate
	Parent    *ParentState
}
type Decision struct {
	Visible  bool
	Resolved bool
}
type FactRequest struct {
	ID         string
	Projection *plan.Node
	Parameters storage.Record
	WorkLimit  int
}
type FactResult struct {
	Value    storage.Value
	Complete bool
	Work     int
}
type Resolver interface {
	Resolve(context.Context, FactRequest) (FactResult, error)
}

func relationName(key storage.RelationKey) storage.RelationName {
	return storage.RelationName{Namespace: key.Namespace, Name: key.Name}
}

// Evaluation pins policies fetched during one publication pass, independently
// of shared-cache replacement/eviction. Each relation is fetched at most once.
type Evaluation struct {
	mu     sync.Mutex
	owner  *Evaluator
	loaded map[storage.RelationName]policyLoad
}
type policyLoad struct {
	policy *compiledRelation
	stamp  Stamp
	err    error
}

func (e *Evaluator) Begin() *Evaluation {
	return &Evaluation{owner: e, loaded: map[storage.RelationName]policyLoad{}}
}
func (e *Evaluator) Close() error {
	if e.store != nil {
		return e.store.Close()
	}
	return nil
}
func (e *Evaluator) Evaluate(ctx context.Context, input Input, resolver Resolver) (Decision, error) {
	return e.Begin().Evaluate(ctx, input, resolver)
}
func (p *Evaluation) load(ctx context.Context, key storage.RelationName) (*compiledRelation, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if v, ok := p.loaded[key]; ok {
		return v.policy, v.err
	}
	v := policyLoad{}
	if p.owner.store == nil {
		v.policy = p.owner.relations[key]
	} else {
		v.policy, v.stamp, v.err = p.owner.store.Get(ctx, key)
	}
	p.loaded[key] = v
	return v.policy, v.err
}

// Stamps reports only the specified dependencies; another occurrence sharing
// this pass must not become dependent on unrelated policies read earlier.
func (p *Evaluation) Stamps(keys []storage.RelationName) map[storage.RelationName]Stamp {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := map[storage.RelationName]Stamp{}
	for _, key := range keys {
		if v, ok := p.loaded[key]; ok {
			out[key] = v.stamp
		}
	}
	return out
}
func (p *Evaluation) Evaluate(ctx context.Context, input Input, resolver Resolver) (Decision, error) {
	if err := ctx.Err(); err != nil {
		return Decision{}, err
	}
	definition, err := p.load(ctx, relationName(input.Candidate.Key.Relation))
	if err != nil {
		return Decision{}, err
	}
	if definition == nil {
		return Decision{Resolved: true}, nil
	}
	remaining := p.owner.maxSteps
	s := evaluation{ctx: ctx, input: input, resolver: resolver, policy: definition, remaining: &remaining, facts: map[string]factValue{}}
	requirementsPending := false
	for _, rule := range definition.require {
		value, err := s.eval(rule.program)
		if err != nil {
			return Decision{}, fmt.Errorf("require %s: %w", rule.id, err)
		}
		if value == no {
			return Decision{Resolved: true}, nil
		}
		// Unknown is not failure. Discover other evidence while this requirement
		// loads, but never publish a grant until all requirements are satisfied.
		requirementsPending = requirementsPending || value == unknown
	}
	grant := no
	if input.Parent != nil && input.Parent.Visible && input.Parent.Candidate.Key.Relation.Source == input.Candidate.Key.Relation.Source {
		parentKey := relationName(input.Parent.Candidate.Key.Relation)
		parent, err := p.load(ctx, parentKey)
		if err != nil {
			return Decision{}, err
		}
		if parent != nil {
			for _, edge := range parent.propagate {
				if edge.to != relationName(input.Candidate.Key.Relation) || !receives(definition.definition.Receive, parentKey, edge.id) {
					continue
				}
				value, err := s.eval(edge.program)
				if err != nil {
					return Decision{}, fmt.Errorf("propagation %s: %w", edge.id, err)
				}
				grant = or(grant, value)
				if grant == yes {
					break
				}
			}
		}
	}
	if grant != yes {
		for _, rule := range definition.allow {
			value, err := s.eval(rule.program)
			if err != nil {
				return Decision{}, fmt.Errorf("allow %s: %w", rule.id, err)
			}
			grant = or(grant, value)
			if grant == yes {
				break
			}
		}
	}
	if grant == no {
		return Decision{Resolved: true}, nil
	}
	if grant == unknown || requirementsPending {
		return Decision{}, nil
	}
	return Decision{Visible: true, Resolved: true}, nil
}
func receives(refs []EdgeReference, from storage.RelationName, id string) bool {
	if refs == nil {
		return true
	}
	for _, edge := range refs {
		if edge.From == from && edge.ID == id {
			return true
		}
	}
	return false
}

type truth uint8

const (
	no truth = iota
	yes
	unknown
)

func or(a, b truth) truth {
	if a == yes || b == yes {
		return yes
	}
	if a == unknown || b == unknown {
		return unknown
	}
	return no
}

type factValue struct {
	value    storage.Value
	complete bool
	err      error
}
type evaluation struct {
	ctx       context.Context
	input     Input
	resolver  Resolver
	policy    *compiledRelation
	remaining *int
	facts     map[string]factValue
	failure   error
}

func (s *evaluation) step() error {
	if err := s.ctx.Err(); err != nil {
		return err
	}
	if *s.remaining <= 0 {
		return errors.New("authorization evaluation work limit exceeded")
	}
	*s.remaining--
	return nil
}
