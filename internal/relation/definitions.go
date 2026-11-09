// Package relation compiles named computed values, visibility expressions, and private
// fact projections. It owns no datastore or query subscription machinery.
package relation

import (
	"context"
	"errors"

	"github.com/adiom-data/weave/internal/projection/plan"
	"github.com/adiom-data/weave/internal/storage"
)

type Fact struct {
	ID         string
	Projection *plan.Node
}

// Incoming restricts a named export to an immediate parent relation.
type Incoming struct {
	From  storage.RelationName
	Field string
}
type Relation struct {
	Facts    []Fact
	Computed map[string]Computed
	// Empty permits this configured relation. Otherwise this is a lazy boolean
	// expression over the same inputs as Computed. An absent relation denies.
	Visibility string
	// nil accepts any configured incoming export; non-nil empty accepts none.
	Receive []Incoming
}

// Computed defines a lazy, occurrence-specific value. Expression may read item,
// claims, declared facts, local computed values, and inherited.<exportedName>.
// Type is checked without coercion. The definition is independent of selection:
// visibility or descendants may demand a value the client did not select.
// Names beginning with '_' are private and cannot be selected by clients.
type Computed struct {
	Type       storage.ValueType
	Expression string
	Propagate  []Propagation
}

// Propagation exports the declaring computed value under its own name. Only one
// export of that value to each destination relation is allowed. When compares
// the immediate parent/item records; it cannot discover facts or compute values.
type Propagation struct {
	To   storage.RelationName
	When string
}
type Definitions map[storage.RelationName]Relation

type Evaluator struct {
	maxSteps int
	store    *Store
}
type ParentState struct {
	Candidate Candidate
	Visible   bool
}
type Input struct {
	Claims    Claims
	Candidate Candidate
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
	Value        storage.Value
	Complete     bool
	Work         int
	Dependencies map[string]struct{}
}
type Resolver interface {
	Resolve(context.Context, FactRequest) (FactResult, error)
}

func relationName(key storage.RelationKey) storage.RelationName {
	return storage.RelationName{Namespace: key.Namespace, Name: key.Name}
}

// Evaluation pins the entire immutable definition set for one publication pass.
// Replacements cannot change any relation, including one first read later.
type Evaluation struct {
	owner    *Evaluator
	snapshot *definitionSet
}

func (e *Evaluator) Begin() *Evaluation {
	return &Evaluation{owner: e, snapshot: e.store.current.Load()}
}
func (e *Evaluator) Evaluate(ctx context.Context, input Input, resolver Resolver) (Decision, error) {
	return e.Begin().Evaluate(ctx, input, resolver)
}
func (p *Evaluation) load(ctx context.Context, key storage.RelationName) (*compiledRelation, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return p.snapshot.relations[key], nil
}

// Stamps reports only the specified dependencies; another occurrence sharing
// this pass must not become dependent on unrelated policies read earlier.
func (p *Evaluation) Stamps(keys []storage.RelationName) map[storage.RelationName]Stamp {
	out := map[storage.RelationName]Stamp{}
	for _, key := range keys {
		out[key] = Stamp{definition: p.snapshot.relations[key]}
	}
	return out
}
func (p *Evaluation) Evaluate(ctx context.Context, input Input, resolver Resolver) (Decision, error) {
	c := p.NewContext(input, resolver, nil, false)
	result, err := c.Visibility(ctx)
	return decision(result), err
}
func receives(refs []Incoming, from storage.RelationName, field string) bool {
	if refs == nil {
		return true
	}
	for _, edge := range refs {
		if edge.From == from && edge.Field == field {
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

type factValue struct {
	value        storage.Value
	complete     bool
	err          error
	dependencies map[string]struct{}
}
type evaluation struct {
	ctx          context.Context
	input        Input
	resolver     Resolver
	policy       *compiledRelation
	remaining    *int
	facts        map[string]factValue
	failure      error
	owner        *Context
	dependencies map[string]struct{}
	definitions  map[Key]struct{}
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
