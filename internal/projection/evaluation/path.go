package evaluation

import (
	"context"
	"errors"
	"reflect"

	relationdef "github.com/adiom-data/weave/internal/relation"
	"github.com/adiom-data/weave/internal/storage"
)

// pathEvaluation is a claims-specific DAG owned by one immutable evidence
// view. Nodes are concrete captured occurrences, not entity IDs. Only direct
// checks are evaluated here; a child consumes its parent's established result.
// resolverScope pins discovered selectors until the view closes, independently
// of occurrence scopes being replaced or released during publication.
type pathEvaluation struct {
	evaluator     *relationdef.Evaluator
	claims        relationdef.Claims
	resolverScope *Scope
	nodes         map[*relationdef.ProjectionPath]*pathDecision
}

type pathDecision struct {
	state        *relationdef.Context
	decision     relationdef.Decision
	parent       *pathDecision
	dependencies map[string]struct{}
	stamps       map[relationdef.Key]relationdef.Stamp
	err          error
}

// authorize runs under evaluationMu. The returned evidence is adopted by the
// caller's occurrence scope before that lock is released.
func (v *View) authorize(ctx context.Context, evaluator *relationdef.Evaluator, claims relationdef.Claims, candidate relationdef.Candidate) (*pathDecision, *Scope, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	if _, err := v.policyPass(evaluator); err != nil {
		return nil, nil, err
	}
	var evaluation *pathEvaluation
	for _, existing := range v.paths {
		if existing.evaluator == evaluator && reflect.DeepEqual(existing.claims, claims) {
			evaluation = existing
			break
		}
	}
	if evaluation == nil {
		scope, err := NewScope(v.owner)
		if err != nil {
			return nil, nil, err
		}
		evaluation = &pathEvaluation{evaluator: evaluator, claims: claims, resolverScope: scope, nodes: make(map[*relationdef.ProjectionPath]*pathDecision)}
		v.paths = append(v.paths, evaluation)
	}
	parent, err := evaluation.ancestor(ctx, candidate.Context.Projection.Parent(), v)
	if err != nil {
		return parent, evaluation.resolverScope, err
	}
	path := candidate.Context.Projection
	// Explicit invalidation candidates may override the captured leaf image.
	// Such a decision must not populate or consume that captured node's cache.
	cacheable := path != nil && path.Step().Complete == candidate.Complete && reflect.DeepEqual(path.Step().Record, candidate.Record)
	if cacheable {
		if cached := evaluation.nodes[path]; cached != nil {
			return cached, evaluation.resolverScope, cached.err
		}
	}
	result, err := evaluation.check(ctx, candidate, parent, v)
	if cacheable {
		evaluation.nodes[path] = result
	}
	return result, evaluation.resolverScope, err
}

func (p *pathEvaluation) ancestor(ctx context.Context, path *relationdef.ProjectionPath, view *View) (*pathDecision, error) {
	if path == nil {
		return nil, nil
	}
	if result := p.nodes[path]; result != nil {
		return result, result.err
	}
	parent, err := p.ancestor(ctx, path.Parent(), view)
	if err != nil {
		return parent, err
	}
	step := path.Step()
	result, err := p.check(ctx, relationdef.Candidate{Key: step.Key, Record: step.Record, Complete: step.Complete, Context: relationdef.Scope{Projection: path}}, parent, view)
	if result != nil {
		p.nodes[path] = result
	}
	return result, err
}

func (p *pathEvaluation) check(ctx context.Context, candidate relationdef.Candidate, parent *pathDecision, view *View) (*pathDecision, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if candidate.Key.Relation.Source != view.at.SourceID {
		return nil, errors.New("authorization path crosses data sources")
	}
	if parent != nil && !parent.decision.Visible {
		return &pathDecision{parent: parent, decision: parent.decision}, nil
	}
	input := relationdef.Input{Claims: p.claims, Candidate: candidate}
	if path := candidate.Context.Projection.Parent(); path != nil {
		step := path.Step()
		input.Parent = &relationdef.ParentState{Candidate: relationdef.Candidate{Key: step.Key, Record: step.Record, Complete: step.Complete}, Visible: true}
	}
	resolver := &scopeResolver{scope: p.resolverScope, view: view, used: make(map[string]struct{})}
	var parentState *relationdef.Context
	if parent != nil {
		parentState = parent.state
	}
	result, err := evaluateNode(ctx, p.evaluator, input, resolver, view, parentState)
	if result != nil {
		result.parent = parent
	}
	return result, err
}

// evaluateNode is the common single-hop policy operation for direct checks and
// projection graph nodes. Dependency ownership is handled by their callers.
func evaluateNode(ctx context.Context, evaluator *relationdef.Evaluator, input relationdef.Input, resolver *scopeResolver, view *View, parent *relationdef.Context) (*pathDecision, error) {
	if input.Candidate.Key.Relation.Source != view.at.SourceID {
		return nil, errors.New("authorization path crosses data sources")
	}
	pass, err := view.policyPass(evaluator)
	if err != nil {
		return nil, err
	}
	result := &pathDecision{}
	result.state = pass.NewContext(input, resolver, parent, view.owner.allowAll)
	value, err := result.state.Visibility(ctx)
	result.err = err
	result.decision = relationdef.Decision{Visible: value.Complete && value.Value.Type == storage.TypeBool && string(value.Value.Data) == "true", Resolved: value.Complete}
	var keys []relationdef.Key
	dependencies, definitions := result.state.Evidence()
	for key := range definitions {
		keys = append(keys, key)
	}
	result.dependencies, result.stamps = dependencies, pass.Stamps(keys)
	return result, err
}

// adopt preserves the existing occurrence invalidation contract: ancestor fact
// or policy changes invalidate descendants even if their own rows did not
// change. Selectors are retained directly; no ancestor query is rebound.
func (s *Scope) adopt(ctx context.Context, result *pathDecision, source *Scope) error {
	used := make(map[string]struct{})
	stamps := make(map[relationdef.Key]relationdef.Stamp)
	added := make(map[string]*Lease)
	for node := result; node != nil; node = node.parent {
		for key := range node.dependencies {
			used[key] = struct{}{}
			if s.leases[key] == nil && added[key] == nil {
				lease, err := source.leases[key].retain()
				if err != nil {
					for _, lease := range added {
						err = errors.Join(err, lease.Close(ctx))
					}
					return err
				}
				added[key] = lease
			}
		}
		for key, stamp := range node.stamps {
			stamps[key] = stamp
		}
	}
	for key, lease := range added {
		s.leases[key] = lease
	}
	var err error
	for key, lease := range s.leases {
		if _, keep := used[key]; !keep {
			err = errors.Join(err, lease.Close(ctx))
			delete(s.leases, key)
		}
	}
	s.policyStamps = stamps
	return err
}
