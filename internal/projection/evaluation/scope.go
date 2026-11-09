package evaluation

import (
	"context"
	"errors"
	"maps"
	"sync"

	"github.com/adiom-data/weave/internal/projection/plan"
	q "github.com/adiom-data/weave/internal/querysubscription/model"
	relationdef "github.com/adiom-data/weave/internal/relation"
	"github.com/adiom-data/weave/internal/storage"
)

// Scope retains exactly the dependency selectors visited by one occurrence's
// most recent evaluation. Distinct occurrences can depend on the same selector
// without duplicating its live upstream subscription. Within an immutable view,
// concrete ancestors share decisions, but each occurrence owns its dependencies.
type Scope struct {
	values       storage.Record
	mu           sync.Mutex
	facts        *Facts
	leases       map[string]*Lease
	closed       bool
	policyStamps map[relationdef.Key]relationdef.Stamp
}

func NewScope(facts *Facts) (*Scope, error) {
	if facts == nil {
		return nil, errors.New("authorization scope requires fact dependencies")
	}
	return &Scope{facts: facts, leases: make(map[string]*Lease)}, nil
}

// Evaluate runs outside the CDC path. Close/release may perform subscription IO,
// while all discovery loads themselves remain asynchronous and bounded.
func (s *Scope) Evaluate(ctx context.Context, evaluator *relationdef.Evaluator, input relationdef.Input, at *q.Position) (relationdef.Decision, error) {
	view, err := s.facts.OpenView(at)
	if err != nil {
		return relationdef.Decision{}, err
	}
	defer view.Close()
	return s.evaluate(ctx, evaluator, input, view)
}

// EvaluatePath evaluates the next hop after its parent is authorized. The view
// establishes missing parents top down once; siblings reuse their results.
// A denied or unresolved parent prunes descendant work. Dependency ownership
// still spans the complete path so ancestor changes invalidate descendants.
func (s *Scope) EvaluatePath(ctx context.Context, evaluator *relationdef.Evaluator, claims relationdef.Claims, candidate relationdef.Candidate, view *View) (relationdef.Decision, error) {
	path := candidate.Context.Projection
	if path != nil && path.Step().Key != candidate.Key {
		return relationdef.Decision{}, errors.New("policy path ends at another entity")
	}
	if evaluator == nil || view == nil || view.owner != s.facts {
		return relationdef.Decision{}, errors.New("authorization evaluation requires a policy and the candidate's source position")
	}
	view.evaluationMu.Lock()
	defer view.evaluationMu.Unlock()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return relationdef.Decision{}, q.ErrClosed
	}
	result, source, err := view.authorize(ctx, evaluator, claims, candidate)
	if err != nil {
		return relationdef.Decision{}, err
	}
	result, values, err := selectedValues(ctx, result, candidate.Context.Computed, view, evaluator)
	if err != nil {
		return relationdef.Decision{}, err
	}
	if err := s.adopt(ctx, result, source); err != nil {
		return relationdef.Decision{}, err
	}
	s.values = values
	return result.decision, nil
}

func (s *Scope) evaluate(ctx context.Context, evaluator *relationdef.Evaluator, input relationdef.Input, view *View) (relationdef.Decision, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return relationdef.Decision{}, q.ErrClosed
	}
	if evaluator == nil || view == nil || view.owner != s.facts {
		return relationdef.Decision{}, errors.New("authorization evaluation requires a policy and the candidate's source position")
	}
	resolver := &scopeResolver{scope: s, view: view, used: make(map[string]struct{}), added: make(map[string]struct{})}
	result, err := evaluateNode(ctx, evaluator, input, resolver, view, nil)
	if err != nil {
		for key := range resolver.added {
			err = errors.Join(err, s.leases[key].Close(ctx))
			delete(s.leases, key)
		}
		return relationdef.Decision{}, err
	}
	for key, lease := range s.leases {
		if _, used := resolver.used[key]; used {
			continue
		}
		err = errors.Join(err, lease.Close(ctx))
		delete(s.leases, key)
	}
	if err != nil {
		return relationdef.Decision{}, err
	}
	s.policyStamps = result.stamps
	return result.decision, nil
}

func selectedValues(ctx context.Context, original *pathDecision, names []string, view *View, evaluator *relationdef.Evaluator) (*pathDecision, storage.Record, error) {
	if !original.decision.Visible || len(names) == 0 {
		return original, nil, nil
	}
	result := *original
	result.dependencies = maps.Clone(original.dependencies)
	if result.dependencies == nil {
		result.dependencies = map[string]struct{}{}
	}
	result.stamps = maps.Clone(original.stamps)
	if result.stamps == nil {
		result.stamps = map[relationdef.Key]relationdef.Stamp{}
	}
	values := make(storage.Record, len(names))
	pass, err := view.policyPass(evaluator)
	if err != nil {
		return nil, nil, err
	}
	for _, name := range names {
		value, err := result.state.Compute(ctx, name)
		if err != nil {
			return nil, nil, err
		}
		for key := range value.Dependencies {
			result.dependencies[key] = struct{}{}
		}
		var keys []relationdef.Key
		for key := range value.Definitions {
			keys = append(keys, key)
		}
		for key, stamp := range pass.Stamps(keys) {
			result.stamps[key] = stamp
		}
		if !value.Complete {
			result.decision = relationdef.Decision{}
		} else {
			values[name] = value.Value
		}
	}
	return &result, values, nil
}

// ComputedValues borrows immutable results from the last completed evaluation.
func (s *Scope) ComputedValues() storage.Record {
	s.mu.Lock()
	defer s.mu.Unlock()
	return maps.Clone(s.values)
}

func (s *Scope) PolicyStamps() map[relationdef.Key]relationdef.Stamp {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := map[relationdef.Key]relationdef.Stamp{}
	for key, stamp := range s.policyStamps {
		result[key] = stamp
	}
	return result
}

func (s *Scope) DependencyIDs() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	ids := make([]string, 0, len(s.leases))
	for _, lease := range s.leases {
		ids = append(ids, lease.ID())
	}
	return ids
}

func (s *Scope) needsRetry(view *View) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, lease := range s.leases {
		if view.needsRetry(lease) {
			return true
		}
	}
	return false
}

func (s *Scope) Close(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	var err error
	for _, lease := range s.leases {
		err = errors.Join(err, lease.Close(ctx))
	}
	clear(s.leases)
	return err
}

type scopeResolver struct {
	scope       *Scope
	view        *View
	used, added map[string]struct{}
}

func (r *scopeResolver) Resolve(ctx context.Context, request relationdef.FactRequest) (relationdef.FactResult, error) {
	work := 0
	read := &scopeResolver{scope: r.scope, view: r.view, used: map[string]struct{}{}, added: r.added}
	value, complete, err := read.project(ctx, request.Projection, request.Parameters, nil, &work, request.WorkLimit)
	for key := range read.used {
		r.used[key] = struct{}{}
	}
	return relationdef.FactResult{Value: value, Complete: complete, Work: work, Dependencies: read.used}, err
}

func (r *scopeResolver) project(ctx context.Context, node *plan.Node, parameters, parent storage.Record, work *int, limit int) (storage.Value, bool, error) {
	*work++
	if *work > limit {
		return storage.Value{}, false, errors.New("authorization projection work limit exceeded")
	}
	if err := ctx.Err(); err != nil {
		return storage.Value{}, false, err
	}
	query, matchable, err := plan.Bind(node, parameters, parent)
	if err != nil {
		return storage.Value{}, false, err
	}
	result := QueryResult{Complete: true}
	if matchable {
		result, err = r.resolveQuery(ctx, QueryRequest{ID: node.Name, Query: query})
		if err != nil {
			return storage.Value{}, false, err
		}
	}
	if node.Entity && len(result.Rows) > 1 {
		return storage.Value{}, false, errors.New("authorization entity projection matched multiple rows")
	}
	complete := result.Complete
	var records []storage.Value
	for _, row := range result.Rows {
		*work++
		if *work > limit {
			return storage.Value{}, false, errors.New("authorization projection work limit exceeded")
		}
		if !row.Complete {
			complete = false
			continue
		}
		children := map[string]storage.Value{}
		for _, child := range node.Children {
			value, ready, err := r.project(ctx, child, parameters, row.Record, work, limit)
			if err != nil {
				return storage.Value{}, false, err
			}
			children[child.Name] = value
			complete = complete && ready
		}
		records = append(records, storage.Value{Type: storage.TypeObject, State: storage.ValuePresent, Object: map[string]storage.Value{
			"item":     {Type: storage.TypeObject, State: storage.ValuePresent, Object: map[string]storage.Value(row.Record)},
			"children": {Type: storage.TypeObject, State: storage.ValuePresent, Object: children},
		}})
	}
	if node.Entity {
		if len(records) == 0 {
			return storage.Value{State: storage.ValueNull}, complete, nil
		}
		return records[0], complete, nil
	}
	return storage.Value{Type: storage.TypeList, State: storage.ValuePresent, List: records}, complete, nil
}

func (r *scopeResolver) resolveQuery(ctx context.Context, request QueryRequest) (QueryResult, error) {
	if err := ctx.Err(); err != nil {
		return QueryResult{}, err
	}
	key, _, err := queryKey(request.Query)
	if err != nil {
		return QueryResult{}, err
	}
	r.used[key] = struct{}{}
	lease := r.scope.leases[key]
	if lease == nil {
		lease, err = r.scope.facts.Acquire(request)
		if err != nil {
			return QueryResult{}, err
		}
		r.scope.leases[key] = lease
		r.added[key] = struct{}{}
	}
	return r.view.read(lease)
}
