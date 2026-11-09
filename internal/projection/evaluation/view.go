package evaluation

import (
	"context"
	"errors"
	"sync"

	q "github.com/adiom-data/weave/internal/querysubscription/model"
	relationdef "github.com/adiom-data/weave/internal/relation"
)

// View memoizes immutable evidence used by one publication attempt. It does
// not pin the whole store or retain a source history. A selector first read
// after the replica has advanced is unresolved, never replaced by newer data.
// Already captured evidence remains valid until this view is closed.
type View struct {
	// Evaluation and Close serialize graph ownership separately from evidence
	// reads. Policy evaluation may resolve evidence through mu below.
	evaluationMu sync.Mutex
	mu           sync.Mutex
	owner        *Facts
	at           *q.Position
	values       map[string]evidence
	closed       bool
	policyOwner  *relationdef.Evaluator
	policies     *relationdef.Evaluation
	paths        []*pathEvaluation
}
type evidence struct {
	result QueryResult
	err    error
}

func (f *Facts) OpenView(at *q.Position) (*View, error) {
	if at == nil || at.SourceID == "" {
		return nil, errors.New("authorization view requires a source position")
	}
	return &View{owner: f, at: copyPosition(at), values: make(map[string]evidence)}, nil
}
func (v *View) Position() *q.Position { return copyPosition(v.at) }
func (v *View) Close() error {
	v.evaluationMu.Lock()
	defer v.evaluationMu.Unlock()
	v.mu.Lock()
	v.closed = true
	clear(v.values)
	v.policies = nil
	paths := v.paths
	v.paths = nil
	v.mu.Unlock()
	var err error
	for _, path := range paths {
		err = errors.Join(err, path.resolverScope.Close(context.Background()))
	}
	return err
}

func (v *View) policyPass(owner *relationdef.Evaluator) (*relationdef.Evaluation, error) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.closed {
		return nil, q.ErrClosed
	}
	if v.policyOwner != nil && v.policyOwner != owner {
		return nil, errors.New("authorization view belongs to another policy evaluator")
	}
	if v.policies == nil {
		v.policyOwner = owner
		v.policies = owner.Begin()
	}
	return v.policies, nil
}
func (v *View) read(lease *Lease) (QueryResult, error) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.closed {
		return QueryResult{}, q.ErrClosed
	}
	if lease.owner != v.owner {
		return QueryResult{}, errors.New("authorization view belongs to another consumer")
	}
	key := lease.entry.key
	if value, exists := v.values[key]; exists {
		return value.result, value.err
	}
	result, err := lease.Read(v.at)
	v.values[key] = evidence{result, err}
	return result, err
}

// needsRetry closes the installation/index-registration gap without replacing
// this view's immutable evidence. It checks readiness only, never copies rows.
func (v *View) needsRetry(lease *Lease) bool {
	v.mu.Lock()
	defer v.mu.Unlock()
	value, read := v.values[lease.entry.key]
	if v.closed || !read || value.err != nil || value.result.Complete {
		return false
	}
	f := lease.owner
	f.mu.Lock()
	defer f.mu.Unlock()
	e := lease.entry
	if lease.closed || f.closed {
		return false
	}
	if e.err != nil {
		return true
	}
	return e.installed && q.SamePosition(v.at, f.position) && e.baseline.SourceID == v.at.SourceID && e.baseline.Value <= v.at.Value
}
