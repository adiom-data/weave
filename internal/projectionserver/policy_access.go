package projectionserver

import (
	"context"
	"errors"
	"sync"

	"github.com/adiom-data/weave/internal/accesscontrol"
	"github.com/adiom-data/weave/internal/projection/evaluation"
	"github.com/adiom-data/weave/internal/projection/retry"
	q "github.com/adiom-data/weave/internal/querysubscription/model"
	relationdef "github.com/adiom-data/weave/internal/relation"
	"github.com/adiom-data/weave/internal/storage"
)

func (s *server) prepareAccess(ctx context.Context, feed q.Feed, id accesscontrol.PreparedID, claims accesscontrol.Claims) (*server, accesscontrol.Prepared, error) {
	if s.evaluator != nil {
		configuration := s.options.Relations
		limits := s.options.ProjectionLimits
		access, err := newRelationAccess(ctx, feed, s.evaluator, claims, evaluation.Options{MaxQueries: configuration.MaxFactQueries, MaxRows: configuration.MaxFactRows, LoadConcurrency: configuration.FactLoadConcurrency, Recovery: retry.Policy{InitialDelay: limits.RecoveryInitialDelay, MaxDelay: limits.RecoveryMaxDelay, MaxAttempts: limits.RecoveryMaxAttempts}}, configuration.AllowAll)
		if err != nil {
			return nil, nil, err
		}
		copy := *s
		copy.access = access
		copy.relationAccess = access
		s = &copy
	}
	prepared, err := s.access.Prepare(ctx, accesscontrol.PrepareRequest{ID: id, Claims: claims})
	if err != nil && s.relationAccess != nil {
		_ = s.relationAccess.Close()
	}
	return s, prepared, err
}

// runProgress is the only worker that can block on requesting a source report.
// Fact/policy wakeups are consumed directly by the publication owner instead.
func (p *relationAccess) runProgress(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-p.progress.wake:
			if p.progress.take() {
				if err := p.session.RequestProgress(ctx); err != nil {
					return err
				}
			}
		}
	}
}

func (p *relationAccess) observe(event q.Event) ([]evaluation.Occurrence, error) {
	affected, err := p.session.Observe(event)
	if err == nil {
		if event.Progress != nil {
			p.progress.observe(event.Progress.Position, event.Progress.Revision)
		} else if event.Delivery != nil && event.Delivery.Delivery.Kind == q.DeliveryCommit {
			p.progress.observe(event.Delivery.Delivery.Checkpoint, event.Delivery.Delivery.Revision)
		}
	}
	return affected, err
}

// refresh runs only on the serialized publication consumer. Local evidence
// changes wake evaluation independently of source progress and never advance it.
func (p *relationAccess) refresh() {
	p.session.Refresh()
	p.progress.require(p.session.ProgressTargets()...)
}

func (p *relationAccess) requestProgress(targets ...*q.Position) {
	p.progress.require(targets...)
}

// An already-failed path can retry evidence privately without holding healthy
// branches behind it. Other unresolved checks retain the existing publication
// hold semantics. The session indexes pending occurrences, not every row.
func (p *relationAccess) pendingProjection(v *projectionVisibility) bool {
	if !v.partial {
		return p.session.PendingIn(func(id string) bool { return v.instances[id] != nil })
	}
	return p.session.PendingMatching(func(occurrence evaluation.Occurrence) bool {
		instance := v.instances[occurrence.Scope]
		if instance == nil {
			return false
		}
		key := occurrence.Key
		for instance != nil {
			if instance.failure != nil {
				return false
			}
			if row := instance.rows[key]; row != nil && row.failure != nil {
				return false
			}
			key, instance = instance.open.ParentEntityKey, v.instances[instance.open.ParentInstanceID]
		}
		return true
	})
}

// relationAccess is a connection-owned adapter for the existing publication
// gate. Views carry partial, immutable evidence at an explicit source position;
// an unresolved decision withholds output and keeps its dependency ownership.
// No historical datastore read is fabricated to satisfy an older snapshot.
type relationAccess struct {
	mu       sync.Mutex
	session  *evaluation.Session
	id       accesscontrol.PreparedID
	prepared bool
	progress *policyProgress
}

func newRelationAccess(ctx context.Context, feed q.Feed, evaluator *relationdef.Evaluator, claims accesscontrol.Claims, options evaluation.Options, allowAll bool) (*relationAccess, error) {
	session, err := evaluation.NewSession(ctx, feed, evaluator, claims, options, allowAll)
	if err != nil {
		return nil, err
	}
	return &relationAccess{session: session, progress: newPolicyProgress()}, nil
}
func (p *relationAccess) Prepare(_ context.Context, request accesscontrol.PrepareRequest) (accesscontrol.Prepared, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.prepared || request.ID == "" {
		return nil, errors.New("policy session requires one unique prepared context")
	}
	p.id = request.ID
	p.prepared = true
	return p, nil
}
func (p *relationAccess) ID() accesscontrol.PreparedID { return p.id }
func (p *relationAccess) Synchronize(_ context.Context, id accesscontrol.PreparedID, position storage.Checkpoint) (accesscontrol.Checkpoint, error) {
	if id != p.id || position == nil {
		return accesscontrol.Checkpoint{}, errors.New("policy checkpoint has an invalid context or position")
	}
	view, err := p.session.OpenView(&q.Position{SourceID: position.Source(), Value: position.Order()})
	if err != nil {
		return accesscontrol.Checkpoint{}, err
	}
	var targets []accesscontrol.InvalidationTarget
	for _, occurrence := range p.session.Invalidated() {
		targets = append(targets, accesscontrol.InvalidationTarget{InstanceID: occurrence.Scope, Key: occurrence.Key})
	}
	return accesscontrol.Checkpoint{Position: position, View: view, Invalidation: accesscontrol.Invalidation{Targets: targets}}, nil
}
func (p *relationAccess) AcquireSnapshot(_ context.Context, fence storage.Fence) (accesscontrol.SnapshotCheckpoint, error) {
	if fence == nil {
		return accesscontrol.SnapshotCheckpoint{}, errors.New("policy snapshot has no fence")
	}
	view, err := p.session.OpenView(&q.Position{SourceID: fence.Source(), Value: fence.Order()})
	return accesscontrol.SnapshotCheckpoint{Fence: fence, View: view}, err
}
func (p *relationAccess) Evaluate(ctx context.Context, view accesscontrol.View, candidates []accesscontrol.Candidate) ([]accesscontrol.Decision, error) {
	evidence, ok := view.(*evaluation.View)
	if !ok {
		return nil, errors.New("policy evaluation requires its session's evidence view")
	}
	visible := make([]accesscontrol.Decision, len(candidates))
	for i, candidate := range candidates {
		decision, err := p.session.Evaluate(ctx, evaluation.Occurrence{Scope: candidate.Context.InstanceID, Key: candidate.Key}, candidate, evidence)
		visible[i] = accesscontrol.Decision{Visible: decision.Visible, Err: err, Pending: err == nil && !decision.Resolved}
	}
	return visible, nil
}
func (p *relationAccess) Release(ctx context.Context, instance string, keys []storage.EntityKey) error {
	if keys == nil {
		return p.session.ForgetScope(ctx, instance)
	}
	var err error
	for _, key := range keys {
		err = errors.Join(err, p.session.Forget(ctx, evaluation.Occurrence{Scope: instance, Key: key}))
	}
	return err
}
func (p *relationAccess) Close() error { return p.session.Close() }

var _ accesscontrol.Controller = (*relationAccess)(nil)
var _ accesscontrol.Prepared = (*relationAccess)(nil)

func (p *relationAccess) ComputedValues(instance string, key storage.EntityKey) storage.Record {
	return p.session.ComputedValues(evaluation.Occurrence{Scope: instance, Key: key})
}

// computedResults is an internal capability of the built-in relation evaluator.
// Custom visibility controllers have no computations; they need not expose or
// manage them through the public authorization contract. The publication owner
// reads these immutable results immediately after evaluating the same candidates.
type computedResults interface {
	ComputedValues(string, storage.EntityKey) storage.Record
}

func computedValues(prepared accesscontrol.Prepared, instance string, key storage.EntityKey) storage.Record {
	if values, ok := prepared.(computedResults); ok {
		return values.ComputedValues(instance, key)
	}
	return nil
}
