package projectionserver

import (
	"context"
	"errors"
	"sync"

	"github.com/adiom-data/weave/internal/accesscontrol"
	"github.com/adiom-data/weave/internal/accesscontrol/policy"
	"github.com/adiom-data/weave/internal/projection/authorization"
	q "github.com/adiom-data/weave/internal/querysubscription/model"
	"github.com/adiom-data/weave/internal/storage"
)

func (s *server) prepareAccess(ctx context.Context, feed q.Feed, id accesscontrol.PreparedID, claims accesscontrol.Claims) (*server, accesscontrol.Prepared, error) {
	if s.policy != nil {
		configuration := s.options.Policies
		access, err := newPolicyAccess(ctx, feed, s.policy, claims, authorization.Options{MaxQueries: configuration.MaxFactQueries, MaxRows: configuration.MaxFactRows, LoadConcurrency: configuration.FactLoadConcurrency})
		if err != nil {
			return nil, nil, err
		}
		copy := *s
		copy.access = access
		copy.policyAccess = access
		s = &copy
	}
	prepared, err := s.access.Prepare(ctx, accesscontrol.PrepareRequest{ID: id, Claims: claims})
	if err != nil && s.policyAccess != nil {
		_ = s.policyAccess.Close()
	}
	return s, prepared, err
}

func (p *policyAccess) watch(ctx context.Context) error {
	requested, awaiting := false, false
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-p.session.FactChanges():
			p.session.Refresh()
			requested = true
		case <-p.session.PolicyChanges():
			p.session.Refresh()
			requested = true
		case <-p.progress:
			requested = true
		case <-p.progressConsumed:
			awaiting = false
		}
		if !requested || awaiting {
			continue
		}
		// RequestProgress queues a report; returning does not mean the
		// connection has consumed it. Coalesce further wakeups until that
		// ordered report reaches the consumer, retaining a follow-up request
		// for facts installed after the first request captured progress.
		requested, awaiting = false, true
		if err := p.session.RequestProgress(ctx); err != nil {
			return err
		}
	}
}

func (p *policyAccess) observe(event q.Event) ([]authorization.Occurrence, error) {
	affected, err := p.session.Observe(event)
	if err == nil && event.Progress != nil {
		select {
		case p.progressConsumed <- struct{}{}:
		default:
		}
	}
	return affected, err
}
func (p *policyAccess) requestProgress() {
	select {
	case p.progress <- struct{}{}:
	default:
	}
}

// policyAccess is a connection-owned adapter for the existing publication
// gate. Views carry partial, immutable evidence at an explicit source position;
// an unresolved decision withholds output and keeps its dependency ownership.
// No historical datastore read is fabricated to satisfy an older snapshot.
type policyAccess struct {
	mu               sync.Mutex
	session          *authorization.Session
	id               accesscontrol.PreparedID
	prepared         bool
	progress         chan struct{}
	progressConsumed chan struct{}
}

func newPolicyAccess(ctx context.Context, feed q.Feed, evaluator *policy.Evaluator, claims accesscontrol.Claims, options authorization.Options) (*policyAccess, error) {
	session, err := authorization.NewSession(ctx, feed, evaluator, claims, options)
	if err != nil {
		return nil, err
	}
	return &policyAccess{session: session, progress: make(chan struct{}, 1), progressConsumed: make(chan struct{}, 1)}, nil
}
func (p *policyAccess) Prepare(_ context.Context, request accesscontrol.PrepareRequest) (accesscontrol.Prepared, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.prepared || request.ID == "" {
		return nil, errors.New("policy session requires one unique prepared context")
	}
	p.id = request.ID
	p.prepared = true
	return p, nil
}
func (p *policyAccess) ID() accesscontrol.PreparedID { return p.id }
func (p *policyAccess) Synchronize(_ context.Context, id accesscontrol.PreparedID, position storage.Checkpoint) (accesscontrol.Checkpoint, error) {
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
func (p *policyAccess) AcquireSnapshot(_ context.Context, fence storage.Fence) (accesscontrol.SnapshotCheckpoint, error) {
	if fence == nil {
		return accesscontrol.SnapshotCheckpoint{}, errors.New("policy snapshot has no fence")
	}
	view, err := p.session.OpenView(&q.Position{SourceID: fence.Source(), Value: fence.Order()})
	return accesscontrol.SnapshotCheckpoint{Fence: fence, View: view}, err
}
func (p *policyAccess) Evaluate(ctx context.Context, view accesscontrol.View, candidates []accesscontrol.Candidate) ([]bool, error) {
	evidence, ok := view.(*authorization.View)
	if !ok {
		return nil, errors.New("policy evaluation requires its session's evidence view")
	}
	visible := make([]bool, len(candidates))
	for i, candidate := range candidates {
		decision, err := p.session.Evaluate(ctx, authorization.Occurrence{Scope: candidate.Context.InstanceID, Key: candidate.Key}, candidate, evidence)
		if err != nil {
			return nil, err
		}
		visible[i] = decision.Visible
	}
	return visible, nil
}
func (p *policyAccess) Release(ctx context.Context, instance string, keys []storage.EntityKey) error {
	if keys == nil {
		return p.session.ForgetScope(ctx, instance)
	}
	var err error
	for _, key := range keys {
		err = errors.Join(err, p.session.Forget(ctx, authorization.Occurrence{Scope: instance, Key: key}))
	}
	return err
}
func (p *policyAccess) Close() error { return p.session.Close() }

var _ accesscontrol.Controller = (*policyAccess)(nil)
var _ accesscontrol.Prepared = (*policyAccess)(nil)
