package projectionserver

import (
	"sync"

	q "github.com/adiom-data/weave/internal/querysubscription/model"
	"github.com/adiom-data/weave/internal/storage"
)

// policyProgress coalesces source-coverage demands. A successful request is
// pinned to its target, not to an assumed reply: RequestProgress may emit no
// report. Only a higher target can request again; ordered feed observation is
// the sole proof that the target has actually been covered.
type policyProgress struct {
	mu      sync.Mutex
	wake    chan struct{}
	sources map[storage.SourceID]*policySourceProgress
}

type policySourceProgress struct {
	required, requested, observed          uint64
	hasRequired, hasRequested, hasObserved bool
	revision                               uint64
}

func newPolicyProgress() *policyProgress {
	return &policyProgress{wake: make(chan struct{}, 1), sources: make(map[storage.SourceID]*policySourceProgress)}
}

func (p *policyProgress) source(id storage.SourceID) *policySourceProgress {
	state := p.sources[id]
	if state == nil {
		state = &policySourceProgress{}
		p.sources[id] = state
	}
	return state
}

func (p *policyProgress) require(targets ...*q.Position) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, target := range targets {
		if target == nil {
			continue
		}
		state := p.source(target.SourceID)
		if !state.hasRequired || target.Value > state.required {
			state.required, state.hasRequired = target.Value, true
		}
		if state.needed() {
			select {
			case p.wake <- struct{}{}:
			default:
			}
		}
	}
}

// Missing and zero positions are deliberately distinct.
func (s *policySourceProgress) needed() bool {
	if !s.hasRequired {
		return false
	}
	if s.hasObserved && s.observed >= s.required {
		return false
	}
	return !s.hasRequested || s.requested < s.required
}

// take captures all current demands before starting one asynchronous request.
// A higher demand arriving during that call leaves one coalesced wake behind.
func (p *policyProgress) take() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	needed := false
	for _, state := range p.sources {
		if state.needed() {
			state.requested, state.hasRequested = state.required, true
			needed = true
		}
	}
	return needed
}

func (p *policyProgress) observe(position *q.Position, revision uint64) {
	if position == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	state := p.source(position.SourceID)
	if !state.hasObserved || state.observed < position.Value {
		state.observed, state.hasObserved = position.Value, true
	}
	if state.revision < revision {
		state.revision = revision
	}
}

// position returns only source progress consumed on this logical feed. A newly
// installed query can reuse that proof without requesting an identical report.
func (p *policyProgress) position(source storage.SourceID) (*q.Position, uint64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	state := p.sources[source]
	if state == nil || !state.hasObserved {
		return nil, 0
	}
	return &q.Position{SourceID: source, Value: state.observed}, state.revision
}
