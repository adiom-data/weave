package projection

import (
	"errors"

	q "github.com/adiom-data/weave/internal/querysubscription/model"
)

// AdmissionWait describes why an initial image cannot yet be published. Wait
// is an activity notification, never a source-position proof. When Progress
// is nonempty the owner should request coverage for those explicit targets.
type AdmissionWait struct {
	Accepted bool
	Wait     <-chan struct{}
	Progress []*q.Position
}

// TryAdmitInitial requires CurrentImageAdmission and the same serialized
// consumer as ApplyEvent. The first call supplies the current image; later
// calls supply coalesced changes only. accept must retain/apply every delta,
// including when it returns false, and may start asynchronous work but not wait
// for IO. No source event can apply between consuming this cut and accepting it.
// Acceptance starts staging post-cut effects until output is released. Source
// transaction messages remain connection-owned, independent of admission.
func (p *Projection) TryAdmitInitial(accept func(ProjectionBatch) (bool, error)) (AdmissionWait, error) {
	if accept == nil {
		return AdmissionWait{}, errors.New("initial admission consumer is required")
	}
	return p.tryCurrentImage(true, func(root string) (bool, error) {
		batch, err := p.initialDelta(root)
		if err != nil {
			return false, err
		}
		return accept(batch)
	})
}

// TryPublishCurrent captures an already-running projection after a publication
// hold. Like initial admission it waits for expansions and their source fences,
// without blocking the feed consumer. Accepted images supersede older dynamic
// publication tokens, but never consume connection-owned transaction messages.
func (p *Projection) TryPublishCurrent(accept func([]ProjectionEvent) (bool, error)) (AdmissionWait, error) {
	if accept == nil {
		return AdmissionWait{}, errors.New("current-image consumer is required")
	}
	return p.tryCurrentImage(false, func(root string) (bool, error) {
		events, err := p.currentEvents(root)
		if err != nil {
			return false, err
		}
		return accept(events)
	})
}

func (p *Projection) tryCurrentImage(initial bool, accept func(string) (bool, error)) (AdmissionWait, error) {
	if accept == nil {
		return AdmissionWait{}, errors.New("initial snapshot admission callback is required")
	}
	p.processMu.Lock()
	defer p.processMu.Unlock()
	select {
	case <-p.ready:
	default:
		return AdmissionWait{}, errors.New("projection initialization has not completed")
	}
	p.mu.Lock()
	if p.terminalErr != nil {
		err := p.terminalErr
		p.mu.Unlock()
		return AdmissionWait{}, err
	}
	if err := p.publication.validateAdmissionLocked(initial); err != nil {
		p.mu.Unlock()
		return AdmissionWait{}, err
	}
	root := p.graph.rootID
	progress, err := pendingAdmissionProgressLocked(&p.graph, p.latestCheckpoints)
	if err != nil {
		p.mu.Unlock()
		return AdmissionWait{}, err
	}
	changed := p.progressChanged
	p.mu.Unlock()
	p.expansions.mu.Lock()
	idle := p.expansions.idle
	p.expansions.mu.Unlock()
	select {
	case <-idle:
	default:
		return AdmissionWait{Wait: idle}, nil
	}
	if len(progress) > 0 {
		return AdmissionWait{Wait: changed, Progress: progress}, nil
	}
	accepted, err := accept(root)
	if err != nil || !accepted {
		return AdmissionWait{}, err
	}
	p.mu.Lock()
	retired := p.graph.acceptCurrentLocked()
	if initial {
		p.publication.acceptInitialLocked()
	}
	p.mu.Unlock()
	for _, id := range retired {
		p.closeInstance(id)
	}
	p.signalRecovery()
	return AdmissionWait{Accepted: true}, nil
}
