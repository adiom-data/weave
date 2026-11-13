package projection

import (
	"context"
	"errors"
	"fmt"

	querysubscription "github.com/adiom-data/weave/internal/querysubscription/model"
	"github.com/adiom-data/weave/internal/storage"
)

func (p *Projection) initialize(root *ProjectionNode, synchronize bool, fence string) {
	if fence != "" {
		if _, err := p.queries.WaitForFence(p.ctx, fence); err != nil {
			p.finishReady(nil, nil, err)
			p.shutdown(err)
			return
		}
	}
	spec, matchable, err := root.BuildQuery(nil)
	var opened *projectionInstance
	if err == nil {
		opened, err = p.openNode(root, boundQuery{spec: spec, matchable: matchable}, []string{root.Name}, nil)
	}
	if err != nil && p.partialErrors && p.ctx.Err() == nil && !errors.Is(err, querysubscription.ErrBackpressure) {
		if failed := p.failedNode(root, spec, []string{root.Name}, nil, err); failed != nil {
			opened, err = failed, nil
		}
	}
	if err != nil {
		p.finishReady(nil, nil, err)
		p.shutdown(err)
		return
	}
	rootID := opened.id
	p.mu.Lock()
	p.graph.rootID = rootID
	p.mu.Unlock()
	if synchronize {
		initial, positions, initialErr := p.synchronizedInitialImage(p.ctx)
		p.finishReady(initial, positions, initialErr)
		if initialErr != nil {
			p.shutdown(initialErr)
		}
		return
	}
	for {
		if err := p.expansions.waitIdle(p.ctx); err != nil {
			p.finishReady(nil, nil, err)
			p.shutdown(err)
			return
		}
		p.processMu.Lock()
		// ApplyDelivery schedules expansion work before releasing processMu. If
		// work appeared after waitIdle observed an empty scheduler, retry rather
		// than declaring the projection initialized across that handoff.
		if !p.expansions.isIdle() {
			p.processMu.Unlock()
			continue
		}
		var initial []ProjectionEvent
		var initialErr error
		// Admission owns the first capture. Readiness is only a completion
		// signal in this mode, not an image to copy and immediately discard.
		p.mu.Lock()
		captureInitial := p.publication.initialChanges == nil
		p.mu.Unlock()
		if captureInitial {
			initial, initialErr = p.currentEvents(rootID)
		}
		if initialErr == nil {
			p.mu.Lock()
			p.graph.markAllPublishedLocked()
			p.publication.finishLoadingLocked()
			p.mu.Unlock()
		}
		p.processMu.Unlock()
		p.finishReady(initial, nil, initialErr)
		if initialErr != nil {
			p.shutdown(initialErr)
		}
		return
	}
}

func (p *Projection) finishReady(events []ProjectionEvent, positions map[storage.SourceID]*querysubscription.Position, err error) {
	p.readyOnce.Do(func() {
		p.mu.Lock()
		p.publication.recordReadyLocked(events, positions, err, p.terminalErr == nil)
		p.mu.Unlock()
		close(p.ready)
	})
}

// WaitReady waits for load completion without taking an image. Admission uses
// this signal before TryAdmitInitial; ordinary consumers use TakeInitial.
func (p *Projection) WaitReady(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case <-p.ready:
		p.mu.Lock()
		err := p.publication.readyErr
		if p.terminalErr != nil {
			err = p.terminalErr
		}
		p.mu.Unlock()
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// TakeInitial transfers the reconciled initial image exactly once. The caller
// owns its mutable payloads and must install it before OutputScheduler.Release.
// Positions describe the same captured cut when WaitForSync was requested;
// an unsynchronized image does not advertise a common cut.
// CurrentImageAdmission consumers obtain their image through TryAdmitInitial.
func (p *Projection) TakeInitial(ctx context.Context) ([]ProjectionEvent, map[storage.SourceID]*querysubscription.Position, error) {
	if err := p.WaitReady(ctx); err != nil {
		return nil, nil, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.terminalErr != nil {
		return nil, nil, p.terminalErr
	}
	return p.publication.takeInitialLocked()
}

// synchronizedInitialImage waits until expansion is idle and the ordered feed
// covers every reachable snapshot. Both one-shot and reactive consumers take
// this same initial image and its cut; there is no later recapture path.
// Capturing the image and enabling live delivery share processMu.
func (p *Projection) synchronizedInitialImage(ctx context.Context) ([]ProjectionEvent, map[storage.SourceID]*querysubscription.Position, error) {
	synchronizer := initialSynchronization{requested: make(map[storage.SourceID]*querysubscription.Position)}
	for {
		if err := p.expansions.waitIdle(ctx); err != nil {
			return nil, nil, err
		}
		p.processMu.Lock()
		if !p.expansions.isIdle() {
			p.processMu.Unlock()
			continue
		}
		p.mu.Lock()
		if err := p.ctx.Err(); err != nil {
			p.mu.Unlock()
			p.processMu.Unlock()
			return nil, nil, err
		}
		plan, err := synchronizer.inspectLocked(&p.graph, p.latestCheckpoints)
		if err != nil {
			p.mu.Unlock()
			p.processMu.Unlock()
			return nil, nil, err
		}
		changed := p.progressChanged
		rootID := p.graph.rootID
		captureInitial := p.publication.initialChanges == nil
		p.mu.Unlock()
		if !plan.covered {
			p.processMu.Unlock()
			if len(plan.progressTargets) > 0 {
				if err := p.queries.RequestProgress(ctx); err != nil {
					return nil, nil, fmt.Errorf("request projection progress: %w", err)
				}
				synchronizer.recordRequest(plan.progressTargets)
				continue
			}
			select {
			case <-changed:
				continue
			case <-p.ctx.Done():
				return nil, nil, p.ctx.Err()
			case <-ctx.Done():
				return nil, nil, ctx.Err()
			}
		}
		var events []ProjectionEvent
		if captureInitial {
			events, err = p.currentEvents(rootID)
		}
		if err == nil {
			p.mu.Lock()
			p.graph.markAllPublishedLocked()
			p.publication.finishLoadingLocked()
			p.mu.Unlock()
		}
		p.processMu.Unlock()
		if err != nil {
			return nil, nil, err
		}
		// A synchronized image incorporates every source through its observed
		// cut. Stamp instance events with that common source position so visibility
		// is evaluated against the same cut rather than an older local snapshot.
		for index := range events {
			source := storage.SourceID("")
			if events[index].Checkpoint != nil {
				source = events[index].Checkpoint.SourceID
			} else if events[index].Fence != nil {
				source = events[index].Fence.SourceID
			}
			if position := plan.cut[source]; position != nil {
				events[index].Checkpoint = clonePosition(position)
			}
		}
		return events, plan.cut, nil
	}
}

// releaseOutput synchronously drains staged batches after the caller has
// installed the initial image. The same serialized consumer that calls
// ApplyEvent must process each batch in consume before processing another feed
// event. New output stays staged until this drain is complete. consume runs
// outside projection locks and may complete subtree publications.
func (p *Projection) releaseOutput(consume func(ProjectionBatch) error) error {
	defer p.signalRecovery()
	if consume == nil {
		return errors.New("projection output consumer is required")
	}
	select {
	case <-p.ready:
	default:
		return errors.New("projection is not ready")
	}
	p.releaseMu.Lock()
	defer p.releaseMu.Unlock()
	for {
		p.processMu.Lock()
		p.mu.Lock()
		if p.terminalErr != nil {
			err := p.terminalErr
			p.mu.Unlock()
			p.processMu.Unlock()
			return err
		}
		pending, err := p.publication.drainLocked()
		if err != nil {
			p.mu.Unlock()
			p.processMu.Unlock()
			return err
		}
		p.mu.Unlock()
		p.processMu.Unlock()
		if len(pending) == 0 {
			return nil
		}
		for _, batch := range pending {
			if err := consume(batch); err != nil {
				p.shutdown(err)
				return err
			}
			if err := p.Err(); err != nil {
				return err
			}
		}
	}
}
