package local

import (
	"context"
	"errors"

	q "github.com/adiom-data/weave/internal/querysubscription/model"
)

type fenceProgressReport struct {
	done chan struct{}
	err  error // immutable after done closes
}

// ensureFenceProgress reuses source proof already accepted by this feed, or
// joins one in-flight report. It never infers proof from snapshot checkpoints.
// Each caller still validates its own opaque token through the coordinator.
func (s *localFeed) ensureFenceProgress(ctx context.Context, target *q.Position) (*q.Position, error) {
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		s.feedMu.Lock()
		if proof := s.sourceProof; proof != nil && proof.SourceID == target.SourceID && proof.Value >= target.Value {
			copy := *proof
			s.feedMu.Unlock()
			return &copy, nil
		}
		call := s.fenceReport
		if call == nil {
			call = &fenceProgressReport{done: make(chan struct{})}
			s.fenceReport = call
			s.feedMu.Unlock()
			position, err := s.reportProgress(ctx)
			if err == nil && (position == nil || position.SourceID != target.SourceID || position.Value < target.Value) {
				err = errors.New("read fence report does not cover its source position")
			}
			s.feedMu.Lock()
			call.err = err
			s.fenceReport = nil
			close(call.done)
			s.feedMu.Unlock()
			if err != nil {
				return nil, err
			}
			continue
		}
		s.feedMu.Unlock()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-call.done:
		}
		// A canceled leader must not cancel the independent demand of followers.
		if call.err != nil && !errors.Is(call.err, context.Canceled) && !errors.Is(call.err, context.DeadlineExceeded) {
			return nil, call.err
		}
	}
}

// The caller holds feedMu. Both normal CDC delivery and accepted reports can
// provide proof; hydration/window checkpoints cannot advance source history.
func (s *localFeed) rememberSourceProof(position q.Position) {
	if position.SourceID == "" {
		return
	}
	if s.sourceProof == nil {
		copy := position
		s.sourceProof = &copy
	} else if s.sourceProof.SourceID == position.SourceID && s.sourceProof.Value < position.Value {
		*s.sourceProof = position
	}
}
