package projection

import (
	"errors"

	q "github.com/adiom-data/weave/internal/querysubscription/model"
	"github.com/adiom-data/weave/internal/storage"
)

// publicationLifecycle owns the initial image, admission journal, and staged
// output until live delivery is released. It performs no IO and adds no locks.
// All methods and borrowed state require Projection.mu; transitions across a
// materialization cut additionally require processMu. The runtime owns readiness
// notifications, source progress, waiting, and delivery of detached output.
//
// Loading completion, admission acceptance, and output release are distinct:
// admission may keep consuming the current image after loading has completed,
// and accepted images must be installed before staged live effects are drained.
type publicationLifecycle struct {
	initializing      bool
	awaitingAdmission bool
	initialTaken      bool
	outputReleased    bool
	readyErr          error
	readyEvents       []ProjectionEvent
	readyPositions    map[storage.SourceID]*q.Position
	initialChanges    *initialChanges
	pendingOutput     []ProjectionBatch
}

func newPublicationLifecycle(admission bool) publicationLifecycle {
	state := publicationLifecycle{initializing: true, awaitingAdmission: admission, initialTaken: admission}
	if admission {
		state.initialChanges = newInitialChanges()
	}
	return state
}

func (s *publicationLifecycle) finishLoadingLocked() { s.initializing = false }

func (s *publicationLifecycle) recordReadyLocked(events []ProjectionEvent, positions map[storage.SourceID]*q.Position, err error, active bool) {
	// Capture already owns its payloads; retain them until the single handoff.
	if active && !s.initialTaken {
		s.readyEvents, s.readyPositions = events, positions
	}
	s.readyErr = err
}

func (s *publicationLifecycle) takeInitialLocked() ([]ProjectionEvent, map[storage.SourceID]*q.Position, error) {
	if s.awaitingAdmission || s.initialTaken {
		return nil, nil, errors.New("projection initial image is unavailable or already taken")
	}
	s.initialTaken = true
	events, positions := s.readyEvents, s.readyPositions
	s.readyEvents, s.readyPositions = nil, nil
	return events, positions, nil
}

func (s *publicationLifecycle) validateAdmissionLocked(initial bool) error {
	if initial && s.outputReleased {
		return errors.New("projection initial image has already been released")
	}
	if initial && s.initialChanges == nil {
		return errors.New("projection requires current-image admission")
	}
	return nil
}

func (s *publicationLifecycle) acceptInitialLocked() {
	s.initialChanges = nil
	s.awaitingAdmission = false
	s.discardLocked()
}

func (s *publicationLifecycle) advanceJournalLocked() {
	s.initialChanges = newInitialChanges()
	s.initialChanges.started = true
}

func (s *publicationLifecycle) stageLocked(batch ProjectionBatch, limit int) error {
	if s.awaitingAdmission {
		// The admission journal already incorporates these effects. Source transaction
		// messages remain connection-owned, independently of this image.
		return nil
	}
	if len(s.pendingOutput) >= limit {
		return q.ErrBackpressure
	}
	s.pendingOutput = append(s.pendingOutput, batch)
	return nil
}

// drainLocked transfers queued batches without enabling live output until a
// subsequent drain observes no remaining work. Runtime consumption happens
// outside its locks and may itself stage more output.
func (s *publicationLifecycle) drainLocked() ([]ProjectionBatch, error) {
	if s.readyErr != nil {
		return nil, s.readyErr
	}
	if s.outputReleased {
		return nil, nil
	}
	if s.awaitingAdmission {
		return nil, errors.New("projection initial image has not been admitted")
	}
	pending := s.pendingOutput
	s.pendingOutput = nil
	if len(pending) == 0 {
		s.outputReleased = true
	}
	return pending, nil
}

func (s *publicationLifecycle) discardLocked() {
	s.readyEvents, s.readyPositions, s.pendingOutput = nil, nil, nil
}
