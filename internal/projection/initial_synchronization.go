package projection

import (
	q "github.com/adiom-data/weave/internal/querysubscription/model"
	"github.com/adiom-data/weave/internal/storage"
)

// initialSynchronization tracks which uncovered snapshot targets have already
// prompted a progress request. It is local to one initialization attempt; no
// wait, network call, or source-proof mutation happens here.
type initialSynchronization struct {
	requested map[storage.SourceID]*q.Position
}

type initialSyncPlan struct {
	covered         bool
	cut             map[storage.SourceID]*q.Position
	progressTargets map[storage.SourceID]*q.Position
}

// inspectLocked borrows graph snapshots and consumed feed proof under the
// runtime locks. A single source snapshot can stand alone; multiple snapshots
// require consumed progress covering every target before a common image cut.
func (s *initialSynchronization) inspectLocked(graph *projectionGraph, observed map[storage.SourceID]*q.Position) (initialSyncPlan, error) {
	targets := make(map[storage.SourceID]*q.Position)
	checkpoints := make(map[storage.SourceID]*q.Position)
	snapshotCount := 0
	for _, instance := range graph.instances {
		position, err := instance.snapshotPosition()
		if err != nil {
			return initialSyncPlan{}, err
		}
		if position == nil {
			continue
		}
		snapshotCount++
		if checkpoint := instance.checkpoint; checkpoint != nil && (checkpoints[checkpoint.SourceID] == nil || checkpoints[checkpoint.SourceID].Value < checkpoint.Value) {
			checkpoints[checkpoint.SourceID] = clonePosition(checkpoint)
		}
		if targets[position.SourceID] == nil || targets[position.SourceID].Value < position.Value {
			targets[position.SourceID] = clonePosition(position)
		}
	}
	// Local empty branches add no independent snapshot to synchronize.
	covered := snapshotCount <= 1
	if !covered {
		covered = true
		for source, target := range targets {
			if current := observed[source]; current == nil || current.Value < target.Value {
				covered = false
				break
			}
		}
	}
	cut := make(map[storage.SourceID]*q.Position, len(targets))
	for source, target := range targets {
		position := checkpoints[source]
		if current := observed[source]; current != nil && (position == nil || current.Value > position.Value) {
			position = current
		}
		// A single table's fence can establish its own snapshot without
		// naming a committed feed checkpoint. Keep that distinction for
		// snapshot authorization rather than fabricating a checkpoint.
		if position != nil && position.Value >= target.Value {
			cut[source] = clonePosition(position)
		}
	}
	progressTargets := make(map[storage.SourceID]*q.Position)
	if !covered {
		for source, target := range targets {
			current := observed[source]
			previous := s.requested[source]
			if (current == nil || current.Value < target.Value) && (previous == nil || previous.Value < target.Value) {
				progressTargets[source] = clonePosition(target)
			}
		}
	}

	return initialSyncPlan{covered: covered, cut: cut, progressTargets: progressTargets}, nil
}

// recordRequest is called only after the runtime successfully requests progress.
// Remember the reason for that request, not the eventual response position.
func (s *initialSynchronization) recordRequest(targets map[storage.SourceID]*q.Position) {
	for source, target := range targets {
		s.requested[source] = target
	}
}

// Admission needs consumed feed proof even for one snapshot, because the outer
// consumer evaluates its separately materialized authorization dependencies.
// Returned report positions retain the distinction between checkpoints and
// fences. Requires Projection.mu and processMu.
func pendingAdmissionProgressLocked(graph *projectionGraph, observed map[storage.SourceID]*q.Position) ([]*q.Position, error) {
	var progress []*q.Position
	for _, instance := range graph.instances {
		target, err := instance.snapshotPosition()
		if err != nil {
			return nil, err
		}
		if target == nil {
			continue
		}
		current := observed[target.Source()]
		if current == nil || current.Order() < target.Order() {
			// The completed checkpoint bounds what a report can promise. A
			// newer fence remains a readiness requirement, not invented progress.
			report := instance.checkpoint
			if report == nil {
				report = target // Best effort for fence-only query clients.
			}
			progress = append(progress, clonePosition(report))
		}
	}
	return progress, nil
}
