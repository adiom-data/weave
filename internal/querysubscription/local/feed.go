package local

import (
	"errors"
	"fmt"

	"github.com/adiom-data/weave/internal/engine/delivery"
	"github.com/adiom-data/weave/internal/querysubscription/model"
	sourceposition "github.com/adiom-data/weave/internal/querysubscription/position"
)

type pendingDelivery struct {
	owners   map[string]*localSubscription
	metadata querysubscription.DeliveryMetadata
	progress bool
	effects  []querysubscription.SubscriptionEffect
}

// acceptProgress joins a report captured by the coordinator to the same chain
// as subscription batches. Reports can arrive before their predecessors have
// finished draining from the native group queue.
func (s *localFeed) acceptProgress(report delivery.ProgressReport) error {
	position := sourceposition.FromSource(report.Checkpoint)
	event := querysubscription.Progress{Position: position, Revision: report.Revision, PreviousRevision: report.PreviousGroupRevision}
	if err := (querysubscription.Event{Progress: &event}).Validate(); err != nil {
		return fmt.Errorf("query engine returned invalid progress report: %w", err)
	}
	// Keep proof independent of metadata transferred to the event consumer.
	proof := *position
	s.feedMu.Lock()
	defer s.feedMu.Unlock()
	if report.Revision <= s.flushedRevision {
		return errors.New("query engine returned a retired progress report")
	}
	if _, exists := s.deliveries[report.Revision]; exists {
		return errors.New("query engine returned a duplicate progress revision")
	}
	if err := s.addDeliveryLocked(report.Revision, &pendingDelivery{
		progress: true,
		metadata: querysubscription.DeliveryMetadata{
			Checkpoint: position, Revision: report.Revision, PreviousRevision: report.PreviousGroupRevision,
		},
	}); err != nil {
		return err
	}
	if err := s.drainFeedLocked(); err != nil {
		return err
	}
	s.rememberSourceProof(proof)
	return nil
}

// addDeliveryLocked bounds the shared reorder buffer regardless of whether a
// revision arrived through the group consumer or a direct progress report.
// Additional members of an existing revision don't consume another slot.
func (s *localFeed) addDeliveryLocked(revision uint64, state *pendingDelivery) error {
	if len(s.deliveries) >= s.bufferLimit {
		return querysubscription.ErrBackpressure
	}
	s.deliveries[revision] = state
	return nil
}

func sameDeliveryMetadata(left, right querysubscription.DeliveryMetadata) bool {
	return left.Kind == right.Kind && left.Atomic == right.Atomic && left.Revision == right.Revision && left.PreviousRevision == right.PreviousRevision &&
		querysubscription.SamePosition(left.Checkpoint, right.Checkpoint) && querysubscription.SamePosition(left.Fence, right.Fence)
}

// drainFeedLocked assembles and enqueues complete deliveries while holding
// feedMu. Queue insertion is nonblocking; keeping it in this critical section
// prevents a concurrent progress report from overtaking earlier effects.
func (s *localFeed) drainFeedLocked() error {
	for {
		var revision uint64
		var state *pendingDelivery
		for candidate, pending := range s.deliveries {
			if pending.metadata.PreviousRevision != s.flushedRevision {
				continue
			}
			if state == nil || candidate < revision {
				revision, state = candidate, pending
			}
		}
		if state == nil {
			return nil
		}
		delete(s.deliveries, revision)
		s.flushedRevision = revision
		s.releaseRetiredLocked()
		if state.progress {
			if err := s.offer(querysubscription.Event{Progress: &querysubscription.Progress{
				Position: state.metadata.Checkpoint, Revision: state.metadata.Revision, PreviousRevision: state.metadata.PreviousRevision,
			}}); err != nil {
				return err
			}
			continue
		}
		effects, entities, jobs, err := s.referenceEffects(state)
		if err != nil {
			return err
		}
		var proof querysubscription.Position
		if state.metadata.Kind == querysubscription.DeliveryCommit && state.metadata.Checkpoint != nil {
			proof = *state.metadata.Checkpoint
		}
		if err := s.offer(querysubscription.Event{Delivery: &querysubscription.DeliveryEnvelope{Delivery: state.metadata, Effects: effects, Entities: entities}}); err != nil {
			return err
		}
		s.rememberSourceProof(proof)
		for _, job := range jobs {
			// Post-publication actions only enqueue bounded work or attach an
			// already-loaded interest; they never perform IO here.
			job()
		}
	}
}

// Resolve parent membership before child effects, independent of which native
// queue drained first. All contributions retain the original delivery metadata.
func (s *localFeed) referenceEffects(state *pendingDelivery) ([]querysubscription.SubscriptionEffect, []querysubscription.EntityEffect, []func(), error) {
	var effects []querysubscription.SubscriptionEffect
	var entities []querysubscription.EntityEffect
	var jobs []func()
	for _, effect := range state.effects {
		owner := state.owners[effect.SubscriptionID]
		// Failure can occur after conversion while this delivery waits for
		// an earlier progress response. Recheck at the publication boundary.
		if owner != nil && owner.discardsQueuedEffects() {
			continue
		}
		if owner != nil && owner.referenceChild != nil {
			continue
		}
		if owner != nil && owner.selector != nil {
			batch := effect.Batch
			entities = append(entities, querysubscription.EntityEffect{Target: querysubscription.EntityTarget{SubscriptionID: effect.SubscriptionID}, Delta: querysubscription.EntityDelta{Clear: batch.Clear, ClearChangeIndex: batch.ClearChangeIndex, Changes: batch.Changes}})
			continue
		}
		if owner != nil && owner.references != nil {
			pending, err := owner.references.advance(&effect.Batch)
			if err != nil {
				owner.references.fail(err)
				continue
			}
			jobs = append(jobs, pending...)
		}
		effects = append(effects, effect)
	}
	for _, effect := range state.effects {
		owner := state.owners[effect.SubscriptionID]
		if owner == nil || owner.discardsQueuedEffects() || owner.referenceChild == nil {
			continue
		}
		child := owner.referenceChild
		change := child.advance(state.metadata, effect.Batch)
		if change == nil {
			continue
		}
		entities = append(entities, *change)
	}
	return effects, entities, jobs, nil
}

func batchHasEffect(batch querysubscription.Batch) bool {
	return batch.Clear || len(batch.Changes) != 0 || len(batch.Membership.Removed) != 0 || len(batch.Membership.Insertions) != 0 || batch.Window != nil
}
