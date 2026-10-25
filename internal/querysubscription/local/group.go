package local

import (
	"context"
	"errors"

	"github.com/adiom-data/weave/internal/engine/delivery"
	q "github.com/adiom-data/weave/internal/querysubscription/model"
	sourceposition "github.com/adiom-data/weave/internal/querysubscription/position"
)

// watchGroup is the single native delivery consumer for this feed. Native
// registration and local handle installation are separate bounded operations;
// a delivery may arrive between them. Wait only for the missing local handle,
// not for its snapshot or projection expansion. The native queue remains
// bounded and CDC never waits for this consumer.
func (s *localFeed) watchGroup() {
	for {
		message, err := s.groupQueue.Next(s.ctx)
		if err != nil {
			if !errors.Is(err, delivery.ErrClientClosed) && !errors.Is(err, context.Canceled) {
				if errors.Is(err, delivery.ErrClientQueueFull) {
					err = q.ErrBackpressure
				}
				s.fail(err)
			}
			return
		}
		if err := s.acceptGroup(message); err != nil {
			s.fail(err)
			return
		}
	}
}

func (s *localFeed) groupSubscription(nativeID string) (*localSubscription, error) {
	id, err := s.publicID(nativeID)
	if err != nil {
		return nil, err
	}
	for {
		s.mu.Lock()
		subscription, exists := s.subscriptions[id]
		s.mu.Unlock()
		if subscription != nil {
			return subscription, nil
		}
		if !exists {
			// A closed/failed initialization still participates in the frame's
			// predecessor chain, but must not publish retired effects.
			return &localSubscription{id: id, nativeID: nativeID, owner: s, discard: true}, nil
		}
		select {
		case <-s.ctx.Done():
			return nil, s.ctx.Err()
		case <-s.registered:
		}
	}
}

// acceptGroup consumes one complete native delivery. Membership is represented
// by its contributions, not repeated lists copied into every contribution.
func (s *localFeed) acceptGroup(message delivery.GroupMessage) error {
	if len(message.Batches) > 0 {
		state, failures, err := s.convertGroup(message.Batches)
		if err != nil {
			return err
		}
		s.feedMu.Lock()
		if s.isClosed() {
			s.feedMu.Unlock()
			return q.ErrClosed
		}
		if state.metadata.Revision <= s.flushedRevision {
			s.feedMu.Unlock()
			return errors.New("query engine returned a retired delivery")
		}
		if _, exists := s.deliveries[state.metadata.Revision]; exists {
			s.feedMu.Unlock()
			return errors.New("query engine returned a duplicate delivery")
		}
		err = s.addDeliveryLocked(state.metadata.Revision, state)
		if err == nil {
			err = s.drainFeedLocked()
		}
		s.feedMu.Unlock()
		if err != nil {
			return err
		}
		for _, failure := range failures {
			failure.subscription.failSubscription(failure.cause)
		}
	}
	for _, failure := range message.Failures {
		subscription, err := s.groupSubscription(failure.ID)
		if err != nil {
			return err
		}
		if !subscription.discardsQueuedEffects() {
			subscription.failSubscription(failure.Cause)
		}
		s.retire(failure.ID)
	}
	return nil
}

type failedContribution struct {
	subscription *localSubscription
	cause        error
}

func (s *localFeed) convertGroup(batches []delivery.SubscriptionBatch) (*pendingDelivery, []failedContribution, error) {
	first := batches[0]
	progress := first.Delivery == delivery.DeliveryProgress
	kind, err := deliveryKind(first.Delivery)
	if err != nil {
		return nil, nil, err
	}
	metadata := q.DeliveryMetadata{
		Kind: kind, Checkpoint: sourceposition.FromSource(first.Checkpoint), Fence: sourceposition.FromSource(first.Fence),
		Atomic: first.Atomic, Revision: first.Revision, PreviousRevision: first.PreviousGroupRevision,
	}
	if progress {
		metadata.Kind = 0
		if metadata.Checkpoint == nil || metadata.Revision == 0 {
			return nil, nil, errors.New("query engine returned invalid progress metadata")
		}
	} else if err := metadata.Validate(); err != nil {
		return nil, nil, err
	}
	state := &pendingDelivery{owners: make(map[string]*localSubscription), metadata: metadata, progress: progress}
	var failures []failedContribution
	seen := make(map[string]struct{}, len(batches))
	for _, native := range batches {
		// Check constant-sized metadata per contribution; never rescan the group.
		if native.Delivery != first.Delivery || native.Revision != first.Revision ||
			native.PreviousGroupRevision != first.PreviousGroupRevision || native.Atomic != first.Atomic ||
			!sameDeliveryMetadata(metadata, q.DeliveryMetadata{
				Kind: metadata.Kind, Checkpoint: sourceposition.FromSource(native.Checkpoint), Fence: sourceposition.FromSource(native.Fence),
				Atomic: native.Atomic, Revision: native.Revision, PreviousRevision: native.PreviousGroupRevision,
			}) {
			return nil, nil, errors.New("query engine returned inconsistent grouped delivery metadata")
		}
		if _, duplicate := seen[native.SubscriptionID]; duplicate {
			return nil, nil, errors.New("query engine delivered a subscription twice in one delivery")
		}
		seen[native.SubscriptionID] = struct{}{}
		subscription, err := s.groupSubscription(native.SubscriptionID)
		if err != nil {
			return nil, nil, err
		}
		if subscription.discardsQueuedEffects() {
			continue
		}
		batch, err := subscription.convertBatch(native)
		if err != nil {
			failures = append(failures, failedContribution{subscription, err})
			continue
		}
		if batchHasEffect(batch) {
			state.owners[subscription.id] = subscription
			state.effects = append(state.effects, q.SubscriptionEffect{SubscriptionID: subscription.id, Batch: batch})
		}
	}
	// An owned parent and its children form one failure boundary. Do not
	// publish child data from that boundary when another contribution failed.
	if len(failures) > 0 {
		failedRoots := make(map[string]bool, len(failures))
		rootID := func(sub *localSubscription) string {
			if sub.referenceChild != nil {
				return sub.referenceChild.owner.parent.id
			}
			return sub.id
		}
		for _, failure := range failures {
			failedRoots[rootID(failure.subscription)] = true
		}
		kept := state.effects[:0]
		for _, effect := range state.effects {
			if !failedRoots[rootID(state.owners[effect.SubscriptionID])] {
				kept = append(kept, effect)
			}
		}
		state.effects = kept
	}
	return state, failures, nil
}

// retireAfter is synchronized with delivery publication, including a progress
// response racing an unsubscribe response. No queue control message is needed.
func (s *localFeed) retireAfter(nativeID string, revision uint64) {
	s.feedMu.Lock()
	defer s.feedMu.Unlock()
	if s.isClosed() {
		return
	}
	if revision <= s.flushedRevision {
		s.retire(nativeID)
		return
	}
	if s.retirements == nil {
		s.retirements = make(map[uint64][]string)
	}
	s.retirements[revision] = append(s.retirements[revision], nativeID)
}

func (s *localFeed) isClosed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed
}

func (s *localFeed) releaseRetiredLocked() {
	for _, id := range s.retirements[s.flushedRevision] {
		s.retire(id)
	}
	delete(s.retirements, s.flushedRevision)
}

func (s *localFeed) retire(nativeID string) {
	id, err := s.publicID(nativeID)
	if err != nil {
		s.fail(err)
		return
	}
	s.removeReservation(id)
}
