// Package deliverytest provides group-backed producers for higher-layer tests.
// It does not emulate the removed private-queue delivery implementation.
package deliverytest

import (
	"errors"
	"sync/atomic"

	"github.com/adiom-data/weave/internal/engine/delivery"
)

type Interest struct {
	*delivery.Subscription
	Queue *Producer
}

type Producer struct {
	Group    *delivery.ClientQueue[delivery.GroupMessage]
	id       string
	revision atomic.Uint64
}

func New(subscription *delivery.Subscription, group *delivery.ClientQueue[delivery.GroupMessage]) *Interest {
	return &Interest{Subscription: subscription, Queue: &Producer{Group: group, id: subscription.ID}}
}

func (p *Producer) Offer(batch delivery.SubscriptionBatch) error {
	batch.SubscriptionID = p.id
	return p.OfferGroup([]delivery.SubscriptionBatch{batch})
}

func (p *Producer) OfferGroup(batches []delivery.SubscriptionBatch) error {
	if err := p.Group.Offer(delivery.GroupMessage{Batches: batches}); err != nil {
		return err
	}
	for _, batch := range batches {
		for previous := p.revision.Load(); batch.Revision > previous; previous = p.revision.Load() {
			if p.revision.CompareAndSwap(previous, batch.Revision) {
				break
			}
		}
	}
	return nil
}

func (p *Producer) RetirementRevision() uint64 { return p.revision.Load() }

func (p *Producer) Retire(cause error) {
	if errors.Is(cause, delivery.ErrClientClosed) {
		return
	}
	_ = p.Group.Offer(delivery.GroupMessage{Failures: []delivery.SubscriptionFailure{{ID: p.id, Cause: cause}}})
}

func (p *Producer) Close(cause error) { p.Retire(cause) }
