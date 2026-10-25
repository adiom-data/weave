package engine

import (
	"context"
	"errors"
	"sync"

	"github.com/adiom-data/weave/internal/engine/delivery"
)

var testSubscriptionQueues sync.Map

func nextTestBatch(ctx context.Context, sub *delivery.Subscription) (delivery.SubscriptionBatch, error) {
	value, ok := testSubscriptionQueues.Load(sub)
	if !ok {
		return delivery.SubscriptionBatch{}, errors.New("test subscription has no opened group")
	}
	for {
		message, err := value.(*delivery.ClientQueue[delivery.GroupMessage]).Next(ctx)
		if err != nil {
			return delivery.SubscriptionBatch{}, err
		}
		if len(message.Failures) > 0 {
			return delivery.SubscriptionBatch{}, message.Failures[0].Cause
		}
		if len(message.Batches) != 1 {
			return delivery.SubscriptionBatch{}, errors.New("expected one subscription in this test group")
		}
		batch := message.Batches[0]
		// These coordinator tests assert data effects; idle commit markers are
		// covered independently by native group and feed-ordering tests.
		if batch.Delivery == delivery.DeliveryCommit && !batch.Clear && len(batch.Events) == 0 && batch.Window == nil {
			continue
		}
		return batch, nil
	}
}
