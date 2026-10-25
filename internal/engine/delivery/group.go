package delivery

// GroupMessage is one queue entry for an entire connection-local transaction,
// or an ordered set of isolated subscription failures. A transaction touching
// many entities consumes one queue slot, not one slot per entity.
type GroupMessage struct {
	Batches  []SubscriptionBatch
	Failures []SubscriptionFailure
}

type SubscriptionFailure struct {
	ID    string
	Cause error
}

// OpenGroup establishes a shared delivery queue before any subscriptions are
// installed. The coordinator owns the group lifetime; one consumer drains it.
func (h *SubscriptionHub) OpenGroup(group string, limit int) *ClientQueue[GroupMessage] {
	h.mu.Lock()
	defer h.mu.Unlock()
	if queue := h.groups[group]; queue != nil {
		return queue
	}
	queue := NewClientQueue[GroupMessage](limit)
	h.groups[group] = queue
	return queue
}

func (h *SubscriptionHub) retireSubscriptionLocked(subscription *hubSubscription, cause error) {
	_ = h.groups[subscription.public.Group].Offer(GroupMessage{Failures: []SubscriptionFailure{{ID: subscription.public.ID, Cause: cause}}})
}
