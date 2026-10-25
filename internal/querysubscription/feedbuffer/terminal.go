package feedbuffer

import (
	"slices"

	q "github.com/adiom-data/weave/internal/querysubscription/model"
)

// terminal IDs are retained for the buffer lifetime: subscription IDs cannot
// be reused in a feed. The publication lock orders this transition with every
// Offer, including asynchronous images prepared before the failure occurred.
func (b *Buffer) markTerminalLocked(id string) {
	if b.terminal == nil {
		b.terminal = make(map[string]struct{})
	}
	b.terminal[id] = struct{}{}
}

// withoutSubscriptions never mutates caller-owned events or slices. Complete
// envelopes survive even when all their effects are removed: their ordering
// and checkpoint metadata still belongs to the healthy remainder of the feed.
func withoutSubscriptions(event q.Event, excluded map[string]struct{}) (q.Event, bool) {
	if len(excluded) == 0 {
		return event, true
	}
	if event.Entity != nil {
		_, blocked := excluded[event.Entity.Target.SubscriptionID]
		return event, !blocked
	}
	if event.Failure != nil {
		_, blocked := excluded[event.Failure.SubscriptionID]
		return event, !blocked
	}
	if event.Delivery == nil {
		return event, true
	}
	blockedQuery := func(effect q.SubscriptionEffect) bool {
		_, blocked := excluded[effect.SubscriptionID]
		return blocked
	}
	blockedEntity := func(effect q.EntityEffect) bool {
		_, blocked := excluded[effect.Target.SubscriptionID]
		return blocked
	}
	queries := slices.ContainsFunc(event.Delivery.Effects, blockedQuery)
	entities := slices.ContainsFunc(event.Delivery.Entities, blockedEntity)
	if !queries && !entities {
		return event, true
	}
	envelope := *event.Delivery
	if queries {
		envelope.Effects = slices.DeleteFunc(slices.Clone(envelope.Effects), blockedQuery)
	}
	if entities {
		envelope.Entities = slices.DeleteFunc(slices.Clone(envelope.Entities), blockedEntity)
	}
	event.Delivery = &envelope
	return event, true
}
