package feedbuffer

import (
	"slices"

	q "github.com/adiom-data/weave/internal/querysubscription/model"
)

// withoutSubscriptions never mutates caller-owned events or slices. Complete
// envelopes survive even when all their effects are removed: their ordering
// and checkpoint metadata still belongs to the healthy remainder of the feed.
func withoutSubscriptions(event q.Event, excluded map[string]struct{}) (q.Event, bool) {
	if len(excluded) == 0 {
		return event, true
	}
	return filterSubscriptions(event, func(id string) bool { _, blocked := excluded[id]; return !blocked }, false)
}

func filterSubscriptions(event q.Event, keep func(string) bool, preserveWindows bool) (q.Event, bool) {
	if event.Entity != nil {
		return event, keep(event.Entity.Target.SubscriptionID)
	}
	if event.Failure != nil {
		return event, keep(event.Failure.SubscriptionID)
	}
	if event.Delivery == nil {
		return event, true
	}
	blockedQuery := func(effect q.SubscriptionEffect) bool {
		return !keep(effect.SubscriptionID)
	}
	blockedEntity := func(effect q.EntityEffect) bool {
		return !keep(effect.Target.SubscriptionID)
	}
	queries := slices.ContainsFunc(event.Delivery.Effects, blockedQuery)
	entities := slices.ContainsFunc(event.Delivery.Entities, blockedEntity)
	if !queries && !entities {
		return event, true
	}
	envelope := *event.Delivery
	if queries {
		envelope.Effects = make([]q.SubscriptionEffect, 0, len(event.Delivery.Effects))
		for _, effect := range event.Delivery.Effects {
			if keep(effect.SubscriptionID) {
				envelope.Effects = append(envelope.Effects, effect)
			} else if preserveWindows && effect.Batch.Window != nil {
				// A request accepted before retirement still needs its ordered
				// completion, but no retired row or membership may escape.
				envelope.Effects = append(envelope.Effects, q.SubscriptionEffect{SubscriptionID: effect.SubscriptionID, Batch: q.Batch{Window: effect.Batch.Window}})
			}
		}
	}
	if entities {
		envelope.Entities = slices.DeleteFunc(slices.Clone(envelope.Entities), blockedEntity)
	}
	event.Delivery = &envelope
	return event, true
}
