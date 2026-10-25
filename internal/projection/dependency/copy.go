package dependency

import q "github.com/adiom-data/weave/internal/querysubscription/model"
import "github.com/adiom-data/weave/internal/storage"

func copyPosition(p *q.Position) *q.Position {
	if p == nil {
		return nil
	}
	copy := *p
	return &copy
}
func cloneBounds(bounds q.Bounds) q.Bounds {
	bounds.Left.Cursor = append([]byte(nil), bounds.Left.Cursor...)
	bounds.Right.Cursor = append([]byte(nil), bounds.Right.Cursor...)
	return bounds
}
func cloneMetadata(metadata q.DeliveryMetadata) q.DeliveryMetadata {
	metadata.Checkpoint = copyPosition(metadata.Checkpoint)
	metadata.Fence = copyPosition(metadata.Fence)
	return metadata
}
func cloneBatch(batch q.Batch) q.Batch {
	value := batch
	value.Changes = make([]q.Change, len(batch.Changes))
	for index, change := range batch.Changes {
		change.Record = storage.CloneRecord(change.Record)
		change.Cursor = append([]byte(nil), change.Cursor...)
		value.Changes[index] = change
	}
	value.Membership.Removed = append([]storage.EntityKey(nil), batch.Membership.Removed...)
	value.BindingResets = append([]storage.EntityKey(nil), batch.BindingResets...)
	value.Membership.Insertions = append([]q.Insertion(nil), batch.Membership.Insertions...)
	if batch.Window != nil {
		window := *batch.Window
		window.Bounds = cloneBounds(window.Bounds)
		window.Checkpoint = copyPosition(window.Checkpoint)
		window.Fence = copyPosition(window.Fence)
		value.Window = &window
	}
	return value
}
func cloneEvent(event q.Event) q.Event {
	if event.Progress != nil {
		value := *event.Progress
		value.Position = copyPosition(value.Position)
		return q.Event{Progress: &value}
	}
	if event.Failure != nil {
		value := *event.Failure
		return q.Event{Failure: &value}
	}
	value := *event.Delivery
	value.Delivery = cloneMetadata(value.Delivery)
	value.Effects = make([]q.SubscriptionEffect, len(event.Delivery.Effects))
	for i, effect := range event.Delivery.Effects {
		value.Effects[i] = q.SubscriptionEffect{SubscriptionID: effect.SubscriptionID, Batch: cloneBatch(effect.Batch)}
	}
	value.Entities = make([]q.EntityEffect, len(event.Delivery.Entities))
	for i, effect := range event.Delivery.Entities {
		value.Entities[i] = effect
		if route := effect.Target.Reference; route != nil {
			copy := *route
			copy.Parents = append([]storage.EntityKey(nil), route.Parents...)
			value.Entities[i].Target.Reference = &copy
		}
		value.Entities[i].Delta.Changes = cloneBatch(q.Batch{Changes: effect.Delta.Changes}).Changes
	}
	return q.Event{Delivery: &value}
}
