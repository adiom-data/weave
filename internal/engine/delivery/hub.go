package delivery

import (
	"errors"
	"sort"
	"sync"

	"github.com/adiom-data/weave/internal/datastore"
	"github.com/adiom-data/weave/internal/engine/changeprocessor"
	"github.com/adiom-data/weave/internal/engine/entitycache"
	"github.com/adiom-data/weave/internal/engine/querywindow"
	"github.com/adiom-data/weave/internal/storage"
)

var ErrSubscriptionExists = errors.New("subscription already exists")

type SubscriptionEvent struct {
	Kind        changeprocessor.QueryChangeKind
	Key         storage.EntityKey
	Record      storage.Record
	Pending     bool
	ChangeIndex uint32
}

type MembershipInsertion struct {
	Key        storage.EntityKey
	FinalIndex int
}

// MembershipDelta transforms the preceding ordered window by removing keys
// first and then applying insertions in ascending final-index order.
type MembershipDelta struct {
	Removed    []storage.EntityKey
	Insertions []MembershipInsertion
}

// SubscriptionBatch is the delivery unit for a subscription. Changes from one
// committed source transaction stay together all the way to the client queue.
type SubscriptionBatch struct {
	SubscriptionID string
	Checkpoint     storage.Checkpoint
	Fence          storage.Fence
	Atomic         bool
	Revision       uint64
	// PreviousGroupRevision orders complete group deliveries and progress reports.
	PreviousGroupRevision uint64
	Delivery              DeliveryKind
	// Clear replaces the subscription's prior membership before Events are
	// applied. It preserves a bulk source operation without queuing one removal
	// per previously visible entity.
	Clear            bool
	ClearChangeIndex uint32
	Events           []SubscriptionEvent
	Membership       MembershipDelta
	Window           *SubscriptionWindowTransition
}

// ProgressReport captures an already-consumed source checkpoint and its place
// in one feed's delivery chain. It has no subscription-specific effects.
type ProgressReport struct {
	Checkpoint            storage.Checkpoint
	Revision              uint64
	PreviousGroupRevision uint64
}

type SubscriptionWindowTransition struct {
	Token      string
	Bounds     datastore.Bounds
	Fence      storage.Fence
	Checkpoint storage.Checkpoint
}

// WindowMembershipDelta is the already-bounded symmetric difference between
// two windows over one shared ordered query. It avoids rescanning unrelated
// shared members in the delivery layer.
type WindowMembershipDelta struct {
	Removed []storage.EntityKey
	Added   []entitycache.EntitySnapshot
}

type windowUpdatePlan struct {
	subscription *hubSubscription
	bounds       datastore.Bounds
	token        string
	revision     uint64
	fence        storage.Fence
	checkpoint   storage.Checkpoint
	events       []SubscriptionEvent
	membership   MembershipDelta
}

type Subscription struct {
	ID    string
	Group string
	Query datastore.QueryFingerprint
}

type hubSubscription struct {
	public   *Subscription
	plan     datastore.PreparedQuery
	bounds   datastore.Bounds
	identity bool
}

// SubscriptionHub fans one shared live-query transition out to independent
// raw client windows. Subject-specific visibility is intentionally absent and
// is evaluated from a stable view by the outer connection delivery layer.
type SubscriptionHub struct {
	mu                sync.Mutex
	byID              map[string]*hubSubscription
	byQuery           map[datastore.QueryFingerprint]map[string]*hubSubscription
	byGroup           map[string]map[string]*hubSubscription
	windows           map[datastore.QueryFingerprint]*windowIndex
	lastGroupRevision map[string]uint64
	groups            map[string]*ClientQueue[GroupMessage]
	order             WindowOrder
}

// WindowOrder exposes only the canonical rank operation needed by delivery.
// The query registry implements it without exposing its tree or sort keys.
// During delivery planning, it must represent the final query state for exactly the
// change batch being planned. The coordinator's single-writer actor guarantees
// that query state cannot advance until planning has captured every index.
type WindowOrder interface {
	PositionInBounds(datastore.QueryFingerprint, datastore.Bounds, storage.EntityKey) (int, bool, error)
}

func NewSubscriptionHub(order WindowOrder) *SubscriptionHub {
	return &SubscriptionHub{
		byID:              make(map[string]*hubSubscription),
		byQuery:           make(map[datastore.QueryFingerprint]map[string]*hubSubscription),
		byGroup:           make(map[string]map[string]*hubSubscription),
		windows:           make(map[datastore.QueryFingerprint]*windowIndex),
		lastGroupRevision: make(map[string]uint64),
		groups:            make(map[string]*ClientQueue[GroupMessage]),
		order:             order,
	}
}

// FilterOwnedInitial performs every fallible query evaluation needed before a
// subscription is registered with the hub. It consumes caller-owned snapshots:
// kept records transfer to the result without copying; neither the hub nor the
// source cache retains them.
func FilterOwnedInitial(plan datastore.PreparedQuery, bounds datastore.Bounds, initial []entitycache.EntitySnapshot) ([]entitycache.EntitySnapshot, error) {
	filtered := make([]entitycache.EntitySnapshot, 0, len(initial))
	for _, entity := range initial {
		var contained bool
		var err error
		if _, identity := plan.(datastore.IdentityQuery); identity {
			contained, err = plan.Matches(entity.Record)
		} else {
			contained, err = querywindow.ContainsRecord(plan, entity.Record, bounds)
		}
		if err != nil {
			return nil, err
		}
		if !contained {
			continue
		}
		filtered = append(filtered, entity)
	}
	return filtered, nil
}

// SubscribePreparedGrouped registers a window in an explicitly opened group.
// Initial row validation and ownership remain with the coordinator.
func (h *SubscriptionHub) SubscribePreparedGrouped(id, group string, plan datastore.PreparedQuery, bounds datastore.Bounds) (*Subscription, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if id == "" {
		return nil, errors.New("subscription ID is required")
	}
	if h.byID[id] != nil {
		return nil, ErrSubscriptionExists
	}
	if h.groups[group] == nil {
		return nil, errors.New("delivery group must be opened before subscribing")
	}
	public := &Subscription{ID: id, Group: group, Query: plan.Fingerprint()}
	state := &hubSubscription{public: public, plan: plan, bounds: bounds}
	_, state.identity = plan.(datastore.IdentityQuery)
	if !state.identity {
		index := h.windows[plan.Fingerprint()]
		if index == nil {
			index = newWindowIndex(plan)
		}
		if err := index.add(id, bounds); err != nil {
			return nil, err
		}
		h.windows[plan.Fingerprint()] = index
	}
	h.byID[id] = state
	set := h.byQuery[plan.Fingerprint()]
	if set == nil {
		set = make(map[string]*hubSubscription)
		h.byQuery[plan.Fingerprint()] = set
	}
	set[id] = state
	if h.byGroup[group] == nil {
		h.byGroup[group] = make(map[string]*hubSubscription)
	}
	h.byGroup[group][id] = state
	return public, nil
}

type PublishResult struct {
	Dropped []string
}

type pendingDelivery struct {
	subscription *hubSubscription
	clear        bool
	clearIndex   uint32
	events       []SubscriptionEvent
	transitions  map[storage.EntityKey]*membershipTransition
	membership   MembershipDelta
}

type membershipTransition struct {
	beforePresent bool
	beforeKey     datastore.SortKey
	afterPresent  bool
	afterKey      datastore.SortKey
}

// deliveryPlan is an immutable capture of the subscriptions and cursor bounds
// affected when one source transaction was applied.
type deliveryPlan struct {
	checkpoint storage.Checkpoint
	fence      storage.Fence
	atomic     bool
	revision   uint64
	delivery   DeliveryKind
	pending    map[string]*pendingDelivery
}

func (h *SubscriptionHub) PublishDelivery(checkpoint storage.Checkpoint, atomic bool, revision uint64, delivery DeliveryKind, changes []changeprocessor.QueryChange) PublishResult {
	plan, result := h.planDelivery(checkpoint, nil, atomic, revision, delivery, changes)
	committed := h.commitDelivery(plan)
	result.Dropped = append(result.Dropped, committed.Dropped...)
	return result
}

// CaptureGroupProgress inserts a report into the group's delivery chain. The
// caller must deliver the returned marker even if its request is canceled;
// subsequent deliveries refer to this revision as their predecessor.
func (h *SubscriptionHub) CaptureGroupProgress(group string, checkpoint storage.Checkpoint, revision uint64) ProgressReport {
	h.mu.Lock()
	defer h.mu.Unlock()
	report := ProgressReport{Checkpoint: checkpoint, Revision: revision, PreviousGroupRevision: h.lastGroupRevision[group]}
	h.lastGroupRevision[group] = revision
	return report
}

func (h *SubscriptionHub) PublishHydration(fence storage.Fence, checkpoint storage.Checkpoint, revision uint64, changes []changeprocessor.QueryChange) PublishResult {
	plan, result := h.planDelivery(checkpoint, fence, false, revision, DeliveryHydration, changes)
	committed := h.commitDelivery(plan)
	result.Dropped = append(result.Dropped, committed.Dropped...)
	return result
}

func (h *SubscriptionHub) planDelivery(checkpoint storage.Checkpoint, fence storage.Fence, atomic bool, revision uint64, delivery DeliveryKind, changes []changeprocessor.QueryChange) (*deliveryPlan, PublishResult) {
	h.mu.Lock()
	defer h.mu.Unlock()
	pending := make(map[string]*pendingDelivery)
	result := PublishResult{}
	for _, change := range changes {
		candidates, candidateErr := h.windowCandidates(change)
		if candidateErr != nil {
			// Query evaluation errors remain isolated by the established per-window
			// path; the index is an optimization and never a correctness gate.
			candidates = h.byQuery[change.Query]
		}
		for id, subscription := range candidates {
			queued := pending[id]
			if queued == nil {
				queued = &pendingDelivery{subscription: subscription, transitions: make(map[storage.EntityKey]*membershipTransition)}
				pending[id] = queued
			}
			if change.Kind == changeprocessor.QueryCleared {
				queued.clear = true
				queued.clearIndex = change.ChangeIndex
				// Changes preceding a truncate in the same source transaction
				// cannot affect its committed query result.
				queued.events = nil
				clear(queued.transitions)
				continue
			}
			var wasMember bool
			var err error
			if subscription.identity {
				wasMember = change.Kind != changeprocessor.QueryAdded && change.PreviousRecord != nil
			} else {
				wasMember, err = changeWasVisibleInWindow(subscription, change)
			}
			if err != nil {
				dropSubscriptionLocked(h, id, subscription, err)
				delete(pending, id)
				result.Dropped = append(result.Dropped, id)
				continue
			}
			isMember := false
			if change.Kind != changeprocessor.QueryRemoved {
				contained := true
				var err error
				if !subscription.identity {
					contained, err = querywindow.ContainsRecord(subscription.plan, change.Record, subscription.bounds)
				}
				if err != nil {
					dropSubscriptionLocked(h, id, subscription, err)
					delete(pending, id)
					result.Dropped = append(result.Dropped, id)
					continue
				}
				isMember = contained
			}
			var event *SubscriptionEvent
			switch {
			case wasMember && !isMember:
				event = &SubscriptionEvent{Kind: changeprocessor.QueryRemoved, Key: change.Key, ChangeIndex: change.ChangeIndex}
			case !wasMember && isMember:
				event = &SubscriptionEvent{Kind: changeprocessor.QueryAdded, Key: change.Key, Record: storage.CloneRecord(change.Record), Pending: change.Pending, ChangeIndex: change.ChangeIndex}
			case wasMember && isMember:
				event = &SubscriptionEvent{Kind: changeprocessor.QueryUpdated, Key: change.Key, Record: storage.CloneRecord(change.Record), Pending: change.Pending, ChangeIndex: change.ChangeIndex}
			}
			if event != nil {
				queued.events = append(queued.events, *event)
				if err := captureMembershipTransition(subscription.plan, queued, change.Key, change.PreviousRecord, change.Record, wasMember, isMember); err != nil {
					dropSubscriptionLocked(h, id, subscription, err)
					delete(pending, id)
					result.Dropped = append(result.Dropped, id)
					continue
				}
			} else if !queued.clear && len(queued.events) == 0 {
				delete(pending, id)
			}
		}
		if len(h.byQuery[change.Query]) == 0 {
			delete(h.byQuery, change.Query)
			delete(h.windows, change.Query)
		}
	}
	for id, queued := range pending {
		if err := h.finalizeMembership(queued); err != nil {
			dropSubscriptionLocked(h, id, queued.subscription, err)
			delete(pending, id)
			result.Dropped = append(result.Dropped, id)
		}
	}
	// Every active connection group receives one internal commit marker even
	// when none of its data queries changed. This is the transaction clock used
	// by higher delivery layers to join authorization invalidations (and similar
	// side streams) at the same source checkpoint. Empty markers never become
	// client-visible model batches.
	if (delivery == DeliveryCommit || delivery == DeliveryProgress) && checkpoint != nil {
		markedGroups := make(map[string]struct{})
		for _, queued := range pending {
			if group := queued.subscription.public.Group; group != "" {
				markedGroups[group] = struct{}{}
			}
		}
		for group, subscriptions := range h.byGroup {
			if group == "" {
				continue
			}
			if _, marked := markedGroups[group]; marked {
				continue
			}
			for _, subscription := range subscriptions {
				pending[subscription.public.ID] = &pendingDelivery{subscription: subscription}
				break
			}
		}
	}
	return &deliveryPlan{checkpoint: checkpoint, fence: fence, atomic: atomic, revision: revision, delivery: delivery, pending: pending}, result
}

func captureMembershipTransition(plan datastore.PreparedQuery, queued *pendingDelivery, key storage.EntityKey, beforeRecord, afterRecord storage.Record, beforePresent, afterPresent bool) error {
	if queued.subscription.identity {
		transition := queued.transitions[key]
		if transition == nil {
			transition = &membershipTransition{beforePresent: beforePresent}
			queued.transitions[key] = transition
		}
		transition.afterPresent = afterPresent
		return nil
	}
	transition := queued.transitions[key]
	if transition == nil {
		transition = &membershipTransition{beforePresent: beforePresent}
		if beforePresent {
			beforeKey, err := plan.Key(beforeRecord)
			if err != nil {
				return err
			}
			transition.beforeKey = beforeKey
		}
		queued.transitions[key] = transition
	}
	transition.afterPresent = afterPresent
	transition.afterKey = nil
	if afterPresent {
		afterKey, err := plan.Key(afterRecord)
		if err != nil {
			return err
		}
		transition.afterKey = afterKey
	}
	return nil
}

func (h *SubscriptionHub) finalizeMembership(queued *pendingDelivery) error {
	if len(queued.transitions) == 0 {
		return nil
	}
	if h.order == nil {
		return errors.New("subscription delivery requires canonical window ordering")
	}
	for key, transition := range queued.transitions {
		moved := false
		if !queued.subscription.identity && transition.beforePresent && transition.afterPresent {
			compared, err := queued.subscription.plan.Compare(transition.beforeKey, transition.afterKey)
			if err != nil {
				return err
			}
			moved = compared != 0
		}
		if transition.beforePresent && (!transition.afterPresent || moved) {
			queued.membership.Removed = append(queued.membership.Removed, key)
		}
		if transition.afterPresent && (!transition.beforePresent || moved) {
			index, present, err := h.order.PositionInBounds(queued.subscription.public.Query, queued.subscription.bounds, key)
			if err != nil {
				return err
			}
			if !present {
				return errors.New("final query member is absent from its subscription window")
			}
			queued.membership.Insertions = append(queued.membership.Insertions, MembershipInsertion{Key: key, FinalIndex: index})
		}
	}
	sort.Slice(queued.membership.Insertions, func(i, j int) bool {
		return queued.membership.Insertions[i].FinalIndex < queued.membership.Insertions[j].FinalIndex
	})
	return nil
}

func (h *SubscriptionHub) windowCandidates(change changeprocessor.QueryChange) (map[string]*hubSubscription, error) {
	all := h.byQuery[change.Query]
	if change.Kind == changeprocessor.QueryCleared || len(all) == 0 {
		return all, nil
	}
	index := h.windows[change.Query]
	if index == nil {
		return all, nil
	}
	selected := make(map[string]*hubSubscription)
	collect := func(record storage.Record) error {
		key, err := index.plan.Key(record)
		if err != nil {
			return err
		}
		ids, err := index.containing(key)
		if err != nil {
			return err
		}
		for _, id := range ids {
			if subscription := all[id]; subscription != nil {
				selected[id] = subscription
			}
		}
		return nil
	}
	if change.Kind != changeprocessor.QueryAdded && change.PreviousRecord != nil {
		if err := collect(change.PreviousRecord); err != nil {
			return nil, err
		}
	}
	if change.Kind != changeprocessor.QueryRemoved {
		if err := collect(change.Record); err != nil {
			return nil, err
		}
	}
	return selected, nil
}

// commitDelivery offers a previously captured plan to client queues. Targets
// that were unsubscribed or replaced while the plan waited are ignored; newly
// registered subscriptions can never receive the older transaction.
func (h *SubscriptionHub) commitDelivery(plan *deliveryPlan) PublishResult {
	if plan == nil {
		return PublishResult{}
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	pending := make(map[string]*pendingDelivery, len(plan.pending))
	for id, queued := range plan.pending {
		if h.byID[id] == queued.subscription {
			pending[id] = queued
		}
	}
	result := PublishResult{}
	groups := make(map[string][]string)
	for id := range pending {
		if subscription := h.byID[id]; subscription != nil {
			groups[subscription.public.Group] = append(groups[subscription.public.Group], id)
		}
	}
	for group, ids := range groups {
		sort.Strings(ids)
		queue := h.groups[group]
		batches := make([]SubscriptionBatch, 0, len(ids))
		previous := h.lastGroupRevision[group]
		for _, id := range ids {
			queued := pending[id]
			batches = append(batches, SubscriptionBatch{SubscriptionID: id, Checkpoint: plan.checkpoint, Fence: plan.fence, Atomic: plan.atomic, Revision: plan.revision, PreviousGroupRevision: previous, Delivery: plan.delivery, Clear: queued.clear, ClearChangeIndex: queued.clearIndex, Events: queued.events, Membership: queued.membership})
			delete(pending, id)
		}
		if err := queue.Offer(GroupMessage{Batches: batches}); err != nil {
			result.Dropped = append(result.Dropped, h.dropGroupLocked(group, err)...)
		} else {
			h.lastGroupRevision[group] = plan.revision
		}
	}
	return result
}

func (h *SubscriptionHub) QueueWindowTransition(id string, bounds datastore.Bounds, delta WindowMembershipDelta, token string, revision uint64, fence storage.Fence, checkpoint storage.Checkpoint) ([]SubscriptionEvent, error) {
	h.mu.Lock()
	subscription := h.byID[id]
	if subscription == nil {
		h.mu.Unlock()
		return nil, errors.New("subscription not found")
	}
	events := make([]SubscriptionEvent, 0, len(delta.Removed)+len(delta.Added))
	for _, key := range delta.Removed {
		events = append(events, SubscriptionEvent{Kind: changeprocessor.QueryRemoved, Key: key})
	}
	for _, entity := range delta.Added {
		events = append(events, SubscriptionEvent{Kind: changeprocessor.QueryAdded, Key: entity.Key, Record: storage.CloneRecord(entity.Record), Pending: entity.State != entitycache.EntityCurrent})
	}
	membership, err := h.windowMembership(subscription, bounds, delta.Removed, delta.Added)
	if err != nil {
		h.mu.Unlock()
		return nil, err
	}
	plan := &windowUpdatePlan{subscription: subscription, bounds: bounds, token: token, revision: revision, fence: fence, checkpoint: checkpoint, events: events, membership: membership}
	h.mu.Unlock()
	return events, h.commitWindowUpdate(plan)
}

func (h *SubscriptionHub) commitWindowUpdate(plan *windowUpdatePlan) error {
	if plan == nil {
		return errors.New("window update plan is required")
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	subscription := h.byID[plan.subscription.public.ID]
	if subscription != plan.subscription {
		return errors.New("subscription changed while window update was pending")
	}
	if index := h.windows[subscription.public.Query]; index != nil {
		if err := index.replace(subscription.public.ID, subscription.bounds, plan.bounds); err != nil {
			return err
		}
	}
	subscription.bounds = plan.bounds
	batch := SubscriptionBatch{Checkpoint: plan.checkpoint, Fence: plan.fence, Revision: plan.revision, Events: plan.events, Membership: plan.membership, Window: &SubscriptionWindowTransition{Token: plan.token, Bounds: plan.bounds, Fence: plan.fence, Checkpoint: plan.checkpoint}}
	if group := subscription.public.Group; group != "" {
		batch.Delivery = DeliveryWindow
		batch.PreviousGroupRevision = h.lastGroupRevision[group]
	}
	batch.SubscriptionID = subscription.public.ID
	offerErr := h.groups[subscription.public.Group].Offer(GroupMessage{Batches: []SubscriptionBatch{batch}})
	if err := offerErr; err != nil {
		id := subscription.public.ID
		dropSubscriptionLocked(h, id, subscription, err)
		return err
	}
	if group := subscription.public.Group; group != "" {
		h.lastGroupRevision[group] = plan.revision
	}
	return nil
}

func (h *SubscriptionHub) windowMembership(subscription *hubSubscription, bounds datastore.Bounds, removed []storage.EntityKey, added []entitycache.EntitySnapshot) (MembershipDelta, error) {
	if h.order == nil && len(added) > 0 {
		return MembershipDelta{}, errors.New("subscription delivery requires canonical window ordering")
	}
	delta := MembershipDelta{Removed: append([]storage.EntityKey(nil), removed...)}
	for _, entity := range added {
		index, present, err := h.order.PositionInBounds(subscription.public.Query, bounds, entity.Key)
		if err != nil {
			return MembershipDelta{}, err
		}
		if !present {
			return MembershipDelta{}, errors.New("added query member is absent from its new subscription window")
		}
		delta.Insertions = append(delta.Insertions, MembershipInsertion{Key: entity.Key, FinalIndex: index})
	}
	sort.Slice(delta.Insertions, func(i, j int) bool {
		return delta.Insertions[i].FinalIndex < delta.Insertions[j].FinalIndex
	})
	return delta, nil
}

// Unsubscribe returns the last delivery revision accepted for this group.
// The consumer may release routing once it has applied through that revision;
// retiring an interest never consumes queue capacity or discards accepted work.
func (h *SubscriptionHub) Unsubscribe(id string) (uint64, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	subscription := h.byID[id]
	if subscription == nil {
		return 0, false
	}
	delete(h.byID, id)
	h.removeGroupMemberLocked(subscription)
	delete(h.byQuery[subscription.public.Query], id)
	if index := h.windows[subscription.public.Query]; index != nil {
		_ = index.delete(id, subscription.bounds)
	}
	if len(h.byQuery[subscription.public.Query]) == 0 {
		delete(h.byQuery, subscription.public.Query)
		delete(h.windows, subscription.public.Query)
	}
	return h.lastGroupRevision[subscription.public.Group], true
}

func (h *SubscriptionHub) dropGroupLocked(group string, cause error) []string {
	if queue := h.groups[group]; queue != nil {
		queue.Close(cause)
	}
	var dropped []string
	for id, subscription := range h.byGroup[group] {
		delete(h.byID, id)
		delete(h.byQuery[subscription.public.Query], id)
		if index := h.windows[subscription.public.Query]; index != nil {
			_ = index.delete(id, subscription.bounds)
		}
		if len(h.byQuery[subscription.public.Query]) == 0 {
			delete(h.byQuery, subscription.public.Query)
			delete(h.windows, subscription.public.Query)
		}
		dropped = append(dropped, id)
	}
	delete(h.byGroup, group)
	delete(h.lastGroupRevision, group)
	return dropped
}

func (h *SubscriptionHub) ReleaseGroup(group string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.byGroup[group]) > 0 {
		return false
	}
	delete(h.lastGroupRevision, group)
	if queue := h.groups[group]; queue != nil {
		queue.Close(ErrClientClosed)
		delete(h.groups, group)
	}
	return true
}

// FailQuery closes and removes every subscriber sharing a broken query.
func (h *SubscriptionHub) FailQuery(fingerprint datastore.QueryFingerprint, err error) []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	set := h.byQuery[fingerprint]
	ids := make([]string, 0, len(set))
	failures := make(map[string][]SubscriptionFailure)
	for id, subscription := range set {
		ids = append(ids, id)
		delete(h.byID, id)
		h.removeGroupMemberLocked(subscription)
		// Earlier accepted group deliveries still carry predecessor metadata that
		// the connection must observe before it can isolate this failed query.
		failures[subscription.public.Group] = append(failures[subscription.public.Group], SubscriptionFailure{ID: id, Cause: err})
	}
	for group, failures := range failures {
		_ = h.groups[group].Offer(GroupMessage{Failures: failures})
	}
	delete(h.byQuery, fingerprint)
	delete(h.windows, fingerprint)
	return ids
}

func (h *SubscriptionHub) CloseAll(err error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, queue := range h.groups {
		queue.Close(err)
	}
	h.byID = make(map[string]*hubSubscription)
	h.byQuery = make(map[datastore.QueryFingerprint]map[string]*hubSubscription)
	h.byGroup = make(map[string]map[string]*hubSubscription)
	h.windows = make(map[datastore.QueryFingerprint]*windowIndex)
	h.lastGroupRevision = make(map[string]uint64)
	h.groups = make(map[string]*ClientQueue[GroupMessage])
}

func changeWasVisibleInWindow(subscription *hubSubscription, change changeprocessor.QueryChange) (bool, error) {
	if change.Kind == changeprocessor.QueryAdded || change.Kind == changeprocessor.QueryCleared || change.PreviousRecord == nil {
		return false, nil
	}
	return querywindow.ContainsRecord(subscription.plan, change.PreviousRecord, subscription.bounds)
}

func dropSubscriptionLocked(h *SubscriptionHub, id string, subscription *hubSubscription, cause error) {
	h.retireSubscriptionLocked(subscription, cause)
	delete(h.byID, id)
	h.removeGroupMemberLocked(subscription)
	delete(h.byQuery[subscription.public.Query], id)
	if index := h.windows[subscription.public.Query]; index != nil {
		_ = index.delete(id, subscription.bounds)
	}
	if len(h.byQuery[subscription.public.Query]) == 0 {
		delete(h.byQuery, subscription.public.Query)
		delete(h.windows, subscription.public.Query)
	}
}

func (h *SubscriptionHub) removeGroupMemberLocked(subscription *hubSubscription) {
	group := subscription.public.Group
	delete(h.byGroup[group], subscription.public.ID)
	if len(h.byGroup[group]) == 0 {
		delete(h.byGroup, group)
	}
}
