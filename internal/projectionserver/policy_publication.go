package projectionserver

import (
	"bytes"
	"context"
	"errors"
	"sync"

	"github.com/adiom-data/weave/internal/accesscontrol"
	proj "github.com/adiom-data/weave/internal/projection"
	"github.com/adiom-data/weave/internal/querysubscription/membership"
	q "github.com/adiom-data/weave/internal/querysubscription/model"
	"github.com/adiom-data/weave/internal/storage"
	reactivev1 "github.com/adiom-data/weave/pkg/gen/reactive/v1"
)

// publicationHold separates private materialization from public state. The
// visibility gate keeps consuming updates, but connectionModel remains frozen.
// No transaction backlog or extra copies of entity payloads are retained here.
type publicationHold struct {
	held   bool
	wait   <-chan struct{}
	cancel context.CancelFunc
	acks   []func() error
}

func (h *publicationHold) close() {
	if h.cancel != nil {
		h.cancel()
	}
	h.wait, h.cancel, h.acks = nil, nil, nil
	h.held = false
}

// policyPublications is owned by the serialized connection consumer. Workers
// only notify it of activity; authorization and model reconciliation stay here.
type policyPublications struct {
	ctx             context.Context
	service         *server
	prepared        accesscontrol.Prepared
	model           *connectionModel
	subscriptions   map[string]*sharedSubscription
	projections     map[string]*sharedProjection
	stream          Stream
	outbound        chan<- outboundMessage
	loaders         *sync.WaitGroup
	position        *q.Position
	revision        uint64
	closeDirect     func(*sharedSubscription, error) error
	closeProjection func(*sharedProjection, error) error
}

func (p *policyPublications) observe(event q.Event) {
	var position *q.Position
	var revision uint64
	if event.Progress != nil {
		position, revision = event.Progress.Position, event.Progress.Revision
	}
	if event.Delivery != nil {
		revision = event.Delivery.Delivery.Revision
		if event.Delivery.Delivery.Kind == q.DeliveryCommit {
			position = event.Delivery.Delivery.Checkpoint
		}
	}
	if position != nil && (p.position == nil || p.position.SourceID == position.SourceID && p.position.Value <= position.Value) {
		copy := *position
		p.position = &copy
	}
	if revision > p.revision {
		p.revision = revision
	}
}

func (p *policyPublications) withhold(raw deliveryPayload) bool {
	switch part := raw.(type) {
	case subscriptionBatchMessage:
		s := p.subscriptions[part.externalID]
		if s == nil {
			return true
		}
		if p.service.relationAccess.session.Pending([]string{s.visibility.id}) {
			s.publication.held = true
		}
		return s.publication.held
	case projectionBatchMessage:
		s := p.projections[part.externalID]
		if s == nil {
			return true
		}
		if p.service.relationAccess.session.PendingIn(func(id string) bool { return s.visibility.instances[id] != nil }) {
			s.publication.held = true
		}
		return s.publication.held
	}
	return false
}

func (p *policyPublications) wait(h *publicationHold, activity <-chan struct{}) {
	if activity == nil || h.wait == activity {
		return
	}
	if h.cancel != nil {
		h.cancel()
	}
	ctx, cancel := context.WithCancel(p.ctx)
	h.wait, h.cancel = activity, cancel
	p.loaders.Add(1)
	go func() {
		defer p.loaders.Done()
		select {
		case <-activity:
			select {
			case p.outbound <- completionMessage{run: func() error { return nil }}:
			case <-ctx.Done():
			}
		case <-ctx.Done():
		}
	}()
}

func (p *policyPublications) send(id string, sequence *uint64, h *publicationHold, batch *reactivev1.ModelBatch) error {
	batch.Kind = reactivev1.ModelBatch_KIND_HYDRATION
	batch.Checkpoint = checkpointString(p.position)
	coalesceEntityEvents(batch)
	if len(batch.Entities)+len(batch.Queries)+len(batch.Projections)+len(batch.Computed) > 0 {
		*sequence++
		if err := p.stream.Send(&reactivev1.ServerMessage{SubscriptionId: id, Sequence: *sequence, Event: &reactivev1.ServerMessage_ModelBatch{ModelBatch: batch}}); err != nil {
			return err
		}
	}
	acks := h.acks
	h.close()
	for _, ack := range acks {
		if err := ack(); err != nil {
			return err
		}
	}
	return nil
}

// Window data is reconciled once, at its final bounds. Acknowledgments retain
// request identity, without replaying superseded bounds or closing a whole
// projection merely because its target disappeared while permission loaded.
func (p *policyPublications) acknowledgeWindow(scope, request, query string, sequence *uint64) error {
	*sequence++
	message := &reactivev1.ServerMessage{SubscriptionId: scope, RequestId: request, Sequence: *sequence}
	if p.model.queries[query] == nil {
		message.Event = &reactivev1.ServerMessage_Error{Error: &reactivev1.SubscriptionError{Code: "WINDOW_FAILED", Message: "window target is no longer visible"}}
	} else {
		message.Event = &reactivev1.ServerMessage_ModelBatch{ModelBatch: &reactivev1.ModelBatch{Kind: reactivev1.ModelBatch_KIND_WINDOW}}
	}
	return p.stream.Send(message)
}

func (p *policyPublications) retry() error {
	if p.position == nil {
		return nil
	}
	for _, s := range p.subscriptions {
		if !s.publication.held || p.service.relationAccess.session.Pending([]string{s.visibility.id}) {
			continue
		}
		if baseline := s.visibility.checkpoint; baseline != nil && (baseline.SourceID != p.position.SourceID || baseline.Value > p.position.Value) {
			p.service.relationAccess.requestProgress(baseline)
			continue
		}
		view, err := acquireVisibilityAt(p.ctx, p.service.access, p.prepared, p.position, nil)
		if err != nil {
			return err
		}
		// Reevaluate the current raw image, not the transaction that first
		// encountered an unknown grant. Preserve each record's own version.
		snapshot := q.QuerySnapshot{Checkpoint: p.position, Revision: p.revision, Bounds: s.visibility.bounds}
		for _, key := range s.visibility.order.Keys() {
			snapshot.Rows = append(snapshot.Rows, q.QueryRow{Row: s.visibility.rows[key].Row})
		}
		fresh := newSubscriptionVisibility()
		fresh.id = s.visibility.id
		_, err = fresh.filterSnapshot(p.ctx, p.prepared, view, snapshot)
		err = errors.Join(err, view.Close())
		if err != nil {
			if err := p.closeDirect(s, err); err != nil {
				return err
			}
			continue
		}
		for key, row := range fresh.rows {
			row.version = s.visibility.rows[key].version
			fresh.rows[key] = row
		}
		if p.service.relationAccess.session.Pending([]string{fresh.id}) {
			continue
		}
		batch, err := p.model.reconcileSubscription(s.queryID, fresh, q.DeliveryMetadata{Kind: q.DeliveryHydration, Checkpoint: p.position, Revision: p.revision})
		if err != nil {
			if err := p.closeDirect(s, err); err != nil {
				return err
			}
			continue
		}
		s.visibility = fresh
		if err := p.send(s.externalID, &s.sequence, &s.publication, batch); err != nil {
			return err
		}
	}
	for _, s := range p.projections {
		if !s.publication.held {
			continue
		}
		if p.service.relationAccess.session.PendingIn(func(id string) bool { return s.visibility.instances[id] != nil }) {
			continue
		}
		var fresh *projectionVisibility
		var visible []proj.ProjectionEvent
		wait, err := s.projection.TryPublishCurrent(func(events []proj.ProjectionEvent) (bool, error) {
			if err := s.policyOwnership.capture(p.ctx, p.prepared, s.visibility, events); err != nil {
				return false, err
			}
			fresh = newProjectionVisibility(s.selected)
			var err error
			visible, err = fresh.filterInitial(p.ctx, p.service.access, p.prepared, events)
			if err != nil {
				return false, err
			}
			return !p.service.relationAccess.session.PendingIn(func(id string) bool { return fresh.instances[id] != nil }), nil
		})
		if err != nil {
			if err := p.closeProjection(s, err); err != nil {
				return err
			}
			continue
		}
		if !wait.Accepted {
			p.service.relationAccess.requestProgress(wait.Progress...)
			p.wait(&s.publication, wait.Wait)
			continue
		}
		batch, err := p.model.reconcileProjection(s.externalID, visible, s.selected)
		if err != nil {
			if err := p.closeProjection(s, err); err != nil {
				return err
			}
			continue
		}
		s.visibility = fresh
		s.policyOwnership.attempt = nil // ownership is now represented by the adopted gate
		if err := p.send(s.externalID, &s.sequence, &s.publication, batch); err != nil {
			return err
		}
	}
	return nil
}

func (m *connectionModel) reconcileSubscription(id string, visible *subscriptionVisibility, delivery q.DeliveryMetadata) (*reactivev1.ModelBatch, error) {
	query := m.queries[id]
	if query == nil {
		return nil, errors.New("held query is no longer published")
	}
	keys := visible.order.VisibleKeys()
	batch := q.Batch{Membership: membership.Diff(query.ordered, keys)}
	versions := make(map[storage.EntityKey]q.RecordVersion)
	for key := range query.members {
		if yes, _ := visible.order.Visible(key); !yes {
			batch.Changes = append(batch.Changes, q.Change{Kind: q.Removed, Row: q.Row{Key: key}})
		}
	}
	for _, key := range keys {
		row := visible.rows[key]
		kind := q.Updated
		if _, exists := query.members[key]; !exists {
			kind = q.Added
		}
		batch.Changes = append(batch.Changes, q.Change{Kind: kind, Row: row.Row})
		versions[key] = row.version
	}
	result, err := m.applySubscriptionBatch(versions, id, delivery, batch)
	if err == nil && !sameBounds(query.publicBounds, visible.bounds) {
		window, windowErr := m.applyProjectionWindow(id, visible.bounds)
		if windowErr != nil {
			return nil, windowErr
		}
		appendModelBatch(result, window)
	}
	return result, err
}

// Reconciliation is exceptional (after an unknown visibility decision), not
// the normal per-transaction path. Existing lists receive membership deltas;
// only genuinely new query instances receive an initial reset.
func (m *connectionModel) reconcileProjection(scope string, events []proj.ProjectionEvent, selected map[string]*fieldSelection) (*reactivev1.ModelBatch, error) {
	desired := make(map[string]proj.ProjectionEvent)
	rows := make(map[string][]proj.ProjectionEvent)
	for _, event := range events {
		if event.Type == proj.ProjectionInstanceOpened {
			desired[event.InstanceID] = event
		}
		if event.Type == proj.ProjectionRowChanged {
			rows[event.InstanceID] = append(rows[event.InstanceID], event)
		}
	}
	batch := proj.ProjectionBatch{Membership: make(map[string]q.MembershipDelta)}
	for id, old := range m.occurrences {
		if old.scope != scope {
			continue
		}
		if _, exists := desired[id]; !exists {
			batch.Events = append(batch.Events, proj.ProjectionEvent{Type: proj.ProjectionInstanceClosed, InstanceID: id})
		}
	}
	for _, open := range events {
		if open.Type != proj.ProjectionInstanceOpened {
			continue
		}
		old, exists := m.occurrences[open.InstanceID]
		if !exists {
			batch.Events = append(batch.Events, open)
		}
		keys := make([]storage.EntityKey, 0, len(rows[open.InstanceID]))
		present := make(map[storage.EntityKey]struct{})
		for _, row := range rows[open.InstanceID] {
			keys = append(keys, row.Key)
			present[row.Key] = struct{}{}
		}
		if query := m.queries[open.InstanceID]; query != nil {
			for key := range query.members {
				if _, yes := present[key]; !yes {
					batch.Events = append(batch.Events, proj.ProjectionEvent{Type: proj.ProjectionRowChanged, InstanceID: open.InstanceID, Kind: q.Removed, Key: key})
				}
			}
			batch.Membership[open.InstanceID] = membership.Diff(query.ordered, keys)
		} else if exists && old.entityID != "" && len(keys) == 0 {
			batch.Events = append(batch.Events, proj.ProjectionEvent{Type: proj.ProjectionRowChanged, InstanceID: open.InstanceID, EntityNode: true, Kind: q.Removed})
		}
		for _, row := range rows[open.InstanceID] {
			row.Kind = q.Added
			batch.Events = append(batch.Events, row)
		}
	}
	result, err := m.applyProjection(scope, batch, selected)
	if err != nil {
		return nil, err
	}
	for id, open := range desired {
		if query := m.queries[id]; query != nil && !sameBounds(query.publicBounds, open.Bounds) {
			window, err := m.applyProjectionWindow(id, open.Bounds)
			if err != nil {
				return nil, err
			}
			appendModelBatch(result, window)
		}
	}
	return result, nil
}

func sameBounds(before *q.Bounds, after q.Bounds) bool {
	return before != nil && before.Left.Kind == after.Left.Kind && before.Right.Kind == after.Right.Kind && bytes.Equal(before.Left.Cursor, after.Left.Cursor) && bytes.Equal(before.Right.Cursor, after.Right.Cursor)
}
