// Package referenceview adapts parent-owned reference interests to ordinary
// local entity handles. It consumes only the query protocol, equally in process
// and over gRPC. No storage access or extra subscription RPC is required.
package referenceview

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sync"

	"github.com/adiom-data/weave/internal/querysubscription/feedbuffer"
	q "github.com/adiom-data/weave/internal/querysubscription/model"
	"github.com/adiom-data/weave/internal/storage"
)

func WrapFeed(parentCtx context.Context, base q.Feed, options q.FeedOptions) q.Feed {
	ctx, cancel := context.WithCancel(parentCtx)
	f := &feed{Feed: base, ctx: ctx, cancel: cancel, events: feedbuffer.New(options.BufferLimit), parents: map[string]*parent{}, aliases: map[string]*alias{}}
	go f.run()
	return f
}

type feed struct {
	q.Feed
	ctx     context.Context
	cancel  context.CancelFunc
	mu      sync.Mutex
	closed  bool
	events  *feedbuffer.Buffer
	parents map[string]*parent
	aliases map[string]*alias
}
type parent struct {
	shape   *storage.ReferenceFetch
	owned   bool
	rows    map[storage.EntityKey]storage.Record
	pending map[storage.EntityKey]bool
	cells   map[storage.EntityKey]*cell
	aliases map[storage.EntityKey]map[string]*alias
}
type cell struct {
	ready       chan struct{}
	set, closed bool
	snapshot    q.EntitySnapshot
}
type alias struct {
	id, parent string
	selector   string
	key        storage.EntityKey
	feed       *feed
	closed     bool
	// The installed image and subsequent deliveries determine this handle's
	// floor. Advancing the shared cell must not advance existing handles.
	revision uint64
}

func (a *alias) ID() string { return a.id }
func (a *alias) Close(context.Context) error {
	a.feed.mu.Lock()
	defer a.feed.mu.Unlock()
	a.closed = true
	delete(a.feed.aliases, a.id)
	a.feed.events.RetireSubscription(a.id)
	if p := a.feed.parents[a.parent]; p != nil {
		delete(p.aliases[a.key], a.id)
		if len(p.aliases[a.key]) == 0 {
			delete(p.aliases, a.key)
		}
	}
	return nil
}

type queryHandle struct {
	q.QuerySubscription
	feed *feed
}

func (h queryHandle) Close(ctx context.Context) error {
	err := h.QuerySubscription.Close(ctx)
	if err == nil {
		h.feed.mu.Lock()
		h.feed.removeParent(h.ID())
		h.feed.events.RetireSubscription(h.ID())
		h.feed.mu.Unlock()
	}
	return err
}

func (f *feed) reserve(id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return q.ErrClosed
	}
	return f.events.RegisterSubscription(id)
}
func (f *feed) SubscribeQuery(ctx context.Context, request q.QuerySubscribeRequest, install q.QuerySnapshotInstaller) (err error) {
	if err := q.ValidateQuerySubscribeRequest(request); err != nil {
		return err
	}
	if install == nil {
		return errors.New("snapshot installer is required")
	}
	if err := f.reserve(request.ID); err != nil {
		return err
	}
	defer func() {
		if err != nil {
			f.events.DiscardSubscription(request.ID)
		}
	}()
	return f.Feed.SubscribeQuery(ctx, request, func(native q.QuerySubscription, snapshot q.QuerySnapshot) error {
		if snapshot.OwnedReferences {
			encoded, err := json.Marshal(request.Reference)
			if err != nil {
				return err
			}
			p := &parent{owned: true, rows: map[storage.EntityKey]storage.Record{}, pending: map[storage.EntityKey]bool{}, cells: map[storage.EntityKey]*cell{}, aliases: map[storage.EntityKey]map[string]*alias{}}
			if err := json.Unmarshal(encoded, &p.shape); err != nil {
				return err
			}
			if p.shape == nil {
				return errors.New("unrequested reference expansion")
			}
			for _, row := range snapshot.Rows {
				if row.Pending {
					p.pending[row.Key] = true
					continue
				}
				p.rows[row.Key] = bindingFields(p.shape, row.Record)
			}
			f.mu.Lock()
			f.parents[request.ID] = p
			for _, row := range snapshot.Rows {
				if row.Expanded == nil || row.Pending {
					continue
				}
				// The enclosing row provides the routing for an initial image.
				// No parent ID, parent-key list, or selector belongs on the wire.
				c := &cell{ready: make(chan struct{}), set: true, snapshot: cloneSnapshot(*row.Expanded)}
				close(c.ready)
				p.cells[row.Key] = c
			}
			f.mu.Unlock()
		}
		if err := install(queryHandle{native, f}, snapshot); err != nil {
			f.mu.Lock()
			f.removeParent(request.ID)
			f.mu.Unlock()
			return err
		}
		return nil
	})
}

func bindingFields(shape *storage.ReferenceFetch, record storage.Record) storage.Record {
	selected := storage.Record{}
	for _, binding := range shape.Bindings {
		root := binding.Source.Root()
		if _, copied := selected[root]; !copied {
			if value, exists := record[root]; exists {
				selected[root] = storage.CloneValue(value)
			}
		}
	}
	return selected
}

func (f *feed) SubscribeEntity(ctx context.Context, request q.EntitySubscribeRequest, install q.EntitySnapshotInstaller) (result error) {
	if err := q.ValidateEntitySubscribeRequest(request); err != nil {
		return err
	}
	if install == nil {
		return errors.New("snapshot installer is required")
	}
	if err := f.reserve(request.ID); err != nil {
		return err
	}
	defer func() {
		if result != nil {
			f.events.DiscardSubscription(request.ID)
		}
	}()
	f.mu.Lock()
	var p *parent
	if request.FetchFrom != nil {
		p = f.parents[request.FetchFrom.SubscriptionID]
	}
	if p == nil || !p.owned {
		f.mu.Unlock()
		return f.subscribeUnreferenced(ctx, request, install)
	}
	key := request.FetchFrom.ParentKey
	record, exists := p.rows[key]
	if !exists || p.pending[key] {
		f.mu.Unlock()
		return f.subscribeUnreferenced(ctx, request, install)
	}
	expected, _, matchable, err := storage.BindReference(*p.shape, record)
	if err != nil || !matchable || !reflect.DeepEqual(expected, request.Query) {
		f.mu.Unlock()
		return f.subscribeUnreferenced(ctx, request, install)
	}
	left, err := json.Marshal(expected)
	if err != nil {
		f.mu.Unlock()
		return f.subscribeUnreferenced(ctx, request, install)
	}
	c := p.cells[key]
	if c == nil {
		c = &cell{ready: make(chan struct{})}
		p.cells[key] = c
	}
	f.mu.Unlock()
	select {
	case <-c.ready:
	case <-ctx.Done():
		return ctx.Err()
	case <-f.ctx.Done():
		return q.ErrClosed
	}
	f.events.BeginInstallation()
	f.mu.Lock()
	if f.closed {
		f.mu.Unlock()
		_ = f.events.FinishInstallation()
		return q.ErrClosed
	}
	if c.closed || p.pending[key] || f.parents[request.FetchFrom.SubscriptionID] != p || p.cells[key] != c {
		f.mu.Unlock()
		if err := f.events.FinishInstallation(); err != nil {
			return err
		}
		return f.subscribeUnreferenced(ctx, request, install)
	}
	a := &alias{id: request.ID, parent: request.FetchFrom.SubscriptionID, selector: fmt.Sprintf("%x", sha256.Sum256(left)), key: key, feed: f, revision: c.snapshot.Revision}
	f.aliases[a.id] = a
	if p.aliases[key] == nil {
		p.aliases[key] = map[string]*alias{}
	}
	p.aliases[key][a.id] = a
	snapshot := cloneSnapshot(c.snapshot)
	f.mu.Unlock()
	err = install(a, snapshot)
	if err != nil {
		_ = a.Close(context.WithoutCancel(ctx))
		f.events.DiscardSubscription(a.id)
	}
	finishErr := f.events.FinishInstallation()
	return errors.Join(err, finishErr)
}

// An origin is only a reuse hint. Once it is stale, use the supplied selector
// independently, without forwarding the stale hint to another adapter layer.
func (f *feed) subscribeUnreferenced(ctx context.Context, request q.EntitySubscribeRequest, install q.EntitySnapshotInstaller) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	request.FetchFrom = nil
	return f.Feed.SubscribeEntity(ctx, request, func(handle q.EntitySubscription, snapshot q.EntitySnapshot) error {
		return install(entityHandle{handle, f}, snapshot)
	})
}

type entityHandle struct {
	q.EntitySubscription
	feed *feed
}

func (h entityHandle) Close(ctx context.Context) error {
	err := h.EntitySubscription.Close(ctx)
	if err == nil {
		h.feed.events.RetireSubscription(h.ID())
	}
	return err
}

func cloneSnapshot(snapshot q.EntitySnapshot) q.EntitySnapshot {
	if snapshot.Checkpoint != nil {
		copy := *snapshot.Checkpoint
		snapshot.Checkpoint = &copy
	}
	if snapshot.Fence != nil {
		copy := *snapshot.Fence
		snapshot.Fence = &copy
	}
	if snapshot.Row != nil {
		row := *snapshot.Row
		row.Record = storage.CloneRecord(row.Record)
		row.Cursor = nil
		snapshot.Row = &row
	}
	return snapshot
}

// Match the immutable bound selector, not just the association key: a late
// asynchronous load may belong to a reference the parent has already replaced.
func (p *parent) matches(key storage.EntityKey, selector string) bool {
	record, exists := p.rows[key]
	if !exists {
		return false
	}
	query, _, matchable, err := storage.BindReference(*p.shape, record)
	if err != nil || !matchable {
		return false
	}
	encoded, err := json.Marshal(query)
	return err == nil && fmt.Sprintf("%x", sha256.Sum256(encoded)) == selector
}

func (f *feed) installEntity(p *parent, entity q.EntityInstallation) {
	route := entity.Target.Reference
	for _, key := range route.Parents {
		if !p.matches(key, route.Selector) {
			continue
		}
		if _, exists := p.rows[key]; !exists {
			continue
		}
		c := p.cells[key]
		if c == nil {
			c = &cell{ready: make(chan struct{})}
			p.cells[key] = c
		}
		if !c.set || entity.Snapshot.Revision >= c.snapshot.Revision {
			c.snapshot = cloneSnapshot(entity.Snapshot)
		}
		if !c.set {
			c.set = true
			close(c.ready)
		}
	}
}

func (f *feed) removeParent(id string) {
	if p := f.parents[id]; p != nil {
		for _, aliases := range p.aliases {
			for id, a := range aliases {
				a.closed = true
				delete(f.aliases, id)
				f.events.RetireSubscription(id)
			}
		}
		for _, c := range p.cells {
			c.closed = true
			if !c.set {
				close(c.ready)
			}
		}
		delete(f.parents, id)
	}
}

func (f *feed) run() {
	for {
		event, err := f.Feed.Next(f.ctx)
		if err != nil {
			f.fail(err)
			return
		}
		f.mu.Lock()
		if event.Entity != nil && event.Entity.Target.Reference != nil {
			if p := f.parents[event.Entity.Target.SubscriptionID]; p != nil {
				f.installEntity(p, *event.Entity)
			}
		}
		if event.Failure != nil {
			f.removeParent(event.Failure.SubscriptionID)
		}
		if envelope := event.Delivery; envelope != nil {
			value := *envelope
			value.Entities = append([]q.EntityEffect(nil), envelope.Entities...)
			for _, effect := range envelope.Effects {
				p := f.parents[effect.SubscriptionID]
				if p == nil {
					continue
				}
				batch := effect.Batch
				if batch.Clear {
					for key, c := range p.cells {
						c.closed = true
						if !c.set {
							close(c.ready)
						}
						delete(p.cells, key)
					}
					clear(p.rows)
					clear(p.pending)
				}
				for _, key := range batch.BindingResets {
					if c := p.cells[key]; c != nil {
						c.closed = true
						if !c.set {
							close(c.ready)
						}
						delete(p.cells, key)
					}
				}
				inserted := make(map[storage.EntityKey]bool, len(batch.Membership.Insertions))
				for _, insertion := range batch.Membership.Insertions {
					inserted[insertion.Key] = true
				}
				for _, key := range batch.Membership.Removed {
					if !inserted[key] {
						delete(p.rows, key)
						delete(p.pending, key)
					}
				}
				for _, change := range batch.Changes {
					if change.Kind == q.Removed {
						delete(p.rows, change.Key)
						delete(p.pending, change.Key)
					} else if change.Pending {
						// Existing entity handles keep their proven selector and
						// continue receiving its transactions. New handles cannot
						// reuse the parent's uncertain binding.
						p.pending[change.Key] = true
					} else {
						delete(p.pending, change.Key)
						p.rows[change.Key] = bindingFields(p.shape, change.Record)
					}
				}
			}
			// All query bindings are advanced before entity payloads, independent
			// of the order in which native subscription queues drained.
			for _, entity := range envelope.Entities {
				route := entity.Target.Reference
				p := f.parents[entity.Target.SubscriptionID]
				if route == nil || p == nil {
					continue
				}
				delta := entity.Delta
				for _, key := range route.Parents {
					if !p.matches(key, route.Selector) {
						continue
					}
					c := p.cells[key]
					if c == nil {
						c = &cell{ready: make(chan struct{})}
						p.cells[key] = c
					}
					if !c.set || envelope.Delivery.Revision > c.snapshot.Revision {
						if delta.Clear {
							c.snapshot.Row = nil
						}
						for _, change := range delta.Changes {
							if change.Kind == q.Removed {
								c.snapshot.Row = nil
							} else {
								row := change.Row
								row.Record = storage.CloneRecord(row.Record)
								c.snapshot.Row = &row
							}
						}
						c.snapshot.Revision = envelope.Delivery.Revision
						c.snapshot.Checkpoint = envelope.Delivery.Checkpoint
						c.snapshot.Fence = envelope.Delivery.Fence
						if !c.set {
							c.set = true
							close(c.ready)
						}
					}
					for _, a := range p.aliases[key] {
						if !a.closed && a.selector == route.Selector && envelope.Delivery.Revision > a.revision {
							a.revision = envelope.Delivery.Revision
							value.Entities = append(value.Entities, q.EntityEffect{Target: q.EntityTarget{SubscriptionID: a.id}, Delta: delta})
						}
					}
				}
			}
			event.Delivery = &value
		}
		f.mu.Unlock()
		if err := f.events.Offer(event); err != nil {
			f.fail(err)
			return
		}
	}
}

func (f *feed) fail(err error) {
	f.mu.Lock()
	if f.closed {
		f.mu.Unlock()
		return
	}
	f.closed = true
	for id := range f.parents {
		f.removeParent(id)
	}
	f.mu.Unlock()
	f.events.Close(err)
	f.cancel()
	go func() { _ = f.Feed.Close(context.Background()) }()
}
func (f *feed) Next(ctx context.Context) (q.Event, error) { return f.events.Next(ctx) }
func (f *feed) Close(ctx context.Context) error {
	f.mu.Lock()
	f.closed = true
	for id := range f.parents {
		f.removeParent(id)
	}
	f.mu.Unlock()
	f.events.Close(q.ErrClosed)
	// Mark intentional closure before waking the pump. Otherwise its canceled
	// Next can start a competing failure shutdown and abort the transport's
	// normal close acknowledgment.
	f.cancel()
	return f.Feed.Close(ctx)
}
