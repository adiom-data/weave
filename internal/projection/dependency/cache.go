// Package dependency owns projection-side shared subscriptions and current
// materialized dependencies. It consumes the query interface, never storage or
// engine internals. Queues own immutable-by-convention delivery copies; this is
// not a historical entity store.
package dependency

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/adiom-data/weave/internal/expiryindex"
	q "github.com/adiom-data/weave/internal/querysubscription/model"
	sourceposition "github.com/adiom-data/weave/internal/querysubscription/position"
	"github.com/adiom-data/weave/internal/storage"
)

type Options struct {
	UpstreamBufferLimit int
	MaxSubscriptions    int
	IdleTTL             time.Duration
	SweepInterval       time.Duration
	EvictionBatchSize   int
	CleanupBatchSize    int
}

// Cache multiplexes logical consumer feeds onto one upstream feed. An upstream
// failure invalidates that generation; a later OpenFeed can reconnect cleanly.
type Cache struct {
	mu       sync.Mutex
	ctx      context.Context
	cancel   context.CancelFunc
	client   q.Client
	options  Options
	closed   bool
	current  *generation
	sequence uint64
}

type generation struct {
	owner       *Cache
	ctx         context.Context
	cancel      context.CancelFunc
	ready, done chan struct{}
	upstream    q.Feed
	err         error
	closeErr    error
	stopping    bool
	workers     sync.WaitGroup
	wake        chan struct{}
	feeds       map[*consumer]struct{}
	entries     map[string]*entry
	shared      map[string]*entry
	entities    map[storage.EntityKey]*entityCell
	retired     []*entry
	cleanup     []map[storage.EntityKey]row
	idle        expiryindex.Index[string]
	revision    uint64
	checkpoint  *q.Position
	positions   sourceposition.Tracker
}

type entry struct {
	id, fingerprint      string
	generation           *generation
	entity, private      bool
	query                storage.QuerySpec
	window               q.InitialWindow
	reference            *storage.ReferenceFetch
	fetchFrom            *q.ReferenceSource
	ready                chan struct{}
	finished             bool
	err                  error
	cancel               context.CancelFunc
	native               q.Subscription
	waiters              int
	watchers             map[*subscription]struct{}
	lastUsed             time.Time
	retired, closeQueued bool
	state                result
}

func New(ctx context.Context, client q.Client, options Options) (*Cache, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if client == nil {
		return nil, errors.New("dependency cache requires a query client")
	}
	options = options.WithDefaults()
	if options.UpstreamBufferLimit < 1 || options.MaxSubscriptions < 1 || options.IdleTTL < 0 || options.SweepInterval <= 0 || options.EvictionBatchSize < 1 || options.CleanupBatchSize < 1 {
		return nil, errors.New("invalid dependency cache limits")
	}
	runCtx, cancel := context.WithCancel(ctx)
	return &Cache{ctx: runCtx, cancel: cancel, client: client, options: options}, nil
}

func (c *Cache) OpenFeed(ctx context.Context, options q.FeedOptions) (q.Feed, error) {
	return c.open(ctx, options, false)
}

// OpenFactFeed shares exact query/window definitions in addition to entity
// selectors. Fact windows are fixed: callers cannot mutate them. Projection
// list subscriptions use OpenFeed and retain independent mutable windows.
func (c *Cache) OpenFactFeed(ctx context.Context, options q.FeedOptions) (q.Feed, error) {
	return c.open(ctx, options, true)
}

func (c *Cache) open(ctx context.Context, options q.FeedOptions, facts bool) (q.Feed, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if options.BufferLimit < 1 {
		return nil, errors.New("dependency feed buffer limit must be positive")
	}
	c.mu.Lock()
	if c.closed || c.ctx.Err() != nil {
		c.mu.Unlock()
		return nil, q.ErrClosed
	}
	g := c.current
	if g != nil {
		select {
		case <-g.done:
			c.current = nil
			g = nil
		default:
		}
	}
	if g == nil {
		genCtx, cancel := context.WithCancel(c.ctx)
		g = &generation{owner: c, ctx: genCtx, cancel: cancel, ready: make(chan struct{}), done: make(chan struct{}), wake: make(chan struct{}, 1), feeds: make(map[*consumer]struct{}), entries: make(map[string]*entry), shared: make(map[string]*entry), entities: make(map[storage.EntityKey]*entityCell)}
		c.current = g
		go g.run()
	}
	c.mu.Unlock()
	select {
	case <-g.ready:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if g.err != nil {
		return nil, g.err
	}
	if c.closed || g.stopping {
		return nil, q.ErrClosed
	}
	f := newConsumer(g, options.BufferLimit, facts)
	g.feeds[f] = struct{}{}
	// Context cancellation must release dependencies even when Next isn't
	// called (for example a browser that has already disconnected).
	f.stop = context.AfterFunc(ctx, func() { _ = f.Close(context.Background()) })
	return f, nil
}

func (c *Cache) Close() error {
	c.mu.Lock()
	c.closed = true
	c.cancel()
	g := c.current
	c.mu.Unlock()
	if g != nil {
		<-g.done
		return g.closeErr
	}
	return nil
}

func (g *generation) run() {
	c := g.owner
	defer close(g.done)
	upstream, err := c.client.OpenFeed(g.ctx, q.FeedOptions{BufferLimit: c.options.UpstreamBufferLimit})
	c.mu.Lock()
	g.upstream = upstream
	if err != nil {
		g.err = err
		g.stopping = true
	}
	close(g.ready)
	c.mu.Unlock()
	if err != nil {
		g.cancel()
		if upstream != nil {
			_ = upstream.Close(context.Background())
		}
		return
	}
	g.workers.Add(1)
	go g.maintain()
	for {
		event, receiveErr := upstream.Next(g.ctx)
		if receiveErr == nil {
			receiveErr = g.dispatch(event)
		}
		if receiveErr != nil {
			err = receiveErr
			break
		}
	}
	c.mu.Lock()
	if g.err != nil {
		err = g.err
	}
	g.err, g.stopping = err, true
	for f := range g.feeds {
		f.closeLocked(err)
	}
	for _, e := range g.entries {
		e.cancel()
	}
	c.mu.Unlock()
	g.cancel()
	// Close waits for upstream installation operations; none of our callbacks
	// perform IO or wait for a logical consumer.
	closeErr := upstream.Close(context.Background())
	if !errors.Is(closeErr, q.ErrClosed) && !errors.Is(closeErr, context.Canceled) {
		g.closeErr = closeErr
	}
	g.workers.Wait()
	c.mu.Lock()
	g.entries = nil
	g.shared = nil
	g.entities = nil
	g.retired = nil
	g.cleanup = nil
	g.idle = expiryindex.Index[string]{}
	c.mu.Unlock()
}

func (g *generation) maintain() {
	defer g.workers.Done()
	ticker := time.NewTicker(g.owner.options.SweepInterval)
	defer ticker.Stop()
	for {
		select {
		case <-g.ctx.Done():
			return
		case <-ticker.C:
		case <-g.wake:
		}
		c := g.owner
		c.mu.Lock()
		if g.stopping {
			c.mu.Unlock()
			return
		}
		if g.expireIdleLocked(time.Now()) {
			g.signal()
		}
		closing := g.retired
		g.retired = nil
		cleanup := g.cleanup
		g.cleanup = nil
		c.mu.Unlock()
		for index, rows := range cleanup {
			if !g.cleanupRows(rows) {
				return
			}
			cleanup[index] = nil
		}
		for _, e := range closing {
			var closeErr error
			if e.native != nil {
				closeErr = e.native.Close(g.ctx)
			}
			c.mu.Lock()
			delete(g.entries, e.id)
			if closeErr != nil && !errors.Is(closeErr, q.ErrClosed) && g.ctx.Err() == nil {
				g.err = fmt.Errorf("release upstream dependency: %w", closeErr)
				g.cancel()
			}
			c.mu.Unlock()
		}
	}
}

// expireIdleLocked bounds retirement work under the cache lock. Close of the
// resulting upstream handles belongs to maintain, after releasing that lock.
func (g *generation) expireIdleLocked(now time.Time) bool {
	for count := 0; count < g.owner.options.EvictionBatchSize; count++ {
		id, expiry, ok := g.idle.First()
		if !ok || expiry.After(now) {
			break
		}
		g.retireLocked(g.entries[id])
	}
	_, expiry, ok := g.idle.First()
	return ok && !expiry.After(now)
}

func (g *generation) signal() {
	select {
	case g.wake <- struct{}{}:
	default:
	}
}

func (g *generation) retireLocked(e *entry) {
	g.idle.Remove(e.id)
	if !e.retired {
		e.retired = true
		e.cancel()
		if g.shared[e.fingerprint] == e {
			delete(g.shared, e.fingerprint)
		}
		if len(e.state.rows) > 0 && !g.stopping {
			// Transfer ownership without iterating. Retired state is no longer
			// reachable by snapshots or source dispatch; the worker alone owns it.
			g.cleanup = append(g.cleanup, e.state.rows)
			g.signal()
		}
		e.state.rows = nil
		e.state.order = nil
	}
	if e.finished && !e.closeQueued && !g.stopping {
		e.closeQueued = true
		g.retired = append(g.retired, e)
		g.signal()
	}
}

func (g *generation) load(e *entry, ctx context.Context) {
	defer g.workers.Done()
	install := func(native q.Subscription, snapshot q.QuerySnapshot) error {
		g.owner.mu.Lock()
		defer g.owner.mu.Unlock()
		if e.retired || g.stopping {
			return q.ErrClosed
		}
		e.native = native
		return g.install(e, snapshot)
	}
	var err error
	if e.entity {
		err = g.upstream.SubscribeEntity(ctx, q.EntitySubscribeRequest{ID: e.id, Query: e.query, FetchFrom: e.fetchFrom}, func(native q.EntitySubscription, snapshot q.EntitySnapshot) error {
			value := q.QuerySnapshot{Checkpoint: snapshot.Checkpoint, Fence: snapshot.Fence, Revision: snapshot.Revision}
			if snapshot.Row != nil {
				value.Rows = []q.QueryRow{{Row: *snapshot.Row}}
			}
			return install(native, value)
		})
	} else {
		err = g.upstream.SubscribeQuery(ctx, q.QuerySubscribeRequest{ID: e.id, Query: e.query, Window: e.window, Reference: e.reference}, func(native q.QuerySubscription, snapshot q.QuerySnapshot) error { return install(native, snapshot) })
	}
	g.owner.mu.Lock()
	defer g.owner.mu.Unlock()
	if err == nil && e.native == nil {
		err = errors.New("query client did not install a dependency snapshot")
	}
	if !e.finished {
		e.finished = true
		if e.err == nil {
			e.err = err
		}
		close(e.ready)
	}
	g.syncIdleLocked(e)
	if err != nil {
		g.failEntryLocked(e, err)
	}
	if e.retired {
		g.retireLocked(e)
	}
}

// syncIdleLocked runs at eligibility transitions, not on every source update.
func (g *generation) syncIdleLocked(e *entry) {
	if !e.retired && e.finished && len(e.watchers) == 0 && e.waiters == 0 {
		g.idle.Set(e.id, e.lastUsed.Add(g.owner.options.IdleTTL))
	} else {
		g.idle.Remove(e.id)
	}
}

func (g *generation) failEntryLocked(e *entry, cause error) {
	e.err = cause
	for watcher := range e.watchers {
		event := q.Event{Failure: &q.SubscriptionFailure{SubscriptionID: watcher.id, Cause: cause}}
		if err := watcher.feed.events.Offer(event); err != nil {
			watcher.feed.closeLocked(err)
		}
	}
	g.retireLocked(e)
}

func (g *generation) dispatch(event q.Event) error {
	if err := event.Validate(); err != nil {
		return err
	}
	c := g.owner
	c.mu.Lock()
	defer c.mu.Unlock()
	if g.stopping {
		return q.ErrClosed
	}
	if event.Entity != nil {
		return nil
	} // query client installs parent-owned images
	if failure := event.Failure; failure != nil {
		if e := g.entries[failure.SubscriptionID]; e != nil && !e.retired {
			g.failEntryLocked(e, failure.Cause)
		}
		return nil
	}
	if event.Progress != nil {
		if g.revision != 0 && event.Progress.PreviousRevision != g.revision {
			return errors.New("dependency upstream progress has a revision gap")
		}
		if err := g.positions.ObserveReport(event.Progress.Position); err != nil {
			return err
		}
		g.revision = event.Progress.Revision
		g.checkpoint = copyPosition(event.Progress.Position)
		for f := range g.feeds {
			if err := f.events.Offer(cloneEvent(event)); err != nil {
				f.closeLocked(err)
			}
		}
		return nil
	}
	envelope := event.Delivery
	if g.revision != 0 && envelope.Delivery.PreviousRevision != g.revision {
		return errors.New("dependency upstream delivery has a revision gap")
	}
	g.revision = envelope.Delivery.Revision
	if envelope.Delivery.Kind == q.DeliveryCommit {
		if err := g.positions.Observe(envelope.Delivery.Checkpoint); err != nil {
			return err
		}
		g.checkpoint = copyPosition(envelope.Delivery.Checkpoint)
	}
	byFeed := make(map[*consumer]*q.DeliveryEnvelope)
	for effect := range envelope.SubscriptionEffects() {
		e := g.entries[effect.SubscriptionID]
		if e == nil || e.retired {
			continue
		}
		if envelope.Delivery.Revision <= e.state.revision {
			continue
		}
		if err := g.apply(e, envelope.Delivery, effect.Batch); err != nil {
			g.failEntryLocked(e, fmt.Errorf("materialize dependency: %w", err))
			continue
		}
		for watcher := range e.watchers {
			value := byFeed[watcher.feed]
			if value == nil {
				value = &q.DeliveryEnvelope{Delivery: cloneMetadata(envelope.Delivery)}
				byFeed[watcher.feed] = value
			}
			batch := cloneBatch(effect.Batch)
			if e.entity {
				value.Entities = append(value.Entities, q.EntityEffect{Target: q.EntityTarget{SubscriptionID: watcher.id}, Delta: q.EntityDelta{Clear: batch.Clear, ClearChangeIndex: batch.ClearChangeIndex, Changes: batch.Changes}})
			} else {
				value.Effects = append(value.Effects, q.SubscriptionEffect{SubscriptionID: watcher.id, Batch: batch})
			}
		}
	}
	for f := range g.feeds {
		env := byFeed[f]
		if env == nil {
			env = &q.DeliveryEnvelope{Delivery: cloneMetadata(envelope.Delivery)}
		}
		value := q.Event{Delivery: env}
		if err := f.events.Offer(value); err != nil {
			f.closeLocked(err)
		}
	}
	return nil
}
