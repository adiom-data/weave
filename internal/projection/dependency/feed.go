package dependency

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/adiom-data/weave/internal/querysubscription/feedbuffer"
	q "github.com/adiom-data/weave/internal/querysubscription/model"
	"github.com/adiom-data/weave/internal/storage"
)

type consumer struct {
	generation    *generation
	events        *feedbuffer.Buffer
	facts         bool
	closed        bool
	used          map[string]struct{}
	subscriptions map[string]*subscription
	stop          func() bool
}

func newConsumer(g *generation, limit int, facts bool) *consumer {
	return &consumer{generation: g, events: feedbuffer.New(limit), facts: facts, used: make(map[string]struct{}), subscriptions: make(map[string]*subscription)}
}

func (f *consumer) Next(ctx context.Context) (q.Event, error) { return f.events.Next(ctx) }
func (f *consumer) Close(context.Context) error {
	f.generation.owner.mu.Lock()
	defer f.generation.owner.mu.Unlock()
	f.closeLocked(q.ErrClosed)
	return nil
}
func (f *consumer) closeLocked(reason error) {
	if f.closed {
		return
	}
	f.closed = true
	if f.stop != nil {
		f.stop()
	}
	for _, subscription := range f.subscriptions {
		subscription.detachLocked()
	}
	delete(f.generation.feeds, f)
	f.events.Close(reason)
}

type subscription struct {
	id       string
	feed     *consumer
	entry    *entry
	closed   bool
	fixed    bool
	windowMu sync.Mutex
}

func (s *subscription) ID() string { return s.id }
func (s *subscription) Close(context.Context) error {
	s.feed.generation.owner.mu.Lock()
	defer s.feed.generation.owner.mu.Unlock()
	s.detachLocked()
	return nil
}
func (s *subscription) detachLocked() {
	if s.closed {
		return
	}
	s.closed = true
	delete(s.feed.subscriptions, s.id)
	if s.entry != nil {
		e := s.entry
		delete(e.watchers, s)
		e.lastUsed = time.Now()
		if len(e.watchers) == 0 && e.waiters == 0 && (e.private || !e.finished) {
			e.generation.retireLocked(e)
		}
		e.generation.syncIdleLocked(e)
	}
}
func (s *subscription) UpdateWindow(ctx context.Context, changes []q.BoundaryChange, token string) error {
	s.windowMu.Lock()
	defer s.windowMu.Unlock()
	c := s.feed.generation.owner
	c.mu.Lock()
	if s.closed || s.feed.closed || s.entry == nil || s.entry.retired {
		c.mu.Unlock()
		return q.ErrClosed
	}
	if s.fixed {
		c.mu.Unlock()
		return errors.New("fact dependency windows are immutable")
	}
	native, ok := s.entry.native.(q.QuerySubscription)
	c.mu.Unlock()
	if !ok {
		return errors.New("entity dependencies have no mutable window")
	}
	return native.UpdateWindow(ctx, changes, token)
}

func (f *consumer) SubscribeEntity(ctx context.Context, request q.EntitySubscribeRequest, install q.EntitySnapshotInstaller) error {
	if err := q.ValidateEntitySubscribeRequest(request); err != nil {
		return err
	}
	if install == nil {
		return errors.New("snapshot installer is required")
	}
	return f.subscribe(ctx, request.ID, request.Query, q.InitialWindow{Count: 1}, true, true, loadHints{from: request.FetchFrom}, func(subscription *subscription, snapshot q.QuerySnapshot) error {
		value := q.EntitySnapshot{Checkpoint: snapshot.Checkpoint, Fence: snapshot.Fence, Revision: snapshot.Revision}
		if len(snapshot.Rows) > 0 {
			row := snapshot.Rows[0].Row
			value.Row = &row
		}
		return install(subscription, value)
	})
}
func (f *consumer) SubscribeQuery(ctx context.Context, request q.QuerySubscribeRequest, install q.QuerySnapshotInstaller) error {
	if err := q.ValidateQuerySubscribeRequest(request); err != nil {
		return err
	}
	if install == nil {
		return errors.New("snapshot installer is required")
	}
	return f.subscribe(ctx, request.ID, request.Query, request.Window, false, f.facts, loadHints{reference: request.Reference}, func(s *subscription, snapshot q.QuerySnapshot) error { return install(s, snapshot) })
}

// SubscribeFixedQuery shares an immutable selector while delivering its effects
// on this same feed as ordinary projection subscriptions. It is an internal
// dependency-cache capability, not an extension of the query wire protocol.
func (f *consumer) SubscribeFixedQuery(ctx context.Context, request q.QuerySubscribeRequest, install q.QuerySnapshotInstaller) error {
	if err := q.ValidateQuerySubscribeRequest(request); err != nil {
		return err
	}
	if install == nil {
		return errors.New("snapshot installer is required")
	}
	return f.subscribe(ctx, request.ID, request.Query, request.Window, false, true, loadHints{reference: request.Reference}, func(s *subscription, snapshot q.QuerySnapshot) error { return install(s, snapshot) })
}

// Structural fingerprints intentionally do not promise adapter-level semantic
// normalization. Equal definitions share; equivalent but differently expressed
// queries may miss the cache, never collide or inherit another query's results.
func definition(query storage.QuerySpec, window q.InitialWindow, entity bool) (string, storage.QuerySpec, error) {
	data, err := json.Marshal(struct {
		Query  storage.QuerySpec
		Window q.InitialWindow
		Entity bool
	}{query, window, entity})
	if err != nil {
		return "", storage.QuerySpec{}, err
	}
	var copied struct {
		Query  storage.QuerySpec
		Window q.InitialWindow
		Entity bool
	}
	if err := json.Unmarshal(data, &copied); err != nil {
		return "", storage.QuerySpec{}, err
	}
	return fmt.Sprintf("%x", sha256.Sum256(data)), copied.Query, nil
}

type loadHints struct {
	reference *storage.ReferenceFetch
	from      *q.ReferenceSource
}

func (f *consumer) subscribe(ctx context.Context, id string, query storage.QuerySpec, window q.InitialWindow, entity, fixed bool, hints loadHints, install func(*subscription, q.QuerySnapshot) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	fingerprint, query, err := definition(query, window, entity)
	g, c := f.generation, f.generation.owner
	c.mu.Lock()
	if f.closed || g.stopping {
		c.mu.Unlock()
		return q.ErrClosed
	}
	if _, used := f.used[id]; used {
		c.mu.Unlock()
		return errors.New("subscription ID was already used in feed")
	}
	f.used[id] = struct{}{}
	if err != nil {
		c.mu.Unlock()
		return err
	}
	s := &subscription{id: id, feed: f, fixed: fixed}
	f.subscriptions[id] = s
	e := g.shared[fingerprint]
	// Parent-owned handles cannot outlive or be shared independently of their
	// parent interest. Their entity cells are still shared by the generation.
	private := !entity && !fixed || hints.from != nil
	if private {
		e = nil
	}
	if e == nil {
		if len(g.entries) >= c.options.MaxSubscriptions {
			delete(f.subscriptions, id)
			c.mu.Unlock()
			return errors.New("shared dependency subscription limit reached")
		}
		c.sequence++
		loadCtx, cancel := context.WithCancel(g.ctx)
		e = &entry{id: fmt.Sprintf("dependency-%d", c.sequence), fingerprint: fingerprint, generation: g, entity: entity, private: private, query: query, window: window, ready: make(chan struct{}), cancel: cancel, watchers: make(map[*subscription]struct{}), lastUsed: time.Now()}
		g.entries[e.id] = e
		if from := hints.from; from != nil {
			if parent := f.subscriptions[from.SubscriptionID]; parent != nil && !parent.closed && parent.entry != nil && !parent.entry.retired {
				e.fetchFrom = &q.ReferenceSource{SubscriptionID: parent.entry.id, ParentKey: from.ParentKey}
			}
		}
		// Mutable public list windows are private entries; the hint changes
		// their loading, not the shared definition or entity/fact cache keys.
		if reference := hints.reference; reference != nil {
			encoded, copyErr := json.Marshal(reference)
			if copyErr != nil {
				delete(g.entries, e.id)
				delete(f.subscriptions, id)
				cancel()
				c.mu.Unlock()
				return copyErr
			}
			if copyErr := json.Unmarshal(encoded, &e.reference); copyErr != nil {
				delete(g.entries, e.id)
				delete(f.subscriptions, id)
				cancel()
				c.mu.Unlock()
				return copyErr
			}
		}
		if !private {
			g.shared[fingerprint] = e
		}
		g.workers.Add(1)
		go g.load(e, loadCtx)
	}
	e.waiters++
	g.syncIdleLocked(e)
	s.entry = e
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		e.waiters--
		e.lastUsed = time.Now()
		if e.waiters == 0 && len(e.watchers) == 0 && (e.private || !e.finished) {
			g.retireLocked(e)
		}
		g.syncIdleLocked(e)
		c.mu.Unlock()
	}()
	select {
	case <-e.ready:
	case <-ctx.Done():
		_ = s.Close(context.Background())
		return ctx.Err()
	case <-g.ctx.Done():
		_ = s.Close(context.Background())
		return g.ctx.Err()
	}
	c.mu.Lock()
	err = ctx.Err()
	if err == nil {
		err = e.err
	}
	// A failed load retires its entry. Report that cause rather than replacing
	// it with ErrClosed merely because retirement has already completed.
	if err == nil && (s.closed || f.closed || e.retired || g.stopping) {
		err = q.ErrClosed
	}
	if err != nil {
		s.detachLocked()
		c.mu.Unlock()
		return err
	}
	// Snapshot callbacks are short consumer-local operations, but may block.
	// Only this logical feed stages output; shared ingestion keeps progressing.
	f.events.BeginInstallation()
	e.watchers[s] = struct{}{}
	snapshot := g.snapshot(e)
	c.mu.Unlock()
	err = install(s, snapshot)
	if err != nil {
		_ = s.Close(context.Background())
		f.events.DiscardSubscription(id)
	}
	finishErr := f.events.FinishInstallation()
	if finishErr != nil {
		_ = f.Close(context.Background())
	}
	return errors.Join(err, finishErr)
}

var _ q.Client = (*Cache)(nil)
var _ q.Feed = (*consumer)(nil)
