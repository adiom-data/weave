// Package authorization owns projection-side authorization dependencies. It
// consumes only the query subscription contract; it cannot inspect query
// engine caches or read a datastore behind the ordered feed.
package authorization

import (
	"container/list"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"github.com/adiom-data/weave/internal/accesscontrol"
	q "github.com/adiom-data/weave/internal/querysubscription/model"
	"github.com/adiom-data/weave/internal/querysubscription/position"
	"github.com/adiom-data/weave/internal/storage"
)

var ErrCoverageLimit = errors.New("authorization fact selector exceeds its complete-coverage limit")

// QueryRequest/Result are private leaf materialization contracts. Policies
// request projection plans; only the authorization runtime deals in selectors.
type QueryRequest struct {
	ID    string
	Query storage.QuerySpec
}
type QueryResult struct {
	Rows     []accesscontrol.Candidate
	Complete bool
}

type Options struct {
	MaxQueries      int
	MaxRows         int
	LoadConcurrency int
}

// Facts materializes one ordered consumer's private authorization state. Feed
// should come from the projection server's shared dependency cache: separate
// consumers then share upstream subscriptions without sharing mutable clocks.
// The caller owns the single Next loop and joins returned frames with ordinary
// projection frames before evaluating a view. Facts never pumps ahead itself.
type Facts struct {
	mu        sync.Mutex
	ctx       context.Context
	cancel    context.CancelFunc
	feed      q.Feed
	options   Options
	entries   map[string]*factEntry
	byID      map[string]*factEntry
	awaiting  map[string]*factEntry // installed snapshots awaiting source coverage
	sequence  uint64
	position  *q.Position
	revision  uint64
	positions position.Tracker
	jobs      chan struct{}
	pending   list.List
	wake      chan struct{}
	dirty     map[string]struct{}
	workers   sync.WaitGroup
	closed    bool
	failure   error
	closeOnce sync.Once
	closeErr  error
}

type factEntry struct {
	id, key   string
	query     storage.QuerySpec
	refs      int
	ctx       context.Context
	cancel    context.CancelFunc
	handle    q.QuerySubscription
	installed bool
	err       error
	baseline  *q.Position
	revision  uint64
	rows      map[storage.EntityKey]q.Row
	order     []storage.EntityKey
	job       *list.Element
}

type Lease struct {
	owner  *Facts
	entry  *factEntry
	closed bool
}

// retain creates an additional ownership reference without rebinding or
// fingerprinting an already registered selector.
func (l *Lease) retain() (*Lease, error) {
	f := l.owner
	f.mu.Lock()
	defer f.mu.Unlock()
	if l.closed || f.closed {
		return nil, q.ErrClosed
	}
	l.entry.refs++
	return &Lease{owner: f, entry: l.entry}, nil
}

// NewObservedFacts borrows the projection's feed. Its owner passes every frame
// to Observe before applying projection effects; there is no second reader or
// independent stream to synchronize. Closing the store releases only its own
// fact subscriptions, not the borrowed feed.
func NewObservedFacts(ctx context.Context, feed q.Feed, options Options) (*Facts, error) {
	if feed == nil || options.MaxQueries < 1 || options.MaxRows < 1 || options.LoadConcurrency < 1 || options.MaxRows == int(^uint(0)>>1) {
		return nil, errors.New("authorization facts require a feed and positive bounded options")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	run, cancel := context.WithCancel(ctx)
	f := &Facts{ctx: run, cancel: cancel, feed: feed, options: options, entries: make(map[string]*factEntry), byID: make(map[string]*factEntry), awaiting: make(map[string]*factEntry), jobs: make(chan struct{}, 1), wake: make(chan struct{}, 1), dirty: make(map[string]struct{})}
	for range options.LoadConcurrency {
		f.workers.Add(1)
		go f.loadWorker()
	}
	return f, nil
}

// Changed wakes the owner when an asynchronous installation completes or fails.
// A wake is not a source checkpoint. The owner may request existing progress
// from its query feeds, but must not fabricate an ordered transaction from it.
func (f *Facts) Changed() <-chan struct{}                  { return f.wake }
func (f *Facts) RequestProgress(ctx context.Context) error { return f.feed.RequestProgress(ctx) }
func (f *Facts) notify() {
	select {
	case f.wake <- struct{}{}:
	default:
	}
}

// DrainChanges returns dependency IDs, not entity invalidations. The owner can
// use its occurrence dependency index to reevaluate only affected decisions.
// Read still enforces the consumed-position boundary after this notification.
func (f *Facts) DrainChanges() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	ids := make([]string, 0, len(f.dirty))
	for id := range f.dirty {
		ids = append(ids, id)
	}
	clear(f.dirty)
	return ids
}
func (f *Facts) mark(e *factEntry) {
	if e.refs > 0 && !f.closed {
		f.dirty[e.id] = struct{}{}
	}
}

// Acquire only registers work. IO runs in the bounded worker pool, never on
// the projection actor or the query server's CDC path.
func (f *Facts) Acquire(request QueryRequest) (*Lease, error) {
	key, query, err := queryKey(request.Query)
	if err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return nil, q.ErrClosed
	}
	if f.failure != nil {
		return nil, f.failure
	}
	if err := f.ctx.Err(); err != nil {
		return nil, err
	}
	e := f.entries[key]
	if e == nil {
		if len(f.entries) >= f.options.MaxQueries {
			return nil, errors.New("authorization fact subscription limit reached")
		}
		f.sequence++
		ctx, cancel := context.WithCancel(f.ctx)
		e = &factEntry{id: fmt.Sprintf("authorization-%d", f.sequence), key: key, query: query, ctx: ctx, cancel: cancel, rows: make(map[storage.EntityKey]q.Row)}
		f.entries[key] = e
		f.byID[e.id] = e
		e.job = f.pending.PushBack(e)
		f.signalJob()
	}
	e.refs++
	return &Lease{owner: f, entry: e}, nil
}

func (l *Lease) Close(ctx context.Context) error {
	f := l.owner
	f.mu.Lock()
	if l.closed {
		f.mu.Unlock()
		return nil
	}
	l.closed = true
	if f.closed {
		f.mu.Unlock()
		return nil
	}
	e := l.entry
	e.refs--
	var handle q.QuerySubscription
	if e.refs == 0 {
		delete(f.entries, e.key)
		delete(f.byID, e.id)
		delete(f.awaiting, e.id)
		delete(f.dirty, e.id)
		e.cancel()
		e.rows = nil
		e.order = nil
		if e.job != nil {
			f.pending.Remove(e.job)
			e.job = nil
		}
		handle = e.handle
		e.handle = nil
	}
	f.mu.Unlock()
	if handle != nil {
		return handle.Close(ctx)
	}
	return nil
}

func (l *Lease) ID() string { return l.entry.id }

// Read returns evidence only at the consumer's explicitly consumed position.
// It never substitutes newer cache contents for an older evaluation. Baselines
// newer than that position remain unresolved until the feed catches up.
func (l *Lease) Read(at *q.Position) (QueryResult, error) {
	f := l.owner
	f.mu.Lock()
	defer f.mu.Unlock()
	if l.closed || f.closed {
		return QueryResult{}, q.ErrClosed
	}
	if f.failure != nil {
		return QueryResult{}, f.failure
	}
	if err := f.ctx.Err(); err != nil {
		return QueryResult{}, err
	}
	e := l.entry
	if e.err != nil {
		return QueryResult{}, e.err
	}
	if !e.installed || at == nil || f.position == nil || !q.SamePosition(at, f.position) {
		return QueryResult{}, nil
	}
	compared, err := q.ComparePositions(at, e.baseline)
	if err != nil {
		return QueryResult{}, err
	}
	if compared < 0 {
		return QueryResult{}, nil
	}
	result := QueryResult{Complete: true, Rows: make([]accesscontrol.Candidate, 0, len(e.order))}
	for _, key := range e.order {
		row := e.rows[key]
		result.Rows = append(result.Rows, accesscontrol.Candidate{Key: key, Record: storage.CloneRecord(row.Record), Complete: !row.Pending})
	}
	return result, nil
}

func (f *Facts) loadWorker() {
	defer f.workers.Done()
	for {
		select {
		case <-f.ctx.Done():
			return
		case <-f.jobs:
			f.mu.Lock()
			next := f.pending.Front()
			if next == nil {
				f.mu.Unlock()
				continue
			}
			e := next.Value.(*factEntry)
			f.pending.Remove(next)
			e.job = nil
			if f.pending.Len() > 0 {
				f.signalJob()
			}
			f.mu.Unlock()
			if e.ctx.Err() != nil {
				continue
			}
			subscribe := f.feed.SubscribeQuery
			if fixed, ok := f.feed.(interface {
				SubscribeFixedQuery(context.Context, q.QuerySubscribeRequest, q.QuerySnapshotInstaller) error
			}); ok {
				subscribe = fixed.SubscribeFixedQuery
			}
			err := subscribe(e.ctx, q.QuerySubscribeRequest{ID: e.id, Query: e.query, Window: q.InitialWindow{Count: f.options.MaxRows + 1}}, func(handle q.QuerySubscription, snapshot q.QuerySnapshot) error {
				f.mu.Lock()
				defer f.mu.Unlock()
				if f.closed || e.refs == 0 || e.ctx.Err() != nil {
					return q.ErrClosed
				}
				if e.err != nil {
					return e.err
				}
				if e.installed {
					return errors.New("authorization fact snapshot installed twice")
				}
				if snapshot.Bounds.Left.Kind != q.BoundaryStart || snapshot.Bounds.Right.Kind != q.BoundaryEnd || len(snapshot.Rows) > f.options.MaxRows {
					return ErrCoverageLimit
				}
				baseline, err := q.SnapshotPosition(snapshot.Checkpoint, snapshot.Fence)
				if err != nil {
					return err
				}
				if f.position != nil && f.position.SourceID != baseline.SourceID {
					return errors.New("authorization fact snapshot has a foreign source")
				}
				rows := make(map[storage.EntityKey]q.Row, len(snapshot.Rows))
				order := make([]storage.EntityKey, 0, len(snapshot.Rows))
				for _, row := range snapshot.Rows {
					if err := validateRow(e.query.Relation, baseline.SourceID, row.Row); err != nil {
						return err
					}
					if _, exists := rows[row.Key]; exists {
						return errors.New("authorization fact snapshot has duplicate identities")
					}
					rows[row.Key] = copyRow(row.Row)
					order = append(order, row.Key)
				}
				e.handle = handle
				e.baseline = copyPosition(baseline)
				e.revision = snapshot.Revision
				e.rows = rows
				e.order = order
				e.installed = true
				if f.position == nil || f.position.Value < baseline.Value {
					f.awaiting[e.id] = e
				}
				return nil
			})
			f.mu.Lock()
			if err == nil && !e.installed {
				err = errors.New("query feed completed without installing an authorization fact snapshot")
			}
			var closeHandle q.QuerySubscription
			if err != nil || e.err != nil || f.closed || e.refs == 0 {
				if err != nil {
					e.err = err
				}
				closeHandle = e.handle
				e.handle = nil
			}
			f.mark(e)
			f.notify()
			f.mu.Unlock()
			if closeHandle != nil {
				_ = closeHandle.Close(context.WithoutCancel(f.ctx))
			}
		}
	}
}

func (f *Facts) signalJob() {
	select {
	case f.jobs <- struct{}{}:
	default:
	}
}

func (f *Facts) Observe(event q.Event) error {
	err := f.apply(event)
	if err != nil {
		f.mu.Lock()
		if f.failure == nil {
			f.failure = err
		}
		f.mu.Unlock()
	}
	return err
}

func (f *Facts) apply(event q.Event) error {
	if err := event.Validate(); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return q.ErrClosed
	}
	if event.Failure != nil {
		if e := f.byID[event.Failure.SubscriptionID]; e != nil {
			e.err = event.Failure.Cause
			f.mark(e)
		}
		return nil
	}
	var revision, previous uint64
	var at *q.Position
	if event.Progress != nil {
		revision, previous = event.Progress.Revision, event.Progress.PreviousRevision
		at = event.Progress.Position
		if err := f.positions.ObserveReport(at); err != nil {
			return err
		}
	} else {
		metadata := event.Delivery.Delivery
		revision, previous = metadata.Revision, metadata.PreviousRevision
		if metadata.Kind == q.DeliveryCommit {
			at = metadata.Checkpoint
			if err := f.positions.Observe(at); err != nil {
				return err
			}
		}
	}
	// Shared dependency feeds can attach to an already-running generation.
	// Its first frame establishes the predecessor; every later link is strict.
	if revision <= f.revision || f.revision != 0 && previous != f.revision {
		return errors.New("authorization fact feed has a broken revision chain")
	}
	if at != nil {
		if f.position != nil && f.position.SourceID != at.SourceID {
			return errors.New("authorization fact feed changed sources")
		}
		f.position = copyPosition(at)
		// Only installed, not-yet-covered snapshots need a readiness edge.
		// Repeated reports and unrelated progress do not invalidate every
		// unresolved occurrence; outstanding IO has nothing new to evaluate.
		for id, e := range f.awaiting {
			if e.baseline.SourceID != at.SourceID {
				return errors.New("authorization fact snapshot has a foreign source")
			}
			if e.baseline.Value <= at.Value {
				f.mark(e)
				delete(f.awaiting, id)
			}
		}
	}
	if event.Delivery != nil {
		for effect := range event.Delivery.SubscriptionEffects() {
			e := f.byID[effect.SubscriptionID]
			if e == nil || e.err != nil || !e.installed || revision <= e.revision {
				continue
			}
			if err := f.applyEffect(e, effect.Batch); err != nil {
				e.err = err
			}
			e.revision = revision
			f.mark(e)
		}
	}
	f.revision = revision
	return nil
}

func (f *Facts) applyEffect(e *factEntry, batch q.Batch) error {
	if batch.Window != nil {
		return errors.New("authorization fact window unexpectedly changed")
	}
	if batch.Clear {
		e.rows = make(map[storage.EntityKey]q.Row)
		e.order = nil
	}
	order, err := q.ApplyMembership(e.order, batch.Membership)
	if err != nil {
		return err
	}
	if len(order) > f.options.MaxRows {
		return ErrCoverageLimit
	}
	for _, change := range batch.Changes {
		switch change.Kind {
		case q.Removed:
			delete(e.rows, change.Key)
		case q.Added, q.Updated:
			if err := validateRow(e.query.Relation, e.baseline.SourceID, change.Row); err != nil {
				return err
			}
			e.rows[change.Key] = copyRow(change.Row)
		default:
			return errors.New("authorization fact change has an unknown kind")
		}
	}
	if len(order) != len(e.rows) {
		return errors.New("authorization fact membership and rows disagree")
	}
	for _, key := range order {
		if _, exists := e.rows[key]; !exists {
			return errors.New("authorization fact membership has no row")
		}
	}
	e.order = order
	return nil
}

func (f *Facts) Close() error {
	f.closeOnce.Do(func() {
		f.mu.Lock()
		f.closed = true
		f.cancel()
		var handles []q.QuerySubscription
		for _, e := range f.entries {
			e.cancel()
			if e.handle != nil {
				handles = append(handles, e.handle)
				e.handle = nil
			}
			e.rows = nil
			e.order = nil
		}
		f.entries = make(map[string]*factEntry)
		f.byID = make(map[string]*factEntry)
		f.pending.Init()
		clear(f.dirty)
		clear(f.awaiting)
		f.mu.Unlock()
		for _, handle := range handles {
			f.closeErr = errors.Join(f.closeErr, handle.Close(context.WithoutCancel(f.ctx)))
		}
		f.workers.Wait()
	})
	return f.closeErr
}

func queryKey(query storage.QuerySpec) (string, storage.QuerySpec, error) {
	encoded, err := json.Marshal(query)
	if err != nil {
		return "", storage.QuerySpec{}, err
	}
	var copy storage.QuerySpec
	if err = json.Unmarshal(encoded, &copy); err != nil {
		return "", storage.QuerySpec{}, err
	}
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:]), copy, nil
}

func copyPosition(at *q.Position) *q.Position {
	if at == nil {
		return nil
	}
	copy := *at
	return &copy
}
func copyRow(row q.Row) q.Row {
	row.Record = storage.CloneRecord(row.Record)
	row.Cursor = append([]byte(nil), row.Cursor...)
	return row
}
func validateRow(relation storage.RelationName, source storage.SourceID, row q.Row) error {
	if row.Key.Value == "" || row.Key.Relation.Source != source || row.Key.Relation.Namespace != relation.Namespace || row.Key.Relation.Name != relation.Name {
		return errors.New("authorization fact row has an invalid or foreign identity")
	}
	if !row.Pending {
		if row.Record == nil {
			return errors.New("authorization fact row has no authoritative record")
		}
		for field, value := range row.Record {
			if err := value.ValidateComplete(); err != nil {
				return fmt.Errorf("authorization fact field %s: %w", field, err)
			}
		}
	}
	return nil
}
