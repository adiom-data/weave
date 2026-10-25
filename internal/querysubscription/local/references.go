package local

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	q "github.com/adiom-data/weave/internal/querysubscription/model"
	"github.com/adiom-data/weave/internal/storage"
)

// ownedReferences is a one-hop subscription lifecycle, not a second query
// evaluator. Bound children use the ordinary entity subscription machinery and
// transaction group. Only their identities and ownership are private here.
type ownedReferences struct {
	mu      sync.Mutex
	parent  *localSubscription
	shape   storage.ReferenceFetch
	closed  bool
	failed  bool
	parents map[storage.EntityKey]*ownedReferenceChild
	// Pending rows retain their last known interest, but cannot establish a
	// new binding until an authoritative image arrives.
	pending  map[storage.EntityKey]bool
	children map[string]*ownedReferenceChild
	initial  []referenceSeed
}

type ownedReferenceChild struct {
	owner             *ownedReferences
	id, fingerprint   string
	query             storage.QuerySpec
	parents           map[storage.EntityKey]bool
	first             storage.EntityKey
	pins              int
	loading, finished bool
	queued, retired   bool
	waiting           map[storage.EntityKey]bool
	ready             chan struct{}
	err               error
	cancel            context.CancelFunc
	native            *localSubscription
	// Retained images are private and immutable: updates replace Row rather
	// than mutating its record, and outgoing images are detached with
	// cloneEntityImage. Initial and current can therefore share an image.
	initial q.EntitySnapshot
	current q.EntitySnapshot
}

type referenceSeed struct {
	child   *ownedReferenceChild
	parents []storage.EntityKey
}

var referenceSequence atomic.Uint64

func newOwnedReferences(parent *localSubscription, shape storage.ReferenceFetch, rows []q.Row) (*ownedReferences, error) {
	encoded, err := json.Marshal(shape)
	if err != nil {
		return nil, err
	}
	var detached storage.ReferenceFetch
	if err := json.Unmarshal(encoded, &detached); err != nil {
		return nil, err
	}
	shape = detached
	r := &ownedReferences{parent: parent, shape: shape, parents: map[storage.EntityKey]*ownedReferenceChild{}, pending: map[storage.EntityKey]bool{}, children: map[string]*ownedReferenceChild{}}
	seeds := map[*ownedReferenceChild]int{}
	for _, row := range rows {
		if row.Pending {
			r.pending[row.Key] = true
			continue
		}
		child, _, err := r.bindLocked(row.Key, row.Record)
		if err != nil {
			return nil, err
		}
		if child == nil {
			continue
		}
		index, exists := seeds[child]
		if !exists {
			index = len(r.initial)
			seeds[child] = index
			child.pins++
			r.initial = append(r.initial, referenceSeed{child: child})
		}
		r.initial[index].parents = append(r.initial[index].parents, row.Key)
	}
	return r, nil
}

func (r *ownedReferences) bindLocked(key storage.EntityKey, record storage.Record) (*ownedReferenceChild, bool, error) {
	query, _, matchable, err := storage.BindReference(r.shape, record)
	if err != nil {
		return nil, false, err
	}
	var fp string
	if matchable {
		encoded, err := json.Marshal(query)
		if err != nil {
			return nil, false, err
		}
		fp = fmt.Sprintf("%x", sha256.Sum256(encoded))
	}
	previous := r.parents[key]
	if previous != nil && previous.fingerprint == fp {
		return previous, false, nil
	}
	if previous != nil {
		delete(previous.parents, key)
		delete(previous.waiting, key)
		delete(r.parents, key)
	}
	if !matchable {
		return nil, previous != nil, nil
	}
	child := r.children[fp]
	if child == nil {
		child = &ownedReferenceChild{owner: r, id: fmt.Sprintf("\x00reference:%d", referenceSequence.Add(1)), fingerprint: fp, query: query, parents: map[storage.EntityKey]bool{}, waiting: map[storage.EntityKey]bool{}, first: key, ready: make(chan struct{})}
		r.children[fp] = child
	}
	child.parents[key] = true
	r.parents[key] = child
	return child, true, nil
}

func (r *ownedReferences) retireLocked(child *ownedReferenceChild) *localSubscription {
	if child.retired || len(child.parents) != 0 || child.pins != 0 {
		return nil
	}
	child.retired = true
	if r.children[child.fingerprint] == child {
		delete(r.children, child.fingerprint)
	}
	if child.cancel != nil {
		child.cancel()
	}
	return child.native
}

func (r *ownedReferences) retireNative(native []*localSubscription) {
	if len(native) == 0 {
		return
	}
	r.parent.owner.retireReference(func() error {
		var result error
		for _, child := range native {
			result = errors.Join(result, child.Close(r.parent.owner.ctx))
		}
		return result
	})
}

func entityImage(snapshot q.QuerySnapshot) q.EntitySnapshot {
	image := q.EntitySnapshot{Checkpoint: snapshot.Checkpoint, Fence: snapshot.Fence, Revision: snapshot.Revision}
	if len(snapshot.Rows) > 0 {
		row := snapshot.Rows[0].Row
		row.Cursor = nil
		image.Row = &row
	}
	return image
}

func cloneEntityImage(image q.EntitySnapshot) q.EntitySnapshot {
	if image.Checkpoint != nil {
		value := *image.Checkpoint
		image.Checkpoint = &value
	}
	if image.Fence != nil {
		value := *image.Fence
		image.Fence = &value
	}
	if image.Row != nil {
		row := *image.Row
		row.Record = storage.CloneRecord(row.Record)
		row.Cursor = nil
		image.Row = &row
	}
	return image
}

// adoptInitial consumes the installer's detached image under owner.mu. The
// caller must not subsequently mutate it. No data copy is needed between these
// internal owners; publication still copies at the mutable caller boundary.
func (child *ownedReferenceChild) adoptInitial(image q.EntitySnapshot) {
	if child.pins > 0 {
		child.initial = image
	}
	if image.Revision >= child.current.Revision {
		child.current = image
	}
}

func (r *ownedReferences) load(ctx context.Context, child *ownedReferenceChild) error {
	r.mu.Lock()
	if r.closed || r.failed || child.retired {
		r.mu.Unlock()
		return q.ErrClosed
	}
	if child.loading || child.finished {
		ready := child.ready
		r.mu.Unlock()
		select {
		case <-ready:
			r.mu.Lock()
			err := child.err
			r.mu.Unlock()
			return err
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	child.loading = true
	child.queued = false
	loadCtx, cancel := context.WithCancel(ctx)
	child.cancel = cancel
	first := child.first
	for key := range child.parents {
		first = key
		break
	}
	r.mu.Unlock()
	request := q.QuerySubscribeRequest{ID: child.id, Query: child.query, Window: q.InitialWindow{Count: 1}}
	err := r.parent.owner.subscribe(loadCtx, request, true, &q.ReferenceSource{SubscriptionID: r.parent.id, ParentKey: first}, func(native *localSubscription, snapshot q.QuerySnapshot) error {
		image := entityImage(snapshot)
		r.mu.Lock()
		defer r.mu.Unlock()
		if r.closed || r.failed || child.retired {
			return q.ErrClosed
		}
		child.native = native
		child.adoptInitial(image)
		return nil
	}, child)
	r.finishLoad(child, err)
	return err
}

func (r *ownedReferences) finishLoad(child *ownedReferenceChild, err error) {
	r.mu.Lock()
	child.err = err
	child.finished = true
	close(child.ready)
	native := r.retireLocked(child)
	r.mu.Unlock()
	if native != nil {
		r.retireNative([]*localSubscription{native})
	}
	if err == nil {
		r.publishAvailable(child)
	}
}

func (r *ownedReferences) loadInitial(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var children []*ownedReferenceChild
	r.mu.Lock()
	if r.closed || r.failed {
		r.mu.Unlock()
		return q.ErrClosed
	}
	for _, seed := range r.initial {
		child := seed.child
		if child.loading || child.finished {
			continue
		}
		child.loading, child.queued = true, false
		child.cancel = cancel
		children = append(children, child)
	}
	r.mu.Unlock()
	var firstErr error
	r.parent.owner.subscribeReferences(ctx, r.parent.id, children, func(child *ownedReferenceChild, err error) {
		if err != nil && firstErr == nil {
			firstErr = err
			cancel()
		}
		r.finishLoad(child, err)
	})
	if firstErr != nil {
		return firstErr
	}
	// A live binding may already have started an interest before initialization
	// reached it. Join that one load rather than registering it twice.
	for _, seed := range r.initial {
		if err := r.load(ctx, seed.child); err != nil {
			return err
		}
	}
	return nil
}

// schedule is a nonblocking post-publication action. A child has at most one
// queued/running load; newly attached parents wait on that same result.
func (r *ownedReferences) schedule(child *ownedReferenceChild) {
	r.mu.Lock()
	if r.closed || r.failed || child.retired {
		r.mu.Unlock()
		return
	}
	if child.finished {
		err := child.err
		r.mu.Unlock()
		if err != nil {
			r.fail(err)
		} else {
			r.publishAvailable(child)
		}
		return
	}
	if child.queued || child.loading {
		r.mu.Unlock()
		return
	}
	child.queued = true
	r.mu.Unlock()
	err := r.parent.owner.referenceWork.submit(func() {
		if err := r.load(r.parent.owner.ctx, child); err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, q.ErrClosed) {
			r.fail(err)
		}
	}, false)
	if err != nil {
		r.fail(err)
	}
}

func (r *ownedReferences) fail(err error) {
	r.mu.Lock()
	if r.closed || r.failed {
		r.mu.Unlock()
		return
	}
	r.failed = true
	r.parent.markFailed()
	for _, child := range r.children {
		if child.native != nil {
			child.native.markFailed()
		}
		if child.cancel != nil {
			child.cancel()
		}
	}
	r.mu.Unlock()
	if offerErr := r.parent.owner.offer(q.Event{Failure: &q.SubscriptionFailure{SubscriptionID: r.parent.id, Cause: err}}); offerErr != nil {
		r.parent.owner.fail(offerErr)
	}
	r.parent.owner.retireReference(func() error { return r.parent.Close(r.parent.owner.ctx) })
}

// Publication is grouped by selector, not by parent. The adapter checks the
// selector and each handle's own delivery floor if this image races a commit.
func (r *ownedReferences) publishAvailable(child *ownedReferenceChild) {
	r.mu.Lock()
	if r.closed || r.failed || child.retired || !child.finished || child.err != nil || len(child.waiting) == 0 {
		r.mu.Unlock()
		return
	}
	installation := q.EntityInstallation{Target: child.target(), Snapshot: cloneEntityImage(child.current)}
	for key := range child.waiting {
		if r.parents[key] == child {
			installation.Target.Reference.Parents = append(installation.Target.Reference.Parents, key)
		}
		delete(child.waiting, key)
	}
	r.mu.Unlock()
	if len(installation.Target.Reference.Parents) > 0 {
		if err := r.parent.owner.offer(q.Event{Entity: &installation}); err != nil {
			r.parent.owner.fail(err)
		}
	}
}

func (r *ownedReferences) attachInitialSnapshots(ctx context.Context, rows []q.QueryRow) error {
	if err := r.loadInitial(ctx); err != nil {
		return err
	}
	indices := make(map[storage.EntityKey]int, len(rows))
	for index, row := range rows {
		indices[row.Key] = index
	}
	for _, seed := range r.initial {
		r.mu.Lock()
		for _, key := range seed.parents {
			if index, exists := indices[key]; exists {
				// Each result row owns its image even when the underlying
				// interest and cached entity are shared by several rows.
				image := cloneEntityImage(seed.child.initial)
				rows[index].Expanded = &image
			}
		}
		seed.child.initial = q.EntitySnapshot{}
		r.mu.Unlock()
	}
	r.mu.Lock()
	var retired []*localSubscription
	for _, seed := range r.initial {
		seed.child.pins--
		if native := r.retireLocked(seed.child); native != nil {
			retired = append(retired, native)
		}
	}
	r.initial = nil
	r.mu.Unlock()
	r.retireNative(retired)
	return nil
}

// advance runs at the complete-delivery boundary. It schedules IO only after
// the containing parent transaction has been enqueued, never on the CDC actor.
func (r *ownedReferences) advance(batch *q.Batch) ([]func(), error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed || r.failed {
		return nil, q.ErrClosed
	}
	// Keep old children discoverable until final ownership for this complete
	// delivery is known. A remove/add transfer must not retire a live interest.
	retire := map[*ownedReferenceChild]bool{}
	defer func() {
		var native []*localSubscription
		for child := range retire {
			if retired := r.retireLocked(child); retired != nil {
				native = append(native, retired)
			}
		}
		// One cleanup unit per transition, not a job per removed item.
		r.retireNative(native)
	}()
	remove := func(key storage.EntityKey) {
		delete(r.pending, key)
		if child := r.parents[key]; child != nil {
			delete(child.parents, key)
			delete(child.waiting, key)
			delete(r.parents, key)
			retire[child] = true
		}
		batch.BindingResets = append(batch.BindingResets, key)
	}
	if batch.Clear {
		clear(r.pending)
		for key := range r.parents {
			remove(key)
		}
	}
	inserted := map[storage.EntityKey]bool{}
	for _, insertion := range batch.Membership.Insertions {
		inserted[insertion.Key] = true
	}
	for _, key := range batch.Membership.Removed {
		if !inserted[key] {
			remove(key)
		}
	}
	var jobs []func()
	changedChildren := map[*ownedReferenceChild]bool{}
	for _, change := range batch.Changes {
		if change.Kind == q.Removed {
			continue
		}
		if change.Pending {
			r.pending[change.Key] = true
			continue
		}
		resumed := r.pending[change.Key]
		delete(r.pending, change.Key)
		if previous := r.parents[change.Key]; previous != nil {
			retire[previous] = true
		}
		child, changed, err := r.bindLocked(change.Key, change.Record)
		if err != nil {
			return nil, err
		}
		if !changed && !resumed {
			continue
		}
		batch.BindingResets = append(batch.BindingResets, change.Key)
		if child == nil {
			continue
		}
		child.waiting[change.Key] = true
		changedChildren[child] = true
	}
	for child := range changedChildren {
		jobs = append(jobs, func() { r.schedule(child) })
	}
	return jobs, nil
}

func (child *ownedReferenceChild) target() q.EntityTarget {
	return q.EntityTarget{SubscriptionID: child.owner.parent.id, Reference: &q.ReferenceRoute{Selector: child.fingerprint}}
}

func (child *ownedReferenceChild) advance(metadata q.DeliveryMetadata, batch q.Batch) *q.EntityEffect {
	r := child.owner
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed || r.failed {
		return nil
	}
	if metadata.Revision > child.current.Revision {
		if batch.Clear {
			child.current.Row = nil
		}
		for _, change := range batch.Changes {
			if change.Kind == q.Removed {
				child.current.Row = nil
			} else {
				row := change.Row
				row.Record = storage.CloneRecord(row.Record)
				child.current.Row = &row
			}
		}
		child.current.Revision = metadata.Revision
		child.current.Checkpoint = metadata.Checkpoint
		child.current.Fence = metadata.Fence
	}
	if len(child.parents) == 0 {
		return nil
	}
	value := &q.EntityEffect{Target: child.target(), Delta: q.EntityDelta{Clear: batch.Clear, ClearChangeIndex: batch.ClearChangeIndex, Changes: batch.Changes}}
	for key := range child.parents {
		value.Target.Reference.Parents = append(value.Target.Reference.Parents, key)
	}
	return value
}

func (r *ownedReferences) close(ctx context.Context) error {
	r.mu.Lock()
	r.closed = true
	var native []*localSubscription
	for _, child := range r.children {
		if child.cancel != nil {
			child.cancel()
		}
		if child.native != nil {
			native = append(native, child.native)
		}
	}
	r.mu.Unlock()
	var result error
	for _, child := range native {
		result = errors.Join(result, child.Close(ctx))
	}
	return result
}
