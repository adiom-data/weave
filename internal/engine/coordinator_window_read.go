package engine

import (
	"context"
	"errors"

	"github.com/adiom-data/weave/internal/datastore"
	"github.com/adiom-data/weave/internal/engine/referencefetch"
	"github.com/adiom-data/weave/internal/engine/windowloader"
	"github.com/adiom-data/weave/internal/storage"
)

// windowRead is actor-owned. Revision counts captured changes, including those
// handed off to a worker. It is an in-memory admission revision, not a source
// fence: publication still waits for PendingWindow.Fence to be applied.
type windowRead struct {
	relation    storage.RelationKey
	identity    *storage.EntityKey
	keys        map[storage.EntityKey]struct{}
	changes     []datastore.Change
	revision    uint64
	touched     map[storage.EntityKey]uint64
	allRevision uint64
	err         error
}

// windowReadSession is worker-owned. Detached change chunks stay immutable;
// retaining history permits a failed coalesced load to be prepared again safely.
type windowReadSession struct {
	capture  *windowRead
	history  [][]datastore.Change
	revision uint64
}

func (r *windowRead) observe(change datastore.Change, identity referencefetch.Invalidation, limit int) {
	if r.err != nil {
		return
	}
	if r.keys != nil && !identity.All() {
		before, after := identity.Keys()
		_, oldTracked := r.keys[before]
		_, newTracked := r.keys[after]
		if !oldTracked && !newTracked {
			return
		}
		// Follow observed key moves until admission. If an entity leaves the
		// requested identity and later returns, intervening updates still matter.
		r.keys[before], r.keys[after] = struct{}{}, struct{}{}
	} else if r.identity != nil && change.Kind() == datastore.ChangeClearRelation {
		r.keys = map[storage.EntityKey]struct{}{*r.identity: {}}
	}
	if r.revision >= uint64(limit) {
		r.err = errors.New("snapshot change buffer limit exceeded; resynchronize")
		r.changes = nil
		return
	}
	r.changes = append(r.changes, change)
	r.revision++
	if identity.All() {
		r.allRevision = r.revision
	} else {
		if r.touched == nil {
			r.touched = make(map[storage.EntityKey]uint64)
		}
		before, after := identity.Keys()
		r.touched[before], r.touched[after] = r.revision, r.revision
	}
}

func (c *Coordinator) beginWindowRead(ctx context.Context, query datastore.PreparedQuery) (*windowReadSession, error) {
	read := &windowRead{relation: query.Relation()}
	if identity, ok := query.(datastore.IdentityQuery); ok {
		key := identity.LookupKey()
		read.identity = &key
		read.keys = map[storage.EntityKey]struct{}{key: {}}
	}
	done := make(chan error, 1)
	err := c.waitCommand(ctx, coordinatorCommand{run: func() {
		if c.windowReads == nil {
			c.windowReads = make(map[storage.RelationKey]map[*windowRead]struct{})
		}
		if c.windowReads[read.relation] == nil {
			c.windowReads[read.relation] = make(map[*windowRead]struct{})
		}
		c.windowReads[read.relation][read] = struct{}{}
		done <- nil
	}}, done)
	session := &windowReadSession{capture: read}
	if err != nil {
		c.abortWindowRead(session)
		return nil, err
	}
	return session, nil
}

func (c *Coordinator) abortWindowRead(session *windowReadSession) {
	read := session.capture
	done := make(chan error, 1)
	_ = c.waitCommand(context.Background(), coordinatorCommand{run: func() {
		c.removeWindowRead(read)
		read.changes = nil
		done <- nil
	}}, done)
}

// removeWindowRead runs on the actor, including for repeated cancellation.
func (c *Coordinator) removeWindowRead(read *windowRead) {
	reads := c.windowReads[read.relation]
	delete(reads, read)
	if len(reads) == 0 {
		delete(c.windowReads, read.relation)
	}
}

func (c *Coordinator) stageWindowRead(ctx context.Context, read *windowReadSession, pending *windowloader.PendingWindow) error {
	staging, err := pending.PrepareStage()
	if err != nil {
		return err
	}
	return c.prepareWindowRead(ctx, read, staging)
}

func (c *Coordinator) retryWindowRead(ctx context.Context, read *windowReadSession, pending *windowloader.PendingWindow, indexes []int) error {
	staging, err := pending.PrepareRetry(indexes)
	if err != nil {
		return err
	}
	return c.prepareWindowRead(ctx, read, staging)
}

func (c *Coordinator) prepareWindowRead(ctx context.Context, read *windowReadSession, staging *windowloader.Staging) error {
	for _, changes := range read.history {
		if err := staging.Replay(ctx, changes); err != nil {
			return err
		}
	}
	for {
		commit, err := staging.PrepareCommit(ctx)
		if err != nil {
			return err
		}
		advance := c.advanceWindowRead(ctx, read.capture, read.revision, commit)
		if advance.err != nil {
			return advance.err
		}
		if advance.committed {
			return nil
		}
		read.revision = advance.revision
		read.history = append(read.history, advance.changes)
		if err := staging.Replay(ctx, advance.changes); err != nil {
			return err
		}
	}
}

type windowReadAdvance struct {
	changes   []datastore.Change
	revision  uint64
	committed bool
	err       error
}

// advanceWindowRead either detaches a newer immutable prefix for the worker or
// installs a prepared row set once all relevant captured revisions are covered.
// Full row decoding, historical scans and reconciliation never run here.
func (c *Coordinator) advanceWindowRead(ctx context.Context, read *windowRead, revision uint64, commit *windowloader.StagingCommit) windowReadAdvance {
	result := make(chan windowReadAdvance, 1)
	command := coordinatorCommand{run: func() {
		if err := ctx.Err(); err != nil {
			result <- windowReadAdvance{err: err}
			return
		}
		if read.err != nil {
			result <- windowReadAdvance{err: read.err}
			return
		}
		// A busy relation need not become quiet. Only changes to the images
		// this commit relies on require another worker pass. The key index
		// checks that proof without decoding or scanning the captured log.
		changed := false
		keys := commit.Keys()
		if len(keys) > 0 {
			changed = read.allRevision > revision
			for _, key := range keys {
				changed = changed || read.touched[key] > revision
			}
		}
		if changed {
			changes := read.changes
			read.changes = nil // the worker owns this backing array now
			result <- windowReadAdvance{changes: changes, revision: read.revision}
			return
		}
		err := commit.Apply()
		if err == nil {
			// Once snapshot keys are known, unrelated changes need no history.
			// Keep existing lineage keys when retrying only part of a window.
			if read.keys == nil {
				read.keys = make(map[storage.EntityKey]struct{}, len(keys))
			}
			for _, key := range keys {
				read.keys[key] = struct{}{}
			}
		}
		result <- windowReadAdvance{committed: true, err: err}
	}}
	select {
	case c.commands <- command:
	case <-ctx.Done():
		return windowReadAdvance{err: ctx.Err()}
	case <-c.done:
		return windowReadAdvance{err: c.terminalError()}
	}
	// Join an accepted handoff before its caller can cancel the PendingWindow.
	// The actor only performs bounded cache-token admission, never replay/IO.
	select {
	case advance := <-result:
		return advance
	case <-c.done:
		return windowReadAdvance{err: c.terminalError()}
	}
}

func (c *Coordinator) finishWindowRead(ctx context.Context, session *windowReadSession) error {
	read := session.capture
	done := make(chan error, 1)
	err := c.waitCommand(ctx, coordinatorCommand{run: func() {
		c.removeWindowRead(read)
		read.changes = nil
		done <- read.err
	}}, done)
	if err == nil {
		session.history = nil
	}
	return err
}
