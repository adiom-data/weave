package windowloader

import (
	"context"
	"errors"

	"github.com/adiom-data/weave/internal/datastore"
	"github.com/adiom-data/weave/internal/engine/entitycache"
	"github.com/adiom-data/weave/internal/storage"
)

// ReadIdentity captures load ownership BEFORE starting the database snapshot.
// A concurrent complete tuple or deletion supersedes this exact load token, so
// an older point read cannot resurrect a deleted row or overwrite newer CDC.
// It reads the whole entity, independently of additional selector predicates.
// Publication and fence admission use the same PendingWindow contract as lists.
func (l *WindowLoader) ReadIdentity(ctx context.Context, query datastore.IdentityQuery) (_ *PendingWindow, err error) {
	key := query.LookupKey()
	loads := l.entities.StageSnapshotBatch([]entitycache.SnapshotLoadRequest{{Key: key, ForceRefresh: true}})
	if len(loads) != 1 {
		return nil, errors.New("entity cache returned the wrong load count")
	}
	pending := &PendingWindow{entities: l.entities, queries: l.queries, query: query, staged: true, refresh: true, pending: []pendingEntity{{load: loads[0]}}}
	complete := false
	defer func() {
		if !complete {
			pending.Cancel(err)
		}
	}()
	snapshot, err := l.source.BeginSnapshot(ctx, query)
	if err != nil {
		return nil, err
	}
	defer snapshot.Abort(context.WithoutCancel(ctx))
	if snapshot.Relation() != query.Relation() {
		return nil, errors.New("identity snapshot belongs to another relation")
	}
	row, err := snapshot.FetchEntity(ctx, key)
	absent := errors.Is(err, datastore.ErrNotFound)
	if err != nil && !absent {
		return nil, err
	}
	if absent {
		row.Record = nil
	} else if err := row.Validate(); err != nil {
		return nil, err
	}
	if row.Record != nil {
		actual, err := query.EntityKey(row.Record)
		if err != nil || actual != key {
			return nil, errors.New("point read returned a foreign entity")
		}
		pending.pending[0].record = storage.CloneRecord(row.Record)
	} else {
		// An empty map is a valid absence baseline and can safely receive a
		// concrete CDC overlay. Missing fields never match a PK equality.
		pending.pending[0].record = storage.Record{}
		pending.pending[0].absent = true
	}
	pending.pending[0].image = entitycache.PrepareSnapshotRecord(pending.pending[0].record)
	pending.Fence, err = snapshot.Finish(ctx)
	if err != nil {
		return nil, err
	}
	if pending.Fence == nil || pending.Fence.Source() != l.source.ID() {
		return nil, errors.New("identity snapshot has a foreign or nil fence")
	}
	complete = true
	return pending, nil
}
