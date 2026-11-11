package windowloader

import (
	"context"
	"errors"

	"github.com/adiom-data/weave/internal/datastore"
	"github.com/adiom-data/weave/internal/engine/entitycache"
	"github.com/adiom-data/weave/internal/storage"
)

// Staging prepares private snapshot images outside the coordinator. Replay may
// decode full rows and must never run on the CDC actor. A Commit is built from
// the prepared images and admitted only if it covers every captured change to
// the identities those images depend on.
// The caller owns this preparation; the cache is untouched until Commit.Apply.
type Staging struct {
	window *PendingWindow
	images map[storage.EntityKey]stagingRow
	order  []storage.EntityKey
	failed map[int]bool
	absent map[storage.EntityKey]bool
	noop   bool
}

// A changed row replaces its whole staging entry. Its immutable cache image is
// prepared once, so another replay pass only copies rows that actually changed.
type stagingRow struct {
	record storage.Record
	image  *entitycache.SnapshotRecord
}

func (w *PendingWindow) PrepareStage() (*Staging, error) {
	if w.used {
		return nil, errors.New("cannot stage an activated window")
	}
	if w.staged {
		return &Staging{window: w, noop: true}, nil
	}
	return newStaging(w, w.rows), nil
}

func (w *PendingWindow) PrepareRetry(indexes []int) (*Staging, error) {
	if w.used || !w.staged || len(indexes) == 0 {
		return nil, errors.New("cannot retry an unstaged or activated window")
	}
	rows := make([]validatedWindowRecord, 0, len(indexes))
	failed := make(map[int]bool, len(indexes))
	absent := make(map[storage.EntityKey]bool, len(indexes))
	for _, index := range indexes {
		if index < 0 || index >= len(w.pending) {
			return nil, errors.New("pending load index is out of range")
		}
		if failed[index] {
			continue
		}
		pending := w.pending[index]
		rows = append(rows, validatedWindowRecord{entity: pending.load.Key(), record: pending.record})
		failed[index], absent[pending.load.Key()] = true, pending.absent
	}
	s := newStaging(w, rows)
	s.failed, s.absent = failed, absent
	return s, nil
}

func newStaging(w *PendingWindow, rows []validatedWindowRecord) *Staging {
	s := &Staging{window: w, images: make(map[storage.EntityKey]stagingRow, len(rows)), order: make([]storage.EntityKey, 0, len(rows))}
	for _, row := range rows {
		if _, found := s.images[row.entity]; !found {
			s.order = append(s.order, row.entity)
		}
		s.images[row.entity] = stagingRow{record: row.record}
	}
	return s
}

// Replay advances this private preparation through one immutable capture prefix.
// Deleted identities remain tracked until preparation ends, so later inserts
// and key moves are reconciled too. Unrelated payloads are never decoded.
func (s *Staging) Replay(ctx context.Context, changes []datastore.Change) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.noop {
		return nil
	} // exact identity tokens were installed before IO
	for _, change := range changes {
		if err := ctx.Err(); err != nil {
			return err
		}
		if change.Relation() != s.window.query.Relation() {
			return errors.New("snapshot replay belongs to another relation")
		}
		switch change.Kind() {
		case datastore.ChangeClearRelation:
			for key := range s.images {
				s.images[key] = stagingRow{}
			}
			continue
		case datastore.ChangeInvalidate:
			return datastore.ErrInvalidated
		}
		before, err := change.BeforeKey()
		if err != nil {
			return err
		}
		after, err := change.AfterKey()
		if err != nil {
			return err
		}
		row, tracked := s.images[before]
		if !tracked {
			row, tracked = s.images[after]
			if !tracked {
				continue
			}
		}
		previous := row.record
		if before != after {
			// The snapshot may already contain a different occupant of the old
			// key. Only a self-contained move can be replayed without guessing
			// the historical owner of unchanged fields.
			previous = nil
		}
		applied, err := change.Apply(storage.CloneRecord(previous))
		if err != nil {
			return err
		}
		if _, tracked := s.images[before]; tracked {
			s.images[before] = stagingRow{}
		}
		if !applied.Exists() {
			continue
		}
		if applied.Key.Relation != s.window.query.Relation() || applied.Key.Value == "" {
			return errors.New("snapshot replay returned a foreign entity")
		}
		if !applied.Complete() {
			return storage.ErrResumeRequired
		}
		if err := (datastore.AuthoritativeRecord{Record: applied.Record}).Validate(); err != nil {
			return err
		}
		if _, tracked := s.images[applied.Key]; !tracked {
			s.order = append(s.order, applied.Key)
		}
		s.images[applied.Key] = stagingRow{record: applied.Record}
	}
	return nil
}

// StagingCommit is an immutable cache handoff, apart from its single-use guard.
// Preparation, record copies, and scanning historical keys occur off the actor;
// Apply only assigns cache tokens and retains the prepared immutable records.
type StagingCommit struct {
	window   *PendingWindow
	requests []entitycache.SnapshotLoadRequest
	pending  []pendingEntity
	retained []pendingEntity
	noop     bool
	used     bool
	keys     []storage.EntityKey
}

// Keys borrows the immutable identities whose images or absence this handoff
// relies on, including identities removed by the captured prefix.
func (s *StagingCommit) Keys() []storage.EntityKey { return s.keys }

func (s *Staging) PrepareCommit(ctx context.Context) (*StagingCommit, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	commit := &StagingCommit{window: s.window, noop: s.noop}
	if s.noop {
		return commit, nil
	}
	commit.keys = append([]storage.EntityKey(nil), s.order...)
	for _, key := range s.order {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		row := s.images[key]
		record := row.record
		if record == nil {
			continue
		}
		if row.image == nil {
			image := entitycache.PrepareSnapshotRecord(record)
			row.image = &image
			s.images[key] = row
		}
		image := *row.image
		commit.requests = append(commit.requests, entitycache.SnapshotLoadRequest{Key: key, Record: image, ForceRefresh: s.window.refresh})
		commit.pending = append(commit.pending, pendingEntity{record: record, image: image, absent: s.absent[key] && len(record) == 0})
	}
	if s.failed != nil {
		commit.retained = make([]pendingEntity, 0, len(s.window.pending))
		for index, pending := range s.window.pending {
			if !s.failed[index] {
				commit.retained = append(commit.retained, pending)
			}
		}
	}
	return commit, nil
}

// Apply must be serialized with CDC application and invoked only after the
// caller verifies that no relevant capture revision is newer than preparation.
func (s *StagingCommit) Apply() error {
	if s.used || s.window.used {
		return errors.New("snapshot staging commit was already used")
	}
	s.used = true
	if s.noop {
		return nil
	}
	loads := s.window.entities.StageSnapshotBatch(s.requests)
	if len(loads) != len(s.pending) {
		return errors.New("entity cache returned the wrong snapshot load count")
	}
	for index, load := range loads {
		s.pending[index].load = load
	}
	s.window.pending = append(s.retained, s.pending...)
	s.window.rows = nil
	s.window.staged = true
	return nil
}

// Stage is for already-protected reads or recovery paths that buffer CDC until
// activation. Normal range reads use an asynchronously prepared StagingCommit.
func (w *PendingWindow) Stage() error {
	staging, err := w.PrepareStage()
	if err != nil {
		return err
	}
	commit, err := staging.PrepareCommit(context.Background())
	if err != nil {
		return err
	}
	return commit.Apply()
}

func (w *PendingWindow) RetryLoads(indexes []int) error {
	staging, err := w.PrepareRetry(indexes)
	if err != nil {
		return err
	}
	commit, err := staging.PrepareCommit(context.Background())
	if err != nil {
		return err
	}
	return commit.Apply()
}
