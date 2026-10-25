package projectionserver

import (
	"context"
	"errors"

	"github.com/adiom-data/weave/internal/accesscontrol"
	"github.com/adiom-data/weave/internal/projection/authorization"
	q "github.com/adiom-data/weave/internal/querysubscription/model"
	"github.com/adiom-data/weave/internal/storage"
)

// initialQueryImage folds feed effects only while a direct query is awaiting
// initial visibility admission. It holds a current image, not a transaction
// backlog; connection-owned transaction messages are delivered independently.
type initialQueryImage struct {
	id         string
	snapshot   q.QuerySnapshot
	rows       map[storage.EntityKey]q.Row
	order      []storage.EntityKey
	observed   *q.Position
	err        error
	visibility *subscriptionVisibility
	versions   map[storage.EntityKey]q.RecordVersion
	dirty      map[storage.EntityKey]bool
	moves      map[storage.EntityKey]bool
	started    bool
	epoch      uint64
}

func newInitialQueryImage(id string, snapshot q.QuerySnapshot) (*initialQueryImage, error) {
	image := &initialQueryImage{id: id, snapshot: snapshot, rows: make(map[storage.EntityKey]q.Row), visibility: newSubscriptionVisibility(), versions: make(map[storage.EntityKey]q.RecordVersion), dirty: make(map[storage.EntityKey]bool), moves: make(map[storage.EntityKey]bool)}
	for _, row := range snapshot.Rows {
		if _, exists := image.rows[row.Key]; exists {
			return nil, errors.New("initial query snapshot contains duplicate rows")
		}
		image.rows[row.Key] = copyInitialRow(row.Row)
		image.versions[row.Key] = q.RecordVersion{Revision: snapshot.Revision, ChangeIndex: row.ChangeIndex}
		image.order = append(image.order, row.Key)
	}
	image.snapshot.Rows = nil
	return image, nil
}
func copyInitialRow(row q.Row) q.Row {
	row.Record = storage.CloneRecord(row.Record)
	row.Cursor = append([]byte(nil), row.Cursor...)
	return row
}
func (i *initialQueryImage) observe(position *q.Position, revision uint64) {
	if revision > i.snapshot.Revision {
		i.snapshot.Revision = revision
	}
	if position != nil {
		if i.observed != nil && i.observed.SourceID == position.SourceID && position.Value < i.observed.Value {
			return
		}
		copy := *position
		i.observed = &copy
		if i.snapshot.Checkpoint == nil || i.snapshot.Checkpoint.SourceID == position.SourceID && i.snapshot.Checkpoint.Value < position.Value {
			i.snapshot.Checkpoint = &copy
		}
	}
}
func (i *initialQueryImage) apply(envelope q.DeliveryEnvelope) error {
	if i.err != nil {
		return i.err
	}
	baseline := i.snapshot.Revision
	if envelope.Delivery.Revision > baseline {
		for effect := range envelope.SubscriptionEffects() {
			if effect.SubscriptionID != i.id {
				continue
			}
			batch := effect.Batch
			membershipChanged := batch.Clear || len(batch.Membership.Removed) > 0 || len(batch.Membership.Insertions) > 0
			// The existing image already proves membership consistency. With no
			// membership delta, validate only touched keys against that image,
			// including remove/re-add sequences with unchanged final membership.
			var before map[storage.EntityKey]bool
			if !membershipChanged {
				before = make(map[storage.EntityKey]bool, len(batch.Changes))
				for _, change := range batch.Changes {
					_, before[change.Key] = i.rows[change.Key]
				}
			}
			if i.started {
				remember := func(set map[storage.EntityKey]bool, key storage.EntityKey) {
					if _, exists := set[key]; !exists {
						_, present := i.rows[key]
						set[key] = present
					}
				}
				for _, change := range batch.Changes {
					remember(i.dirty, change.Key)
				}
				for _, key := range batch.Membership.Removed {
					remember(i.moves, key)
				}
				for _, insertion := range batch.Membership.Insertions {
					remember(i.moves, insertion.Key)
				}
				if batch.Clear {
					for key := range i.rows {
						remember(i.dirty, key)
						remember(i.moves, key)
					}
				}
			}
			if batch.Clear {
				i.rows = make(map[storage.EntityKey]q.Row)
				clear(i.versions)
				i.order = nil
			}
			order := i.order
			if membershipChanged {
				var err error
				order, err = q.ApplyMembership(i.order, batch.Membership)
				if err != nil {
					i.err = err
					return err
				}
			}
			for _, change := range batch.Changes {
				switch change.Kind {
				case q.Removed:
					delete(i.rows, change.Key)
					delete(i.versions, change.Key)
				case q.Added, q.Updated:
					i.rows[change.Key] = copyInitialRow(change.Row)
					i.versions[change.Key] = q.RecordVersion{Revision: envelope.Delivery.Revision, ChangeIndex: change.ChangeIndex}
				default:
					i.err = errors.New("invalid initial query row change")
					return i.err
				}
			}
			if len(order) != len(i.rows) {
				i.err = errors.New("initial query rows disagree with membership")
				return i.err
			}
			if membershipChanged {
				for _, key := range order {
					if _, exists := i.rows[key]; !exists {
						i.err = errors.New("initial query membership has no row")
						return i.err
					}
				}
			} else {
				for key, expected := range before {
					if _, present := i.rows[key]; present != expected {
						i.err = errors.New("initial query row change requires a membership delta")
						return i.err
					}
				}
			}
			i.order = order
		}
	}
	var position *q.Position
	if envelope.Delivery.Kind == q.DeliveryCommit {
		position = envelope.Delivery.Checkpoint
	}
	i.observe(position, envelope.Delivery.Revision)
	return nil
}
func (i *initialQueryImage) covered() (bool, error) {
	if i.err != nil {
		return false, i.err
	}
	baseline, err := q.SnapshotPosition(i.snapshot.Checkpoint, i.snapshot.Fence)
	if err != nil {
		return false, err
	}
	if i.observed == nil || i.observed.SourceID != baseline.SourceID || i.observed.Value < baseline.Value {
		return false, nil
	}
	return true, nil
}

// authorize retains the private gate across retries. Only the first attempt
// reads the full image; later attempts apply final coalesced changes and newly
// usable fact evidence. The caller has already proved source coverage.
func (i *initialQueryImage) authorize(ctx context.Context, session *authorization.Session, prepared accesscontrol.Prepared) (bool, error) {
	view, err := session.OpenView(i.observed)
	if err != nil {
		return false, err
	}
	defer view.Close()
	epoch, changed := session.FactChangesAfter(i.epoch)
	if !i.started {
		snapshot := i.snapshot
		for _, key := range i.order {
			snapshot.Rows = append(snapshot.Rows, q.QueryRow{Row: i.rows[key]})
		}
		if _, err := i.visibility.filterSnapshot(ctx, prepared, view, snapshot); err != nil {
			return false, err
		}
		for key, version := range i.versions {
			row := i.visibility.rows[key]
			row.version = version
			i.visibility.rows[key] = row
		}
		i.started = true
	} else {
		part := subscriptionBatchMessage{delivery: q.DeliveryMetadata{Kind: q.DeliveryHydration, Checkpoint: i.observed, Revision: i.snapshot.Revision}}
		for key, before := range i.dirty {
			row, present := i.rows[key]
			if !before && !present {
				continue
			}
			kind := q.Removed
			if present {
				kind = q.Added
				if before {
					kind = q.Updated
				}
			} else {
				row.Key = key
			}
			part.batch.Changes = append(part.batch.Changes, q.Change{Kind: kind, Row: row})
		}
		for key, before := range i.moves {
			if before {
				part.batch.Membership.Removed = append(part.batch.Membership.Removed, key)
			}
		}
		if len(i.moves) > 0 {
			for index, key := range i.order {
				if _, moved := i.moves[key]; moved {
					part.batch.Membership.Insertions = append(part.batch.Membership.Insertions, q.Insertion{Key: key, FinalIndex: index})
				}
			}
		}
		for _, target := range changed {
			if target.Scope == i.visibility.id {
				part.invalidation.Targets = append(part.invalidation.Targets, accesscontrol.InvalidationTarget{InstanceID: target.Scope, Key: target.Key})
			}
		}
		if _, err := i.visibility.filterTransaction(ctx, prepared, view, part); err != nil {
			return false, err
		}
		for key := range i.dirty {
			if row, present := i.visibility.rows[key]; present {
				row.version = i.versions[key]
				i.visibility.rows[key] = row
			}
		}
	}
	clear(i.dirty)
	clear(i.moves)
	i.epoch = epoch
	return !session.Pending([]string{i.visibility.id}), nil
}

func (i *initialQueryImage) visibleSnapshot() q.QuerySnapshot {
	snapshot := i.snapshot
	for _, key := range i.visibility.order.VisibleKeys() {
		snapshot.Rows = append(snapshot.Rows, q.QueryRow{Row: i.visibility.rows[key].Row})
	}
	return snapshot
}
