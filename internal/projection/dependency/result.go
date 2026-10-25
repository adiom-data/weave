package dependency

import (
	"errors"
	"reflect"

	q "github.com/adiom-data/weave/internal/querysubscription/model"
	"github.com/adiom-data/weave/internal/storage"
)

// Shared cells are immutable. A result may temporarily own an older cell when
// its independently installed snapshot is older than another result. There is
// no checkpoint lookup API or retained history once those owners release it.
type entityCell struct {
	record  storage.Record
	pending bool
	refs    int
}
type row struct {
	cell        *entityCell
	cursor      []byte
	changeIndex uint32
}
type result struct {
	rows              map[storage.EntityKey]row
	order             []storage.EntityKey
	bounds            q.Bounds
	checkpoint, fence *q.Position
	revision          uint64
}

func (g *generation) retain(value q.Row) *entityCell {
	cell := g.entities[value.Key]
	if cell == nil || cell.pending != value.Pending || !reflect.DeepEqual(cell.record, value.Record) {
		cell = &entityCell{record: storage.CloneRecord(value.Record), pending: value.Pending}
		g.entities[value.Key] = cell
	}
	cell.refs++
	return cell
}
func (g *generation) release(key storage.EntityKey, cell *entityCell) {
	cell.refs--
	if cell.refs == 0 && g.entities[key] == cell {
		delete(g.entities, key)
	}
}
func (g *generation) releaseRows(rows map[storage.EntityKey]row) {
	for key, row := range rows {
		g.release(key, row.cell)
	}
}

func (g *generation) install(e *entry, snapshot q.QuerySnapshot) error {
	if e.state.rows != nil {
		return errors.New("dependency snapshot installed more than once")
	}
	if e.entity && len(snapshot.Rows) > 1 {
		return errors.New("entity dependency snapshot has more than one row")
	}
	seen := make(map[storage.EntityKey]struct{}, len(snapshot.Rows))
	for _, row := range snapshot.Rows {
		if row.Key.Value == "" || row.Key.Relation.Source == "" || !row.Pending && row.Record == nil {
			return errors.New("dependency snapshot has an invalid row")
		}
		if _, duplicate := seen[row.Key]; duplicate {
			return errors.New("dependency snapshot contains duplicate entities")
		}
		seen[row.Key] = struct{}{}
	}
	e.state = result{rows: make(map[storage.EntityKey]row, len(snapshot.Rows)), bounds: cloneBounds(snapshot.Bounds), checkpoint: copyPosition(snapshot.Checkpoint), fence: copyPosition(snapshot.Fence), revision: snapshot.Revision}
	for _, value := range snapshot.Rows {
		e.state.rows[value.Key] = row{cell: g.retain(value.Row), cursor: append([]byte(nil), value.Cursor...), changeIndex: value.ChangeIndex}
		e.state.order = append(e.state.order, value.Key)
	}
	return nil
}

func (g *generation) snapshot(e *entry) q.QuerySnapshot {
	r := e.state
	value := q.QuerySnapshot{Bounds: cloneBounds(r.bounds), Checkpoint: copyPosition(r.checkpoint), Fence: copyPosition(r.fence), Revision: r.revision}
	if g.revision > value.Revision {
		value.Revision = g.revision
		floor, err := q.SnapshotPosition(value.Checkpoint, value.Fence)
		if err == nil && g.checkpoint != nil && g.checkpoint.SourceID == floor.SourceID && g.checkpoint.Value >= floor.Value {
			value.Checkpoint = copyPosition(g.checkpoint)
		}
	}
	for _, key := range r.order {
		row := r.rows[key]
		value.Rows = append(value.Rows, q.QueryRow{Row: q.Row{Key: key, Record: storage.CloneRecord(row.cell.record), Pending: row.cell.pending, Cursor: append([]byte(nil), row.cursor...), ChangeIndex: row.changeIndex}})
	}
	return value
}

func (g *generation) apply(e *entry, metadata q.DeliveryMetadata, batch q.Batch) error {
	if metadata.Revision <= e.state.revision {
		return nil
	}
	state := &e.state
	for _, change := range batch.Changes {
		if change.Kind != q.Added && change.Kind != q.Updated && change.Kind != q.Removed {
			return errors.New("dependency change has an invalid kind")
		}
		if change.Kind != q.Removed && !change.Pending && change.Record == nil {
			return errors.New("dependency change has no authoritative record")
		}
	}
	if batch.Clear {
		g.releaseRows(state.rows)
		state.rows = make(map[storage.EntityKey]row)
		state.order = nil
	}
	if e.entity {
		for _, change := range batch.Changes {
			if change.Kind == q.Removed {
				state.order = nil
			} else {
				state.order = []storage.EntityKey{change.Key}
			}
		}
	} else {
		order, err := q.ApplyMembership(state.order, batch.Membership)
		if err != nil {
			return err
		}
		state.order = order
	}
	for _, change := range batch.Changes {
		if previous, ok := state.rows[change.Key]; ok {
			g.release(change.Key, previous.cell)
			delete(state.rows, change.Key)
		}
		if change.Kind != q.Removed {
			state.rows[change.Key] = row{cell: g.retain(change.Row), cursor: append([]byte(nil), change.Cursor...), changeIndex: change.ChangeIndex}
		}
	}
	if len(state.order) != len(state.rows) {
		return errors.New("dependency rows and membership disagree")
	}
	for _, key := range state.order {
		if _, ok := state.rows[key]; !ok {
			return errors.New("dependency membership has no record")
		}
	}
	state.revision = metadata.Revision
	if metadata.Checkpoint != nil {
		state.checkpoint = copyPosition(metadata.Checkpoint)
	}
	if metadata.Fence != nil {
		state.fence = copyPosition(metadata.Fence)
	}
	if batch.Window != nil {
		state.bounds = cloneBounds(batch.Window.Bounds)
		state.checkpoint = copyPosition(batch.Window.Checkpoint)
		state.fence = copyPosition(batch.Window.Fence)
	}
	return nil
}
