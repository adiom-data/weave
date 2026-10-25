package local

import (
	"context"
	"testing"

	q "github.com/adiom-data/weave/internal/querysubscription/model"
	"github.com/adiom-data/weave/internal/storage"
)

func TestInitialExpansionsAttachToTheirRowsWithIndependentImages(t *testing.T) {
	for _, absent := range []bool{false, true} {
		t.Run(map[bool]string{false: "present", true: "absent"}[absent], func(t *testing.T) {
			key := func(id string) storage.EntityKey {
				return storage.EntityKey{Relation: storage.RelationKey{Source: "test", Name: "links"}, Value: id}
			}
			integer := func(value string) storage.Value {
				return storage.Value{Type: storage.TypeInteger, State: storage.ValuePresent, Data: []byte(value)}
			}
			record := storage.Record{"task_id": integer("1")}
			rows := []q.Row{
				{Key: key("a"), Record: record},
				{Key: key("b"), Record: record},
				{Key: key("null"), Record: storage.Record{"task_id": {State: storage.ValueNull}}},
				{Key: key("pending"), Pending: true, Record: storage.Record{"task_id": {State: storage.ValueUnavailable}}},
			}
			shape := storage.ReferenceFetch{Query: storage.QuerySpec{Relation: storage.RelationName{Name: "tasks"}}, Bindings: []storage.ReferenceBinding{{Source: "task_id", Target: "id"}}}
			r, err := newOwnedReferences(&localSubscription{}, shape, rows)
			if err != nil {
				t.Fatal(err)
			}
			if len(r.initial) != 1 || r.parents[rows[0].Key] != r.parents[rows[1].Key] {
				t.Fatal("duplicate targets did not share the underlying interest")
			}
			child := r.initial[0].child
			child.finished = true
			close(child.ready)
			child.initial = q.EntitySnapshot{Revision: 7, Checkpoint: &q.Position{SourceID: "test", Value: 7}}
			if !absent {
				child.initial.Row = &q.Row{Key: key("target"), Record: storage.Record{"id": integer("1")}}
			}
			wrapped := make([]q.QueryRow, len(rows))
			for i, row := range rows {
				wrapped[i] = q.QueryRow{Row: row}
			}
			if err := r.attachInitialSnapshots(context.Background(), wrapped); err != nil {
				t.Fatal(err)
			}
			if wrapped[0].Expanded == nil || wrapped[1].Expanded == nil || wrapped[2].Expanded != nil || wrapped[3].Expanded != nil {
				t.Fatal("images were attached to the wrong rows")
			}
			if (wrapped[0].Expanded.Row == nil) != absent || wrapped[0].Expanded.Revision != 7 {
				t.Fatal("lost the absence or freshness of the attached image")
			}
			wrapped[0].Expanded.Checkpoint.Value = 99
			if wrapped[1].Expanded.Checkpoint.Value != 7 {
				t.Fatal("row snapshots share mutable freshness metadata")
			}
			if !absent {
				wrapped[0].Expanded.Row.Record["id"].Data[0] = '9'
				if string(wrapped[1].Expanded.Row.Record["id"].Data) != "1" {
					t.Fatal("row snapshots share mutable entity data")
				}
			}
			if child.pins != 0 || r.initial != nil {
				t.Fatal("initial snapshot pins were retained")
			}
		})
	}
}
