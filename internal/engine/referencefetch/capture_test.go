package referencefetch

import (
	"errors"
	"testing"

	"github.com/adiom-data/weave/internal/datastore"
	"github.com/adiom-data/weave/internal/storage"
)

type transition struct {
	kind          datastore.ChangeKind
	before, after storage.EntityKey
	err           error
}

func (c transition) Kind() datastore.ChangeKind            { return c.kind }
func (c transition) Relation() storage.RelationKey         { return c.before.Relation }
func (c transition) BeforeKey() (storage.EntityKey, error) { return c.before, c.err }
func (c transition) Apply(storage.Record) (datastore.AppliedChange, error) {
	return datastore.AppliedChange{Kind: datastore.AppliedPartial, Key: c.after}, c.err
}

func TestCaptureTracksBothIdentitiesAndBoundsMemory(t *testing.T) {
	relation := storage.RelationKey{Source: "test", Name: "tasks"}
	key := func(value string) storage.EntityKey { return storage.EntityKey{Relation: relation, Value: value} }
	for _, mode := range []string{"move", "overflow", "truncate", "invalidate", "malformed"} {
		t.Run(mode, func(t *testing.T) {
			c := New(relation, 2)
			c.Observe(transition{kind: datastore.ChangeUpdate, before: key("a"), after: key("b")})
			if !c.Dirty(key("a")) || !c.Dirty(key("b")) || c.Dirty(key("untouched")) {
				t.Fatal("identity move did not dirty exactly its old/new selectors")
			}
			switch mode {
			case "move":
				return
			case "overflow":
				c.Observe(transition{kind: datastore.ChangeInsert, before: key("c"), after: key("c")})
			case "truncate":
				c.Observe(transition{kind: datastore.ChangeClearRelation, before: key("")})
			case "invalidate":
				c.Observe(transition{kind: datastore.ChangeInvalidate, before: key("")})
			case "malformed":
				c.Observe(transition{kind: datastore.ChangeUpdate, before: key("c"), err: errors.New("bad identity")})
			}
			if !c.Dirty(key("untouched")) || len(c.keys) != 0 {
				t.Fatal("unsafe/overflowed capture must disable speculative reuse and release keys")
			}
		})
	}
	other := New(relation, 1)
	other.Observe(transition{kind: datastore.ChangeClearRelation, before: storage.EntityKey{Relation: storage.RelationKey{Name: "other"}}})
	if other.Dirty(key("a")) {
		t.Fatal("unrelated relation invalidated reference load")
	}
}
