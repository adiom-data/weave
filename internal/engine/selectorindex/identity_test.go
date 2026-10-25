package selectorindex

import (
	"testing"
	"time"

	"github.com/adiom-data/weave/internal/datastore"
	"github.com/adiom-data/weave/internal/engine/entitycache"
	"github.com/adiom-data/weave/internal/storage"
)

type testIdentity struct{ key storage.EntityKey }

func (p testIdentity) LookupKey() storage.EntityKey            { return p.key }
func (p testIdentity) Fingerprint() datastore.QueryFingerprint { return "entity:one" }
func (p testIdentity) Relation() storage.RelationKey           { return p.key.Relation }
func (p testIdentity) EntityKey(record storage.Record) (storage.EntityKey, error) {
	return storage.EntityKey{Relation: p.key.Relation, Value: string(record["id"].Data)}, nil
}
func (p testIdentity) Query() storage.QuerySpec             { return storage.QuerySpec{} }
func (p testIdentity) Matches(storage.Record) (bool, error) { return true, nil }
func (p testIdentity) Key(storage.Record) (datastore.SortKey, error) {
	panic("identity index must not sort")
}
func (p testIdentity) Compare(datastore.SortKey, datastore.SortKey) (int, error) {
	panic("identity index must not compare")
}
func (p testIdentity) EncodeKey(datastore.SortKey) ([]byte, error) {
	panic("identity index must not encode cursors")
}
func (p testIdentity) DecodeKey([]byte) (datastore.SortKey, error) {
	panic("identity index must not decode cursors")
}

func TestIdentityIndexHasOneSlotAndRoutesAbsentEntitiesDirectly(t *testing.T) {
	entities := entitycache.NewEntityCache()
	index := New(entities, time.Second)
	key := storage.EntityKey{Relation: storage.RelationKey{Source: "test", Name: "items"}, Value: "1"}
	plan := testIdentity{key: key}
	fp := plan.Fingerprint()
	d, added := index.Acquire(plan)
	if !added || d.Identity == nil || *d.Identity != key {
		t.Fatal("not classified as identity")
	}
	if _, exists := index.QueryRegistry.Plan(fp); exists {
		t.Fatal("identity allocated a windowed query")
	}
	if len(index.RoutingFields(key.Relation)) != 0 {
		t.Fatal("identity registered predicate routing")
	}
	if len(index.Candidates(key.Relation, key, nil, false)) != 1 {
		t.Fatal("missing entity is not watched")
	}
	if len(index.Candidates(key.Relation, storage.EntityKey{Relation: key.Relation, Value: "2"}, nil, true)) != 0 {
		t.Fatal("unrelated key was routed")
	}
	entities.StoreCurrent(key, storage.Record{"id": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("1")}})
	cached, err := index.PlanInitialWindow(fp, 1, false)
	if err != nil || !cached.Available || !cached.Resolved {
		t.Fatalf("cached=%+v err=%v", cached, err)
	}
	if ready, err := index.InstallWindow(fp, "owner", full()); err != nil || !ready {
		t.Fatal(ready, err)
	}
	rows, err := index.EntitiesInBounds(fp, []datastore.Bounds{full()})
	if err != nil || len(rows) != 1 {
		t.Fatal(rows, err)
	}
	if position, member, err := index.PositionInBounds(fp, full(), key); err != nil || position != 0 || !member {
		t.Fatal(position, member, err)
	}
	index.RemoveWindow(fp, "owner")
	index.Release(fp)
	if _, exists := index.Plan(fp); !exists {
		t.Fatal("idle grace was lost")
	}
	if failures, more := index.Maintain(time.Now().Add(2*time.Second), 1); len(failures) != 0 || more {
		t.Fatal(failures, more)
	}
	if _, exists := index.Plan(fp); exists {
		t.Fatal("idle identity not evicted")
	}
	row, _ := entities.Get(key)
	if row.References != 0 {
		t.Fatal("membership pin leaked")
	}
}

func TestIdentityClearReleasesPinsButKeepsAbsenceAndRouting(t *testing.T) {
	entities := entitycache.NewEntityCache()
	index := New(entities, time.Second)
	key := storage.EntityKey{Relation: storage.RelationKey{Source: "test", Name: "items"}, Value: "1"}
	plan := testIdentity{key: key}
	fp := plan.Fingerprint()
	index.Acquire(plan)
	index.InstallWindow(fp, "owner", full())
	index.MarkWindowReady(fp, "owner")
	entities.StoreCurrent(key, storage.Record{"id": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("1")}})
	if _, _, err := index.AddMember(fp, key); err != nil {
		t.Fatal(err)
	}
	if cleared, err := index.ClearRelation(key.Relation); err != nil || len(cleared) != 1 {
		t.Fatal(cleared, err)
	}
	if index.IsMember(fp, key) || len(index.Candidates(key.Relation, key, nil, false)) != 1 {
		t.Fatal("clear lost identity semantics")
	}
	if gaps, err := index.ReadyGaps(fp, full()); err != nil || len(gaps) != 0 {
		t.Fatal("clear lost known absence", gaps, err)
	}
	if err := entities.DeleteAuthoritative(key); err != nil {
		t.Fatal("clear leaked pin", err)
	}
}
