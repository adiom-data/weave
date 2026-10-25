package windowloader

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/adiom-data/weave/internal/datastore"
	"github.com/adiom-data/weave/internal/engine/entitycache"
	"github.com/adiom-data/weave/internal/engine/selectorindex"
	"github.com/adiom-data/weave/internal/storage"
)

type pointPlan struct {
	fakePlan
	key storage.EntityKey
}

func (p pointPlan) LookupKey() storage.EntityKey { return p.key }

type pointSource struct {
	windowSource
	fetch func() (datastore.AuthoritativeRecord, error)
}

func (s pointSource) BeginSnapshot(context.Context, datastore.PreparedQuery) (datastore.SnapshotSession, error) {
	return pointSnapshot{windowSnapshot: &windowSnapshot{relation: s.snapshotRelation}, fetch: s.fetch}, nil
}

type pointSnapshot struct {
	*windowSnapshot
	fetch func() (datastore.AuthoritativeRecord, error)
}

func (s pointSnapshot) FetchEntity(context.Context, storage.EntityKey) (datastore.AuthoritativeRecord, error) {
	return s.fetch()
}

func TestIdentityReadCapturesOwnershipBeforeIO(t *testing.T) {
	for _, scenario := range []string{"snapshot", "absent", "complete-update", "delete", "guard-change", "partial-update", "absent-partial-insert", "read-error-with-references"} {
		t.Run(scenario, func(t *testing.T) {
			key := storage.EntityKey{Relation: storage.RelationKey{Source: "test", Name: "items"}, Value: "1"}
			record := func(title string) storage.Record {
				return storage.Record{"id": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("1")}, "title": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte(title)}}
			}
			entities := entitycache.NewEntityCache()
			index := selectorindex.New(entities, time.Second)
			plan := pointPlan{fakePlan: fakePlan{fingerprint: "entity:one", relation: key.Relation, matches: func(row storage.Record) bool {
				return string(row["id"].Data) == "1" && string(row["title"].Data) != "hidden"
			}}, key: key}
			index.Acquire(plan)
			index.InstallWindow(plan.Fingerprint(), "owner", fullWindowRequest(1).Bounds)
			source := pointSource{windowSource: windowSource{snapshotRelation: key.Relation}, fetch: func() (datastore.AuthoritativeRecord, error) {
				switch scenario {
				case "absent":
					return datastore.AuthoritativeRecord{}, datastore.ErrNotFound
				case "absent-partial-insert":
					entities.StorePartial(key, record("pending"))
					if _, _, err := index.AddMember(plan.Fingerprint(), key); err != nil {
						t.Fatal(err)
					}
					return datastore.AuthoritativeRecord{}, datastore.ErrNotFound
				case "complete-update":
					entities.StoreCurrent(key, record("new"))
				case "delete":
					if err := entities.DeleteAuthoritative(key); err != nil {
						t.Fatal(err)
					}
				case "guard-change":
					entities.StoreCurrent(key, record("hidden"))
					entities.MarkUnreferenced(key)
				case "partial-update":
					entities.StorePartial(key, storage.Record{"id": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("1")}, "title": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("new")}})
				case "read-error-with-references":
					entities.StorePartial(key, record("pending"))
					if _, _, err := index.AddMember(plan.Fingerprint(), key); err != nil {
						t.Fatal(err)
					}
					return datastore.AuthoritativeRecord{}, errors.New("read failed")
				}
				return datastore.AuthoritativeRecord{Record: record("old")}, nil
			}}
			loader := NewWindowLoader(source, entities, index)
			pending, err := loader.ReadIdentity(context.Background(), plan)
			if scenario == "read-error-with-references" {
				if err == nil {
					t.Fatal("read failure lost")
				}
				if err := index.Discard(plan.Fingerprint()); err != nil {
					t.Fatal("failed read corrupted reference ownership", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			rows, err := pending.Activate(context.Background())
			if scenario == "absent-partial-insert" {
				if !errors.Is(err, storage.ErrResumeRequired) {
					t.Fatal("published incomplete absence overlay", rows, err)
				}
				if err := index.Discard(plan.Fingerprint()); err != nil {
					t.Fatal(err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if scenario == "delete" || scenario == "guard-change" || scenario == "absent" {
				if len(rows) != 0 {
					t.Fatal("old snapshot resurrected entity", rows)
				}
				return
			}
			want := "old"
			if scenario == "complete-update" || scenario == "partial-update" {
				want = "new"
			}
			if len(rows) != 1 || string(rows[0].Record["title"].Data) != want {
				t.Fatalf("rows=%+v want=%s", rows, want)
			}
		})
	}
}
