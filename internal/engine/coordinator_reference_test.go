package engine

import (
	"context"
	"testing"
	"time"

	"github.com/adiom-data/weave/internal/datastore"
	"github.com/adiom-data/weave/internal/storage"
)

type fixedPreparedReference struct {
	plan datastore.PreparedQuery
	spec storage.QuerySpec
}

func (r fixedPreparedReference) Fetch() datastore.ReferenceFetch {
	return datastore.ReferenceFetch{Target: r.plan}
}
func (r fixedPreparedReference) Bind(storage.Record) (datastore.PreparedQuery, storage.QuerySpec, storage.EntityKey, bool, error) {
	return r.plan, r.spec, storage.EntityKey{Relation: r.plan.Relation(), Value: "child"}, true, nil
}

type fixedReferenceLookup struct{ fixedPreparedReference }

func (r fixedReferenceLookup) UnfilteredIdentity() bool { return true }

func (r fixedReferenceLookup) Key() storage.EntityKey {
	return storage.EntityKey{Relation: r.plan.Relation(), Value: "child"}
}
func (r fixedReferenceLookup) Matches(record storage.Record) (bool, error) {
	return r.plan.Matches(record)
}
func (r fixedPreparedReference) Resolve(storage.Record) (datastore.ReferenceLookup, bool, error) {
	return fixedReferenceLookup{r}, true, nil
}

func TestReferencePreparationIsScopedToExactSelectorAndParentWindow(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	relation := storage.RelationKey{Source: "test", Name: "parents"}
	parentPlan := fakePlan{fingerprint: "parents", relation: relation}
	record := func(id string) storage.Record {
		return storage.Record{"id": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte(id)}}
	}
	source := &coordinatorSource{plan: parentPlan, records: []storage.Record{record("1"), record("2")}, feed: newCoordinatorFeed()}
	c, err := StartCoordinator(ctx, source)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { cancel(); c.Wait(context.Background()) }()
	if _, err := c.Subscribe(ctx, "parent", storage.QuerySpec{}, InitialWindow{Count: 1}, 16); err != nil {
		t.Fatal(err)
	}
	// Populate shared membership beyond the first subscription's own window.
	if _, err := c.Subscribe(ctx, "other-window", storage.QuerySpec{}, InitialWindow{Count: 2}, 16); err != nil {
		t.Fatal(err)
	}
	spec := storage.QuerySpec{Relation: storage.RelationName{Name: "children"}}
	childPlan := fakePlan{fingerprint: "child", relation: storage.RelationKey{Source: "test", Name: "children"}}
	done := make(chan error, 1)
	if err := c.waitCommand(ctx, coordinatorCommand{run: func() {
		active, _ := c.subscriptions.Active("parent")
		active.Reference = fixedPreparedReference{plan: childPlan, spec: spec}
		c.subscriptions.Activate("parent", active)
		done <- nil
	}}, done); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name, parent, key string
		query             storage.QuerySpec
		want              bool
	}{
		{"matching", "parent", "1", spec, true},
		{"other shared window is not covered", "parent", "2", spec, false},
		{"unknown parent", "missing", "1", spec, false},
		{"unhinted parent", "other-window", "1", spec, false},
		{"different query", "parent", "1", storage.QuerySpec{Relation: storage.RelationName{Name: "unrelated"}}, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := c.prepareFromReference(ctx, test.query, &ReferenceSource{SubscriptionID: test.parent, ParentKey: storage.EntityKey{Relation: relation, Value: test.key}})
			if err != nil || (got != nil) != test.want {
				t.Fatalf("prepared=%v err=%v want=%v", got, err, test.want)
			}
		})
	}
	if _, err := c.Unsubscribe(ctx, "parent"); err != nil {
		t.Fatal(err)
	}
	if got, err := c.prepareFromReference(ctx, spec, &ReferenceSource{SubscriptionID: "parent", ParentKey: storage.EntityKey{Relation: relation, Value: "1"}}); err != nil || got != nil {
		t.Fatal("retired parent retained preparation authority")
	}
}
