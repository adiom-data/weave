package changeprocessor

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"github.com/adiom-data/weave/internal/datastore"
	"github.com/adiom-data/weave/internal/engine/entitycache"
	"github.com/adiom-data/weave/internal/engine/queryregistry"
	"github.com/adiom-data/weave/internal/engine/routing"
	"github.com/adiom-data/weave/internal/storage"
)

type fakePlan struct {
	fingerprint datastore.QueryFingerprint
	relation    storage.RelationKey
	routing     routing.RoutingPlan
	matches     func(storage.Record) bool
	matchError  error
}

func (p fakePlan) Fingerprint() datastore.QueryFingerprint { return p.fingerprint }
func (p fakePlan) Relation() storage.RelationKey           { return p.relation }
func (p fakePlan) EntityKey(record storage.Record) (storage.EntityKey, error) {
	return storage.EntityKey{Relation: p.relation, Value: string(record["id"].Data)}, nil
}
func (p fakePlan) Matches(record storage.Record) (bool, error) {
	if p.matchError != nil {
		return false, p.matchError
	}
	if p.matches != nil {
		return p.matches(record), nil
	}
	return true, nil
}
func (p fakePlan) Key(record storage.Record) (datastore.SortKey, error) {
	return datastore.EncodedSortKey(record["id"].Data), nil
}
func (p fakePlan) Compare(a, b datastore.SortKey) (int, error) {
	return bytes.Compare(a.(datastore.EncodedSortKey), b.(datastore.EncodedSortKey)), nil
}
func (p fakePlan) EncodeKey(key datastore.SortKey) ([]byte, error) {
	return append([]byte(nil), key.(datastore.EncodedSortKey)...), nil
}
func (p fakePlan) DecodeKey(encoded []byte) (datastore.SortKey, error) {
	return datastore.EncodedSortKey(append([]byte(nil), encoded...)), nil
}
func (p fakePlan) Query() storage.QuerySpec {
	query := storage.QuerySpec{}
	var operands []*storage.Predicate
	for _, exact := range p.routing.Exact {
		value := string(exact.Token)
		if _, suffix, found := strings.Cut(value, "="); found {
			value = suffix
		}
		operands = append(operands, &storage.Predicate{Comparison: &storage.Comparison{Field: exact.Field, Operator: storage.ComparisonEqual, Value: storage.Literal{Type: storage.TypeText, Data: []byte(value)}}})
	}
	for _, field := range p.routing.WatchFields {
		operands = append(operands, &storage.Predicate{Comparison: &storage.Comparison{Field: field, Operator: storage.ComparisonNotEqual, Value: storage.Literal{Data: []byte("ignored")}}})
	}
	if len(operands) == 1 {
		query.Filter = operands[0]
	} else if len(operands) > 1 {
		query.Filter = &storage.Predicate{Junction: &storage.Junction{Operator: storage.JunctionAnd, Operands: operands}}
	}
	query.Order = make([]storage.OrderTerm, 0, len(p.routing.OrderFields))
	for _, field := range p.routing.OrderFields {
		query.Order = append(query.Order, storage.OrderTerm{Field: field})
	}
	return query
}

type fakeChange struct {
	kind     datastore.ChangeKind
	relation storage.RelationKey
	before   storage.EntityKey
	after    storage.EntityKey
	record   storage.Record
	complete bool
	applyErr error
	applied  *datastore.AppliedChange
}

func (c fakeChange) Kind() datastore.ChangeKind            { return c.kind }
func (c fakeChange) Relation() storage.RelationKey         { return c.relation }
func (c fakeChange) BeforeKey() (storage.EntityKey, error) { return c.before, nil }
func (c fakeChange) Apply(storage.Record) (datastore.AppliedChange, error) {
	if c.applyErr != nil {
		return datastore.AppliedChange{}, c.applyErr
	}
	if c.applied != nil {
		return *c.applied, nil
	}
	if c.kind == datastore.ChangeDelete {
		return datastore.AppliedChange{Kind: datastore.AppliedDeleted, Key: c.before}, nil
	}
	kind := datastore.AppliedPartial
	if c.complete {
		kind = datastore.AppliedComplete
	}
	return datastore.AppliedChange{Kind: kind, Key: c.after, Record: c.record}, nil
}

func TestProcessorRejectsContradictoryAppliedChange(t *testing.T) {
	relation := storage.RelationKey{Source: "test", Name: "tasks"}
	key := storage.EntityKey{Relation: relation, Value: "1"}
	entities := entitycache.NewEntityCache()
	queries := queryregistry.NewQueryRegistry(entities)
	processor := NewProcessor("test", entities, queries)
	invalid := datastore.AppliedChange{Kind: datastore.AppliedDeleted, Key: key, Record: storage.Record{"id": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("1")}}}
	_, err := processor.ApplyRelationBatch(datastore.CommittedChanges{Changes: []datastore.Change{
		fakeChange{kind: datastore.ChangeUpdate, relation: relation, before: key, applied: &invalid},
	}})
	if !errors.Is(err, ErrFatalSource) {
		t.Fatalf("contract error = %v, want ErrFatalSource", err)
	}
	if _, exists := entities.Get(key); exists {
		t.Fatal("invalid source transition mutated the entity cache")
	}
}

func TestProcessorRejectsUnknownValueState(t *testing.T) {
	relation := storage.RelationKey{Source: "test", Name: "tasks"}
	key := storage.EntityKey{Relation: relation, Value: "1"}
	entities := entitycache.NewEntityCache()
	processor := NewProcessor("test", entities, queryregistry.NewQueryRegistry(entities))
	invalid := datastore.AppliedChange{
		Kind: datastore.AppliedComplete,
		Key:  key,
		Record: storage.Record{
			"id": {State: storage.ValueState(99), Data: []byte("1")},
		},
	}
	_, err := processor.ApplyRelationBatch(datastore.CommittedChanges{Changes: []datastore.Change{
		fakeChange{kind: datastore.ChangeInsert, relation: relation, after: key, applied: &invalid},
	}})
	if !errors.Is(err, ErrFatalSource) {
		t.Fatalf("contract error = %v, want ErrFatalSource", err)
	}
}

func TestProcessorRejectsControlChangesWithIncompleteRelation(t *testing.T) {
	entities := entitycache.NewEntityCache()
	processor := NewProcessor("test", entities, queryregistry.NewQueryRegistry(entities))
	for _, kind := range []datastore.ChangeKind{datastore.ChangeClearRelation, datastore.ChangeInvalidate} {
		_, err := processor.ApplyRelationBatch(datastore.CommittedChanges{Changes: []datastore.Change{
			fakeChange{kind: kind, relation: storage.RelationKey{Source: "test"}},
		}})
		if !errors.Is(err, ErrFatalSource) {
			t.Fatalf("kind %v error = %v, want ErrFatalSource", kind, err)
		}
	}
}

func TestProcessorSharesEntityAndRoutesMembershipTransitions(t *testing.T) {
	relation := storage.RelationKey{Source: "test", Namespace: "app", Name: "tasks"}
	entities := entitycache.NewEntityCache()
	queries := queryregistry.NewQueryRegistry(entities)
	plan := fakePlan{
		fingerprint: "project-a", relation: relation,
		routing: routing.RoutingPlan{Exact: []routing.RoutingExact{{Field: "project", Token: "a"}}},
		matches: func(record storage.Record) bool { return string(record["project"].Data) == "a" },
	}
	queries.Acquire(plan)
	if err := queries.SetWindow("project-a", "viewer-1", datastore.Bounds{Left: datastore.Boundary{Kind: datastore.BoundaryStart}, Right: datastore.Boundary{Kind: datastore.BoundaryEnd}}); err != nil {
		t.Fatal(err)
	}
	processor := NewProcessor("test", entities, queries)
	key := storage.EntityKey{Relation: relation, Value: "1"}
	record := func(project, id string) storage.Record {
		return storage.Record{
			"id":      {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte(id)},
			"project": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte(project)},
		}
	}

	insert, err := processor.ApplyRelationBatch(datastore.CommittedChanges{Changes: []datastore.Change{fakeChange{kind: datastore.ChangeInsert, relation: relation, before: key, after: key, record: record("a", "1"), complete: true}}})
	if err != nil || len(insert.Changes) != 1 || insert.Changes[0].Kind != QueryAdded {
		t.Fatalf("insert = %+v, %v", insert, err)
	}
	entity, ok := entities.Get(key)
	if !ok || entity.References != 1 {
		t.Fatalf("shared entity = %+v, %v", entity, ok)
	}

	leave, err := processor.ApplyRelationBatch(datastore.CommittedChanges{Changes: []datastore.Change{fakeChange{kind: datastore.ChangeUpdate, relation: relation, before: key, after: key, record: record("b", "1"), complete: true}}})
	if err != nil || len(leave.Changes) != 1 || leave.Changes[0].Kind != QueryRemoved {
		t.Fatalf("leave = %+v, %v", leave, err)
	}
	if queries.IsMember("project-a", key) {
		t.Fatal("entity remained in query")
	}
}

func TestProcessorAppliesTruncateAsClearThenLaterTransactionRows(t *testing.T) {
	relation := storage.RelationKey{Source: "test", Namespace: "app", Name: "tasks"}
	entities := entitycache.NewEntityCache()
	queries := queryregistry.NewQueryRegistry(entities)
	plan := fakePlan{fingerprint: "all", relation: relation}
	queries.Acquire(plan)
	if err := queries.SetWindow(plan.Fingerprint(), "viewer", datastore.Bounds{Left: datastore.Boundary{Kind: datastore.BoundaryStart}, Right: datastore.Boundary{Kind: datastore.BoundaryEnd}}); err != nil {
		t.Fatal(err)
	}
	processor := NewProcessor("test", entities, queries)
	record := func(id, title string) storage.Record {
		return storage.Record{
			"id":    {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte(id)},
			"title": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte(title)},
		}
	}
	oldKey := storage.EntityKey{Relation: relation, Value: "old"}
	if _, err := processor.ApplyRelationBatch(datastore.CommittedChanges{Changes: []datastore.Change{
		fakeChange{kind: datastore.ChangeInsert, relation: relation, before: oldKey, after: oldKey, record: record("old", "before"), complete: true},
	}}); err != nil {
		t.Fatal(err)
	}
	newKey := storage.EntityKey{Relation: relation, Value: "new"}
	result, err := processor.ApplyRelationBatch(datastore.CommittedChanges{Changes: []datastore.Change{
		// This update is made irrelevant by the later truncate and must not be
		// published or hydrated.
		fakeChange{kind: datastore.ChangeUpdate, relation: relation, before: oldKey, after: oldKey, record: record("old", "temporary"), complete: false},
		fakeChange{kind: datastore.ChangeClearRelation, relation: relation},
		fakeChange{kind: datastore.ChangeInsert, relation: relation, before: newKey, after: newKey, record: record("new", "after"), complete: true},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.EntityActions) != 1 || result.EntityActions[0].Kind != EntityCancelHydration || result.EntityActions[0].Key != newKey {
		t.Fatalf("post-truncate entity actions = %+v", result.EntityActions)
	}
	if len(result.Resync) != 0 {
		t.Fatalf("truncate requested relation resync: %+v", result.Resync)
	}
	if len(result.Changes) != 2 || result.Changes[0].Kind != QueryCleared || result.Changes[0].ChangeIndex != 2 || result.Changes[1].Kind != QueryAdded || result.Changes[1].Key != newKey || result.Changes[1].ChangeIndex != 3 {
		t.Fatalf("truncate result = %+v", result.Changes)
	}
	if queries.IsMember(plan.Fingerprint(), oldKey) || !queries.IsMember(plan.Fingerprint(), newKey) {
		t.Fatalf("members after truncate: old=%v new=%v", queries.IsMember(plan.Fingerprint(), oldKey), queries.IsMember(plan.Fingerprint(), newKey))
	}
	if old, ok := entities.Get(oldKey); ok {
		t.Fatalf("old entity survived truncate = %+v", old)
	}
}

func TestProcessorRejectsMixedRelationBatch(t *testing.T) {
	tasks := storage.RelationKey{Source: "test", Namespace: "app", Name: "tasks"}
	projects := storage.RelationKey{Source: "test", Namespace: "app", Name: "projects"}
	entities := entitycache.NewEntityCache()
	queries := queryregistry.NewQueryRegistry(entities)
	for _, plan := range []fakePlan{
		{fingerprint: "tasks", relation: tasks},
		{fingerprint: "projects", relation: projects},
	} {
		queries.Acquire(plan)
		if err := queries.SetWindow(plan.Fingerprint(), "viewer", datastore.Bounds{Left: datastore.Boundary{Kind: datastore.BoundaryStart}, Right: datastore.Boundary{Kind: datastore.BoundaryEnd}}); err != nil {
			t.Fatal(err)
		}
	}
	processor := NewProcessor("test", entities, queries)
	record := func(id string) storage.Record {
		return storage.Record{"id": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte(id)}}
	}
	projectKey := storage.EntityKey{Relation: projects, Value: "project"}
	taskKey := storage.EntityKey{Relation: tasks, Value: "task"}
	_, err := processor.ApplyRelationBatch(datastore.CommittedChanges{Changes: []datastore.Change{
		fakeChange{kind: datastore.ChangeInsert, relation: projects, before: projectKey, after: projectKey, record: record("project"), complete: true},
		fakeChange{kind: datastore.ChangeInsert, relation: tasks, before: taskKey, after: taskKey, record: record("task"), complete: true},
	}})
	if !errors.Is(err, ErrFatalSource) {
		t.Fatalf("mixed relation error = %v, want ErrFatalSource", err)
	}
}

func TestProcessorProbesOnlyChangedRegisteredFields(t *testing.T) {
	relation := storage.RelationKey{Source: "test", Namespace: "app", Name: "tasks"}
	entities := entitycache.NewEntityCache()
	queries := queryregistry.NewQueryRegistry(entities)
	plan := fakePlan{
		fingerprint: "complex", relation: relation,
		routing: routing.RoutingPlan{
			Exact:       []routing.RoutingExact{{Field: "A", Token: "A=1"}},
			WatchFields: []storage.FieldPath{"B"},
		},
		matches: func(record storage.Record) bool {
			return string(record["A"].Data) == "1" && string(record["B"].Data) != "2"
		},
	}
	queries.Acquire(plan)
	_ = queries.SetWindow("complex", "viewer", datastore.Bounds{Left: datastore.Boundary{Kind: datastore.BoundaryStart}, Right: datastore.Boundary{Kind: datastore.BoundaryEnd}})
	processor := NewProcessor("test", entities, queries)
	key := storage.EntityKey{Relation: relation, Value: "1"}
	record := func(b, d string) storage.Record {
		return storage.Record{
			"id": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("1")},
			"A":  {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("1")},
			"B":  {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte(b)},
			"D":  {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte(d)},
		}
	}
	if _, err := processor.ApplyRelationBatch(datastore.CommittedChanges{Changes: []datastore.Change{fakeChange{kind: datastore.ChangeInsert, relation: relation, before: key, after: key, record: record("3", "old"), complete: true}}}); err != nil {
		t.Fatal(err)
	}
	result, err := processor.ApplyRelationBatch(datastore.CommittedChanges{Changes: []datastore.Change{fakeChange{kind: datastore.ChangeUpdate, relation: relation, before: key, after: key, record: record("3", "new"), complete: true}}})
	if err != nil || len(result.Changes) != 1 || result.Changes[0].Kind != QueryUpdated {
		t.Fatalf("payload update = %+v, err=%v", result, err)
	}

	result, err = processor.ApplyRelationBatch(datastore.CommittedChanges{Changes: []datastore.Change{fakeChange{kind: datastore.ChangeUpdate, relation: relation, before: key, after: key, record: record("2", "new"), complete: true}}})
	if err != nil || len(result.Changes) != 1 || result.Changes[0].Kind != QueryRemoved {
		t.Fatalf("watcher update = %+v, err=%v", result, err)
	}
	left := record("2", "new")
	left["A"] = storage.Value{Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("3")}
	result, err = processor.ApplyRelationBatch(datastore.CommittedChanges{Changes: []datastore.Change{fakeChange{kind: datastore.ChangeUpdate, relation: relation, before: key, after: key, record: left, complete: true}}})
	if err != nil || len(result.Changes) != 0 {
		t.Fatalf("old exact-value probe = %+v, err=%v", result, err)
	}
}

func TestProcessorDoesNotPublishIncompleteChange(t *testing.T) {
	relation := storage.RelationKey{Source: "test", Name: "tasks"}
	entities := entitycache.NewEntityCache()
	processor := NewProcessor("test", entities, queryregistry.NewQueryRegistry(entities))
	key := storage.EntityKey{Relation: relation, Value: "1"}
	result, err := processor.ApplyRelationBatch(datastore.CommittedChanges{Changes: []datastore.Change{fakeChange{kind: datastore.ChangeUpdate, relation: relation, before: key, after: key}}})
	if err != nil || len(result.Changes) != 0 || len(result.EntityActions) != 1 || result.EntityActions[0].Kind != EntityDiscardPartial {
		t.Fatalf("result = %+v, %v", result, err)
	}
}

func TestProcessorDoesNotHydrateDelete(t *testing.T) {
	relation := storage.RelationKey{Source: "test", Name: "tasks"}
	entities := entitycache.NewEntityCache()
	processor := NewProcessor("test", entities, queryregistry.NewQueryRegistry(entities))
	key := storage.EntityKey{Relation: relation, Value: "1"}
	result, err := processor.ApplyRelationBatch(datastore.CommittedChanges{Changes: []datastore.Change{
		fakeChange{kind: datastore.ChangeDelete, relation: relation, before: key},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.EntityActions) != 1 || result.EntityActions[0].Kind != EntityCancelHydration {
		t.Fatalf("delete entity actions: %+v", result.EntityActions)
	}
}

func TestProcessorNormalizesHydrationToFinalTransactionState(t *testing.T) {
	relation := storage.RelationKey{Source: "test", Name: "tasks"}
	key := storage.EntityKey{Relation: relation, Value: "1"}
	complete := storage.Record{"id": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("1")}}
	entities := entitycache.NewEntityCache()
	processor := NewProcessor("test", entities, queryregistry.NewQueryRegistry(entities))

	result, err := processor.ApplyRelationBatch(datastore.CommittedChanges{Changes: []datastore.Change{
		fakeChange{kind: datastore.ChangeUpdate, relation: relation, before: key, after: key},
		fakeChange{kind: datastore.ChangeUpdate, relation: relation, before: key, after: key, record: complete, complete: true},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.EntityActions) != 1 || result.EntityActions[0].Kind != EntityCancelHydration || result.EntityActions[0].Key != key {
		t.Fatalf("partial then complete hydration actions = %+v", result)
	}

	result, err = processor.ApplyRelationBatch(datastore.CommittedChanges{Changes: []datastore.Change{
		fakeChange{kind: datastore.ChangeUpdate, relation: relation, before: key, after: key},
		fakeChange{kind: datastore.ChangeDelete, relation: relation, before: key},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.EntityActions) != 1 || result.EntityActions[0].Kind != EntityCancelHydration || result.EntityActions[0].Key != key {
		t.Fatalf("partial then delete hydration actions = %+v", result)
	}
}

func TestProcessorRepresentsPrimaryKeyChangeAsRemoveAndAdd(t *testing.T) {
	relation := storage.RelationKey{Source: "test", Name: "tasks"}
	entities := entitycache.NewEntityCache()
	queries := queryregistry.NewQueryRegistry(entities)
	plan := fakePlan{fingerprint: "all", relation: relation, routing: routing.RoutingPlan{Fallback: true}}
	queries.Acquire(plan)
	_ = queries.SetWindow("all", "viewer", datastore.Bounds{Left: datastore.Boundary{Kind: datastore.BoundaryStart}, Right: datastore.Boundary{Kind: datastore.BoundaryEnd}})
	processor := NewProcessor("test", entities, queries)
	oldKey := storage.EntityKey{Relation: relation, Value: "old"}
	newKey := storage.EntityKey{Relation: relation, Value: "new"}
	record := storage.Record{"id": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("new")}, "project": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("a")}}
	if _, err := processor.ApplyRelationBatch(datastore.CommittedChanges{Changes: []datastore.Change{fakeChange{kind: datastore.ChangeInsert, relation: relation, before: oldKey, after: oldKey, record: record, complete: true}}}); err != nil {
		t.Fatal(err)
	}
	result, err := processor.ApplyRelationBatch(datastore.CommittedChanges{Changes: []datastore.Change{fakeChange{kind: datastore.ChangeUpdate, relation: relation, before: oldKey, after: newKey, record: record, complete: true}}})
	if err != nil || len(result.Changes) != 2 || result.Changes[0].Kind != QueryRemoved || result.Changes[0].Key != oldKey || result.Changes[1].Kind != QueryAdded || result.Changes[1].Key != newKey {
		t.Fatalf("primary-key transition = %+v, err=%v", result.Changes, err)
	}
}

func TestProcessorRemovesOldPrimaryKeyWhenNewTupleNeedsHydration(t *testing.T) {
	relation := storage.RelationKey{Source: "test", Name: "tasks"}
	entities := entitycache.NewEntityCache()
	queries := queryregistry.NewQueryRegistry(entities)
	plan := fakePlan{fingerprint: "all", relation: relation, routing: routing.RoutingPlan{Fallback: true}, matchError: storage.ErrIncompleteRecord}
	queries.Acquire(plan)
	_ = queries.SetWindow("all", "viewer", datastore.Bounds{Left: datastore.Boundary{Kind: datastore.BoundaryStart}, Right: datastore.Boundary{Kind: datastore.BoundaryEnd}})
	oldKey := storage.EntityKey{Relation: relation, Value: "old"}
	newKey := storage.EntityKey{Relation: relation, Value: "new"}
	oldRecord := storage.Record{"id": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("old")}}
	entities.StoreCurrent(oldKey, oldRecord)
	if _, _, err := queries.AddMember("all", oldKey); err != nil {
		t.Fatal(err)
	}
	processor := NewProcessor("test", entities, queries)
	partial := storage.Record{"id": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("new")}, "document": {State: storage.ValueUnavailable}}
	result, err := processor.ApplyRelationBatch(datastore.CommittedChanges{Changes: []datastore.Change{
		fakeChange{kind: datastore.ChangeUpdate, relation: relation, before: oldKey, after: newKey, record: partial, complete: false},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Changes) != 1 || result.Changes[0].Kind != QueryRemoved || result.Changes[0].Key != oldKey {
		t.Fatalf("primary-key transition = %+v", result.Changes)
	}
	if queries.IsMember("all", oldKey) {
		t.Fatal("old primary key remained in query membership")
	}
	if len(result.EntityActions) != 2 || result.EntityActions[1].Kind != EntityHydrate || len(result.EntityActions[1].Queries) != 1 || result.EntityActions[1].Queries[0] != "all" {
		t.Fatalf("entity recovery actions = %+v", result.EntityActions)
	}
}

func TestProcessorHydratesOnlyQueriesThatDependOnPartialTuple(t *testing.T) {
	relation := storage.RelationKey{Source: "test", Name: "tasks"}
	entities := entitycache.NewEntityCache()
	queries := queryregistry.NewQueryRegistry(entities)
	dependent := fakePlan{fingerprint: "dependent", relation: relation, routing: routing.RoutingPlan{Fallback: true}, matchError: storage.ErrIncompleteRecord}
	rejected := fakePlan{
		fingerprint: "rejected", relation: relation, routing: routing.RoutingPlan{Fallback: true},
		matches: func(record storage.Record) bool { return string(record["project"].Data) == "other" },
	}
	for _, plan := range []fakePlan{dependent, rejected} {
		queries.Acquire(plan)
		_ = queries.SetWindow(plan.Fingerprint(), string(plan.Fingerprint()), datastore.Bounds{Left: datastore.Boundary{Kind: datastore.BoundaryStart}, Right: datastore.Boundary{Kind: datastore.BoundaryEnd}})
	}
	processor := NewProcessor("test", entities, queries)
	key := storage.EntityKey{Relation: relation, Value: "1"}
	partial := storage.Record{
		"id":      {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("1")},
		"project": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("mine")},
		"body":    {State: storage.ValueUnavailable},
	}
	result, err := processor.ApplyRelationBatch(datastore.CommittedChanges{Changes: []datastore.Change{
		fakeChange{kind: datastore.ChangeInsert, relation: relation, before: key, after: key, record: partial},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.EntityActions) != 1 || result.EntityActions[0].Kind != EntityHydrate || len(result.EntityActions[0].Queries) != 1 || result.EntityActions[0].Queries[0] != dependent.Fingerprint() {
		t.Fatalf("entity recovery actions = %+v", result.EntityActions)
	}
}

func TestProcessorDiscardsPartialTupleRejectedByEveryCandidate(t *testing.T) {
	relation := storage.RelationKey{Source: "test", Name: "tasks"}
	entities := entitycache.NewEntityCache()
	queries := queryregistry.NewQueryRegistry(entities)
	plan := fakePlan{
		fingerprint: "other-project", relation: relation, routing: routing.RoutingPlan{Fallback: true},
		matches: func(record storage.Record) bool { return string(record["project"].Data) == "other" },
	}
	queries.Acquire(plan)
	_ = queries.SetWindow(plan.Fingerprint(), "viewer", datastore.Bounds{Left: datastore.Boundary{Kind: datastore.BoundaryStart}, Right: datastore.Boundary{Kind: datastore.BoundaryEnd}})
	processor := NewProcessor("test", entities, queries)
	key := storage.EntityKey{Relation: relation, Value: "1"}
	result, err := processor.ApplyRelationBatch(datastore.CommittedChanges{Changes: []datastore.Change{
		fakeChange{kind: datastore.ChangeInsert, relation: relation, before: key, after: key, record: storage.Record{
			"id":      {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("1")},
			"project": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("mine")},
			"body":    {State: storage.ValueUnavailable},
		}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Changes) != 0 || len(result.EntityActions) != 1 || result.EntityActions[0].Kind != EntityDiscardPartial {
		t.Fatalf("result = %+v", result)
	}
}

func TestProcessorReconcilePublishesHydratedCurrentValue(t *testing.T) {
	relation := storage.RelationKey{Source: "test", Name: "tasks"}
	entities := entitycache.NewEntityCache()
	queries := queryregistry.NewQueryRegistry(entities)
	plan := fakePlan{fingerprint: "a", relation: relation, routing: routing.RoutingPlan{Exact: []routing.RoutingExact{{Field: "project", Token: "a"}}}, matches: func(record storage.Record) bool { return string(record["project"].Data) == "a" }}
	queries.Acquire(plan)
	_ = queries.SetWindow("a", "viewer", datastore.Bounds{Left: datastore.Boundary{Kind: datastore.BoundaryStart}, Right: datastore.Boundary{Kind: datastore.BoundaryEnd}})
	processor := NewProcessor("test", entities, queries)
	key := storage.EntityKey{Relation: relation, Value: "1"}
	record := storage.Record{"id": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("1")}, "project": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("a")}}
	result, err := processor.Reconcile(key, record, true)
	if err != nil || len(result.Changes) != 1 || result.Changes[0].Kind != QueryAdded {
		t.Fatalf("reconcile = %+v, %v", result, err)
	}
}

func TestProcessorIsolatesOneBrokenQuery(t *testing.T) {
	relation := storage.RelationKey{Source: "test", Name: "tasks"}
	entities := entitycache.NewEntityCache()
	queries := queryregistry.NewQueryRegistry(entities)
	good := fakePlan{fingerprint: "good", relation: relation, routing: routing.RoutingPlan{Fallback: true}}
	broken := fakePlan{fingerprint: "broken", relation: relation, routing: routing.RoutingPlan{Fallback: true}, matchError: errors.New("schema changed")}
	queries.Acquire(good)
	queries.Acquire(broken)
	all := datastore.Bounds{Left: datastore.Boundary{Kind: datastore.BoundaryStart}, Right: datastore.Boundary{Kind: datastore.BoundaryEnd}}
	_ = queries.SetWindow("good", "good-viewer", all)
	_ = queries.SetWindow("broken", "broken-viewer", all)
	processor := NewProcessor("test", entities, queries)
	key := storage.EntityKey{Relation: relation, Value: "1"}
	record := storage.Record{"id": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("1")}}
	result, err := processor.ApplyRelationBatch(datastore.CommittedChanges{Changes: []datastore.Change{
		fakeChange{kind: datastore.ChangeInsert, relation: relation, before: key, after: key, record: record, complete: true},
	}})
	if err != nil || len(result.Changes) != 1 || result.Changes[0].Query != "good" || len(result.QueryFailures) != 1 || result.QueryFailures[0].Query != "broken" {
		t.Fatalf("isolated result = %+v, err=%v", result, err)
	}
}

func TestProcessorReconcileIsolatesOneBrokenQuery(t *testing.T) {
	relation := storage.RelationKey{Source: "test", Name: "tasks"}
	entities := entitycache.NewEntityCache()
	queries := queryregistry.NewQueryRegistry(entities)
	good := fakePlan{fingerprint: "good", relation: relation, routing: routing.RoutingPlan{Fallback: true}}
	broken := fakePlan{fingerprint: "broken", relation: relation, routing: routing.RoutingPlan{Fallback: true}, matchError: errors.New("schema changed")}
	queries.Acquire(good)
	queries.Acquire(broken)
	all := datastore.Bounds{Left: datastore.Boundary{Kind: datastore.BoundaryStart}, Right: datastore.Boundary{Kind: datastore.BoundaryEnd}}
	_ = queries.SetWindow("good", "good-viewer", all)
	_ = queries.SetWindow("broken", "broken-viewer", all)
	processor := NewProcessor("test", entities, queries)
	key := storage.EntityKey{Relation: relation, Value: "1"}
	record := storage.Record{"id": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("1")}}

	result, err := processor.Reconcile(key, record, true)
	if err != nil || len(result.Changes) != 1 || result.Changes[0].Query != "good" || len(result.QueryFailures) != 1 || result.QueryFailures[0].Query != "broken" {
		t.Fatalf("isolated reconciliation = %+v, err=%v", result, err)
	}
}
