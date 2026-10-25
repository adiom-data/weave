package queryregistry

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	"github.com/adiom-data/weave/internal/datastore"
	"github.com/adiom-data/weave/internal/engine/entitycache"
	enginerouting "github.com/adiom-data/weave/internal/engine/routing"
	"github.com/adiom-data/weave/internal/storage"
)

type fakePlan struct {
	fingerprint datastore.QueryFingerprint
	relation    storage.RelationKey
	query       storage.QuerySpec
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
func (p fakePlan) Query() storage.QuerySpec { return p.query }

type descendingPlan struct{ fakePlan }

func (p descendingPlan) Compare(a, b datastore.SortKey) (int, error) {
	compared, err := p.fakePlan.Compare(a, b)
	return -compared, err
}

func TestEntitiesInBoundsPreservesQueryOrderAndOwnership(t *testing.T) {
	for _, descending := range []bool{false, true} {
		t.Run(fmt.Sprintf("descending=%v", descending), func(t *testing.T) {
			relation := storage.RelationKey{Source: "test", Name: "items"}
			entities := entitycache.NewEntityCache()
			registry := NewQueryRegistry(entities)
			var plan datastore.PreparedQuery = fakePlan{fingerprint: "query", relation: relation}
			if descending {
				plan = descendingPlan{plan.(fakePlan)}
			}
			registry.Acquire(plan)
			for _, id := range []string{"b", "a", "c"} {
				key := storage.EntityKey{Relation: relation, Value: id}
				entities.StoreCurrent(key, storage.Record{"id": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte(id)}})
				if _, _, err := registry.AddMember("query", key); err != nil {
					t.Fatal(err)
				}
			}
			bounds := []datastore.Bounds{{Left: datastore.Boundary{Kind: datastore.BoundaryStart}, Right: datastore.Boundary{Kind: datastore.BoundaryEnd}}}
			rows, err := registry.EntitiesInBounds("query", bounds)
			if err != nil {
				t.Fatal(err)
			}
			keys, err := registry.EntityKeysInBounds("query", bounds)
			if err != nil || len(keys) != len(rows) {
				t.Fatalf("key image = %v, %v", keys, err)
			}
			for index, row := range rows {
				if keys[index] != row.Key {
					t.Fatal("key image differs from record image order")
				}
			}
			want := []string{"a", "b", "c"}
			if descending {
				want = []string{"c", "b", "a"}
			}
			if len(rows) != len(want) {
				t.Fatalf("rows = %v", rows)
			}
			for i, row := range rows {
				if row.Key.Value != want[i] {
					t.Fatalf("row %d = %s, want %s", i, row.Key.Value, want[i])
				}
			}
			rows[0].Record["id"].Data[0] = 'x'
			cached, _ := entities.Get(rows[0].Key)
			if string(cached.Record["id"].Data) != want[0] {
				t.Fatal("registry read exposed cache-owned record")
			}
		})
	}
}

func TestQueryRegistryRoutesOrderedRanges(t *testing.T) {
	relation := storage.RelationKey{Source: "test", Name: "tasks"}
	field := storage.FieldPath("position")
	registry := NewQueryRegistry(entitycache.NewEntityCache())
	literal := func(value string) storage.Literal {
		return storage.Literal{Type: storage.TypeText, Data: []byte(value)}
	}
	comparison := func(operator storage.ComparisonOperator, value string) *storage.Predicate {
		return &storage.Predicate{Comparison: &storage.Comparison{Field: field, Operator: operator, Value: literal(value)}}
	}
	_, _ = registry.Acquire(fakePlan{fingerprint: "low", relation: relation, query: storage.QuerySpec{Filter: comparison(storage.ComparisonLess, "10")}})
	_, _ = registry.Acquire(fakePlan{fingerprint: "middle", relation: relation, query: storage.QuerySpec{Filter: &storage.Predicate{Junction: &storage.Junction{Operator: storage.JunctionAnd, Operands: []*storage.Predicate{comparison(storage.ComparisonGreaterOrEqual, "10"), comparison(storage.ComparisonLessOrEqual, "20")}}}}})
	_, _ = registry.Acquire(fakePlan{fingerprint: "high", relation: relation, query: storage.QuerySpec{Filter: comparison(storage.ComparisonGreater, "20")}})

	key := storage.EntityKey{Relation: relation, Value: "task"}
	probe := func(value string) []enginerouting.RoutingProbe {
		tokens, points, complete := enginerouting.Probe(relation, field, storage.Value{Type: storage.TypeText, State: storage.ValuePresent, Data: []byte(value)})
		return []enginerouting.RoutingProbe{{Field: field, Tokens: tokens, Points: points, Complete: complete}}
	}
	assertQueries(t, registry.Candidates(relation, key, probe("05"), false), "low", "middle")
	assertQueries(t, registry.Candidates(relation, key, probe("10"), false), "middle")
	assertQueries(t, registry.Candidates(relation, key, probe("20"), false), "middle")
	assertQueries(t, registry.Candidates(relation, key, probe("21"), false), "high", "middle")

	if _, err := registry.Release("middle"); err != nil {
		t.Fatal(err)
	}
	assertQueries(t, registry.Candidates(relation, key, probe("15"), false))
}

func TestRangeIndexMatchesBruteForceAcrossOverlaps(t *testing.T) {
	compare := storage.CompareValues
	value := func(number int) storage.Value {
		text := fmt.Sprintf("%03d", number)
		return storage.Value{Type: storage.TypeText, State: storage.ValuePresent, Data: []byte(text)}
	}
	ptr := func(value storage.Value) *storage.Value { return &value }
	index := newRangeIndex(compare)
	var entries []rangeEntry
	for number := 0; number < 60; number++ {
		interval := enginerouting.RoutingRange{
			Domain:         "position",
			Lower:          ptr(value(number)),
			LowerInclusive: number%2 == 0,
			Upper:          ptr(value(number + 5 + number%11)),
			UpperInclusive: number%3 == 0,
		}
		if number%17 == 0 {
			interval.Lower = nil
		}
		if number%19 == 0 {
			interval.Upper = nil
		}
		entry := rangeEntry{fingerprint: datastore.QueryFingerprint(fmt.Sprintf("query-%d", number)), interval: interval}
		if !index.add(entry) {
			t.Fatalf("failed to add interval %+v", interval)
		}
		entries = append(entries, entry)
	}
	for number := 0; number < 80; number++ {
		point := value(number)
		actual, ok := index.candidates(point)
		if !ok {
			t.Fatalf("point %v failed comparison", point)
		}
		expected := make(map[datastore.QueryFingerprint]struct{})
		for _, entry := range entries {
			contains, valid := rangeContains(entry.interval, point, compare)
			if !valid {
				t.Fatal("brute-force comparison failed")
			}
			if contains {
				expected[entry.fingerprint] = struct{}{}
			}
		}
		if len(actual) != len(expected) {
			t.Fatalf("point %v candidates=%v expected=%v", point, actual, expected)
		}
		for fingerprint := range expected {
			if _, exists := actual[fingerprint]; !exists {
				t.Fatalf("point %v omitted %s", point, fingerprint)
			}
		}
	}
}

func TestRangeIndexRoutesPendingEntriesAndIgnoresTombstones(t *testing.T) {
	compare := storage.CompareValues
	value := func(text string) storage.Value {
		return storage.Value{Type: storage.TypeText, State: storage.ValuePresent, Data: []byte(text)}
	}
	ptr := func(value storage.Value) *storage.Value { return &value }
	index := newRangeIndex(compare)
	for number := 0; number < 10; number++ {
		entry := rangeEntry{
			fingerprint: datastore.QueryFingerprint(fmt.Sprintf("query-%d", number)),
			interval: enginerouting.RoutingRange{
				Domain: "position", Lower: ptr(value(fmt.Sprintf("%02d", number))), Upper: ptr(value("99")),
			},
		}
		if !index.add(entry) {
			t.Fatalf("add %d failed", number)
		}
	}
	// The first entry builds the tree; the remainder are deliberately still in
	// the pending tail until the geometric rebuild threshold is reached.
	candidates, ok := index.candidates(value("10"))
	if !ok || len(candidates) != 10 {
		t.Fatalf("pending candidates=%v ok=%v", candidates, ok)
	}
	index.remove("query-5")
	candidates, ok = index.candidates(value("10"))
	if !ok {
		t.Fatal("candidate comparison failed after remove")
	}
	if _, exists := candidates["query-5"]; exists {
		t.Fatal("removed fingerprint remained in candidates")
	}
	if !index.add(rangeEntry{fingerprint: "query-5", interval: enginerouting.RoutingRange{
		Domain: "position", Lower: ptr(value("05")), Upper: ptr(value("99")),
	}}) {
		t.Fatal("readd removed fingerprint failed")
	}
	candidates, ok = index.candidates(value("10"))
	if !ok {
		t.Fatal("candidate comparison failed after readd")
	}
	if _, exists := candidates["query-5"]; !exists {
		t.Fatal("readded fingerprint was not routed")
	}
}

func TestQueryRegistryRoutesMembershipIndexesAndFallback(t *testing.T) {
	relation := storage.RelationKey{Source: "test", Namespace: "app", Name: "tasks"}
	entities := entitycache.NewEntityCache()
	registry := NewQueryRegistry(entities)
	projectValue := storage.Value{Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("a")}
	indexedQuery := storage.QuerySpec{Filter: &storage.Predicate{Comparison: &storage.Comparison{Field: "project", Operator: storage.ComparisonEqual, Value: storage.Literal{Type: storage.TypeText, Data: []byte("a")}}}}
	indexed, created := registry.Acquire(fakePlan{fingerprint: "indexed", relation: relation, query: indexedQuery})
	if !created {
		t.Fatal("first indexed query was not created")
	}
	_, _ = registry.Acquire(fakePlan{fingerprint: "fallback", relation: relation})
	key := storage.EntityKey{Relation: relation, Value: "task-1"}
	entities.StoreCurrent(key, storage.Record{"id": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte(key.Value)}})
	if _, _, err := registry.AddMember(indexed.Plan.Fingerprint(), key); err != nil {
		t.Fatal(err)
	}

	assertQueries(t, registry.Candidates(relation, key, nil, true), "fallback", "indexed")
	otherKey := storage.EntityKey{Relation: relation, Value: "task-2"}
	tokens, points, complete := enginerouting.Probe(relation, "project", projectValue)
	assertQueries(t, registry.Candidates(relation, otherKey, []enginerouting.RoutingProbe{{Field: "project", Tokens: tokens, Points: points, Complete: complete}}, true), "fallback", "indexed")
	assertQueries(t, registry.Candidates(relation, otherKey, nil, true), "fallback")
	assertQueries(t, registry.Candidates(relation, otherKey, []enginerouting.RoutingProbe{{Field: "project", Complete: false}}, false), "indexed")

}

func TestQueryRegistryUsesSparseFieldSpecificIndexFamilies(t *testing.T) {
	relation := storage.RelationKey{Source: "test", Namespace: "app", Name: "tasks"}
	registry := NewQueryRegistry(entitycache.NewEntityCache())
	comparison := func(field storage.FieldPath, operator storage.ComparisonOperator, value string) *storage.Predicate {
		return &storage.Predicate{Comparison: &storage.Comparison{Field: field, Operator: operator, Value: storage.Literal{Type: storage.TypeText, Data: []byte(value)}}}
	}
	registry.Acquire(fakePlan{fingerprint: "complex", relation: relation, query: storage.QuerySpec{
		Filter: &storage.Predicate{Junction: &storage.Junction{Operator: storage.JunctionAnd, Operands: []*storage.Predicate{
			comparison("A", storage.ComparisonEqual, "1"), comparison("B", storage.ComparisonNotEqual, "2"), comparison("C", storage.ComparisonGreater, "10"), comparison("C", storage.ComparisonLess, "20"),
		}}},
		Order: []storage.OrderTerm{{Field: "position"}},
	}})
	key := storage.EntityKey{Relation: relation, Value: "row"}
	probe := func(field storage.FieldPath, value string, ordering bool) enginerouting.RoutingProbe {
		tokens, points, complete := enginerouting.Probe(relation, field, storage.Value{Type: storage.TypeText, State: storage.ValuePresent, Data: []byte(value)})
		return enginerouting.RoutingProbe{Field: field, Tokens: tokens, Points: points, Complete: complete, Ordering: ordering}
	}
	assertQueries(t, registry.Candidates(relation, key, []enginerouting.RoutingProbe{probe("A", "1", false)}, false), "complex")
	assertQueries(t, registry.Candidates(relation, key, []enginerouting.RoutingProbe{{Field: "B", Complete: true}}, false), "complex")
	assertQueries(t, registry.Candidates(relation, key, []enginerouting.RoutingProbe{probe("C", "15", false)}, false), "complex")
	assertQueries(t, registry.Candidates(relation, key, []enginerouting.RoutingProbe{probe("position", "1", true)}, false), "complex")
	assertQueries(t, registry.Candidates(relation, key, []enginerouting.RoutingProbe{{Field: "position", Complete: true}}, false))
	assertQueries(t, registry.Candidates(relation, key, []enginerouting.RoutingProbe{{Field: "D", Complete: true}}, false))
	assertQueries(t, registry.Candidates(relation, key, []enginerouting.RoutingProbe{{Field: "A", Complete: false}}, false), "complex")

	fields := registry.RoutingFields(relation)
	if len(fields) != 4 {
		t.Fatalf("routing fields = %v", fields)
	}
	if _, err := registry.Release("complex"); err != nil {
		t.Fatal(err)
	}
	if fields := registry.RoutingFields(relation); len(fields) != 0 {
		t.Fatalf("released routing fields = %v", fields)
	}
}

func TestQueryRegistrySharesAndReleasesPlans(t *testing.T) {
	relation := storage.RelationKey{Source: "test", Name: "tasks"}
	registry := NewQueryRegistry(entitycache.NewEntityCache())
	_, created := registry.Acquire(fakePlan{fingerprint: "same", relation: relation})
	_, createdAgain := registry.Acquire(fakePlan{fingerprint: "same", relation: relation})
	if !created || createdAgain {
		t.Fatalf("queries were not shared: created=%v createdAgain=%v", created, createdAgain)
	}
	firstRemoved, err := registry.Release("same")
	if err != nil {
		t.Fatal(err)
	}
	secondRemoved, err := registry.Release("same")
	if err != nil {
		t.Fatal(err)
	}
	if firstRemoved || !secondRemoved {
		t.Fatal("query should be removed only after its final release")
	}
	if got := registry.RelationCandidates(relation); len(got) != 0 {
		t.Fatalf("released query remains indexed: %+v", got)
	}
}

func TestQueryMembershipOwnsOneEntityReferencePerSharedQuery(t *testing.T) {
	relation := storage.RelationKey{Source: "test", Name: "tasks"}
	key := storage.EntityKey{Relation: relation, Value: "task-1"}
	entities := entitycache.NewEntityCache()
	entities.StoreCurrent(key, storage.Record{"id": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte(key.Value)}})
	registry := NewQueryRegistry(entities)
	first := fakePlan{fingerprint: "first", relation: relation}
	second := fakePlan{fingerprint: "second", relation: relation}
	registry.Acquire(first)
	registry.Acquire(first)
	registry.Acquire(second)
	if _, added, err := registry.AddMember("first", key); err != nil || !added {
		t.Fatalf("add first membership: added=%v err=%v", added, err)
	}
	if _, added, err := registry.AddMember("first", key); err != nil || added {
		t.Fatalf("duplicate first membership: added=%v err=%v", added, err)
	}
	if _, added, err := registry.AddMember("second", key); err != nil || !added {
		t.Fatalf("add second membership: added=%v err=%v", added, err)
	}
	if entity, _ := entities.Get(key); entity.References != 2 {
		t.Fatalf("references after two shared queries = %d", entity.References)
	}
	if removed, err := registry.Release("first"); err != nil || removed {
		t.Fatalf("release first subscriber: removed=%v err=%v", removed, err)
	}
	if entity, _ := entities.Get(key); entity.References != 2 {
		t.Fatalf("first query was released too early: references=%d", entity.References)
	}
	if removed, err := registry.Release("first"); err != nil || removed {
		t.Fatalf("release final first subscriber: removed=%v err=%v", removed, err)
	}
	drainRetention(t, registry, time.Now())
	if entity, _ := entities.Get(key); entity.References != 1 {
		t.Fatalf("references after first query removal = %d", entity.References)
	}
	if removed, err := registry.Release("second"); err != nil || removed {
		t.Fatalf("release second query: removed=%v err=%v", removed, err)
	}
	drainRetention(t, registry, time.Now())
	if entity, _ := entities.Get(key); entity.References != 0 {
		t.Fatalf("references after all query removal = %d", entity.References)
	}
}

func TestRemoveEntityReleasesEveryQueryMembership(t *testing.T) {
	relation := storage.RelationKey{Source: "test", Name: "tasks"}
	key := storage.EntityKey{Relation: relation, Value: "task-1"}
	entities := entitycache.NewEntityCache()
	entities.StoreCurrent(key, storage.Record{"id": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte(key.Value)}})
	registry := NewQueryRegistry(entities)
	registry.Acquire(fakePlan{fingerprint: "first", relation: relation})
	registry.Acquire(fakePlan{fingerprint: "second", relation: relation})
	if _, _, err := registry.AddMember("first", key); err != nil {
		t.Fatal(err)
	}
	if _, _, err := registry.AddMember("second", key); err != nil {
		t.Fatal(err)
	}
	removed, err := registry.RemoveEntity(key)
	if err != nil {
		t.Fatal(err)
	}
	if len(removed) != 2 || registry.IsMember("first", key) || registry.IsMember("second", key) {
		t.Fatalf("memberships were not removed: %+v", removed)
	}
	entity, ok := entities.Get(key)
	if !ok || entity.References != 0 {
		t.Fatalf("entity after membership removal = %+v, exists=%v", entity, ok)
	}
}

func TestQueryRegistryMergesOverlappingAndTouchingCoverage(t *testing.T) {
	relation := storage.RelationKey{Source: "test", Name: "tasks"}
	registry := NewQueryRegistry(entitycache.NewEntityCache())
	registry.Acquire(fakePlan{fingerprint: "query", relation: relation})
	cursor := func(value string) datastore.Boundary {
		return datastore.Boundary{Kind: datastore.BoundaryCursor, Key: datastore.EncodedSortKey(value)}
	}
	if err := registry.SetWindow("query", "a", datastore.Bounds{Left: datastore.Boundary{Kind: datastore.BoundaryStart}, Right: cursor("m")}); err != nil {
		t.Fatal(err)
	}
	if err := registry.SetWindow("query", "b", datastore.Bounds{Left: cursor("h"), Right: cursor("z")}); err != nil {
		t.Fatal(err)
	}
	coverage := registry.Coverage("query")
	if len(coverage) != 1 || coverage[0].Left.Kind != datastore.BoundaryStart || string(coverage[0].Right.Key.(datastore.EncodedSortKey)) != "z" {
		t.Fatalf("coverage = %+v", coverage)
	}
	if _, err := registry.RemoveWindow("query", "a"); err != nil {
		t.Fatal(err)
	}
	coverage = registry.Coverage("query")
	if len(coverage) != 1 || string(coverage[0].Left.Key.(datastore.EncodedSortKey)) != "h" {
		t.Fatalf("coverage after removal = %+v", coverage)
	}
	if err := registry.SetWindow("query", "invalid", datastore.Bounds{Left: cursor("z"), Right: cursor("a")}); err == nil {
		t.Fatal("expected invalid reversed window to fail")
	}
}

func TestPendingWindowIsNotReusableCoverage(t *testing.T) {
	relation := storage.RelationKey{Source: "test", Name: "tasks"}
	registry := NewQueryRegistry(entitycache.NewEntityCache())
	registry.Acquire(fakePlan{fingerprint: "query", relation: relation})
	cursor := func(value string) datastore.Boundary {
		return datastore.Boundary{Kind: datastore.BoundaryCursor, Key: datastore.EncodedSortKey(value)}
	}
	bounds := datastore.Bounds{Left: cursor("a"), Right: cursor("z")}
	covered, err := registry.InstallWindow("query", "loading-a", bounds)
	if err != nil || covered {
		t.Fatalf("first install covered=%v, err=%v", covered, err)
	}
	covered, err = registry.InstallWindow("query", "loading-b", bounds)
	if err != nil || covered {
		t.Fatalf("overlap with loading window covered=%v, err=%v", covered, err)
	}
	if err := registry.MarkWindowReady("query", "loading-a"); err != nil {
		t.Fatal(err)
	}
	covered, err = registry.InstallWindow("query", "reused", bounds)
	if err != nil || !covered {
		t.Fatalf("overlap with ready window covered=%v, err=%v", covered, err)
	}
}

func TestPlanExtensionUsesOnlyContiguousReadyCoverage(t *testing.T) {
	relation := storage.RelationKey{Source: "test", Name: "tasks"}
	entities := entitycache.NewEntityCache()
	registry := NewQueryRegistry(entities)
	registry.Acquire(fakePlan{fingerprint: "query", relation: relation})
	cursor := func(value string) datastore.Boundary {
		return datastore.Boundary{Kind: datastore.BoundaryCursor, Key: datastore.EncodedSortKey(value)}
	}
	if err := registry.SetWindow("query", "left", datastore.Bounds{Left: cursor("1"), Right: cursor("4")}); err != nil {
		t.Fatal(err)
	}
	if err := registry.SetWindow("query", "island", datastore.Bounds{Left: cursor("6"), Right: cursor("8")}); err != nil {
		t.Fatal(err)
	}
	for _, value := range []string{"2", "3", "4", "7", "8"} {
		key := storage.EntityKey{Relation: relation, Value: value}
		entities.StoreCurrent(key, storage.Record{"id": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte(value)}})
		if _, _, err := registry.AddMember("query", key); err != nil {
			t.Fatal(err)
		}
	}
	right, err := registry.PlanExtension("query", cursor("1"), 2, false)
	if err != nil || !right.Resolved || string(right.Boundary.Key.(datastore.EncodedSortKey)) != "3" {
		t.Fatalf("right extension = %+v, err=%v", right, err)
	}
	partial, err := registry.PlanExtension("query", cursor("4"), 2, false)
	if err != nil || partial.Available || partial.Resolved {
		t.Fatalf("extension across gap = %+v, err=%v", partial, err)
	}
	left, err := registry.PlanExtension("query", cursor("4"), 2, true)
	if err != nil || !left.Resolved || string(left.Boundary.Key.(datastore.EncodedSortKey)) != "2" {
		t.Fatalf("left extension = %+v, err=%v", left, err)
	}
	if err := registry.SetWindow("query", "tail", datastore.Bounds{Left: cursor("1"), Right: datastore.Boundary{Kind: datastore.BoundaryEnd}}); err != nil {
		t.Fatal(err)
	}
	tail, err := registry.PlanExtension("query", cursor("1"), 2, false)
	if err != nil || !tail.Resolved || tail.Boundary.Kind != datastore.BoundaryCursor || string(tail.Boundary.Key.(datastore.EncodedSortKey)) != "3" {
		t.Fatalf("truncated tail extension = %+v, err=%v", tail, err)
	}
}

func TestIncrementalPruneRemovesOnlyNewlyUncoveredMembers(t *testing.T) {
	relation := storage.RelationKey{Source: "test", Name: "tasks"}
	entities := entitycache.NewEntityCache()
	registry := NewQueryRegistry(entities)
	registry.Acquire(fakePlan{fingerprint: "query", relation: relation})
	cursor := func(value string) datastore.Boundary {
		return datastore.Boundary{Kind: datastore.BoundaryCursor, Key: datastore.EncodedSortKey(value)}
	}
	if err := registry.SetWindow("query", "left", datastore.Bounds{Left: cursor("a"), Right: cursor("f")}); err != nil {
		t.Fatal(err)
	}
	if err := registry.SetWindow("query", "right", datastore.Bounds{Left: cursor("c"), Right: cursor("h")}); err != nil {
		t.Fatal(err)
	}
	for _, value := range []string{"b", "d", "g"} {
		key := storage.EntityKey{Relation: relation, Value: value}
		entities.StoreCurrent(key, storage.Record{"id": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte(value)}})
		if _, _, err := registry.AddMember("query", key); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := registry.RemoveWindow("query", "left"); err != nil {
		t.Fatal(err)
	}
	if err := registry.PruneOutsideCoverage("query"); err != nil {
		t.Fatal(err)
	}
	if failures, more := registry.Maintain(time.Now(), 128); len(failures) != 0 || more {
		t.Fatalf("prune failed: %v %v", failures, more)
	}
	if registry.IsMember("query", storage.EntityKey{Relation: relation, Value: "b"}) {
		t.Fatal("unique left member survived pruning")
	}
	for _, value := range []string{"d", "g"} {
		if !registry.IsMember("query", storage.EntityKey{Relation: relation, Value: value}) {
			t.Fatalf("covered member %q was pruned", value)
		}
	}
}

func assertQueries(t *testing.T, queries []QueryDescriptor, expected ...datastore.QueryFingerprint) {
	t.Helper()
	actual := make(map[datastore.QueryFingerprint]bool, len(queries))
	for _, query := range queries {
		actual[query.Plan.Fingerprint()] = true
	}
	if len(actual) != len(expected) {
		t.Fatalf("queries = %v, expected %v", actual, expected)
	}
	for _, fingerprint := range expected {
		if !actual[fingerprint] {
			t.Fatalf("query %q missing from %v", fingerprint, actual)
		}
	}
}
