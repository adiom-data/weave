package windowloader

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/adiom-data/weave/internal/datastore"
	"github.com/adiom-data/weave/internal/engine/entitycache"
	"github.com/adiom-data/weave/internal/engine/queryregistry"
	"github.com/adiom-data/weave/internal/storage"
)

type fakePlan struct {
	fingerprint datastore.QueryFingerprint
	relation    storage.RelationKey
	matches     func(storage.Record) bool
}

func (p fakePlan) Fingerprint() datastore.QueryFingerprint { return p.fingerprint }
func (p fakePlan) Relation() storage.RelationKey           { return p.relation }
func (p fakePlan) EntityKey(record storage.Record) (storage.EntityKey, error) {
	return storage.EntityKey{Relation: p.relation, Value: string(record["id"].Data)}, nil
}
func (p fakePlan) Matches(record storage.Record) (bool, error) {
	if p.matches != nil {
		return p.matches(record), nil
	}
	return true, nil
}
func (p fakePlan) Key(record storage.Record) (datastore.SortKey, error) {
	return datastore.EncodedSortKey(record["id"].Data), nil
}
func (fakePlan) Compare(a, b datastore.SortKey) (int, error) {
	return bytes.Compare(a.(datastore.EncodedSortKey), b.(datastore.EncodedSortKey)), nil
}
func (fakePlan) EncodeKey(key datastore.SortKey) ([]byte, error) {
	return append([]byte(nil), key.(datastore.EncodedSortKey)...), nil
}
func (fakePlan) DecodeKey(encoded []byte) (datastore.SortKey, error) {
	return datastore.EncodedSortKey(append([]byte(nil), encoded...)), nil
}
func (fakePlan) Query() storage.QuerySpec { return storage.QuerySpec{} }

type testFence struct{}

func (testFence) StorageFence()            {}
func (testFence) Source() storage.SourceID { return "test" }
func (testFence) StableID() string         { return "test-fence" }
func (testFence) Order() uint64            { return 1 }

func fullWindowRequest(limit int) datastore.WindowRequest {
	return datastore.WindowRequest{Bounds: datastore.Bounds{Left: datastore.Boundary{Kind: datastore.BoundaryStart}, Right: datastore.Boundary{Kind: datastore.BoundaryEnd}}, Limit: limit}
}

type windowSource struct {
	records          []storage.Record
	snapshotRelation storage.RelationKey
	beginCalls       *int
	loadWindowCalls  *int
	loadKeyCalls     *int
	paginate         bool
}

// Read keeps the unit tests focused on a single request while the production
// loader exposes only the batched operations used by the coordinator.
func (l *WindowLoader) Read(ctx context.Context, query datastore.PreparedQuery, request datastore.WindowRequest) (*PendingWindow, error) {
	return l.read(ctx, query, []datastore.WindowRequest{request}, false, false)
}

func (l *WindowLoader) DiscoverWindow(ctx context.Context, query datastore.PreparedQuery, request datastore.WindowRequest) ([]WindowDiscovery, error) {
	windows, err := l.DiscoverWindows(ctx, query, []datastore.WindowRequest{request})
	if err != nil {
		return nil, err
	}
	return windows[0], nil
}

type supersedingEntityState struct {
	*entitycache.EntityCache
	record storage.Record
}

func (s supersedingEntityState) StageSnapshotBatch(requests []entitycache.SnapshotLoadRequest) []entitycache.Load {
	loads := s.EntityCache.StageSnapshotBatch(requests)
	for index, load := range loads {
		if !load.Owned() {
			continue
		}
		// Model a complete CDC tuple arriving immediately after the snapshot batch
		// is staged. It must supersede the staged generation.
		s.EntityCache.StoreCurrent(requests[index].Key, s.record)
	}
	return loads
}

func (windowSource) ID() storage.SourceID { return "test" }
func (s windowSource) BeginSnapshot(_ context.Context, query datastore.PreparedQuery) (datastore.SnapshotSession, error) {
	relation := query.Relation()
	if s.beginCalls != nil {
		*s.beginCalls++
	}
	if s.snapshotRelation != (storage.RelationKey{}) {
		relation = s.snapshotRelation
	}
	return &windowSnapshot{relation: relation, records: s.records, loadWindowCalls: s.loadWindowCalls, loadKeyCalls: s.loadKeyCalls, paginate: s.paginate}, nil
}

func TestWindowLoaderPagesDisjointRangesThroughOneSnapshot(t *testing.T) {
	relation := storage.RelationKey{Source: "test", Name: "tasks"}
	record := func(id string) storage.Record {
		return storage.Record{"id": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte(id)}}
	}
	plan := fakePlan{fingerprint: "all", relation: relation}
	entities := entitycache.NewEntityCache()
	queries := queryregistry.NewQueryRegistry(entities)
	queries.Acquire(plan)
	all := datastore.Bounds{Left: datastore.Boundary{Kind: datastore.BoundaryStart}, Right: datastore.Boundary{Kind: datastore.BoundaryEnd}}
	if err := queries.SetWindow("all", "viewer", all); err != nil {
		t.Fatal(err)
	}
	var beginCalls, loadCalls int
	loader := NewWindowLoader(windowSource{
		records:    []storage.Record{record("1"), record("2"), record("3"), record("4"), record("5")},
		beginCalls: &beginCalls, loadWindowCalls: &loadCalls, paginate: true,
	}, entities, queries)
	pending, err := loader.ReadRanges(context.Background(), plan, []datastore.Bounds{
		{Left: datastore.Boundary{Kind: datastore.BoundaryStart}, Right: datastore.Boundary{Kind: datastore.BoundaryCursor, Key: datastore.EncodedSortKey("2")}},
		{Left: datastore.Boundary{Kind: datastore.BoundaryCursor, Key: datastore.EncodedSortKey("3")}, Right: datastore.Boundary{Kind: datastore.BoundaryEnd}},
	}, 1)
	if err != nil {
		t.Fatal(err)
	}
	if beginCalls != 1 || loadCalls != 5 {
		t.Fatalf("snapshot begins=%d window reads=%d, want 1 and 5", beginCalls, loadCalls)
	}
	if pending.Len() != 4 {
		t.Fatalf("pending rows=%d, want four rows across disjoint ranges", pending.Len())
	}
	if _, err := pending.Activate(context.Background()); err != nil {
		t.Fatal(err)
	}
}

type windowSnapshot struct {
	relation        storage.RelationKey
	records         []storage.Record
	loadWindowCalls *int
	loadKeyCalls    *int
	paginate        bool
}

func (s *windowSnapshot) Relation() storage.RelationKey { return s.relation }
func (s *windowSnapshot) FetchEntity(context.Context, storage.EntityKey) (datastore.AuthoritativeRecord, error) {
	panic("not used")
}
func (s *windowSnapshot) LoadWindow(_ context.Context, _ datastore.PreparedQuery, request datastore.WindowRequest) ([]datastore.AuthoritativeRecord, error) {
	if s.loadWindowCalls != nil {
		*s.loadWindowCalls++
	}
	result := make([]datastore.AuthoritativeRecord, 0, len(s.records))
	for _, record := range s.records {
		if s.paginate {
			key := datastore.EncodedSortKey(record["id"].Data)
			if request.Bounds.Left.Kind == datastore.BoundaryCursor && bytes.Compare(key, request.Bounds.Left.Key.(datastore.EncodedSortKey)) <= 0 {
				continue
			}
			if request.Bounds.Right.Kind == datastore.BoundaryCursor && bytes.Compare(key, request.Bounds.Right.Key.(datastore.EncodedSortKey)) > 0 {
				continue
			}
		}
		result = append(result, datastore.AuthoritativeRecord{Record: record})
	}
	if s.paginate {
		if request.Reverse && len(result) > request.Limit {
			result = result[len(result)-request.Limit:]
		} else if len(result) > request.Limit {
			result = result[:request.Limit]
		}
	}
	return result, nil
}
func (s *windowSnapshot) LoadWindowKeys(_ context.Context, query datastore.PreparedQuery, request datastore.WindowRequest) ([]datastore.SortKey, error) {
	if s.loadKeyCalls != nil {
		*s.loadKeyCalls++
	}
	result := make([]datastore.SortKey, 0, len(s.records))
	for _, record := range s.records {
		key, err := query.Key(record)
		if err != nil {
			return nil, err
		}
		if s.paginate {
			encoded := key.(datastore.EncodedSortKey)
			if request.Bounds.Left.Kind == datastore.BoundaryCursor && bytes.Compare(encoded, request.Bounds.Left.Key.(datastore.EncodedSortKey)) <= 0 {
				continue
			}
			if request.Bounds.Right.Kind == datastore.BoundaryCursor && bytes.Compare(encoded, request.Bounds.Right.Key.(datastore.EncodedSortKey)) > 0 {
				continue
			}
		}
		result = append(result, key)
	}
	if s.paginate {
		if request.Reverse && len(result) > request.Limit {
			result = result[len(result)-request.Limit:]
		} else if len(result) > request.Limit {
			result = result[:request.Limit]
		}
	}
	return result, nil
}

func TestWindowDiscoveryUsesKeyOnlyReadWithoutVisibility(t *testing.T) {
	relation := storage.RelationKey{Source: "test", Name: "tasks"}
	record := storage.Record{"id": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("1")}, "large": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("payload")}}
	var fullReads, keyReads int
	source := windowSource{records: []storage.Record{record}, loadWindowCalls: &fullReads, loadKeyCalls: &keyReads}
	loader := NewWindowLoader(source, entitycache.NewEntityCache(), queryregistry.NewQueryRegistry(entitycache.NewEntityCache()))
	plan := fakePlan{fingerprint: "all", relation: relation}
	rows, err := loader.DiscoverWindow(context.Background(), plan, datastore.WindowRequest{
		Bounds: datastore.Bounds{Left: datastore.Boundary{Kind: datastore.BoundaryStart}, Right: datastore.Boundary{Kind: datastore.BoundaryEnd}}, Limit: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || fullReads != 0 || keyReads != 1 {
		t.Fatalf("discovery rows=%+v fullReads=%d keyReads=%d", rows, fullReads, keyReads)
	}
}

func TestWindowDiscoveryBatchesRequestsInOneSnapshot(t *testing.T) {
	relation := storage.RelationKey{Source: "test", Name: "tasks"}
	record := func(id string) storage.Record {
		return storage.Record{"id": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte(id)}}
	}
	var snapshots, keyReads int
	loader := NewWindowLoader(windowSource{
		records:      []storage.Record{record("1"), record("2"), record("3"), record("4")},
		beginCalls:   &snapshots,
		loadKeyCalls: &keyReads,
		paginate:     true,
	}, entitycache.NewEntityCache(), queryregistry.NewQueryRegistry(entitycache.NewEntityCache()))
	plan := fakePlan{fingerprint: "all", relation: relation}
	windows, err := loader.DiscoverWindows(context.Background(), plan, []datastore.WindowRequest{
		{Bounds: datastore.Bounds{Left: datastore.Boundary{Kind: datastore.BoundaryStart}, Right: datastore.Boundary{Kind: datastore.BoundaryCursor, Key: datastore.EncodedSortKey("2")}}, Limit: 2},
		{Bounds: datastore.Bounds{Left: datastore.Boundary{Kind: datastore.BoundaryCursor, Key: datastore.EncodedSortKey("2")}, Right: datastore.Boundary{Kind: datastore.BoundaryEnd}}, Limit: 2},
	})
	if err != nil {
		t.Fatal(err)
	}
	if snapshots != 1 || keyReads != 2 {
		t.Fatalf("snapshots=%d key reads=%d, want one shared snapshot and two reads", snapshots, keyReads)
	}
	if len(windows) != 2 || len(windows[0]) != 2 || len(windows[1]) != 2 {
		t.Fatalf("discovered windows = %+v", windows)
	}
}
func (*windowSnapshot) Finish(context.Context) (storage.Fence, error) { return testFence{}, nil }
func (*windowSnapshot) Abort(context.Context) error                   { return nil }

func TestWindowLoaderPublishesOnlyAfterActivation(t *testing.T) {
	relation := storage.RelationKey{Source: "test", Name: "tasks"}
	record := storage.Record{"id": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("1")}, "project": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("a")}}
	plan := fakePlan{fingerprint: "a", relation: relation, matches: func(record storage.Record) bool { return string(record["project"].Data) == "a" }}
	entities := entitycache.NewEntityCache()
	queries := queryregistry.NewQueryRegistry(entities)
	queries.Acquire(plan)
	if err := queries.SetWindow("a", "viewer", datastore.Bounds{Left: datastore.Boundary{Kind: datastore.BoundaryStart}, Right: datastore.Boundary{Kind: datastore.BoundaryEnd}}); err != nil {
		t.Fatal(err)
	}
	loader := NewWindowLoader(windowSource{records: []storage.Record{record}}, entities, queries)
	pending, err := loader.Read(context.Background(), plan, fullWindowRequest(10))
	if err != nil {
		t.Fatal(err)
	}
	key, _ := plan.EntityKey(record)
	if entity, ok := entities.Get(key); !ok || entity.State != entitycache.EntityReconciling || queries.IsMember("a", key) {
		t.Fatalf("entity was visible before activation: %+v, exists=%v", entity, ok)
	}
	rows, err := pending.Activate(context.Background())
	if err != nil || len(rows) != 1 || !queries.IsMember("a", key) {
		t.Fatalf("activation = %+v, %v", rows, err)
	}
}

func TestWindowLoaderAcceptsLoadSupersededAfterSnapshotStaging(t *testing.T) {
	relation := storage.RelationKey{Source: "test", Name: "tasks"}
	snapshot := storage.Record{
		"id":    {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("1")},
		"title": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("snapshot")},
	}
	newer := storage.Record{
		"id":    {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("1")},
		"title": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("cdc")},
	}
	plan := fakePlan{fingerprint: "all", relation: relation}
	entities := entitycache.NewEntityCache()
	queries := queryregistry.NewQueryRegistry(entities)
	queries.Acquire(plan)
	if err := queries.SetWindow("all", "viewer", fullWindowRequest(1).Bounds); err != nil {
		t.Fatal(err)
	}
	loader := NewWindowLoader(windowSource{records: []storage.Record{snapshot}}, supersedingEntityState{EntityCache: entities, record: newer}, queries)
	pending, err := loader.Read(context.Background(), plan, fullWindowRequest(1))
	if err != nil {
		t.Fatalf("read rejected a superseded load: %v", err)
	}
	rows, err := pending.Activate(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || string(rows[0].Record["title"].Data) != "cdc" {
		t.Fatalf("activation rows = %+v, want newer CDC value", rows)
	}
}

func TestWindowLoaderRejectsIncompleteAuthoritativeRecord(t *testing.T) {
	relation := storage.RelationKey{Source: "test", Name: "tasks"}
	record := storage.Record{
		"id":    {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("1")},
		"title": {State: storage.ValueUnavailable},
	}
	plan := fakePlan{fingerprint: "all", relation: relation}
	entities := entitycache.NewEntityCache()
	queries := queryregistry.NewQueryRegistry(entities)
	queries.Acquire(plan)
	loader := NewWindowLoader(windowSource{records: []storage.Record{record}}, entities, queries)
	if _, err := loader.Read(context.Background(), plan, fullWindowRequest(10)); err == nil {
		t.Fatal("expected incomplete authoritative record to fail")
	}
	key, _ := plan.EntityKey(record)
	if _, exists := entities.Get(key); exists {
		t.Fatal("invalid authoritative record entered entity cache")
	}
}

func TestWindowLoaderRejectsSnapshotContractViolations(t *testing.T) {
	relation := storage.RelationKey{Source: "test", Name: "tasks"}
	record := func(id, project string) storage.Record {
		return storage.Record{
			"id":      {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte(id)},
			"project": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte(project)},
		}
	}
	all := datastore.Bounds{Left: datastore.Boundary{Kind: datastore.BoundaryStart}, Right: datastore.Boundary{Kind: datastore.BoundaryEnd}}
	tests := []struct {
		name    string
		plan    fakePlan
		bounds  datastore.Bounds
		records []storage.Record
		want    string
	}{
		{name: "order", plan: fakePlan{fingerprint: "all", relation: relation}, bounds: all, records: []storage.Record{record("2", "a"), record("1", "a")}, want: "outside query order"},
		{name: "bounds", plan: fakePlan{fingerprint: "all", relation: relation}, bounds: datastore.Bounds{Left: datastore.Boundary{Kind: datastore.BoundaryCursor, Key: datastore.EncodedSortKey("1")}, Right: datastore.Boundary{Kind: datastore.BoundaryEnd}}, records: []storage.Record{record("1", "a")}, want: "outside the requested window"},
		{name: "predicate", plan: fakePlan{fingerprint: "project-a", relation: relation, matches: func(value storage.Record) bool { return string(value["project"].Data) == "a" }}, bounds: all, records: []storage.Record{record("1", "b")}, want: "does not match"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			entities := entitycache.NewEntityCache()
			queries := queryregistry.NewQueryRegistry(entities)
			queries.Acquire(test.plan)
			loader := NewWindowLoader(windowSource{records: test.records}, entities, queries)
			_, err := loader.Read(context.Background(), test.plan, datastore.WindowRequest{Bounds: test.bounds, Limit: 10})
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want substring %q", err, test.want)
			}
		})
	}
}

func TestValidateWindowKeysAllowsEqualSortKeys(t *testing.T) {
	plan := fakePlan{fingerprint: "all", relation: storage.RelationKey{Source: "test", Name: "tasks"}}
	bounds := datastore.Bounds{Left: datastore.Boundary{Kind: datastore.BoundaryStart}, Right: datastore.Boundary{Kind: datastore.BoundaryEnd}}
	keys := []datastore.SortKey{datastore.EncodedSortKey("same"), datastore.EncodedSortKey("same"), datastore.EncodedSortKey("z")}
	if err := validateWindowKeys(plan, bounds, keys); err != nil {
		t.Fatalf("equal-key window validation failed: %v", err)
	}
}

func TestWindowLoaderRejectsForeignSnapshotSession(t *testing.T) {
	relation := storage.RelationKey{Source: "test", Name: "tasks"}
	foreign := storage.RelationKey{Source: "test", Name: "projects"}
	plan := fakePlan{fingerprint: "all", relation: relation}
	entities := entitycache.NewEntityCache()
	loader := NewWindowLoader(windowSource{snapshotRelation: foreign}, entities, queryregistry.NewQueryRegistry(entities))
	if _, err := loader.Read(context.Background(), plan, fullWindowRequest(10)); err == nil {
		t.Fatal("expected foreign snapshot session to fail")
	}
}

func TestWindowLoaderUsesNewerReconciledEntity(t *testing.T) {
	relation := storage.RelationKey{Source: "test", Name: "tasks"}
	oldRecord := storage.Record{"id": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("1")}, "project": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("a")}}
	newRecord := storage.Record{"id": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("1")}, "project": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("b")}}
	plan := fakePlan{fingerprint: "a", relation: relation, matches: func(record storage.Record) bool { return string(record["project"].Data) == "a" }}
	entities := entitycache.NewEntityCache()
	queries := queryregistry.NewQueryRegistry(entities)
	queries.Acquire(plan)
	_ = queries.SetWindow("a", "viewer", datastore.Bounds{Left: datastore.Boundary{Kind: datastore.BoundaryStart}, Right: datastore.Boundary{Kind: datastore.BoundaryEnd}})
	pending, err := NewWindowLoader(windowSource{records: []storage.Record{oldRecord}}, entities, queries).Read(context.Background(), plan, fullWindowRequest(10))
	if err != nil {
		t.Fatal(err)
	}
	key, _ := plan.EntityKey(oldRecord)
	entities.StoreCurrent(key, newRecord)
	rows, err := pending.Activate(context.Background())
	if err != nil || len(rows) != 0 || queries.IsMember("a", key) {
		t.Fatalf("stale snapshot became visible: rows=%+v err=%v", rows, err)
	}
}

func TestPendingWindowCancelReleasesOwnedLoads(t *testing.T) {
	relation := storage.RelationKey{Source: "test", Name: "tasks"}
	record := storage.Record{"id": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("1")}}
	plan := fakePlan{fingerprint: "all", relation: relation}
	entities := entitycache.NewEntityCache()
	queries := queryregistry.NewQueryRegistry(entities)
	queries.Acquire(plan)
	_ = queries.SetWindow("all", "viewer", datastore.Bounds{Left: datastore.Boundary{Kind: datastore.BoundaryStart}, Right: datastore.Boundary{Kind: datastore.BoundaryEnd}})
	pending, err := NewWindowLoader(windowSource{records: []storage.Record{record}}, entities, queries).Read(context.Background(), plan, fullWindowRequest(10))
	if err != nil {
		t.Fatal(err)
	}
	key, _ := plan.EntityKey(record)
	pending.Cancel(context.Canceled)
	if _, ok := entities.Get(key); ok {
		t.Fatal("cancelled snapshot left an entity load behind")
	}
}

func TestConcurrentWindowLoadsShareEntityHydration(t *testing.T) {
	relation := storage.RelationKey{Source: "test", Name: "tasks"}
	record := storage.Record{"id": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("1")}}
	plan := fakePlan{fingerprint: "all", relation: relation}
	entities := entitycache.NewEntityCache()
	queries := queryregistry.NewQueryRegistry(entities)
	queries.Acquire(plan)
	queries.Acquire(plan)
	all := datastore.Bounds{Left: datastore.Boundary{Kind: datastore.BoundaryStart}, Right: datastore.Boundary{Kind: datastore.BoundaryEnd}}
	_ = queries.SetWindow("all", "one", all)
	_ = queries.SetWindow("all", "two", all)
	loader := NewWindowLoader(windowSource{records: []storage.Record{record}}, entities, queries)
	first, err := loader.Read(context.Background(), plan, fullWindowRequest(10))
	if err != nil {
		t.Fatal(err)
	}
	second, err := loader.Read(context.Background(), plan, fullWindowRequest(10))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := first.Activate(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := second.Activate(context.Background()); err != nil {
		t.Fatal(err)
	}
	key, _ := plan.EntityKey(record)
	entity, ok := entities.Get(key)
	if !ok || entity.References != 1 {
		t.Fatalf("shared entity = %+v, exists=%v", entity, ok)
	}
}

func TestWindowLoaderReacquiresWarmUnreferencedEntity(t *testing.T) {
	relation := storage.RelationKey{Source: "test", Name: "tasks"}
	oldRecord := storage.Record{
		"id":    {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("1")},
		"title": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("old")},
	}
	freshRecord := storage.Record{
		"id":    {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("1")},
		"title": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("fresh")},
	}
	plan := fakePlan{fingerprint: "all", relation: relation}
	entities := entitycache.NewEntityCache()
	queries := queryregistry.NewQueryRegistry(entities)
	queries.Acquire(plan)
	_ = queries.SetWindow("all", "first", datastore.Bounds{Left: datastore.Boundary{Kind: datastore.BoundaryStart}, Right: datastore.Boundary{Kind: datastore.BoundaryEnd}})
	loader := NewWindowLoader(windowSource{records: []storage.Record{oldRecord}}, entities, queries)
	first, _ := loader.Read(context.Background(), plan, fullWindowRequest(10))
	_, _ = first.Activate(context.Background())
	key, _ := plan.EntityKey(oldRecord)
	if _, err := queries.RemoveMember("all", key); err != nil {
		t.Fatal(err)
	}
	second, _ := NewWindowLoader(windowSource{records: []storage.Record{freshRecord}}, entities, queries).Read(context.Background(), plan, fullWindowRequest(10))
	rows, err := second.Activate(context.Background())
	if err != nil || len(rows) != 1 || string(rows[0].Record["title"].Data) != "fresh" {
		t.Fatalf("warm activation = %+v, %v", rows, err)
	}
}

func TestWindowLoaderRefreshesWarmEntityWhenRecordShapeChanges(t *testing.T) {
	relation := storage.RelationKey{Source: "test", Name: "tasks"}
	oldRecord := storage.Record{"id": {Type: "text", State: storage.ValuePresent, Data: []byte("1")}}
	newRecord := storage.Record{
		"id":       {Type: "text", State: storage.ValuePresent, Data: []byte("1")},
		"optional": {Type: "text", State: storage.ValueNull},
	}
	plan := fakePlan{fingerprint: "all", relation: relation}
	entities := entitycache.NewEntityCache()
	queries := queryregistry.NewQueryRegistry(entities)
	queries.Acquire(plan)
	all := datastore.Bounds{Left: datastore.Boundary{Kind: datastore.BoundaryStart}, Right: datastore.Boundary{Kind: datastore.BoundaryEnd}}
	_ = queries.SetWindow("all", "viewer", all)
	first, err := NewWindowLoader(windowSource{records: []storage.Record{oldRecord}}, entities, queries).Read(context.Background(), plan, datastore.WindowRequest{Bounds: all, Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := first.Activate(context.Background()); err != nil {
		t.Fatal(err)
	}
	second, err := NewWindowLoader(windowSource{records: []storage.Record{newRecord}}, entities, queries).Read(context.Background(), plan, datastore.WindowRequest{Bounds: all, Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	rows, err := second.Activate(context.Background())
	if err != nil || len(rows) != 1 || rows[0].Record["optional"].State != storage.ValueNull {
		t.Fatalf("shape refresh = %+v, err=%v", rows, err)
	}
}

func TestPendingWindowRecoversWhenCoalescedOwnerIsCancelled(t *testing.T) {
	entities := entitycache.NewEntityCache()
	key := storage.EntityKey{Relation: storage.RelationKey{Source: "test", Namespace: "app", Name: "tasks"}, Value: "1"}
	record := storage.Record{"value": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("snapshot")}}
	request := []entitycache.SnapshotLoadRequest{{Key: key, Record: record}}
	owner := entities.StageSnapshotBatch(request)[0]
	waiter := entities.StageSnapshotBatch(request)[0]
	want := errors.New("owner cancelled")
	if err := entities.Fail(owner, want); err != nil {
		t.Fatal(err)
	}
	window := &PendingWindow{entities: entities, pending: []pendingEntity{{load: waiter, record: record}}}
	if err := WaitForExternalLoads(context.Background(), []*PendingWindow{window}); err != nil {
		t.Fatalf("recover canceled owner: %v", err)
	}
	if err := window.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	entity, ok := entities.Get(key)
	if !ok || string(entity.Record["value"].Data) != "snapshot" {
		t.Fatalf("recovered entity = %+v, exists=%v", entity, ok)
	}
}

func TestWaitForExternalLoadsHonorsCancellationBeforeReadyLoads(t *testing.T) {
	entities := entitycache.NewEntityCache()
	key := storage.EntityKey{Relation: storage.RelationKey{Source: "test", Namespace: "app", Name: "tasks"}, Value: "1"}
	record := storage.Record{"id": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("1")}}
	load := entities.StageSnapshotBatch([]entitycache.SnapshotLoadRequest{{Key: key, Record: record}})[0]
	window := &PendingWindow{entities: entities, pending: []pendingEntity{{load: load, record: record}}}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := WaitForExternalLoads(ctx, []*PendingWindow{window}); !errors.Is(err, context.Canceled) {
		t.Fatalf("wait error = %v, want context cancellation", err)
	}
	window.Cancel(context.Canceled)
}

func TestWaitForExternalLoadsRejectsOwnerAfterWaiter(t *testing.T) {
	entities := entitycache.NewEntityCache()
	key := storage.EntityKey{Relation: storage.RelationKey{Source: "test", Namespace: "app", Name: "tasks"}, Value: "1"}
	record := storage.Record{"id": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("1")}}
	request := []entitycache.SnapshotLoadRequest{{Key: key, Record: record}}
	owner := entities.StageSnapshotBatch(request)[0]
	waiter := entities.StageSnapshotBatch(request)[0]
	waiterWindow := &PendingWindow{entities: entities, pending: []pendingEntity{{load: waiter, record: record}}}
	ownerWindow := &PendingWindow{entities: entities, pending: []pendingEntity{{load: owner, record: record}}}
	if err := WaitForExternalLoads(context.Background(), []*PendingWindow{waiterWindow, ownerWindow}); err == nil {
		t.Fatal("activation order accepted a waiter before its owner")
	}
	ownerWindow.Cancel(context.Canceled)
}

func TestPendingWindowsRecoverCanceledBatchWithoutCrossedOwnership(t *testing.T) {
	entities := entitycache.NewEntityCache()
	relation := storage.RelationKey{Source: "test", Namespace: "app", Name: "tasks"}
	record := func(id string) storage.Record {
		return storage.Record{"id": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte(id)}}
	}
	requests := []entitycache.SnapshotLoadRequest{
		{Key: storage.EntityKey{Relation: relation, Value: "1"}, Record: record("1")},
		{Key: storage.EntityKey{Relation: relation, Value: "2"}, Record: record("2")},
	}
	owners := entities.StageSnapshotBatch(requests)
	firstWaiters := entities.StageSnapshotBatch(requests)
	secondWaiters := entities.StageSnapshotBatch([]entitycache.SnapshotLoadRequest{requests[1], requests[0]})
	for _, owner := range owners {
		if err := entities.Fail(owner, errors.New("owner cancelled")); err != nil {
			t.Fatal(err)
		}
	}
	first := &PendingWindow{entities: entities, pending: []pendingEntity{
		{load: firstWaiters[0], record: record("1")},
		{load: firstWaiters[1], record: record("2")},
	}}
	second := &PendingWindow{entities: entities, pending: []pendingEntity{
		{load: secondWaiters[0], record: record("2")},
		{load: secondWaiters[1], record: record("1")},
	}}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	errorsOut := make(chan error, 2)
	for _, window := range []*PendingWindow{first, second} {
		go func(window *PendingWindow) {
			if err := WaitForExternalLoads(ctx, []*PendingWindow{window}); err != nil {
				errorsOut <- err
				return
			}
			errorsOut <- window.Reconcile(ctx)
		}(window)
	}
	for range 2 {
		if err := <-errorsOut; err != nil {
			t.Fatal(err)
		}
	}
}

func TestPendingWindowSetRetriesInActivationOrder(t *testing.T) {
	entities := entitycache.NewEntityCache()
	relation := storage.RelationKey{Source: "test", Namespace: "app", Name: "tasks"}
	record := func(id string) storage.Record {
		return storage.Record{"id": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte(id)}}
	}
	requests := []entitycache.SnapshotLoadRequest{
		{Key: storage.EntityKey{Relation: relation, Value: "1"}, Record: record("1")},
		{Key: storage.EntityKey{Relation: relation, Value: "2"}, Record: record("2")},
	}
	owners := entities.StageSnapshotBatch(requests)
	firstWaiters := entities.StageSnapshotBatch(requests)
	secondWaiters := entities.StageSnapshotBatch([]entitycache.SnapshotLoadRequest{requests[1], requests[0]})
	for _, owner := range owners {
		if err := entities.Fail(owner, errors.New("owner cancelled")); err != nil {
			t.Fatal(err)
		}
	}
	first := &PendingWindow{entities: entities, pending: []pendingEntity{
		{load: firstWaiters[0], record: record("1")},
		{load: firstWaiters[1], record: record("2")},
	}}
	second := &PendingWindow{entities: entities, pending: []pendingEntity{
		{load: secondWaiters[0], record: record("2")},
		{load: secondWaiters[1], record: record("1")},
	}}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := WaitForExternalLoads(ctx, []*PendingWindow{first, second}); err != nil {
		t.Fatal(err)
	}
	if err := first.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	if err := second.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestWindowLoaderRejectsNonPositiveLimit(t *testing.T) {
	relation := storage.RelationKey{Source: "test", Name: "tasks"}
	plan := fakePlan{fingerprint: "all", relation: relation}
	entities := entitycache.NewEntityCache()
	loader := NewWindowLoader(windowSource{}, entities, queryregistry.NewQueryRegistry(entities))
	if _, err := loader.Read(context.Background(), plan, fullWindowRequest(0)); err == nil {
		t.Fatal("zero window limit was accepted")
	}
}
