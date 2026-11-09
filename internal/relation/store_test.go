package relation

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/adiom-data/weave/internal/storage"
)

func TestDefinitionSetPinnedAtBeginAcrossRelationLoads(t *testing.T) {
	definitions := func(role, threshold int) Definitions {
		return Definitions{
			projects: {Computed: map[string]Computed{"role": {
				Type: storage.TypeInteger, Expression: fmt.Sprint(role),
				Propagate: []Propagation{{To: tasks, When: "true"}},
			}}},
			tasks: {Computed: map[string]Computed{
				"role":    {Type: storage.TypeInteger, Expression: "inherited.role"},
				"canEdit": {Type: storage.TypeBool, Expression: fmt.Sprintf("computed.role >= %d", threshold)},
			}},
		}
	}
	store, err := NewStore(definitions(2, 2), Limits{})
	if err != nil {
		t.Fatal(err)
	}
	e, err := NewEvaluator(store)
	if err != nil {
		t.Fatal(err)
	}
	// One pass has read the parent; the other has not read anything. Both must
	// retain the complete old set, including relations first read after Replace.
	first, unread := e.Begin(), e.Begin()
	if _, err := first.load(t.Context(), projects); err != nil {
		t.Fatal(err)
	}
	if err := store.Replace(definitions(10, 20)); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		pass *Evaluation
		want string
	}{{first, "true"}, {unread, "true"}, {e.Begin(), "false"}} {
		parentInput := Input{Candidate: candidate(projects, "p1", storage.Record{})}
		parent := test.pass.NewContext(parentInput, nil, nil, false)
		child := test.pass.NewContext(Input{Candidate: candidate(tasks, "t1", storage.Record{}), Parent: &ParentState{Candidate: parentInput.Candidate, Visible: true}}, nil, parent, false)
		value, err := child.Compute(t.Context(), "canEdit")
		if err != nil || !value.Complete || string(value.Value.Data) != test.want {
			t.Fatalf("mixed definition generations: %+v %v, want %s", value, err, test.want)
		}
	}
}

func TestDefinitionReplacementReusesUnchangedCompilationAndTargetsWatchers(t *testing.T) {
	definitions := Definitions{projects: {Visibility: "false"}, tasks: {}}
	store, err := NewStore(definitions, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	e, _ := NewEvaluator(store)
	a, b := e.Watch(), e.Watch()
	defer a.Close()
	defer b.Close()
	pass := e.Begin()
	projectKeys := a.Replace(nil, pass.Stamps([]Key{projects}))
	taskKeys := b.Replace(nil, pass.Stamps([]Key{tasks}))
	original := pass.snapshot
	if err := store.Replace(definitions); err != nil {
		t.Fatal(err)
	}
	if len(a.Drain()) != 0 || len(b.Drain()) != 0 {
		t.Fatal("unchanged replacement invalidated dependents")
	}
	if latest := e.Begin().snapshot; latest.relations[projects] != original.relations[projects] || latest.relations[tasks] != original.relations[tasks] {
		t.Fatal("unchanged compilation was not reused")
	}
	definitions[projects] = Relation{}
	if err := store.Replace(definitions); err != nil {
		t.Fatal(err)
	}
	if got := a.Drain(); !reflect.DeepEqual(got, []Key{projects}) || len(b.Drain()) != 0 {
		t.Fatalf("replacement not targeted: %v", got)
	}
	select {
	case <-a.Changed():
	default:
		t.Fatal("replacement did not wake watcher")
	}
	if latest := e.Begin().snapshot; latest.relations[projects] == original.relations[projects] || latest.relations[tasks] != original.relations[tasks] {
		t.Fatal("changed/unchanged compilation reuse is incorrect")
	}
	a.Release(projectKeys)
	b.Release(taskKeys)
	if len(store.watchers) != 0 {
		t.Fatal("released subscriptions retained watcher entries")
	}
}

func TestDefinitionReplacementClosesRegistrationRaceAndTracksAbsentRelations(t *testing.T) {
	store, err := NewStore(Definitions{projects: {}}, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	e, _ := NewEvaluator(store)
	before := e.Begin()
	watch := e.Watch()
	defer watch.Close()
	// Adding a previously absent relation must invalidate its old negative
	// result, even when publication precedes dependency registration.
	if err := store.Replace(Definitions{projects: {}, tasks: {}}); err != nil {
		t.Fatal(err)
	}
	keys := watch.Replace(nil, before.Stamps([]Key{tasks}))
	if got := watch.Drain(); !reflect.DeepEqual(got, []Key{tasks}) {
		t.Fatalf("lost addition before registration: %v", got)
	}
	if definition, err := before.load(t.Context(), tasks); err != nil || definition != nil {
		t.Fatal("old pass observed a newly added definition")
	}
	watch.Replace(keys, e.Begin().Stamps([]Key{tasks}))
	if err := store.Replace(Definitions{projects: {}}); err != nil {
		t.Fatal(err)
	}
	if got := watch.Drain(); !reflect.DeepEqual(got, []Key{tasks}) {
		t.Fatalf("removal not invalidated: %v", got)
	}
	if definition, err := e.Begin().load(t.Context(), tasks); err != nil || definition != nil {
		t.Fatal("removed definition still present")
	}
}

func TestInvalidDefinitionReplacementLeavesMapAndWatchersUntouched(t *testing.T) {
	store, err := NewStore(Definitions{projects: {}}, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	e, _ := NewEvaluator(store)
	watch := e.Watch()
	defer watch.Close()
	original := e.Begin()
	watch.Replace(nil, original.Stamps([]Key{projects}))
	for _, invalid := range []Definitions{
		{projects: {Visibility: "42"}},
		{projects: {Computed: map[string]Computed{"role": {Type: storage.TypeInteger, Expression: "true"}}}},
		{projects: {Computed: map[string]Computed{"role": {Type: storage.TypeInteger, Expression: "1", Propagate: []Propagation{{To: tasks, When: "true"}}}}}},
	} {
		if err := store.Replace(invalid); err == nil {
			t.Fatal("accepted invalid replacement")
		}
		if e.Begin().snapshot != original.snapshot || len(watch.Drain()) != 0 {
			t.Fatal("failed replacement changed map or notified dependents")
		}
	}
}

func TestDefinitionStoreCopiesInputsAndReusesFacts(t *testing.T) {
	definition := Relation{Facts: []Fact{grantFact()}, Visibility: "true", Receive: []Incoming{}, Computed: map[string]Computed{"role": {Type: storage.TypeInteger, Expression: "1"}}}
	definitions := Definitions{projects: definition}
	store, err := NewStore(definitions, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	original := store.current.Load().relations[projects]
	if original.definition.Receive == nil {
		t.Fatal("copy lost the explicit reject-all receive list")
	}
	if err := store.Replace(definitions); err != nil {
		t.Fatal(err)
	}
	if store.current.Load().relations[projects] != original {
		t.Fatal("unchanged fact projection was recompiled")
	}
	definition.Facts[0].Projection.Name = "mutated"
	definition.Visibility = "false"
	definition.Computed["role"] = Computed{Type: storage.TypeInteger, Expression: "2"}
	delete(definitions, projects)
	if original.definition.Facts[0].Projection.Name == "mutated" || original.definition.Visibility != "true" || original.definition.Computed["role"].Expression != "1" {
		t.Fatal("published map retained caller-owned mutable data")
	}
}

func TestDefinitionWatcherRetainsSharedKeysAndReleasesAllOnClose(t *testing.T) {
	store, err := NewStore(Definitions{projects: {}}, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	e, _ := NewEvaluator(store)
	watch := e.Watch()
	defer watch.Close()
	stamps := e.Begin().Stamps([]Key{projects})
	a, b := watch.Replace(nil, stamps), watch.Replace(nil, stamps)
	watch.Release(a)
	if err := store.Replace(Definitions{projects: {Visibility: "false"}}); err != nil {
		t.Fatal(err)
	}
	if got := watch.Drain(); !reflect.DeepEqual(got, []Key{projects}) {
		t.Fatalf("first release removed another occurrence's dependency: %v", got)
	}
	watch.Close()
	watch.Release(b) // Late cleanup after Close must also be safe.
	if got := watch.Replace(nil, stamps); len(got) != 0 || len(store.watchers) != 0 {
		t.Fatal("closed watcher retained or recreated subscriptions")
	}
}

func TestDefinitionEvaluationDoesNotTakeStoreLocks(t *testing.T) {
	e := compile(t, Definitions{projects: {}})
	e.store.updateMu.Lock()
	defer e.store.updateMu.Unlock()
	e.store.mu.Lock()
	defer e.store.mu.Unlock()
	done := make(chan error, 1)
	go func() {
		_, err := e.Evaluate(t.Context(), Input{Candidate: candidate(projects, "p1", storage.Record{})}, nil)
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("an evaluation waited for store update/subscription locks")
	}
}

func TestDefinitionStoreRequiresConstructionAndValidLimits(t *testing.T) {
	if _, err := NewStore(nil, Limits{MaxDepth: -1}); err == nil {
		t.Fatal("invalid limits were accepted")
	}
	for _, store := range []*Store{nil, {}} {
		if err := store.Replace(nil); err == nil {
			t.Fatal("uninitialized store accepted a replacement")
		}
		if _, err := NewEvaluator(store); err == nil {
			t.Fatal("uninitialized store accepted by evaluator")
		}
	}
}

func TestDefinitionStoresAreIndependentAndConcurrentReadsStayCoherent(t *testing.T) {
	one, err := NewStore(Definitions{projects: {}, tasks: {}}, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	two, err := NewStore(Definitions{projects: {Visibility: "false"}}, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	e, _ := NewEvaluator(one)
	other, _ := NewEvaluator(two)
	original := other.Begin().snapshot
	var group sync.WaitGroup
	for i := 0; i < 8; i++ {
		group.Go(func() {
			for j := 0; j < 100; j++ {
				pass := e.Begin()
				p, err := pass.load(t.Context(), projects)
				if err != nil {
					t.Error(err)
					return
				}
				task, err := pass.load(t.Context(), tasks)
				if err != nil || p.definition.Visibility != task.definition.Visibility {
					t.Errorf("mixed map: %v", err)
					return
				}
			}
		})
	}
	for i := 0; i < 50; i++ {
		definition := Relation{}
		if i%2 == 0 {
			definition.Visibility = "false"
		}
		if err := one.Replace(Definitions{projects: definition, tasks: definition}); err != nil {
			t.Fatal(err)
		}
	}
	group.Wait()
	if other.Begin().snapshot != original {
		t.Fatal("replacement affected a different store")
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := e.Begin().load(ctx, projects); err != context.Canceled {
		t.Fatalf("canceled evaluation: %v", err)
	}
}
