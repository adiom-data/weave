package subscriptions

import (
	"bytes"
	"testing"

	"github.com/adiom-data/weave/internal/datastore"
	"github.com/adiom-data/weave/internal/storage"
)

type testPlan struct {
	fingerprint datastore.QueryFingerprint
	relation    storage.RelationKey
}

type testFence struct{}

func (testFence) StorageFence()            {}
func (testFence) Source() storage.SourceID { return "test" }
func (testFence) StableID() string         { return "fence" }
func (testFence) Order() uint64            { return 1 }

func (p testPlan) Fingerprint() datastore.QueryFingerprint { return p.fingerprint }
func (p testPlan) Relation() storage.RelationKey           { return p.relation }
func (p testPlan) EntityKey(storage.Record) (storage.EntityKey, error) {
	return storage.EntityKey{}, nil
}
func (p testPlan) Matches(storage.Record) (bool, error) { return true, nil }
func (p testPlan) Key(storage.Record) (datastore.SortKey, error) {
	return datastore.EncodedSortKey(nil), nil
}
func (p testPlan) Compare(a, b datastore.SortKey) (int, error) {
	return bytes.Compare(a.(datastore.EncodedSortKey), b.(datastore.EncodedSortKey)), nil
}
func (p testPlan) EncodeKey(key datastore.SortKey) ([]byte, error) {
	return append([]byte(nil), key.(datastore.EncodedSortKey)...), nil
}
func (p testPlan) DecodeKey(value []byte) (datastore.SortKey, error) {
	return datastore.EncodedSortKey(append([]byte(nil), value...)), nil
}
func (p testPlan) Query() storage.QuerySpec { return storage.QuerySpec{} }

func TestRegistryRejectsInitializationAcrossRelationEpoch(t *testing.T) {
	relation := storage.RelationKey{Source: "test", Name: "tasks"}
	plan := testPlan{fingerprint: "query", relation: relation}
	registry := NewRegistry()
	registry.BeginInitialization("subscriber", plan)
	registry.BumpEpoch(relation)
	if registry.InitializationCurrent("subscriber", plan) {
		t.Fatal("initialization remained current after relation epoch advanced")
	}
}

func TestRegistryInitializationLifecycleIsExplicit(t *testing.T) {
	relation := storage.RelationKey{Source: "test", Name: "tasks"}
	plan := testPlan{fingerprint: "query", relation: relation}
	registry := NewRegistry()
	registry.BeginInitialization("subscriber", plan)
	if !registry.InitializationInPhase("subscriber", plan, OperationPlanning) {
		t.Fatal("initialization did not begin in planning phase")
	}
	reads := []datastore.Bounds{{Left: datastore.Boundary{Kind: datastore.BoundaryStart}, Right: datastore.Boundary{Kind: datastore.BoundaryEnd}}}
	if !registry.StageInitialization("subscriber", plan, reads[0], reads) {
		t.Fatal("failed to stage initialization")
	}
	if registry.StageInitialization("subscriber", plan, reads[0], reads) || !registry.InitializationInPhase("subscriber", plan, OperationMaterializing) {
		t.Fatal("initialization accepted an invalid repeated stage transition")
	}
	if registry.ReadyInitialization("subscriber", plan, nil) || !registry.ReadyInitialization("subscriber", plan, testFence{}) || !registry.InitializationInPhase("subscriber", plan, OperationReady) {
		t.Fatal("initialization accepted readiness without a materialization fence")
	}
}

func TestRegistryWindowLifecycleIsAtomic(t *testing.T) {
	relation := storage.RelationKey{Source: "test", Name: "tasks"}
	plan := testPlan{fingerprint: "query", relation: relation}
	registry := NewRegistry()
	registry.BeginInitialization("subscriber", plan)
	registry.Activate("subscriber", State{Fingerprint: plan.Fingerprint(), Plan: plan})

	state, ok := registry.BeginWindow("subscriber", "one")
	if !ok || state.RelationEpoch != 0 {
		t.Fatal("failed to begin first window update")
	}
	if _, ok := registry.BeginWindow("subscriber", "two"); ok {
		t.Fatal("allowed overlapping window updates")
	}
	if registry.FinishWindow("subscriber", "one", state) {
		t.Fatal("finished window update before it was staged")
	}
	reads := []datastore.Bounds{{Left: datastore.Boundary{Kind: datastore.BoundaryStart}, Right: datastore.Boundary{Kind: datastore.BoundaryEnd}}}
	if !registry.StageWindow("subscriber", "one", reads[0], reads) {
		t.Fatal("failed to stage matching window update")
	}
	if registry.FinishWindow("subscriber", "one", state) {
		t.Fatal("finished window update before materialization became ready")
	}
	if !registry.ReadyWindow("subscriber", "one", testFence{}) {
		t.Fatal("failed to mark materialized window ready")
	}
	state.Bounds.Right = datastore.Boundary{Kind: datastore.BoundaryEnd}
	if !registry.FinishWindow("subscriber", "one", state) {
		t.Fatal("failed to finish matching window update")
	}
	if registry.WindowToken("subscriber") != "" {
		t.Fatal("finished window token was retained")
	}
}

func TestRegistryFindsActiveSubscriptionsByFingerprint(t *testing.T) {
	relation := storage.RelationKey{Source: "test", Name: "tasks"}
	plan := testPlan{fingerprint: "query", relation: relation}
	registry := NewRegistry()
	registry.Activate("first", State{Fingerprint: plan.Fingerprint(), Plan: plan})
	registry.Activate("second", State{Fingerprint: plan.Fingerprint(), Plan: plan})
	registry.Activate("other", State{Fingerprint: "other", Plan: plan})
	ids := registry.ActiveIDs(plan.Fingerprint())
	if len(ids) != 2 {
		t.Fatalf("active IDs = %v, want two query subscribers", ids)
	}
}
