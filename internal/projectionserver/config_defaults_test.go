package projectionserver

import (
	"context"
	"testing"

	"github.com/adiom-data/weave/internal/querysubscription/model"
	relationdef "github.com/adiom-data/weave/internal/relation"
	"github.com/adiom-data/weave/internal/storage"
)

func TestPolicyDefinitionsOnlyConstructWithDefaults(t *testing.T) {
	backend := newPolicyBackend(nil) // Construction must not issue a query.
	s, err := New(context.Background(), backend, Options{Relations: &RelationOptions{
		Definitions: relationdef.Definitions{{Namespace: "app", Name: "projects"}: {Visibility: "true"}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	select {
	case <-backend.opened:
		t.Fatal("construction issued a query")
	default:
	}
}

func TestPolicyDefaultsAreOptInAndDoNotMutateCaller(t *testing.T) {
	if (Options{}).withDefaults().Relations != nil {
		t.Fatal("defaults enabled policy evaluation")
	}
	original := &RelationOptions{MaxFactRows: 25}
	got := (Options{Relations: original}).withDefaults().Relations
	d := DefaultRelationOptions()
	if got.MaxFactQueries != d.MaxFactQueries || got.FactLoadConcurrency != d.FactLoadConcurrency || got.MaxFactRows != 25 {
		t.Fatalf("incorrect defaults: %+v", got)
	}
	if original.MaxFactQueries != 0 || original.FactLoadConcurrency != 0 {
		t.Fatal("defaults mutated caller's configuration")
	}
	if (RelationOptions{MaxFactQueries: -1}).withDefaults().MaxFactQueries != -1 {
		t.Fatal("defaults hid an invalid explicit policy limit")
	}
}

func TestProjectionServersCanUseIndependentStoresOverOneQueryBackend(t *testing.T) {
	ctx := t.Context()
	backend := newPolicyBackend(func(context.Context, querysubscription.QuerySubscribeRequest) querysubscription.QuerySnapshot {
		return policySnapshot(10, policyRow("projects", "p1", "project"))
	})
	projects := storage.RelationName{Namespace: "app", Name: "projects"}
	newService := func(expression string) (*server, *relationdef.Store) {
		t.Helper()
		store, err := relationdef.NewStore(relationdef.Definitions{projects: {Computed: map[string]relationdef.Computed{"canEdit": {Type: storage.TypeBool, Expression: expression}}}}, relationdef.Limits{})
		if err != nil {
			t.Fatal(err)
		}
		service, err := New(ctx, backend, Options{Relations: &RelationOptions{Store: store}})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = service.Close() })
		return service.(*server), store
	}
	one, firstStore := newService("true")
	two, secondStore := newService("false")
	check := func(service *server, expected bool) {
		t.Helper()
		request := snapshotTestRequest(false, false)
		request.Root.Computed = []string{"canEdit"}
		response, err := service.GetProjectionSnapshot(ctx, request)
		if err != nil {
			t.Fatal(err)
		}
		rows := response.Data.Fields["projects"].GetListValue().Values
		if len(rows) != 1 || rows[0].GetStructValue().Fields["canEdit"].GetBoolValue() != expected {
			t.Fatalf("independent definition stores mixed: %v", response)
		}
	}
	check(one, true)
	check(two, false)
	if err := firstStore.Replace(nil); err != nil {
		t.Fatal(err)
	}
	check(two, false)
	if err := one.Close(); err != nil {
		t.Fatal(err)
	}
	if err := secondStore.Replace(relationdef.Definitions{projects: {Computed: map[string]relationdef.Computed{"canEdit": {Type: storage.TypeBool, Expression: "true"}}}}); err != nil {
		t.Fatal(err)
	}
	check(two, true)
}
