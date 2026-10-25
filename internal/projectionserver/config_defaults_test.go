package projectionserver

import (
	"context"
	"testing"

	"github.com/adiom-data/weave/internal/accesscontrol/policy"
)

func TestPolicyDefinitionsOnlyConstructWithDefaults(t *testing.T) {
	backend := newPolicyBackend(nil) // Construction must not issue a query.
	s, err := New(context.Background(), backend, Options{Policies: &PolicyOptions{
		Definitions: policy.Map{{Namespace: "app", Name: "projects"}: {
			Allow: []policy.Rule{{ID: "view", When: "true"}},
		}},
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
	if (Options{}).withDefaults().Policies != nil {
		t.Fatal("defaults enabled policy evaluation")
	}
	original := &PolicyOptions{MaxFactRows: 25}
	got := (Options{Policies: original}).withDefaults().Policies
	d := DefaultPolicyOptions()
	if got.MaxFactQueries != d.MaxFactQueries || got.FactLoadConcurrency != d.FactLoadConcurrency || got.MaxFactRows != 25 {
		t.Fatalf("incorrect defaults: %+v", got)
	}
	if original.MaxFactQueries != 0 || original.FactLoadConcurrency != 0 {
		t.Fatal("defaults mutated caller's configuration")
	}
	if (PolicyOptions{MaxFactQueries: -1}).withDefaults().MaxFactQueries != -1 {
		t.Fatal("defaults hid an invalid explicit policy limit")
	}
}
