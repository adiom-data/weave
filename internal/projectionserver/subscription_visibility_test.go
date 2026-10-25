package projectionserver

import (
	"context"
	"reflect"
	"testing"

	"github.com/adiom-data/weave/internal/accesscontrol"
	"github.com/adiom-data/weave/internal/querysubscription/model"
	"github.com/adiom-data/weave/internal/storage"
)

func TestDirectVisibilityEvaluatesPendingReferences(t *testing.T) {
	for _, allowed := range []bool{false, true} {
		t.Run(map[bool]string{false: "deny", true: "allow"}[allowed], func(t *testing.T) {
			key := storage.EntityKey{Value: "pending"}
			gate := newSubscriptionVisibility()
			calls := 0
			policy := visibilityTestPrepared{decide: func(candidate accesscontrol.Candidate) bool {
				calls++
				if candidate.Key != key || candidate.Complete || candidate.Record != nil {
					t.Fatalf("pending candidate = %+v", candidate)
				}
				return allowed
			}}
			part, err := gate.filterTransaction(context.Background(), policy, visibilityTestView{}, subscriptionBatchMessage{
				delivery: testDelivery(1),
				batch: querysubscription.Batch{
					Changes:    []querysubscription.Change{{Kind: querysubscription.Added, Row: querysubscription.Row{Key: key, Pending: true}}},
					Membership: querysubscription.MembershipDelta{Insertions: []querysubscription.Insertion{{Key: key, FinalIndex: 0}}},
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			if calls != 1 || (len(part.batch.Changes) == 1) != allowed {
				t.Fatalf("calls=%d publication=%+v", calls, part)
			}
			if allowed && (!part.batch.Changes[0].Pending || part.batch.Changes[0].Record != nil) {
				t.Fatal("pending reference was hydrated")
			}
		})
	}
}

func TestDirectVisibilityInvalidationPreservesPendingImageAndVersion(t *testing.T) {
	ctx := context.Background()
	key := storage.EntityKey{Value: "pending"}
	gate := newSubscriptionVisibility()
	allow := visibilityTestPrepared{decide: func(accesscontrol.Candidate) bool { return true }}
	deny := visibilityTestPrepared{decide: func(accesscontrol.Candidate) bool { return false }}
	if _, err := gate.filterSnapshot(ctx, deny, visibilityTestView{}, querysubscription.QuerySnapshot{
		Revision: 1, Rows: []querysubscription.QueryRow{{Row: querysubscription.Row{Key: key, Pending: true, ChangeIndex: 7}}},
	}); err != nil {
		t.Fatal(err)
	}
	frame := subscriptionBatchMessage{delivery: testDelivery(3), invalidation: accesscontrol.Invalidation{
		Candidates: []accesscontrol.Candidate{{Key: key, Record: modelRecord("pending", "1", "policy facts"), Complete: true}},
	}}
	part, err := gate.filterTransaction(ctx, allow, visibilityTestView{}, frame)
	if err != nil {
		t.Fatal(err)
	}
	if len(part.batch.Changes) != 1 || part.batch.Changes[0].Kind != querysubscription.Added || !part.batch.Changes[0].Pending || part.batch.Changes[0].Record != nil {
		t.Fatalf("grant replaced raw image: %+v", part)
	}
	if version := part.versions[key]; version.Revision != 1 || version.ChangeIndex != 7 {
		t.Fatalf("grant changed raw freshness: %+v", version)
	}
	part, err = gate.filterTransaction(ctx, deny, visibilityTestView{}, frame)
	if err != nil {
		t.Fatal(err)
	}
	if len(part.batch.Changes) != 1 || part.batch.Changes[0].Kind != querysubscription.Removed || len(part.batch.Membership.Removed) != 1 {
		t.Fatalf("revocation = %+v", part)
	}
}

func TestDirectVisibilityCombinesRawAndPolicyEffects(t *testing.T) {
	ctx := context.Background()
	key := storage.EntityKey{Value: "task"}
	gate := newSubscriptionVisibility()
	policy := visibilityTestPrepared{decide: func(candidate accesscontrol.Candidate) bool {
		return string(candidate.Record["visible"].Data) == "true"
	}}
	if _, err := gate.filterSnapshot(ctx, policy, visibilityTestView{}, querysubscription.QuerySnapshot{
		Revision: 1, Rows: []querysubscription.QueryRow{{Row: querysubscription.Row{Key: key, Record: visibilityRecord(true)}}},
	}); err != nil {
		t.Fatal(err)
	}
	raw := visibilityRecord(false)
	part, err := gate.filterTransaction(ctx, policy, visibilityTestView{}, subscriptionBatchMessage{
		delivery:     testDelivery(2),
		batch:        querysubscription.Batch{Changes: []querysubscription.Change{{Kind: querysubscription.Updated, Row: querysubscription.Row{Key: key, Record: raw}}}},
		invalidation: accesscontrol.Invalidation{Candidates: []accesscontrol.Candidate{{Key: key, Record: visibilityRecord(true), Complete: true}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(part.batch.Membership.Removed) != 0 || len(part.batch.Membership.Insertions) != 0 || len(part.batch.Changes) != 1 || part.batch.Changes[0].Kind != querysubscription.Updated {
		t.Fatalf("same-transaction authorization flicker: %+v", part)
	}
	if !reflect.DeepEqual(part.batch.Changes[0].Record, raw) {
		t.Fatal("policy facts replaced raw update")
	}
}

func TestDirectVisibilityRegainCannotOverwriteNewerSharedEntity(t *testing.T) {
	ctx := context.Background()
	key := modelKey(snapshotTestRelation(), "task")
	old := modelRecord("task", "1", "old")
	newer := modelRecord("task", "1", "newer")
	gate := newSubscriptionVisibility()
	deny := visibilityTestPrepared{decide: func(accesscontrol.Candidate) bool { return false }}
	allow := visibilityTestPrepared{decide: func(accesscontrol.Candidate) bool { return true }}
	hidden, err := gate.filterSnapshot(ctx, deny, visibilityTestView{}, querysubscription.QuerySnapshot{
		Revision: 1, Bounds: snapshotTestBounds(), Rows: []querysubscription.QueryRow{{Row: querysubscription.Row{Key: key, Record: old}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	model := newConnectionModel()
	if _, err := model.openSubscription("hidden", "hidden-query", "", hidden, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := model.openSubscription("newer", "newer-query", "", querysubscription.QuerySnapshot{
		Revision: 2, Bounds: snapshotTestBounds(), Rows: []querysubscription.QueryRow{{Row: querysubscription.Row{Key: key, Record: newer}}},
	}, nil); err != nil {
		t.Fatal(err)
	}
	part, err := gate.filterTransaction(ctx, allow, visibilityTestView{}, subscriptionBatchMessage{
		delivery:     testDelivery(3),
		invalidation: accesscontrol.Invalidation{Candidates: []accesscontrol.Candidate{{Key: key, Record: old, Complete: true}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := model.applySubscriptionBatch(part.versions, "hidden-query", part.delivery, part.batch)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Entities) != 0 || len(result.Queries) != 1 {
		t.Fatalf("regain = %+v", result)
	}
	entity := model.entities[publicEntityID(key)]
	if !reflect.DeepEqual(entity.record, newer) || entity.version.Revision != 2 {
		t.Fatal("regain overwrote newer shared entity")
	}
}
