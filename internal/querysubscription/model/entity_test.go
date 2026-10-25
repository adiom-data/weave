package querysubscription

import (
	"slices"
	"testing"

	"github.com/adiom-data/weave/internal/storage"
)

func TestEntityPayloadRoutesShareOneDelivery(t *testing.T) {
	key := testKey("link")
	delta := EntityDelta{Clear: true, ClearChangeIndex: 7}
	envelope := DeliveryEnvelope{
		Delivery: DeliveryMetadata{Kind: DeliveryCommit, Revision: 1, Checkpoint: &Position{SourceID: "test", Value: 1}, Atomic: true},
		Effects:  []SubscriptionEffect{{SubscriptionID: "parent"}},
		Entities: []EntityEffect{
			{Target: EntityTarget{SubscriptionID: "entity"}, Delta: delta},
			{Target: EntityTarget{SubscriptionID: "parent", Reference: &ReferenceRoute{Selector: "bound", Parents: []storage.EntityKey{key}}}, Delta: delta},
		},
	}
	if err := envelope.Validate(); err != nil {
		t.Fatal(err)
	}
	effects := slices.Collect(envelope.SubscriptionEffects())
	if len(effects) != 2 || effects[0].SubscriptionID != "parent" || effects[1].SubscriptionID != "entity" || effects[1].Batch.ClearChangeIndex != 7 {
		t.Fatalf("subscription materialization mixed in private ownership routing: %+v", effects)
	}
	envelope.Entities[0].Target.SubscriptionID = "parent"
	if err := envelope.Validate(); err == nil {
		t.Fatal("accepted query and explicit entity effects for the same subscription")
	}
}

func TestEntityTargetsAreValidatedForImagesAndChanges(t *testing.T) {
	for _, target := range []EntityTarget{
		{},
		{SubscriptionID: "parent", Reference: &ReferenceRoute{Selector: "bound"}},
		{SubscriptionID: "parent", Reference: &ReferenceRoute{Parents: []storage.EntityKey{testKey("link")}}},
	} {
		if err := (Event{Entity: &EntityInstallation{Target: target}}).Validate(); err == nil {
			t.Fatalf("accepted invalid installation: %+v", target)
		}
		if err := (DeliveryEnvelope{Delivery: DeliveryMetadata{Kind: DeliveryHydration, Revision: 1}, Entities: []EntityEffect{{Target: target}}}).Validate(); err == nil {
			t.Fatalf("accepted invalid effect: %+v", target)
		}
	}
}
