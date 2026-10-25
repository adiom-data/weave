package projectionserver

import (
	"context"
	"fmt"
	"math/rand"
	"reflect"
	"slices"
	"testing"

	"github.com/adiom-data/weave/internal/accesscontrol"
	"github.com/adiom-data/weave/internal/accesscontrol/policy"
	"github.com/adiom-data/weave/internal/projection/authorization"
	q "github.com/adiom-data/weave/internal/querysubscription/model"
	"github.com/adiom-data/weave/internal/storage"
)

func TestInitialQueryImageDoesNotUseLocalSnapshotAsSourceProgress(t *testing.T) {
	snapshot := policySnapshot(20, policyRow("projects", "p1", "project"))
	snapshot.Checkpoint = &q.Position{SourceID: "test", Value: 10}
	image, err := newInitialQueryImage("query", snapshot)
	if err != nil {
		t.Fatal(err)
	}
	image.observe(&q.Position{SourceID: "test", Value: 10}, 1)
	for index, kind := range []q.DeliveryKind{q.DeliveryWindow, q.DeliveryHydration} {
		if err := image.apply(q.DeliveryEnvelope{Delivery: q.DeliveryMetadata{Kind: kind, Checkpoint: &q.Position{SourceID: "test", Value: 30}, Revision: uint64(index + 2)}}); err != nil {
			t.Fatal(err)
		}
		if covered, err := image.covered(); err != nil || covered {
			t.Fatalf("local delivery bypassed source catchup: covered=%v err=%v", covered, err)
		}
	}
	image.observe(&q.Position{SourceID: "test", Value: 20}, 4)
	if covered, err := image.covered(); err != nil || !covered {
		t.Fatalf("covered fence did not become current: %v %v", covered, err)
	}
}

func TestInitialQueryCoalescesChangesAndPreservesVersions(t *testing.T) {
	ctx := context.Background()
	backend := newPolicyBackend(func(context.Context, q.QuerySubscribeRequest) q.QuerySnapshot {
		t.Error("unexpected fact load")
		return q.QuerySnapshot{}
	})
	feed, err := backend.OpenFeed(ctx, q.FeedOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer feed.Close(ctx)
	evaluator, err := policy.Compile(policy.Map{{Namespace: "app", Name: "projects"}: {Allow: []policy.Rule{{ID: "public", When: "true"}}}}, policyOptions().Policies.Limits)
	if err != nil {
		t.Fatal(err)
	}
	access, err := newPolicyAccess(ctx, feed, evaluator, accesscontrol.Claims{}, authorization.Options{MaxQueries: 1, MaxRows: 1, LoadConcurrency: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer access.Close()
	prepared, err := access.Prepare(ctx, accesscontrol.PrepareRequest{ID: "test"})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := policySnapshot(1)
	snapshot.Revision = 1
	for n := 0; n < 20; n++ {
		snapshot.Rows = append(snapshot.Rows, q.QueryRow{Row: policyRow("projects", fmt.Sprint(n), "initial")})
	}
	image, err := newInitialQueryImage("query", snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := access.session.Observe(q.Event{Progress: &q.Progress{Position: snapshot.Checkpoint, Revision: 1}}); err != nil {
		t.Fatal(err)
	}
	image.observe(snapshot.Checkpoint, 1)
	drain := func() {
		t.Helper()
		if resolved, err := image.authorize(ctx, access.session, prepared); err != nil || !resolved {
			t.Fatalf("authorization: %v %v", resolved, err)
		}
		if !slices.Equal(image.order, image.visibility.order.VisibleKeys()) {
			t.Fatal("coalesced membership differs")
		}
		if len(image.rows) != len(image.visibility.rows) {
			t.Fatal("coalesced row count differs")
		}
		for key, row := range image.rows {
			got := image.visibility.rows[key]
			if !reflect.DeepEqual(row, got.Row) || got.version != image.versions[key] {
				t.Fatalf("coalesced row/version differs for %v", key)
			}
		}
	}
	drain()
	rng := rand.New(rand.NewSource(71))
	for step := uint64(2); step < 400; step++ {
		batch := q.Batch{}
		switch operation := rng.Intn(5); {
		case len(image.order) == 0 || operation == 0:
			row := policyRow("projects", fmt.Sprint(step), "new")
			batch.Changes = []q.Change{{Kind: q.Added, Row: row}}
			batch.Membership.Insertions = []q.Insertion{{Key: row.Key, FinalIndex: rng.Intn(len(image.order) + 1)}}
		case operation == 1:
			key := image.order[rng.Intn(len(image.order))]
			batch.Changes = []q.Change{{Kind: q.Removed, Row: q.Row{Key: key}}}
			batch.Membership.Removed = []storage.EntityKey{key}
		case operation == 2 || operation == 3:
			key := image.order[rng.Intn(len(image.order))]
			row := image.rows[key]
			row.Record = modelRecord(key.Value, "1", fmt.Sprint(step))
			batch.Changes = []q.Change{{Kind: q.Updated, Row: row}}
			if operation == 3 {
				batch.Membership = q.MembershipDelta{Removed: []storage.EntityKey{key}, Insertions: []q.Insertion{{Key: key, FinalIndex: rng.Intn(len(image.order))}}}
			}
		default:
			row := policyRow("projects", fmt.Sprint(step), "clear")
			batch.Clear = true
			batch.Changes = []q.Change{{Kind: q.Added, Row: row}}
			batch.Membership.Insertions = []q.Insertion{{Key: row.Key, FinalIndex: 0}}
		}
		metadata := q.DeliveryMetadata{Kind: q.DeliveryCommit, Checkpoint: &q.Position{SourceID: "test", Value: step}, Revision: step, PreviousRevision: step - 1}
		envelope := q.DeliveryEnvelope{Delivery: metadata, Effects: []q.SubscriptionEffect{{SubscriptionID: "query", Batch: batch}}}
		if _, err := access.session.Observe(q.Event{Delivery: &envelope}); err != nil {
			t.Fatal(err)
		}
		if err := image.apply(envelope); err != nil {
			t.Fatal(err)
		}
		if step%7 == 0 {
			drain()
		}
	}
	drain()
	// Opening this delayed image must not overwrite a newer entity already
	// published by another subscription just because its coverage is newer.
	row := policyRow("projects", "shared", "old")
	model := newConnectionModel()
	newer := row
	newer.Record = modelRecord("shared", "1", "new")
	if _, err := model.openSubscription("other", "other", "", q.QuerySnapshot{Revision: 50, Bounds: modelQueryBounds(), Rows: []q.QueryRow{{Row: newer}}}, nil); err != nil {
		t.Fatal(err)
	}
	result, err := model.openSubscriptionVersions("delayed", "delayed", "", q.QuerySnapshot{Revision: 100, Bounds: modelQueryBounds(), Rows: []q.QueryRow{{Row: row}}}, nil, map[storage.EntityKey]q.RecordVersion{row.Key: {Revision: 10}})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Entities) != 0 {
		t.Fatal("coverage freshness replaced newer entity data")
	}
}

func TestPublicationProgressIgnoresLocalDeliveries(t *testing.T) {
	p := &policyPublications{}
	p.observe(q.Event{Progress: &q.Progress{Position: &q.Position{SourceID: "test", Value: 10}, Revision: 1}})
	for index, kind := range []q.DeliveryKind{q.DeliveryWindow, q.DeliveryHydration} {
		revision := uint64(index + 2)
		p.observe(q.Event{Delivery: &q.DeliveryEnvelope{Delivery: q.DeliveryMetadata{Kind: kind, Checkpoint: &q.Position{SourceID: "test", Value: 30}, Revision: revision}}})
		if p.position.Value != 10 || p.revision != revision {
			t.Fatal("local delivery mixed source and consumer clocks")
		}
	}
}

func TestInitialQueryUnchangedMembershipValidatesTouchedKeys(t *testing.T) {
	row := policyRow("projects", "one", "old")
	other := policyRow("projects", "two", "new")
	for _, test := range []struct {
		name    string
		changes []q.Change
		invalid bool
	}{
		{"update", []q.Change{{Kind: q.Updated, Row: row}}, false},
		{"remove-readd", []q.Change{{Kind: q.Removed, Row: q.Row{Key: row.Key}}, {Kind: q.Added, Row: row}}, false},
		{"transient-addition", []q.Change{{Kind: q.Added, Row: other}, {Kind: q.Removed, Row: q.Row{Key: other.Key}}}, false},
		{"missing-removal", []q.Change{{Kind: q.Removed, Row: q.Row{Key: other.Key}}}, false},
		{"missing-update", []q.Change{{Kind: q.Updated, Row: other}}, true},
		{"addition-without-membership", []q.Change{{Kind: q.Added, Row: other}}, true},
		{"removal-without-membership", []q.Change{{Kind: q.Removed, Row: q.Row{Key: row.Key}}}, true},
		{"same-count-replacement", []q.Change{{Kind: q.Removed, Row: q.Row{Key: row.Key}}, {Kind: q.Added, Row: other}}, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			image, err := newInitialQueryImage("query", policySnapshot(1, row))
			if err != nil {
				t.Fatal(err)
			}
			order := image.order
			envelope := q.DeliveryEnvelope{Delivery: q.DeliveryMetadata{Kind: q.DeliveryCommit, Revision: 2, Checkpoint: &q.Position{SourceID: "test", Value: 2}}, Effects: []q.SubscriptionEffect{{SubscriptionID: "query", Batch: q.Batch{Changes: test.changes}}}}
			err = image.apply(envelope)
			if (err != nil) != test.invalid {
				t.Fatalf("expected invalid=%v, got %v", test.invalid, err)
			}
			if test.invalid {
				if _, err := image.covered(); err == nil {
					t.Fatal("invalid image remained usable")
				}
				return
			}
			if &image.order[0] != &order[0] || !slices.Equal(image.order, order) {
				t.Fatal("unchanged membership was rebuilt")
			}
		})
	}
}

func BenchmarkInitialQueryFieldUpdate(b *testing.B) {
	for _, count := range []int{10, 1000, 10000} {
		b.Run(fmt.Sprint(count), func(b *testing.B) {
			snapshot := policySnapshot(1)
			for n := 0; n < count; n++ {
				snapshot.Rows = append(snapshot.Rows, q.QueryRow{Row: policyRow("projects", fmt.Sprint(n), "initial")})
			}
			image, err := newInitialQueryImage("query", snapshot)
			if err != nil {
				b.Fatal(err)
			}
			// Exercise the incremental journal as well as the raw materializer.
			image.started = true
			row := policyRow("projects", "0", "updated")
			envelope := q.DeliveryEnvelope{Delivery: q.DeliveryMetadata{Kind: q.DeliveryCommit, Checkpoint: &q.Position{SourceID: "test", Value: 2}}, Effects: []q.SubscriptionEffect{{SubscriptionID: "query", Batch: q.Batch{Changes: []q.Change{{Kind: q.Updated, Row: row}}}}}}
			b.ReportAllocs()
			b.ResetTimer()
			for n := 0; n < b.N; n++ {
				envelope.Delivery.Revision = uint64(n + 2)
				if err := image.apply(envelope); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
