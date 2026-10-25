package projection

import (
	"context"
	"fmt"
	"math/rand"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	q "github.com/adiom-data/weave/internal/querysubscription/model"
	"github.com/adiom-data/weave/internal/storage"
)

type initialDeltaFeed struct {
	boundarySession
	rows []q.Row
}

func (f *initialDeltaFeed) SubscribeQuery(_ context.Context, r q.QuerySubscribeRequest, install q.QuerySnapshotInstaller) error {
	rows := make([]q.QueryRow, len(f.rows))
	for i, row := range f.rows {
		rows[i] = q.QueryRow{Row: row}
	}
	return install(&boundarySubscription{id: r.ID}, q.QuerySnapshot{Rows: rows, Checkpoint: &q.Position{SourceID: "test", Value: 1}})
}
func deltaRow(id, value string) q.Row {
	return q.Row{Key: storage.EntityKey{Relation: storage.RelationKey{Source: "test", Name: "items"}, Value: id}, Record: storage.Record{"value": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte(value)}}}
}

// A small consumer of the admission contract, deliberately independent of its
// producer and of the authorization gate.
type deltaImage struct {
	rows  map[string]map[storage.EntityKey]ProjectionEvent
	order map[string][]storage.EntityKey
}

func (i *deltaImage) apply(t testing.TB, batch ProjectionBatch) {
	t.Helper()
	if i.rows == nil {
		i.rows = make(map[string]map[storage.EntityKey]ProjectionEvent)
		i.order = make(map[string][]storage.EntityKey)
	}
	opened := make(map[string]bool)
	for _, event := range batch.Events {
		switch event.Type {
		case ProjectionInstanceOpened:
			i.rows[event.InstanceID] = make(map[storage.EntityKey]ProjectionEvent)
			i.order[event.InstanceID] = nil
			opened[event.InstanceID] = true
		case ProjectionInstanceClosed:
			delete(i.rows, event.InstanceID)
			delete(i.order, event.InstanceID)
		case ProjectionRowChanged:
			rows := i.rows[event.InstanceID]
			if rows == nil {
				t.Fatalf("row before its instance: %+v", event)
			}
			_, exists := rows[event.Key]
			if event.Kind == q.Removed {
				delete(rows, event.Key)
			} else {
				rows[event.Key] = event
			}
			if _, controlled := batch.Membership[event.InstanceID]; controlled {
				continue
			}
			if event.Kind == q.Removed {
				for n, key := range i.order[event.InstanceID] {
					if key == event.Key {
						i.order[event.InstanceID] = append(i.order[event.InstanceID][:n], i.order[event.InstanceID][n+1:]...)
						break
					}
				}
			} else if !exists || opened[event.InstanceID] {
				i.order[event.InstanceID] = append(i.order[event.InstanceID], event.Key)
			}
		}
	}
	for id, delta := range batch.Membership {
		order, err := q.ApplyMembership(i.order[id], delta)
		if err != nil {
			t.Fatal(err)
		}
		i.order[id] = order
	}
}

func TestInitialDeltasCoalesceMembershipAndKeepRecordVersions(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	feed := &initialDeltaFeed{}
	for n := 0; n < 100; n++ {
		feed.rows = append(feed.rows, deltaRow(fmt.Sprint(n), "initial"))
	}
	p, err := BeginProjection(ctx, feed, &ProjectionNode{Name: "items", BuildQuery: func(storage.Record) (storage.QuerySpec, error) { return storage.QuerySpec{}, nil }}, StartOptions{QueueLimit: 1, CurrentImageAdmission: true})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if err := p.WaitReady(ctx); err != nil {
		t.Fatal(err)
	}
	if _, _, err := p.ApplyEvent(q.Event{Progress: &q.Progress{Position: &q.Position{SourceID: "test", Value: 1}, Revision: 1}}); err != nil {
		t.Fatal(err)
	}
	image := &deltaImage{}
	drain := func() {
		t.Helper()
		if _, err := p.TryAdmitInitial(func(batch ProjectionBatch) (bool, error) { image.apply(t, batch); return false, nil }); err != nil {
			t.Fatal(err)
		}
	}
	drain()
	rng := rand.New(rand.NewSource(71))
	for step := 2; step < 400; step++ {
		p.mu.Lock()
		order := append([]storage.EntityKey(nil), p.instances[p.rootID].order...)
		p.mu.Unlock()
		batch := q.Batch{}
		switch operation := rng.Intn(5); {
		case len(order) == 0 || operation == 0:
			row := deltaRow(fmt.Sprintf("new-%d", step), fmt.Sprint(step))
			index := rng.Intn(len(order) + 1)
			batch.Changes = []q.Change{{Kind: q.Added, Row: row}}
			batch.Membership.Insertions = []q.Insertion{{Key: row.Key, FinalIndex: index}}
		case operation == 1:
			key := order[rng.Intn(len(order))]
			batch.Changes = []q.Change{{Kind: q.Removed, Row: q.Row{Key: key}}}
			batch.Membership.Removed = []storage.EntityKey{key}
		case operation == 2:
			key := order[rng.Intn(len(order))]
			row := deltaRow(key.Value, fmt.Sprint(step))
			batch.Changes = []q.Change{{Kind: q.Updated, Row: row}}
		case operation == 3:
			key := order[rng.Intn(len(order))]
			row := deltaRow(key.Value, fmt.Sprint(step))
			batch.Changes = []q.Change{{Kind: q.Updated, Row: row}}
			batch.Membership = q.MembershipDelta{Removed: []storage.EntityKey{key}, Insertions: []q.Insertion{{Key: key, FinalIndex: rng.Intn(len(order))}}}
		default:
			row := deltaRow(fmt.Sprintf("clear-%d", step), fmt.Sprint(step))
			batch.Clear = true
			batch.Changes = []q.Change{{Kind: q.Added, Row: row}}
			batch.Membership.Insertions = []q.Insertion{{Key: row.Key, FinalIndex: 0}}
		}
		if _, _, err := p.ApplyDelivery(q.DeliveryEnvelope{Delivery: q.DeliveryMetadata{Kind: q.DeliveryCommit, Checkpoint: &q.Position{SourceID: "test", Value: uint64(step)}, Revision: uint64(step)}, Effects: []q.SubscriptionEffect{{SubscriptionID: p.rootID, Batch: batch}}}); err != nil {
			t.Fatal(err)
		}
		if step%7 != 0 {
			continue
		}
		drain()
		p.mu.Lock()
		instance := p.instances[p.rootID]
		order = append([]storage.EntityKey(nil), instance.order...)
		members := make(map[storage.EntityKey]projectionMember, len(instance.members))
		for key, record := range instance.members {
			members[key] = *record
		}
		p.mu.Unlock()
		if !slices.Equal(image.order[p.rootID], order) {
			t.Fatalf("step %d membership differs: %v != %v", step, image.order[p.rootID], order)
		}
		if len(image.rows[p.rootID]) != len(members) {
			t.Fatalf("step %d row count differs", step)
		}
		for key, record := range members {
			event := image.rows[p.rootID][key]
			if !reflect.DeepEqual(event.Record, record.record) || event.Version != record.version {
				t.Fatalf("step %d row/version differs for %v", step, key)
			}
		}
	}
}

func BenchmarkInitialAdmissionHotRow(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	feed := &initialDeltaFeed{}
	for n := 0; n < 1000; n++ {
		feed.rows = append(feed.rows, deltaRow(fmt.Sprint(n), strings.Repeat("i", 1024)))
	}
	p, err := BeginProjection(ctx, feed, &ProjectionNode{Name: "items", BuildQuery: func(storage.Record) (storage.QuerySpec, error) { return storage.QuerySpec{}, nil }}, StartOptions{QueueLimit: 1, CurrentImageAdmission: true})
	if err != nil {
		b.Fatal(err)
	}
	defer p.Close()
	if err := p.WaitReady(ctx); err != nil {
		b.Fatal(err)
	}
	if _, _, err := p.ApplyEvent(q.Event{Progress: &q.Progress{Position: &q.Position{SourceID: "test", Value: 1}, Revision: 1}}); err != nil {
		b.Fatal(err)
	}
	if _, err := p.TryAdmitInitial(func(ProjectionBatch) (bool, error) { return false, nil }); err != nil {
		b.Fatal(err)
	}
	row := deltaRow("0", strings.Repeat("u", 1024))
	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		point := uint64(n + 2)
		if _, _, err := p.ApplyDelivery(q.DeliveryEnvelope{Delivery: q.DeliveryMetadata{Kind: q.DeliveryCommit, Checkpoint: &q.Position{SourceID: "test", Value: point}, Revision: point}, Effects: []q.SubscriptionEffect{{SubscriptionID: p.rootID, Batch: q.Batch{Changes: []q.Change{{Kind: q.Updated, Row: row}}}}}}); err != nil {
			b.Fatal(err)
		}
		if _, err := p.TryAdmitInitial(func(batch ProjectionBatch) (bool, error) {
			if len(batch.Events) != 1 {
				b.Fatalf("copied %d rows", len(batch.Events))
			}
			return false, nil
		}); err != nil {
			b.Fatal(err)
		}
	}
}
