package projection

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	q "github.com/adiom-data/weave/internal/querysubscription/model"
	"github.com/adiom-data/weave/internal/storage"
)

func TestInitialImageTransfersOwnershipOnce(t *testing.T) {
	key := storage.EntityKey{Value: "one"}
	record := storage.Record{"object": {Type: storage.TypeObject, State: storage.ValuePresent, Object: storage.Record{"name": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("original")}}}}
	p := &Projection{ready: make(chan struct{}), instances: map[string]*projectionInstance{"items": {
		id: "items", node: &ProjectionNode{Name: "items"}, order: []storage.EntityKey{key},
		members: map[storage.EntityKey]*projectionMember{key: {record: record, cursor: []byte("cursor")}},
	}}}
	image, err := p.currentEvents("items")
	if err != nil {
		t.Fatal(err)
	}
	positions := map[storage.SourceID]*q.Position{"test": {SourceID: "test", Value: 10}}
	p.finishReady(image, positions, nil)
	if err := p.WaitReady(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(p.readyEvents) != 2 || &p.readyEvents[0] != &image[0] {
		t.Fatal("readiness copied or consumed the owned capture")
	}
	taken, cut, err := p.TakeInitial(context.Background())
	if err != nil || len(taken) != 2 || &taken[0] != &image[0] || p.readyEvents != nil || p.readyPositions != nil || cut["test"] != positions["test"] {
		t.Fatalf("initial image was not transferred: %v", err)
	}
	taken[1].Record["object"].Object["name"].Data[0] = 'X'
	taken[1].Cursor[0] = 'X'
	if string(record["object"].Object["name"].Data) != "original" || string(p.instances["items"].members[key].cursor) != "cursor" {
		t.Fatal("consumer mutation escaped into live projection state")
	}
	if _, _, err := p.TakeInitial(context.Background()); err == nil {
		t.Fatal("initial image transferred twice")
	}
	if err := p.WaitReady(context.Background()); err != nil {
		t.Fatal("readiness signal stopped being reusable")
	}
}

func TestCancelledInitialWaitDoesNotConsumeImage(t *testing.T) {
	p := &Projection{ready: make(chan struct{})}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, err := p.TakeInitial(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled wait: %v", err)
	}
	p.finishReady([]ProjectionEvent{{InstanceID: "items"}}, nil, nil)
	if _, _, err := p.TakeInitial(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled ready wait: %v", err)
	}
	if image, _, err := p.TakeInitial(context.Background()); err != nil || len(image) != 1 {
		t.Fatalf("cancelled wait consumed the image: %v", err)
	}
}

func TestConcurrentInitialConsumersHaveOneOwner(t *testing.T) {
	p := &Projection{ready: make(chan struct{})}
	p.finishReady([]ProjectionEvent{{InstanceID: "items"}}, nil, nil)
	var workers sync.WaitGroup
	results := make(chan error, 2)
	for i := 0; i < 2; i++ {
		workers.Add(1)
		go func() { defer workers.Done(); _, _, err := p.TakeInitial(context.Background()); results <- err }()
	}
	workers.Wait()
	close(results)
	owners := 0
	for err := range results {
		if err == nil {
			owners++
		}
	}
	if owners != 1 || p.readyEvents != nil {
		t.Fatalf("owners=%d, retained=%d", owners, len(p.readyEvents))
	}
}

func TestClosingProjectionReleasesUntakenImage(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	p, err := BeginProjection(ctx, &admissionTreeFeed{}, &ProjectionNode{Name: "items", BuildQuery: func(storage.Record) (storage.QuerySpec, error) { return storage.QuerySpec{}, nil }}, StartOptions{QueueLimit: 4})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if err := p.WaitReady(ctx); err != nil {
		t.Fatal(err)
	}
	if len(p.readyEvents) == 0 {
		t.Fatal("missing initial image")
	}
	p.Close()
	if p.readyEvents != nil || p.readyPositions != nil {
		t.Fatal("closed projection retained its initial image")
	}
	if _, _, err := p.TakeInitial(ctx); !errors.Is(err, q.ErrClosed) {
		t.Fatalf("closed projection handed off an image: %v", err)
	}
}
