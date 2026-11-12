package dependency

import (
	"context"
	"errors"
	"testing"
	"time"

	q "github.com/adiom-data/weave/internal/querysubscription/model"
)

func TestReadFenceWaitCoversSharedMaterializationNotJustUpstreamAck(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	source := &testClient{}
	cache := newCache(t, source)
	feed := open(t, cache, 16)
	ack := make(chan struct{})
	upstream := source.latest()
	upstream.waitFence = func(context.Context, string) (*q.Position, error) { close(ack); return position(2), nil }
	done := make(chan error, 1)
	go func() { _, err := feed.WaitForFence(ctx, "opaque"); done <- err }()
	select {
	case <-ack:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	select {
	case err := <-done:
		t.Fatalf("upstream acknowledgment bypassed local materialization: %v", err)
	default:
	}
	upstream.emit(nil, true)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	event, err := feed.Next(ctx)
	if err != nil || event.Progress == nil || event.Progress.Position.Value != 2 {
		t.Fatalf("proof missing from ordered feed: %+v %v", event, err)
	}
}

func TestClosingConsumerCancelsOnlyItsFenceWait(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	source := &testClient{}
	cache := newCache(t, source)
	feed := open(t, cache, 16)
	healthy := open(t, cache, 16)
	started := make(chan struct{})
	source.latest().waitFence = func(ctx context.Context, _ string) (*q.Position, error) {
		close(started)
		<-ctx.Done()
		return nil, ctx.Err()
	}
	done := make(chan error, 1)
	go func() { _, err := feed.WaitForFence(ctx, "opaque"); done <- err }()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if err := feed.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	source.latest().emit(nil, true)
	if event, err := healthy.Next(ctx); err != nil || event.Progress == nil {
		t.Fatalf("another consumer lost its feed: %+v %v", event, err)
	}
}
