package queryserver

import (
	"context"
	"errors"
	"testing"
	"time"

	q "github.com/adiom-data/weave/internal/querysubscription/model"
)

type fenceTestClient struct{ q.Feed }

func (c fenceTestClient) OpenFeed(context.Context, q.FeedOptions) (q.Feed, error) {
	return c.Feed, nil
}

type fenceTestFeed struct{ q.Feed }

func (f fenceTestFeed) WaitForFence(ctx context.Context, token string) (*q.Position, error) {
	if token == "covered" {
		return &q.Position{SourceID: "test", Value: 20}, nil
	}
	<-ctx.Done()
	return nil, ctx.Err()
}

func (f fenceTestFeed) Next(context.Context) (q.Event, error) {
	return q.Event{Progress: &q.Progress{Position: &q.Position{SourceID: "test", Value: 20}, Revision: 1}}, nil
}

func TestFenceWaitBudgetIsLocalAndHonorsEarlierCallerDeadline(t *testing.T) {
	for _, test := range []struct {
		name           string
		server, caller time.Duration
	}{
		{"server timeout", 20 * time.Millisecond, time.Second},
		{"caller timeout", time.Second, 20 * time.Millisecond},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
			defer cancel()
			backend := &backend{queries: fenceTestClient{fenceTestFeed{}}, done: make(chan struct{}), fenceWaitTimeout: test.server}
			feed, err := backend.OpenFeed(ctx, q.FeedOptions{BufferLimit: 16})
			if err != nil {
				t.Fatal(err)
			}
			call, stop := context.WithTimeout(ctx, test.caller)
			defer stop()
			if _, err := feed.WaitForFence(call, "blocked"); !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("fence wait did not time out: %v", err)
			}
			if test.server < test.caller && call.Err() != nil {
				t.Fatal("server timeout canceled the caller or waited for its later deadline")
			}
			if _, err := feed.WaitForFence(ctx, "covered"); err != nil {
				t.Fatalf("timed-out wait poisoned the feed: %v", err)
			}
			if event, err := feed.Next(ctx); err != nil || event.Progress == nil {
				t.Fatalf("timed-out wait stopped feed delivery: %+v %v", event, err)
			}
		})
	}
}
