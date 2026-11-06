package projectionserver

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/adiom-data/weave/internal/accesscontrol/policy"
	"github.com/adiom-data/weave/internal/projection/authorization"
	q "github.com/adiom-data/weave/internal/querysubscription/model"
)

type observedProgressFeed struct {
	q.Feed
	requests chan struct{}
}

func (f *observedProgressFeed) RequestProgress(ctx context.Context) error {
	select {
	case f.requests <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func TestPolicyProgressWaitsForConsumptionAndRetainsFollowup(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	evaluator, err := policy.Compile(policy.Map{}, policy.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	feed := &observedProgressFeed{requests: make(chan struct{}, 1024)}
	access, err := newPolicyAccess(ctx, feed, evaluator, nil, authorization.Options{MaxQueries: 10, MaxRows: 10, LoadConcurrency: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer access.Close()
	done := make(chan error, 1)
	go func() { done <- access.watch(ctx) }()
	defer func() {
		cancel()
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Errorf("watch stopped: %v", err)
		}
	}()
	receiveRequest := func() {
		t.Helper()
		select {
		case <-feed.requests:
		case <-ctx.Done():
			t.Fatal("progress request not issued")
		}
	}
	assertQuiet := func() {
		t.Helper()
		select {
		case <-feed.requests:
			t.Fatal("requested another report before consuming the previous one")
		case <-time.After(25 * time.Millisecond):
		}
	}
	access.requestProgress()
	receiveRequest()
	// RequestProgress already returned, but its report has not been consumed.
	// New evidence/request wakeups must coalesce rather than fill the feed.
	for range 1000 {
		access.requestProgress()
	}
	assertQuiet()
	position := &q.Position{SourceID: "test", Value: 10}
	if _, err := access.observe(q.Event{Progress: &q.Progress{Position: position, Revision: 1}}); err != nil {
		t.Fatal(err)
	}
	receiveRequest() // Retain requests made after the first report was captured.
	if _, err := access.observe(q.Event{Progress: &q.Progress{Position: position, Revision: 2, PreviousRevision: 1}}); err != nil {
		t.Fatal(err)
	}
	assertQuiet() // Consuming an unchanged report must not request another.
}
