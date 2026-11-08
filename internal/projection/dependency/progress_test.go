package dependency

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	q "github.com/adiom-data/weave/internal/querysubscription/model"
)

type progressTestClient struct {
	testClient
	feed *progressTestFeed
}

type progressTestFeed struct {
	*testFeed
	requests chan progressTestCall
	through  uint64 // Guarded by testFeed.mu; completed native snapshots only.
}

type progressTestCall struct {
	through uint64
	finish  chan error
}

type completingProgressClient struct {
	*progressTestClient
	installed chan struct{}
	resume    chan struct{}
}

type completingProgressFeed struct {
	q.Feed
	owner *completingProgressClient
}

func (c *completingProgressClient) OpenFeed(ctx context.Context, options q.FeedOptions) (q.Feed, error) {
	f, err := c.progressTestClient.OpenFeed(ctx, options)
	return &completingProgressFeed{Feed: f, owner: c}, err
}

func (f *completingProgressFeed) SubscribeEntity(ctx context.Context, request q.EntitySubscribeRequest, install q.EntitySnapshotInstaller) error {
	return f.Feed.SubscribeEntity(ctx, request, func(handle q.EntitySubscription, snapshot q.EntitySnapshot) error {
		if err := install(handle, snapshot); err != nil {
			return err
		}
		close(f.owner.installed)
		select {
		case <-f.owner.resume:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
}

func (c *progressTestClient) OpenFeed(ctx context.Context, options q.FeedOptions) (q.Feed, error) {
	f, err := c.testClient.OpenFeed(ctx, options)
	if err != nil {
		return nil, err
	}
	c.feed = &progressTestFeed{testFeed: f.(*testFeed), requests: make(chan progressTestCall, 100)}
	return c.feed, nil
}

func (f *progressTestFeed) SubscribeEntity(ctx context.Context, request q.EntitySubscribeRequest, install q.EntitySnapshotInstaller) error {
	handle, err := f.prepare(ctx, request.ID)
	if err != nil {
		return err
	}
	at := uint64(20)
	if request.Query.Relation.Name == "newer" {
		at = 30
	}
	row := testRow("1", "initial")
	if err := install(handle, q.EntitySnapshot{Row: &row, Checkpoint: position(at), Fence: position(at), Revision: 1}); err != nil {
		return err
	}
	f.mu.Lock()
	if f.through < at {
		f.through = at
	}
	f.mu.Unlock()
	return nil
}

func (f *progressTestFeed) RequestProgress(ctx context.Context) error {
	f.mu.Lock()
	call := progressTestCall{through: f.through, finish: make(chan error, 1)}
	f.mu.Unlock()
	select {
	case f.requests <- call:
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case err := <-call.finish:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (f *progressTestFeed) report(at uint64) {
	f.advance(at, true)
}

func (f *progressTestFeed) advance(at uint64, progress bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	revision := f.previous + 1
	if revision == 1 {
		revision = 2
	}
	event := q.Event{Progress: &q.Progress{Position: position(at), Revision: revision, PreviousRevision: f.previous}}
	if !progress {
		event = q.Event{Delivery: &q.DeliveryEnvelope{Delivery: q.DeliveryMetadata{Kind: q.DeliveryCommit, Checkpoint: position(at), Revision: revision, PreviousRevision: f.previous, Atomic: true}}}
	}
	if err := f.events.Offer(event); err != nil {
		panic(err)
	}
	f.previous = revision
}

func progressCache(t *testing.T) (*Cache, *progressTestClient) {
	t.Helper()
	source := &progressTestClient{}
	c, err := New(t.Context(), source, DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c, source
}

func receiveProgressCall(t *testing.T, source *progressTestClient) progressTestCall {
	t.Helper()
	select {
	case call := <-source.feed.requests:
		return call
	case <-time.After(time.Second):
		t.Fatal("shared progress request did not start")
		return progressTestCall{}
	}
}

func assertNoProgressCall(t *testing.T, source *progressTestClient) {
	t.Helper()
	select {
	case call := <-source.feed.requests:
		call.finish <- nil
		t.Fatal("redundant upstream progress request")
	case <-time.After(20 * time.Millisecond):
	}
}

func TestSharedProgressCoalescesConsumersAndPinsAcceptedDemand(t *testing.T) {
	c, source := progressCache(t)
	const viewers = 100
	feeds := make([]q.Feed, viewers)
	for i := range feeds {
		feeds[i] = open(t, c, 8)
		subscribe(t, feeds[i], "entity", "projects")
	}
	results := make(chan error, viewers)
	var started sync.WaitGroup
	started.Add(viewers)
	for _, feed := range feeds {
		go func() {
			started.Done()
			results <- feed.RequestProgress(t.Context())
		}()
	}
	started.Wait()
	call := receiveProgressCall(t, source)
	if call.through != 20 {
		t.Fatal("request did not capture the completed snapshot floor", call.through)
	}
	// Acceptance can precede the report. Requests arriving on either side of
	// acceptance must share the same demand instead of starting another call.
	call.finish <- nil
	for range viewers {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
	for _, feed := range feeds {
		if err := feed.RequestProgress(t.Context()); err != nil {
			t.Fatal(err)
		}
	}
	assertNoProgressCall(t, source)
	source.feed.report(20)
	for _, feed := range feeds {
		if event := next(t, feed); event.Progress == nil || event.Progress.Position.Value != 20 {
			t.Fatal("shared report was not delivered", event)
		}
	}
}

func TestNewConsumersJoinAtConsumedCutWithoutUpstreamReports(t *testing.T) {
	c, source := progressCache(t)
	first := open(t, c, 8)
	subscribe(t, first, "first", "projects")
	finished := make(chan error, 1)
	go func() { finished <- first.RequestProgress(t.Context()) }()
	call := receiveProgressCall(t, source)
	source.feed.report(20)
	call.finish <- nil
	if err := <-finished; err != nil {
		t.Fatal(err)
	}
	proof := next(t, first).Progress
	first.Close(t.Context())
	const viewers = 100
	feeds := make([]q.Feed, viewers)
	for i := range feeds {
		feeds[i] = open(t, c, 8)
		subscribe(t, feeds[i], "entity", "projects")
		if err := feeds[i].RequestProgress(t.Context()); err != nil {
			t.Fatal(err)
		}
		joined := next(t, feeds[i]).Progress
		if joined == nil || !q.SamePosition(joined.Position, proof.Position) || joined.Revision != proof.Revision {
			t.Fatal("new consumer did not join at the consumed cut", joined)
		}
	}
	assertNoProgressCall(t, source)
	// The seed is local to new consumers; do not insert duplicate revisions
	// into established feeds or rewrite the next upstream predecessor.
	source.feed.advance(21, false)
	for _, feed := range feeds {
		event := next(t, feed)
		if event.Delivery == nil || event.Delivery.Delivery.PreviousRevision != proof.Revision {
			t.Fatal("joining broke the revision chain", event)
		}
	}
}

func TestNewSnapshotDemandDuringSharedRequestGetsOneFollowup(t *testing.T) {
	c, source := progressCache(t)
	older, newer := open(t, c, 8), open(t, c, 8)
	subscribe(t, older, "older", "projects")
	oldResult := make(chan error, 1)
	go func() { oldResult <- older.RequestProgress(t.Context()) }()
	first := receiveProgressCall(t, source)
	subscribe(t, newer, "newer", "newer")
	newResult := make(chan error, 1)
	go func() { newResult <- newer.RequestProgress(t.Context()) }()
	first.finish <- nil
	if err := <-oldResult; err != nil {
		t.Fatal(err)
	}
	followup := receiveProgressCall(t, source)
	if followup.through != 30 {
		t.Fatal("new demand reused an insufficient request", followup.through)
	}
	followup.finish <- nil
	if err := <-newResult; err != nil {
		t.Fatal(err)
	}
	assertNoProgressCall(t, source)
}

func TestSharedProgressCancellationAndErrors(t *testing.T) {
	c, source := progressCache(t)
	one, two := open(t, c, 8), open(t, c, 8)
	subscribe(t, one, "one", "projects")
	subscribe(t, two, "two", "projects")
	ctx, cancel := context.WithCancel(t.Context())
	first, second := make(chan error, 1), make(chan error, 1)
	go func() { first <- one.RequestProgress(ctx) }()
	call := receiveProgressCall(t, source)
	cancel()
	if err := <-first; !errors.Is(err, context.Canceled) {
		t.Fatal("caller cancellation was ignored", err)
	}
	go func() { second <- two.RequestProgress(t.Context()) }()
	call.finish <- nil
	if err := <-second; err != nil {
		t.Fatal("one viewer canceled shared progress", err)
	}
	assertNoProgressCall(t, source)
	// A failed request must not permanently pin its target or poison the cache.
	subscribe(t, two, "newer", "newer")
	go func() { second <- two.RequestProgress(t.Context()) }()
	failed := receiveProgressCall(t, source)
	cause := errors.New("report failed")
	failed.finish <- cause
	if err := <-second; !errors.Is(err, cause) {
		t.Fatal("request failure was lost", err)
	}
	go func() { second <- two.RequestProgress(t.Context()) }()
	retry := receiveProgressCall(t, source)
	retry.finish <- nil
	if err := <-second; err != nil {
		t.Fatal(err)
	}
}

func TestSharedProgressUnknownPositionDoesNotRemainInFlight(t *testing.T) {
	c, source := progressCache(t)
	feed := open(t, c, 8)
	for attempt := range 2 {
		finished := make(chan error, 1)
		go func() { finished <- feed.RequestProgress(t.Context()) }()
		call := receiveProgressCall(t, source)
		if call.through != 0 {
			t.Fatal("empty feed invented progress", call.through)
		}
		call.finish <- nil // No position is known; no report follows.
		if err := <-finished; err != nil {
			t.Fatal(fmt.Errorf("attempt %d: %w", attempt, err))
		}
	}
}

func TestSharedProgressPinsOnlyCompletedSubscriptions(t *testing.T) {
	source := &completingProgressClient{progressTestClient: &progressTestClient{}, installed: make(chan struct{}), resume: make(chan struct{})}
	c, err := New(t.Context(), source, DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	feed := open(t, c, 8)
	loaded := make(chan error, 1)
	go func() {
		loaded <- feed.SubscribeEntity(t.Context(), q.EntitySubscribeRequest{ID: "entity", Query: query("projects")}, func(q.EntitySubscription, q.EntitySnapshot) error { return nil })
	}()
	select {
	case <-source.installed:
	case <-time.After(time.Second):
		t.Fatal("snapshot did not install")
	}
	result := make(chan error, 1)
	go func() { result <- feed.RequestProgress(t.Context()) }()
	early := receiveProgressCall(t, source.progressTestClient)
	if early.through != 0 {
		t.Fatal("incomplete subscription established source progress", early.through)
	}
	early.finish <- nil
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	close(source.resume)
	if err := <-loaded; err != nil {
		t.Fatal(err)
	}
	go func() { result <- feed.RequestProgress(t.Context()) }()
	completed := receiveProgressCall(t, source.progressTestClient)
	if completed.through != 20 {
		t.Fatal("completed snapshot reused an early insufficient request", completed.through)
	}
	completed.finish <- nil
	if err := <-result; err != nil {
		t.Fatal(err)
	}
}

func TestHydrationCheckpointCreatesDemandNotSourceProof(t *testing.T) {
	c, source := progressCache(t)
	feed := open(t, c, 8)
	subscribe(t, feed, "entity", "projects")
	source.feed.report(20)
	proof := next(t, feed).Progress
	up := source.feed
	up.mu.Lock()
	up.through = 30
	revision := up.previous + 1
	err := up.events.Offer(q.Event{Delivery: &q.DeliveryEnvelope{Delivery: q.DeliveryMetadata{
		Kind: q.DeliveryHydration, Checkpoint: position(30), Revision: revision, PreviousRevision: up.previous,
	}}})
	up.previous = revision
	up.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	next(t, feed)
	joined := open(t, c, 8)
	seed := next(t, joined).Progress
	if seed == nil || !q.SamePosition(seed.Position, proof.Position) || seed.Revision != revision {
		t.Fatal("hydration fabricated source proof or broke the join cut", seed)
	}
	result := make(chan error, 1)
	go func() { result <- joined.RequestProgress(t.Context()) }()
	call := receiveProgressCall(t, source)
	if call.through != 30 {
		t.Fatal("hydration read did not create fresh report demand", call.through)
	}
	source.feed.report(30)
	call.finish <- nil
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	for _, f := range []q.Feed{feed, joined} {
		event := next(t, f)
		if event.Progress == nil || event.Progress.PreviousRevision != revision || event.Progress.Position.Value != 30 {
			t.Fatal("source catch-up did not preserve ordering", event)
		}
	}
}
