package evaluation

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/adiom-data/weave/internal/projection/retry"
	q "github.com/adiom-data/weave/internal/querysubscription/model"
	"github.com/adiom-data/weave/internal/storage"
)

type installation struct {
	request q.QuerySubscribeRequest
	install q.QuerySnapshotInstaller
}
type testFeed struct {
	requests chan installation
	events   chan q.Event
	done     chan struct{}
	once     sync.Once
	closed   atomic.Int32
	progress atomic.Int32
}

func newTestFeed() *testFeed {
	return &testFeed{requests: make(chan installation, 100), events: make(chan q.Event, 100), done: make(chan struct{})}
}
func (f *testFeed) SubscribeQuery(ctx context.Context, request q.QuerySubscribeRequest, install q.QuerySnapshotInstaller) error {
	result := make(chan error, 1)
	select {
	case f.requests <- installation{request, func(handle q.QuerySubscription, snapshot q.QuerySnapshot) error {
		err := install(handle, snapshot)
		result <- err
		return err
	}}:
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case err := <-result:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (*testFeed) SubscribeEntity(context.Context, q.EntitySubscribeRequest, q.EntitySnapshotInstaller) error {
	return errors.New("unexpected entity subscription")
}
func (f *testFeed) Next(ctx context.Context) (q.Event, error) {
	select {
	case event := <-f.events:
		return event, nil
	case <-f.done:
		return q.Event{}, q.ErrClosed
	case <-ctx.Done():
		return q.Event{}, ctx.Err()
	}
}
func (f *testFeed) RequestProgress(context.Context) error { f.progress.Add(1); return nil }
func (f *testFeed) Close(context.Context) error           { f.once.Do(func() { close(f.done) }); return nil }

type testHandle struct {
	id   string
	feed *testFeed
	once sync.Once
}

func (h *testHandle) ID() string { return h.id }
func (h *testHandle) Close(context.Context) error {
	h.once.Do(func() { h.feed.closed.Add(1) })
	return nil
}
func (*testHandle) UpdateWindow(context.Context, []q.BoundaryChange, string) error {
	return errors.New("unexpected window change")
}

func newFacts(t testing.TB, options Options) (*Facts, *testFeed) {
	t.Helper()
	feed := newTestFeed()
	facts, err := NewObservedFacts(context.Background(), feed, options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := facts.Close(); err != nil {
			t.Error(err)
		}
		_ = feed.Close(context.Background())
	})
	return facts, feed
}

var relation = storage.RelationName{Namespace: "demo", Name: "grants"}

func selector(value string) QueryRequest {
	return QueryRequest{ID: value, Query: storage.QuerySpec{Relation: relation, Filter: &storage.Predicate{Comparison: &storage.Comparison{Field: "project_id", Operator: storage.ComparisonEqual, Value: storage.Literal{Type: storage.TypeText, Data: []byte(value)}}}}}
}
func at(n uint64) *q.Position { return &q.Position{SourceID: "db", Value: n} }
func row(id, title string) q.Row {
	return q.Row{Key: storage.EntityKey{Relation: storage.RelationKey{Source: "db", Namespace: relation.Namespace, Name: relation.Name}, Value: id}, Record: storage.Record{"title": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte(title)}}}
}
func snapshot(n, revision uint64, rows ...q.Row) q.QuerySnapshot {
	wrapped := make([]q.QueryRow, len(rows))
	for i, row := range rows {
		wrapped[i] = q.QueryRow{Row: row}
	}
	return q.QuerySnapshot{Bounds: q.Bounds{Left: q.Boundary{Kind: q.BoundaryStart}, Right: q.Boundary{Kind: q.BoundaryEnd}}, Checkpoint: at(n), Fence: at(n), Revision: revision, Rows: wrapped}
}
func receive(t testing.TB, feed *testFeed) installation {
	t.Helper()
	select {
	case i := <-feed.requests:
		return i
	case <-time.After(time.Second * 3):
		t.Fatal("no subscription request")
		return installation{}
	}
}
func install(t testing.TB, feed *testFeed, i installation, s q.QuerySnapshot) {
	t.Helper()
	if err := i.install(&testHandle{id: i.request.ID, feed: feed}, s); err != nil {
		t.Fatal(err)
	}
}
func consume(t testing.TB, f *Facts, feed *testFeed, event q.Event) {
	t.Helper()
	feed.events <- event
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()
	frame, err := feed.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Observe(frame); err != nil {
		t.Fatal(err)
	}
}
func progress(n, revision, previous uint64) q.Event {
	return q.Event{Progress: &q.Progress{Position: at(n), Revision: revision, PreviousRevision: previous}}
}
func commit(n, revision, previous uint64, effects ...q.SubscriptionEffect) q.Event {
	return q.Event{Delivery: &q.DeliveryEnvelope{Delivery: q.DeliveryMetadata{Kind: q.DeliveryCommit, Checkpoint: at(n), Revision: revision, PreviousRevision: previous, Atomic: true}, Effects: effects}}
}

func TestCoalescedFactsAreFencedAndOwned(t *testing.T) {
	f, feed := newFacts(t, Options{MaxQueries: 10, MaxRows: 10, LoadConcurrency: 2})
	a, err := f.Acquire(selector("p1"))
	if err != nil {
		t.Fatal(err)
	}
	b, err := f.Acquire(selector("p1"))
	if err != nil {
		t.Fatal(err)
	}
	i := receive(t, feed)
	if i.request.Window.Count != 11 {
		t.Fatal(i.request.Window)
	}
	image := snapshot(20, 2, row("g1", "grant"))
	install(t, feed, i, image)
	image.Rows[0].Record["title"].Data[0] = 'X'
	consume(t, f, feed, progress(10, 1, 0))
	got, err := a.Read(at(10))
	if err != nil || got.Complete || len(got.Rows) != 0 {
		t.Fatalf("future evidence leaked %+v %v", got, err)
	}
	consume(t, f, feed, progress(20, 2, 1))
	got, err = a.Read(at(20))
	if err != nil || !got.Complete || len(got.Rows) != 1 || string(got.Rows[0].Record["title"].Data) != "grant" {
		t.Fatalf("%+v %v", got, err)
	}
	got.Rows[0].Record["title"].Data[0] = 'Y'
	got, err = b.Read(at(20))
	if err != nil || string(got.Rows[0].Record["title"].Data) != "grant" {
		t.Fatalf("caller mutated facts: %+v %v", got, err)
	}
	if err = a.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if feed.closed.Load() != 0 {
		t.Fatal("closed referenced selector")
	}
	if err = b.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if feed.closed.Load() != 1 {
		t.Fatal("did not release last reference")
	}
	select {
	case unexpected := <-feed.requests:
		t.Fatalf("duplicate load %s", unexpected.request.ID)
	default:
	}
}

func TestFailedFactRetriesKeepDependencyIdentityAndFenceFreshEvidence(t *testing.T) {
	f, feed := newFacts(t, Options{MaxQueries: 10, MaxRows: 10, LoadConcurrency: 1, Recovery: retry.Policy{InitialDelay: 10 * time.Millisecond, MaxDelay: time.Second, MaxAttempts: 3}})
	lease, err := f.Acquire(selector("p1"))
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Close(context.Background())
	first := receive(t, feed)
	install(t, feed, first, snapshot(10, 1, row("g1", "old")))
	consume(t, f, feed, progress(10, 1, 0))
	cause := errors.New("subscription failed")
	consume(t, f, feed, q.Event{Failure: &q.SubscriptionFailure{SubscriptionID: first.request.ID, Cause: cause}})
	old, err := f.OpenView(at(10))
	if err != nil {
		t.Fatal(err)
	}
	defer old.Close()
	if _, err := old.read(lease); !errors.Is(err, cause) {
		t.Fatalf("failed evidence not reported: %v", err)
	}
	id := lease.ID()
	retry := receive(t, feed)
	if retry.request.ID == first.request.ID || lease.ID() != id {
		t.Fatal("recovery reused upstream identity or changed dependency identity")
	}
	install(t, feed, retry, snapshot(20, 2, row("g1", "new")))
	if result, err := lease.Read(at(10)); err != nil || result.Complete {
		t.Fatalf("newer recovery image leaked before fence: %+v, %v", result, err)
	}
	consume(t, f, feed, progress(20, 2, 1))
	// Late frames from the retired handle must not fail its replacement.
	consume(t, f, feed, q.Event{Failure: &q.SubscriptionFailure{SubscriptionID: first.request.ID, Cause: cause}})
	result, err := lease.Read(at(20))
	if err != nil || !result.Complete || len(result.Rows) != 1 || string(result.Rows[0].Record["title"].Data) != "new" {
		t.Fatalf("fresh fenced evidence unavailable: %+v, %v", result, err)
	}
	if _, err := old.read(lease); !errors.Is(err, cause) {
		t.Fatal("recovery mutated an immutable failed view")
	}
	if !old.needsRetry(lease) {
		t.Fatal("replacement evidence lost the dependency-registration wakeup")
	}
}

func TestLiveFactFailuresRetryAndFenceReplacementEvidence(t *testing.T) {
	for _, cause := range []string{"coverage limit", "invalid row"} {
		t.Run(cause, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
			defer cancel()
			f, feed := newFacts(t, Options{MaxQueries: 10, MaxRows: 1, LoadConcurrency: 1, Recovery: retry.Policy{InitialDelay: 5 * time.Millisecond, MaxDelay: 10 * time.Millisecond, MaxAttempts: 2}})
			lease, err := f.Acquire(selector("p1"))
			if err != nil {
				t.Fatal(err)
			}
			defer lease.Close(ctx)
			first := receive(t, feed)
			install(t, feed, first, snapshot(10, 1, row("g1", "old")))
			// A live failure must schedule itself, not rely on an installation
			// worker still unwinding and incidentally discovering the error.
			for {
				f.mu.Lock()
				completed := lease.entry.completed && !lease.entry.loading
				f.mu.Unlock()
				if completed {
					break
				}
				select {
				case <-f.Changed():
				case <-ctx.Done():
					t.Fatal("initial fact subscription did not complete")
				}
			}
			consume(t, f, feed, progress(10, 1, 0))
			batch := q.Batch{}
			if cause == "coverage limit" {
				added := row("g2", "additional grant")
				batch.Changes = []q.Change{{Kind: q.Added, Row: added}}
				batch.Membership.Insertions = []q.Insertion{{Key: added.Key, FinalIndex: 1}}
			} else {
				invalid := row("g1", "invalid")
				invalid.Record = nil
				batch.Changes = []q.Change{{Kind: q.Updated, Row: invalid}}
			}
			consume(t, f, feed, commit(11, 2, 1, q.SubscriptionEffect{SubscriptionID: first.request.ID, Batch: batch}))
			id := lease.ID()
			replacement := receive(t, feed)
			if replacement.request.ID == first.request.ID || lease.ID() != id {
				t.Fatal("retry reused the failed attempt or changed dependency identity")
			}
			if feed.closed.Load() != 1 {
				t.Fatal("retry did not retire the failed handle")
			}
			install(t, feed, replacement, snapshot(20, 3, row("g1", "recovered")))
			if result, err := lease.Read(at(11)); err != nil || result.Complete {
				t.Fatalf("recovery exposed newer evidence before its fence: %+v, %v", result, err)
			}
			consume(t, f, feed, progress(20, 3, 2))
			// Failed-handle effects cannot poison the recovered dependency.
			consume(t, f, feed, commit(21, 4, 3, q.SubscriptionEffect{SubscriptionID: first.request.ID, Batch: batch}))
			result, err := lease.Read(at(21))
			if err != nil || !result.Complete || len(result.Rows) != 1 || string(result.Rows[0].Record["title"].Data) != "recovered" {
				t.Fatalf("recovered evidence unavailable or overwritten: %+v, %v", result, err)
			}
		})
	}
}

func TestFactRecoveryExhaustsBudgetWithoutReevaluationRestartingIt(t *testing.T) {
	f, feed := newFacts(t, Options{MaxQueries: 10, MaxRows: 10, LoadConcurrency: 1, Recovery: retry.Policy{InitialDelay: time.Millisecond, MaxDelay: 2 * time.Millisecond, MaxAttempts: 2}})
	lease, err := f.Acquire(selector("broken"))
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Close(t.Context())
	seen := map[string]bool{}
	for range 3 { // Original + exactly two retries, all rejected at installation.
		request := receive(t, feed)
		if seen[request.request.ID] {
			t.Fatal("reused attempt ID")
		}
		seen[request.request.ID] = true
		if err := request.install(&testHandle{id: request.request.ID, feed: feed}, q.QuerySnapshot{}); !errors.Is(err, ErrCoverageLimit) {
			t.Fatal(err)
		}
	}
	for range 20 {
		duplicate, err := f.Acquire(selector("broken"))
		if err != nil {
			t.Fatal(err)
		}
		_ = duplicate.Close(t.Context())
		_, _ = lease.Read(at(10))
	}
	select {
	case unexpected := <-feed.requests:
		t.Fatal("exhausted selector restarted", unexpected.request.ID)
	case <-time.After(40 * time.Millisecond):
	}
	// A genuinely different selector is a new dependency, not a retry reset.
	fresh, err := f.Acquire(selector("new"))
	if err != nil {
		t.Fatal(err)
	}
	defer fresh.Close(t.Context())
	request := receive(t, feed)
	if request.request.Query.Filter.Comparison.Value.Data[0] != 'n' {
		t.Fatal("wrong selector")
	}
	install(t, feed, request, snapshot(10, 1))
}

type completingFeed struct {
	*testFeed
	installed chan struct{}
	release   chan struct{}
}

func (f *completingFeed) SubscribeQuery(ctx context.Context, request q.QuerySubscribeRequest, install q.QuerySnapshotInstaller) error {
	if err := f.testFeed.SubscribeQuery(ctx, request, install); err != nil {
		return err
	}
	close(f.installed)
	select {
	case <-f.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func TestFactProgressDemandWaitsForSubscriptionCompletion(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	feed := &completingFeed{testFeed: newTestFeed(), installed: make(chan struct{}), release: make(chan struct{})}
	facts, err := NewObservedFacts(ctx, feed, Options{MaxQueries: 10, MaxRows: 10, LoadConcurrency: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer facts.Close()
	lease, err := facts.Acquire(selector("p1"))
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Close(ctx)
	i := receive(t, feed.testFeed)
	install(t, feed.testFeed, i, snapshot(20, 1))
	select {
	case <-feed.installed:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if targets := facts.ProgressTargets(); len(targets) != 0 {
		t.Fatal("installer callback prematurely justified a progress request", targets)
	}
	close(feed.release)
	select {
	case <-facts.Changed():
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if targets := facts.ProgressTargets(); len(targets) != 1 || targets[0].Value != 20 {
		t.Fatal("completed load did not expose its required checkpoint", targets)
	}
	if err := facts.Observe(progress(20, 2, 0)); err != nil {
		t.Fatal(err)
	}
	if targets := facts.ProgressTargets(); len(targets) != 0 {
		t.Fatal("covered facts still request progress", targets)
	}
}

func TestTransactionAndHydrationKeepEvidenceOrdered(t *testing.T) {
	f, feed := newFacts(t, Options{MaxQueries: 10, MaxRows: 10, LoadConcurrency: 2})
	lease, _ := f.Acquire(selector("p1"))
	i := receive(t, feed)
	install(t, feed, i, snapshot(10, 1, row("g1", "old")))
	consume(t, f, feed, commit(10, 1, 0))
	consume(t, f, feed, commit(11, 2, 1, q.SubscriptionEffect{SubscriptionID: i.request.ID, Batch: q.Batch{Changes: []q.Change{{Kind: q.Updated, Row: row("g1", "new")}}}}))
	old, err := lease.Read(at(10))
	if err != nil || old.Complete || len(old.Rows) != 0 {
		t.Fatalf("served newer facts to old transaction: %+v %v", old, err)
	}
	got, err := lease.Read(at(11))
	if err != nil || !got.Complete || string(got.Rows[0].Record["title"].Data) != "new" {
		t.Fatalf("%+v %v", got, err)
	}
	consume(t, f, feed, commit(12, 3, 2, q.SubscriptionEffect{SubscriptionID: i.request.ID, Batch: q.Batch{Changes: []q.Change{{Kind: q.Removed, Row: q.Row{Key: row("g1", "").Key}}}, Membership: q.MembershipDelta{Removed: []storage.EntityKey{row("g1", "").Key}}}}))
	got, err = lease.Read(at(12))
	if err != nil || !got.Complete || len(got.Rows) != 0 {
		t.Fatalf("%+v %v", got, err)
	}
}

func TestOldQueuedEventsCannotReplaceNewerSnapshot(t *testing.T) {
	f, feed := newFacts(t, Options{MaxQueries: 10, MaxRows: 10, LoadConcurrency: 1})
	lease, _ := f.Acquire(selector("p1"))
	i := receive(t, feed)
	install(t, feed, i, snapshot(20, 3, row("g1", "snapshot")))
	consume(t, f, feed, commit(10, 1, 0, q.SubscriptionEffect{SubscriptionID: i.request.ID, Batch: q.Batch{Changes: []q.Change{{Kind: q.Updated, Row: row("g1", "old")}}}}))
	consume(t, f, feed, progress(20, 2, 1))
	got, err := lease.Read(at(20))
	if err != nil || !got.Complete || string(got.Rows[0].Record["title"].Data) != "snapshot" {
		t.Fatalf("%+v %v", got, err)
	}
}

func TestCoverageAndFailureAreScopedToSelector(t *testing.T) {
	f, feed := newFacts(t, Options{MaxQueries: 10, MaxRows: 1, LoadConcurrency: 2})
	a, _ := f.Acquire(selector("p1"))
	b, _ := f.Acquire(selector("p2"))
	first, second := receive(t, feed), receive(t, feed)
	byProject := map[string]installation{string(first.request.Query.Filter.Comparison.Value.Data): first, string(second.request.Query.Filter.Comparison.Value.Data): second}
	bad := snapshot(10, 1, row("g1", "bad"))
	bad.Bounds.Right = q.Boundary{Kind: q.BoundaryCursor, Cursor: []byte("more")}
	err := byProject["p1"].install(&testHandle{id: byProject["p1"].request.ID, feed: feed}, bad)
	if !errors.Is(err, ErrCoverageLimit) {
		t.Fatal(err)
	}
	select {
	case <-f.Changed():
	case <-time.After(time.Second * 3):
		t.Fatal("installation did not notify")
	}
	install(t, feed, byProject["p2"], snapshot(10, 1))
	consume(t, f, feed, progress(10, 1, 0))
	// Wait for the failed installer's SubscribeQuery call to unwind.
	deadline := time.Now().Add(time.Second * 3)
	for {
		_, err = a.Read(at(10))
		if errors.Is(err, ErrCoverageLimit) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("missing scoped coverage error: %v", err)
		}
		time.Sleep(time.Millisecond)
	}
	got, err := b.Read(at(10))
	if err != nil || !got.Complete {
		t.Fatalf("unrelated selector failed: %+v %v", got, err)
	}
}

func TestCanceledQueuedLoadsDoNotBlockNewAcquires(t *testing.T) {
	f, feed := newFacts(t, Options{MaxQueries: 2, MaxRows: 10, LoadConcurrency: 1})
	first, _ := f.Acquire(selector("busy"))
	_ = first
	_ = receive(t, feed) // occupy the only loader
	done := make(chan error, 1)
	go func() {
		for range 100 {
			lease, err := f.Acquire(selector("queued"))
			if err != nil {
				done <- err
				return
			}
			if err = lease.Close(context.Background()); err != nil {
				done <- err
				return
			}
		}
		done <- nil
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second * 3):
		t.Fatal("canceled jobs filled the load queue")
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if err := first.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestBrokenFeedFailsEvidenceRatherThanReturningNegative(t *testing.T) {
	f, feed := newFacts(t, Options{MaxQueries: 10, MaxRows: 10, LoadConcurrency: 1})
	lease, _ := f.Acquire(selector("p1"))
	i := receive(t, feed)
	install(t, feed, i, snapshot(10, 1))
	consume(t, f, feed, progress(10, 1, 0))
	if err := f.Observe(progress(20, 3, 2)); err == nil {
		t.Fatal("accepted missing frame")
	}
	if got, err := lease.Read(at(10)); err == nil || got.Complete {
		t.Fatalf("failure became negative evidence: %+v %v", got, err)
	}
}

func TestFactsCloseReleasesSubscriptionsButLeavesFeedToOwner(t *testing.T) {
	f, feed := newFacts(t, Options{MaxQueries: 10, MaxRows: 10, LoadConcurrency: 1})
	if _, err := f.Acquire(selector("p1")); err != nil {
		t.Fatal(err)
	}
	i := receive(t, feed)
	install(t, feed, i, snapshot(10, 1))
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if got := feed.closed.Load(); got != 1 {
		t.Fatalf("closed %d subscriptions, want 1", got)
	}
	select {
	case <-feed.done:
		t.Fatal("facts closed the projection owner's feed")
	default:
	}
}

func TestFactFeedCanAttachToLiveRevisionChain(t *testing.T) {
	f, feed := newFacts(t, Options{MaxQueries: 10, MaxRows: 10, LoadConcurrency: 1})
	lease, _ := f.Acquire(selector("p1"))
	i := receive(t, feed)
	install(t, feed, i, snapshot(50, 100))
	consume(t, f, feed, progress(50, 101, 100))
	got, err := lease.Read(at(50))
	if err != nil || !got.Complete {
		t.Fatalf("%+v %v", got, err)
	}
	consume(t, f, feed, progress(51, 102, 101))
}
