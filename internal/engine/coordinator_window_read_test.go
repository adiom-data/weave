package engine

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/adiom-data/weave/internal/datastore"
	"github.com/adiom-data/weave/internal/engine/entitycache"
	"github.com/adiom-data/weave/internal/engine/referencefetch"
	"github.com/adiom-data/weave/internal/engine/routing"
	"github.com/adiom-data/weave/internal/engine/windowloader"
	"github.com/adiom-data/weave/internal/storage"
)

type undecodableReplayChange struct{ fakeChange }

func (c undecodableReplayChange) Apply(storage.Record) (datastore.AppliedChange, error) {
	return datastore.AppliedChange{}, errors.New("row payload was decoded")
}

func TestSnapshotCapturesRouteByRelationAndReleaseEmptyBuckets(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	a := fakePlan{relation: storage.RelationKey{Source: "test", Name: "a"}}
	b := fakePlan{relation: storage.RelationKey{Source: "test", Name: "b"}}
	c, err := StartCoordinator(ctx, &coordinatorSource{plan: a, feed: newCoordinatorFeed()})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { cancel(); _ = c.Wait(context.Background()) }()
	run := func(fn func()) {
		t.Helper()
		done := make(chan error, 1)
		if err := c.waitCommand(ctx, coordinatorCommand{run: func() { fn(); done <- nil }}, done); err != nil {
			t.Fatal(err)
		}
	}
	var windows []*windowReadSession
	var references []*referenceRead
	for _, plan := range []fakePlan{a, a, b} {
		window, err := c.beginWindowRead(ctx, plan)
		if err != nil {
			t.Fatal(err)
		}
		reference, err := c.beginReferenceRead(ctx, fixedPreparedReference{plan: plan})
		if err != nil {
			t.Fatal(err)
		}
		windows = append(windows, window)
		references = append(references, reference)
	}
	key := storage.EntityKey{Relation: a.relation, Value: "1"}
	run(func() {
		change := fakeChange{kind: datastore.ChangeDelete, relation: a.relation, before: key}
		if err := c.captureSnapshotChanges(a.relation, []datastore.Change{change}); err != nil {
			t.Error(err)
		}
		for i := range windows {
			want := i < 2
			if (windows[i].capture.revision > 0) != want || references[i].capture.Dirty(key) != want {
				t.Errorf("capture %d received the wrong relation's changes", i)
			}
		}
	})
	if err := c.finishWindowRead(ctx, windows[0]); err != nil {
		t.Fatal(err)
	}
	c.abortWindowRead(windows[1])
	c.abortWindowRead(windows[1]) // Repeated cleanup must preserve other buckets.
	c.abortReferenceRead(references[0])
	c.abortReferenceRead(references[1])
	run(func() {
		if len(c.windowReads) != 1 || len(c.referenceReads) != 1 || len(c.windowReads[b.relation]) != 1 || len(c.referenceReads[b.relation]) != 1 {
			t.Error("cleanup retained an empty bucket or removed unrelated reads")
		}
		c.lastCheckpoint = testPosition(1)
	})
	if err := c.finishWindowRead(ctx, windows[2]); err != nil {
		t.Fatal(err)
	}
	if err := c.finishReferenceRead(ctx, references[2], nil, testPosition(1)); err != nil {
		t.Fatal(err)
	}
	c.abortReferenceRead(references[2])
	run(func() {
		if len(c.windowReads) != 0 || len(c.referenceReads) != 0 {
			t.Error("completed captures retained relation buckets")
		}
	})
}

func TestPointCaptureFollowsKeyMovesWithoutDecodingPayload(t *testing.T) {
	relation := storage.RelationKey{Source: "test", Name: "tasks"}
	key := func(id string) storage.EntityKey { return storage.EntityKey{Relation: relation, Value: id} }
	initial := key("1")
	read := &windowRead{relation: relation, identity: &initial, keys: map[storage.EntityKey]struct{}{initial: {}}}
	for _, pair := range [][2]string{{"9", "9"}, {"1", "2"}, {"2", "2"}, {"2", "1"}, {"9", "9"}} {
		change := undecodableReplayChange{fakeChange{kind: datastore.ChangeUpdate, relation: relation, before: key(pair[0]), after: key(pair[1])}}
		read.observe(change, referencefetch.Resolve(change), 3)
	}
	if read.err != nil || read.revision != 3 || len(read.changes) != 3 {
		t.Fatalf("point capture = %+v", read)
	}
}

func TestWindowCaptureBoundIncludesDetachedHistory(t *testing.T) {
	relation := storage.RelationKey{Source: "test", Name: "tasks"}
	read := &windowRead{relation: relation}
	change := fakeChange{kind: datastore.ChangeClearRelation, relation: relation}
	read.observe(change, referencefetch.Invalidation{}, 1)
	read.changes = nil // a worker detached the first chunk
	read.observe(change, referencefetch.Invalidation{}, 1)
	if read.err == nil {
		t.Fatal("detaching chunks bypassed the history bound")
	}
}

func TestStagingSkipsUnrelatedPayloadsAndHonorsCancellation(t *testing.T) {
	relation := storage.RelationKey{Source: "test", Name: "tasks"}
	record := storage.Record{"id": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("1")}}
	source := &coordinatorSource{plan: fakePlan{fingerprint: "all", relation: relation}, records: []storage.Record{record}, feed: newCoordinatorFeed()}
	cache := entitycache.NewEntityCache()
	loader := windowloader.NewWindowLoader(source, cache, nil)
	pending, err := loader.ReadRanges(context.Background(), source.plan, []datastore.Bounds{{Left: datastore.Boundary{Kind: datastore.BoundaryStart}, Right: datastore.Boundary{Kind: datastore.BoundaryEnd}}}, 10)
	if err != nil {
		t.Fatal(err)
	}
	staging, err := pending.PrepareStage()
	if err != nil {
		t.Fatal(err)
	}
	other := storage.EntityKey{Relation: relation, Value: "2"}
	changes := []datastore.Change{undecodableReplayChange{fakeChange{kind: datastore.ChangeUpdate, relation: relation, before: other, after: other}}}
	if err := staging.Replay(context.Background(), changes); err != nil {
		t.Fatal(err)
	}
	if cache.Size() != 0 {
		t.Fatal("preparation touched the shared cache")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := staging.Replay(ctx, changes); !errors.Is(err, context.Canceled) {
		t.Fatalf("replay cancellation: %v", err)
	}
	if _, err := staging.PrepareCommit(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("commit preparation cancellation: %v", err)
	}
}

func TestWindowAdmissionDoesNotWaitForUnrelatedCaptureRevisions(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	relation := storage.RelationKey{Source: "test", Name: "tasks"}
	feed := newCoordinatorFeed()
	source := &coordinatorSource{plan: fakePlan{fingerprint: "all", relation: relation}, records: []storage.Record{{"id": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("1")}}}, feed: feed}
	c, err := StartCoordinator(ctx, source)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { cancel(); _ = c.Wait(context.Background()) }()
	read, err := c.beginWindowRead(ctx, source.plan)
	if err != nil {
		t.Fatal(err)
	}
	defer c.abortWindowRead(read)
	pending, err := c.loader.ReadRanges(ctx, source.plan, []datastore.Bounds{{Left: datastore.Boundary{Kind: datastore.BoundaryStart}, Right: datastore.Boundary{Kind: datastore.BoundaryEnd}}}, 10)
	if err != nil {
		t.Fatal(err)
	}
	defer pending.Cancel(context.Canceled)
	feed.events <- datastore.CommittedChanges{Checkpoint: testPosition(1), Changes: []datastore.Change{fakeChange{kind: datastore.ChangeDelete, relation: relation, before: storage.EntityKey{Relation: relation, Value: "2"}}}}
	for feed.acknowledged() != testPosition(1) {
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(time.Millisecond):
		}
	}
	if err := c.stageWindowRead(ctx, read, pending); err != nil {
		t.Fatal(err)
	}
	if read.revision != 0 || len(read.history) != 0 {
		t.Fatal("unrelated source progress forced replay before admitting an unchanged image")
	}
}

func TestWindowReadRetryReconcilesDeletionAfterCoalescedOwnerFailure(t *testing.T) {
	relation := storage.RelationKey{Source: "test", Name: "tasks"}
	key := storage.EntityKey{Relation: relation, Value: "1"}
	record := storage.Record{"id": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("1")}}
	cache := entitycache.NewEntityCache()
	owner := cache.StageSnapshotBatch([]entitycache.SnapshotLoadRequest{{Key: key, Record: entitycache.PrepareSnapshotRecord(record)}})[0]
	source := &coordinatorSource{plan: fakePlan{fingerprint: "all", relation: relation}, records: []storage.Record{record}, feed: newCoordinatorFeed()}
	loader := windowloader.NewWindowLoader(source, cache, nil)
	pending, err := loader.ReadRanges(context.Background(), source.plan, []datastore.Bounds{{Left: datastore.Boundary{Kind: datastore.BoundaryStart}, Right: datastore.Boundary{Kind: datastore.BoundaryEnd}}}, 10)
	if err != nil {
		t.Fatal(err)
	}
	if err := pending.Stage(); err != nil {
		t.Fatal(err)
	}
	if err := cache.Fail(owner, context.Canceled); err != nil {
		t.Fatal(err)
	}
	if err := cache.DeleteAuthoritative(key); err != nil {
		t.Fatal(err)
	}
	// The owner failed, so retrying its waiter's old snapshot now needs the
	// capture prefix; absence in the cache alone cannot prove a deletion.
	staging, err := pending.PrepareRetry([]int{0})
	if err != nil {
		t.Fatal(err)
	}
	if err := staging.Replay(context.Background(), []datastore.Change{fakeChange{kind: datastore.ChangeDelete, relation: relation, before: key}}); err != nil {
		t.Fatal(err)
	}
	commit, err := staging.PrepareCommit(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := commit.Apply(); err != nil {
		t.Fatal(err)
	}
	if err := pending.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, exists := cache.Get(key); exists {
		t.Fatal("retry resurrected a deleted snapshot row")
	}
}

type replayGateChange struct {
	fakeChange
	calls   atomic.Int32
	started chan struct{}
	release <-chan struct{}
}

func (c *replayGateChange) Apply(previous storage.Record) (datastore.AppliedChange, error) {
	if c.calls.Add(1) == 2 {
		close(c.started)
		<-c.release
	}
	return c.fakeChange.Apply(previous)
}

func TestWindowReplayDoesNotBlockCDCAndRejectsSupersededPreparation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	relation := storage.RelationKey{Source: "test", Name: "tasks"}
	key := storage.EntityKey{Relation: relation, Value: "1"}
	record := storage.Record{"id": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("1")}}
	feed := newCoordinatorFeed()
	started, release := make(chan struct{}, 1), make(chan struct{})
	replayStarted, replayRelease := make(chan struct{}), make(chan struct{})
	source := &coordinatorSource{plan: fakePlan{fingerprint: "all", relation: relation, routing: routing.RoutingPlan{Fallback: true}}, records: []storage.Record{record}, feed: feed, fence: testPosition(2)}
	source.windowKeysLoaded = func() { source.loadStarted, source.loadRelease = started, release }
	c, err := StartCoordinator(ctx, source)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
		select {
		case <-replayRelease:
		default:
			close(replayRelease)
		}
		cancel()
		_ = c.Wait(context.Background())
	}()
	type result struct {
		opened *OpenSubscription
		err    error
	}
	completed := make(chan result, 1)
	go func() {
		opened, err := c.Subscribe(ctx, "viewer", storage.QuerySpec{}, InitialWindow{Count: 10}, 8)
		completed <- result{opened, err}
	}()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	change := &replayGateChange{fakeChange: fakeChange{kind: datastore.ChangeUpdate, relation: relation, before: key, after: key, record: record, complete: true}, started: replayStarted, release: replayRelease}
	feed.events <- datastore.CommittedChanges{Checkpoint: testPosition(1), Changes: []datastore.Change{change}}
	for feed.acknowledged() != testPosition(1) {
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(time.Millisecond):
		}
	}
	close(release)
	select {
	case <-replayStarted:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	feed.events <- datastore.CommittedChanges{Checkpoint: testPosition(2), Changes: []datastore.Change{fakeChange{kind: datastore.ChangeDelete, relation: relation, before: key}}}
	deadline := time.After(time.Second)
	for feed.acknowledged() != testPosition(2) {
		select {
		case <-deadline:
			t.Fatal("snapshot replay blocked CDC")
		case <-time.After(time.Millisecond):
		}
	}
	close(replayRelease)
	select {
	case got := <-completed:
		if got.err != nil {
			t.Fatal(got.err)
		}
		if len(got.opened.Initial) != 0 {
			t.Fatalf("preparation missed newer deletion: %+v", got.opened.Initial)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}

type readPointPlan struct {
	fakePlan
	key storage.EntityKey
}

func (p readPointPlan) LookupKey() storage.EntityKey { return p.key }

type readPointSource struct {
	*coordinatorSource
	point readPointPlan
}

func (s readPointSource) PrepareQuery(context.Context, storage.QuerySpec) (datastore.PreparedQuery, error) {
	return s.point, nil
}

func TestExactPointCaptureIgnoresUnrelatedTableTraffic(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	relation := storage.RelationKey{Source: "test", Name: "tasks"}
	key := storage.EntityKey{Relation: relation, Value: "1"}
	feed := newCoordinatorFeed()
	started, release := make(chan struct{}, 1), make(chan struct{})
	base := &coordinatorSource{plan: fakePlan{fingerprint: "entity:one", relation: relation}, records: []storage.Record{{"id": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("1")}}}, feed: feed, fence: testPosition(1), loadStarted: started, loadRelease: release}
	source := readPointSource{base, readPointPlan{base.plan, key}}
	options := DefaultCoordinatorLimits()
	options.MaxResyncChanges = 1
	c, err := StartCoordinatorWithOptions(ctx, source, CoordinatorOptions{Limits: options})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
		cancel()
		_ = c.Wait(context.Background())
	}()
	completed := make(chan error, 1)
	go func() {
		_, err := c.Subscribe(ctx, "one", storage.QuerySpec{}, InitialWindow{Count: 1}, 8)
		completed <- err
	}()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	feed.events <- datastore.CommittedChanges{Checkpoint: testPosition(1), Changes: []datastore.Change{
		fakeChange{kind: datastore.ChangeDelete, relation: relation, before: storage.EntityKey{Relation: relation, Value: "2"}},
		fakeChange{kind: datastore.ChangeDelete, relation: relation, before: storage.EntityKey{Relation: relation, Value: "3"}},
	}}
	for feed.acknowledged() != testPosition(1) {
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(time.Millisecond):
		}
	}
	close(release)
	select {
	case err := <-completed:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}

func TestWindowReadReconcilesChangesBeforeCacheStaging(t *testing.T) {
	for _, explicitBounds := range []bool{false, true} {
		for _, operation := range []string{"delete", "truncate", "update_outside", "move_key", "delete_insert", "overflow"} {
			t.Run(operation+map[bool]string{false: "/count", true: "/bounds"}[explicitBounds], func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				defer cancel()
				relation := storage.RelationKey{Source: "test", Name: "tasks"}
				plan := fakePlan{fingerprint: "all", relation: relation, routing: routing.RoutingPlan{Fallback: true}, matches: func(record storage.Record) bool { return string(record["title"].Data) != "outside" }}
				record := func(id, title string) storage.Record {
					return storage.Record{"id": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte(id)}, "title": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte(title)}}
				}
				feed := newCoordinatorFeed()
				started, release := make(chan struct{}, 1), make(chan struct{})
				defer func() {
					select {
					case <-release:
					default:
						close(release)
					}
				}()
				source := &coordinatorSource{plan: plan, records: []storage.Record{record("1", "old")}, feed: feed, fence: testPosition(1)}
				if explicitBounds {
					source.loadStarted, source.loadRelease = started, release
				} else {
					source.windowKeysLoaded = func() { source.loadStarted, source.loadRelease = started, release }
				}
				options := DefaultCoordinatorLimits()
				if operation == "overflow" {
					options.MaxResyncChanges = 1
				}
				coordinator, err := StartCoordinatorWithOptions(ctx, source, CoordinatorOptions{Limits: options})
				if err != nil {
					t.Fatal(err)
				}
				defer func() { cancel(); _ = coordinator.Wait(context.Background()) }()
				initial := InitialWindow{Count: 10}
				if explicitBounds {
					initial = InitialWindow{Bounds: &storage.CursorBounds{Left: storage.CursorBoundary{Kind: storage.CursorStart}, Right: storage.CursorBoundary{Kind: storage.CursorEnd}}}
				}
				type result struct {
					opened *OpenSubscription
					err    error
				}
				completed := make(chan result, 1)
				go func() {
					opened, err := coordinator.Subscribe(ctx, "viewer", storage.QuerySpec{}, initial, 8)
					completed <- result{opened, err}
				}()
				select {
				case <-started:
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
				key := storage.EntityKey{Relation: relation, Value: "1"}
				changes := []datastore.Change{fakeChange{kind: datastore.ChangeDelete, relation: relation, before: key}}
				want := ""
				switch operation {
				case "truncate":
					changes = []datastore.Change{fakeChange{kind: datastore.ChangeClearRelation, relation: relation}}
				case "update_outside":
					changes = []datastore.Change{fakeChange{kind: datastore.ChangeUpdate, relation: relation, before: key, after: key, record: record("1", "outside"), complete: true}}
				case "move_key":
					want = "2"
					changes = []datastore.Change{fakeChange{kind: datastore.ChangeUpdate, relation: relation, before: key, after: storage.EntityKey{Relation: relation, Value: "2"}, record: record("2", "new"), complete: true}}
				case "delete_insert", "overflow":
					want = "1"
					changes = append(changes, fakeChange{kind: datastore.ChangeInsert, relation: relation, before: key, after: key, record: record("1", "new"), complete: true})
				}
				feed.events <- datastore.CommittedChanges{Checkpoint: testPosition(1), Atomic: true, Changes: changes}
				for feed.acknowledged() != testPosition(1) {
					select {
					case <-ctx.Done():
						t.Fatal(ctx.Err())
					case <-time.After(time.Millisecond):
					}
				}
				close(release)
				var got result
				select {
				case got = <-completed:
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
				if operation == "overflow" {
					if got.err == nil || !strings.Contains(got.err.Error(), "snapshot change buffer limit") {
						t.Fatalf("overflow error = %v", got.err)
					}
					if coordinator.hasTerminalError() {
						t.Fatal("snapshot overflow stopped CDC")
					}
					return
				}
				if got.err != nil {
					t.Fatal(got.err)
				}
				if want == "" {
					if len(got.opened.Initial) != 0 {
						t.Fatalf("stale snapshot rows: %+v", got.opened.Initial)
					}
				} else if len(got.opened.Initial) != 1 || got.opened.Initial[0].Key.Value != want || string(got.opened.Initial[0].Record["title"].Data) != "new" {
					t.Fatalf("reconciled rows: %+v", got.opened.Initial)
				}
			})
		}
	}
}

type toastReplayChange struct{ fakeChange }

func (c toastReplayChange) Apply(previous storage.Record) (datastore.AppliedChange, error) {
	out := storage.CloneRecord(c.record)
	complete := true
	for name, v := range out {
		if v.State == storage.ValueUnavailable {
			if prior, ok := previous[name]; ok && prior.State != storage.ValueUnavailable {
				out[name] = storage.CloneValue(prior)
			} else {
				complete = false
			}
		}
	}
	kind := datastore.AppliedPartial
	if complete {
		kind = datastore.AppliedComplete
	}
	return datastore.AppliedChange{Kind: kind, Key: c.after, Record: out}, nil
}

func TestSnapshotReplayKeyReuseRequiresResynchronization(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	relation := storage.RelationKey{Source: "test", Name: "tasks"}
	key := func(id string) storage.EntityKey { return storage.EntityKey{Relation: relation, Value: id} }
	val := func(s string) storage.Value {
		return storage.Value{Type: storage.TypeText, State: storage.ValuePresent, Data: []byte(s)}
	}
	row := func(id, category, body string) storage.Record {
		return storage.Record{"id": val(id), "category": val(category), "body": val(body)}
	}
	partial := func(id, category string) storage.Record {
		r := row(id, category, "")
		r["body"] = storage.Value{State: storage.ValueUnavailable}
		return r
	}
	plan := fakePlan{fingerprint: "include", relation: relation, routing: routing.RoutingPlan{Fallback: true}, matches: func(r storage.Record) bool { return string(r["category"].Data) == "include" }}
	feed := newCoordinatorFeed()
	started, release := make(chan struct{}, 1), make(chan struct{})
	source := &coordinatorSource{plan: plan, records: []storage.Record{row("1", "include", "B"), row("2", "include", "A")}, feed: feed, fence: testPosition(3), loadStarted: started, loadRelease: release}
	c, err := StartCoordinator(ctx, source)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
		cancel()
		_ = c.Wait(context.Background())
	}()
	type result struct {
		opened *OpenSubscription
		err    error
	}
	done := make(chan result, 1)
	go func() {
		opened, err := c.Subscribe(ctx, "viewer", storage.QuerySpec{}, InitialWindow{Bounds: &storage.CursorBounds{Left: storage.CursorBoundary{Kind: storage.CursorStart}, Right: storage.CursorBoundary{Kind: storage.CursorEnd}}}, 8)
		done <- result{opened, err}
	}()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	// T1 moves A away while it is outside the filter; no retained authoritative
	// baseline remains for key2. T2 installs B at key1. Snapshot also sees future T3.
	feed.events <- datastore.CommittedChanges{Checkpoint: testPosition(2), Atomic: true, Changes: []datastore.Change{
		toastReplayChange{fakeChange{kind: datastore.ChangeUpdate, relation: relation, before: key("1"), after: key("2"), record: partial("2", "exclude")}},
		fakeChange{kind: datastore.ChangeInsert, relation: relation, before: key("1"), after: key("1"), record: row("1", "include", "B"), complete: true},
	}}
	for feed.acknowledged() != testPosition(2) {
		select {
		case <-ctx.Done():
			t.Fatalf("ack timeout: ack=%v terminal=%v", feed.acknowledged(), c.terminalError())
		case <-time.After(time.Millisecond):
		}
	}
	close(release)
	// Replay must fail before an ambiguous snapshot baseline enters the cache.
	select {
	case got := <-done:
		if !errors.Is(got.err, storage.ErrResumeRequired) {
			t.Fatalf("ambiguous replay error = %v", got.err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if got, exists := c.entities.Get(key("2")); exists && string(got.Record["body"].Data) == "B" {
		t.Fatal("ambiguous baseline entered the cache")
	}
	if c.hasTerminalError() {
		t.Fatal("read failure stopped CDC")
	}
	feed.events <- datastore.CommittedChanges{Checkpoint: testPosition(3), Atomic: true, Changes: []datastore.Change{toastReplayChange{fakeChange{kind: datastore.ChangeUpdate, relation: relation, before: key("2"), after: key("2"), record: partial("2", "include")}}}}
	for feed.acknowledged() != testPosition(3) {
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(time.Millisecond):
		}
	}
	// A new read has no ambiguous history and can initialize normally.
	opened, err := c.Subscribe(ctx, "retry", storage.QuerySpec{}, InitialWindow{Bounds: &storage.CursorBounds{Left: storage.CursorBoundary{Kind: storage.CursorStart}, Right: storage.CursorBoundary{Kind: storage.CursorEnd}}}, 8)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range opened.Initial {
		if r.Key == key("2") {
			if string(r.Record["body"].Data) != "A" {
				t.Fatalf("published body=%q, want A", r.Record["body"].Data)
			}
			return
		}
	}
	t.Fatal("entity 2 missing after resynchronization")
}
