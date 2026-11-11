package projection

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	q "github.com/adiom-data/weave/internal/querysubscription/model"
	"github.com/adiom-data/weave/internal/storage"
)

type recoveryFeed struct {
	boundarySession
	fail      atomic.Bool
	failTable string
	requests  chan q.QuerySubscribeRequest
}

func (f *recoveryFeed) SubscribeQuery(_ context.Context, request q.QuerySubscribeRequest, install q.QuerySnapshotInstaller) error {
	f.requests <- request
	if f.fail.Load() && request.Query.Relation.Name == f.failTable {
		return errors.New("query temporarily unavailable")
	}
	bounds := q.Bounds{Left: q.Boundary{Kind: q.BoundaryStart}, Right: q.Boundary{Kind: q.BoundaryEnd}}
	if request.Window.Bounds != nil {
		bounds = *request.Window.Bounds.Clone()
	}
	key := storage.EntityKey{Relation: storage.RelationKey{Source: "test", Name: request.Query.Relation.Name}, Value: "n"}
	return install(&boundarySubscription{id: request.ID}, q.QuerySnapshot{Bounds: bounds, Checkpoint: &q.Position{SourceID: "test", Value: 10}, Fence: &q.Position{SourceID: "test", Value: 10}, Rows: []q.QueryRow{{Row: q.Row{Key: key, Record: storage.Record{"id": {Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("n")}}, Cursor: []byte("n")}}}})
}

func recoveryNode(name string, children ...*ProjectionNode) *ProjectionNode {
	return &ProjectionNode{Name: name, Window: q.InitialWindow{Count: 10}, Children: children, BuildQuery: func(storage.Record) (storage.QuerySpec, bool, error) {
		return storage.QuerySpec{Relation: storage.RelationName{Name: name}}, true, nil
	}}
}

type blockedWindowSubscription struct {
	boundarySubscription
	entered chan struct{}
	closed  chan struct{}
	once    sync.Once
}

func (s *blockedWindowSubscription) UpdateWindow(ctx context.Context, _ []q.BoundaryChange, _ string) error {
	close(s.entered)
	select {
	case <-s.closed:
		return q.ErrClosed
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *blockedWindowSubscription) Close(context.Context) error {
	s.once.Do(func() { close(s.closed) })
	return nil
}

func TestWindowUpdateAndBranchFailureShareHandleLifecycle(t *testing.T) {
	newProjection := func(ctx context.Context, handle q.QuerySubscription) (*Projection, *projectionInstance) {
		instance := &projectionInstance{id: "root", node: recoveryNode("root"), members: map[storage.EntityKey]*projectionMember{}, subscription: handle, query: handle}
		p := &Projection{ctx: ctx, instances: map[string]*projectionInstance{"root": instance}, failed: map[string]*projectionInstance{}, limits: Limits{}.WithDefaults()}
		return p, instance
	}
	t.Run("in-flight update does not block failure", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
		defer cancel()
		handle := &blockedWindowSubscription{boundarySubscription: boundarySubscription{id: "root"}, entered: make(chan struct{}), closed: make(chan struct{})}
		p, instance := newProjection(ctx, handle)
		result := make(chan error, 1)
		go func() { result <- p.QueueWindowUpdate(ctx, instance.id, nil, "in-flight") }()
		select {
		case <-handle.entered:
		case <-ctx.Done():
			t.Fatal("window request did not reach the handle")
		}
		failed := make(chan struct{})
		go func() {
			p.processMu.Lock()
			p.failInstance(instance, errors.New("query failed"))
			p.processMu.Unlock()
			close(failed)
		}()
		select {
		case <-failed:
		case <-ctx.Done():
			t.Fatal("blocked window IO stalled branch failure")
		}
		select {
		case err := <-result:
			if !errors.Is(err, q.ErrClosed) {
				t.Fatalf("in-flight request did not observe handle retirement: %v", err)
			}
		case <-ctx.Done():
			t.Fatal("window request did not finish after handle retirement")
		}
		if err := p.QueueWindowUpdate(ctx, instance.id, nil, "late"); err == nil {
			t.Fatal("failed instance accepted another window request")
		}
	})

	t.Run("concurrent requests and failure", func(t *testing.T) {
		p, instance := newProjection(t.Context(), &boundarySubscription{id: "root"})
		var workers sync.WaitGroup
		start := make(chan struct{})
		for range 8 {
			workers.Go(func() {
				<-start
				for range 1000 {
					_ = p.QueueWindowUpdate(t.Context(), instance.id, nil, "concurrent")
				}
			})
		}
		workers.Go(func() {
			<-start
			p.processMu.Lock()
			p.failInstance(instance, errors.New("query failed"))
			p.processMu.Unlock()
		})
		close(start)
		workers.Wait()
		if err := p.QueueWindowUpdate(t.Context(), instance.id, nil, "late"); err == nil {
			t.Fatal("failed instance accepted another window request")
		}
	})
}

type retirementSubscription struct {
	boundarySubscription
	closes atomic.Int32
}

func (s *retirementSubscription) Close(context.Context) error {
	s.closes.Add(1) // Deliberately not idempotent: the owner must close only once.
	return nil
}

func TestInstanceRetirementOwnsHandleAndCannotBeReversedByFailure(t *testing.T) {
	for _, failureFirst := range []bool{false, true} {
		name := "teardown-first"
		if failureFirst {
			name = "failure-first"
		}
		t.Run(name, func(t *testing.T) {
			handle := &retirementSubscription{boundarySubscription: boundarySubscription{id: "root"}}
			instance := &projectionInstance{id: "root", node: recoveryNode("root"), subscription: handle, query: handle}
			p := &Projection{ctx: t.Context(), limits: Limits{}.WithDefaults(), instances: map[string]*projectionInstance{}, failed: map[string]*projectionInstance{}}
			if err := p.installInstanceLocked(instance); err != nil {
				t.Fatal(err)
			}
			if failureFirst {
				p.processMu.Lock()
				_, applied := p.failInstance(instance, errors.New("query failed"))
				p.processMu.Unlock()
				if !applied || len(p.failed) != 1 {
					t.Fatal("current instance did not enter branch failure")
				}
			}
			p.closeInstance(instance.id)
			p.processMu.Lock()
			batch, applied := p.failInstance(instance, errors.New("late query failure"))
			p.processMu.Unlock()
			if applied || !reflect.DeepEqual(batch, ProjectionBatch{}) {
				t.Fatal("removed instance accepted a late failure or produced output")
			}
			p.closeInstance(instance.id)
			if handle.closes.Load() != 1 || instance.subscription != nil || instance.query != nil {
				t.Fatal("handle was retained or closed more than once")
			}
			if len(p.instances) != 0 || len(p.failed) != 0 || p.activeSlots != 0 {
				t.Fatal("late failure restored retired state or leaked capacity")
			}
		})
	}
}

func TestProjectionShutdownAndBranchFailureRetireConcurrently(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	for range 100 {
		feed := &recoveryFeed{requests: make(chan q.QuerySubscribeRequest, 10)}
		p, err := BeginProjection(ctx, feed, recoveryNode("root"), StartOptions{AllowPartialErrors: true, Limits: Limits{RecoveryInitialDelay: time.Hour, RecoveryMaxDelay: time.Hour}})
		if err != nil {
			t.Fatal(err)
		}
		if _, _, err := p.TakeInitial(ctx); err != nil {
			p.Close()
			t.Fatal(err)
		}
		if err := p.releaseOutput(p.queue.Offer); err != nil {
			p.Close()
			t.Fatal(err)
		}
		p.mu.Lock()
		id := p.rootID
		instance := p.instances[id]
		p.mu.Unlock()
		start := make(chan struct{})
		var workers sync.WaitGroup
		workers.Go(func() {
			<-start
			_, _, _ = p.ApplyEvent(q.Event{Failure: &q.SubscriptionFailure{SubscriptionID: id, Cause: errors.New("query failed")}})
		})
		workers.Go(func() {
			<-start
			p.Close()
		})
		close(start)
		workers.Wait()
		p.mu.Lock()
		retired := len(p.instances) == 0 && len(p.failed) == 0 && p.activeSlots == 0 && instance.subscription == nil && instance.query == nil
		p.mu.Unlock()
		if !retired {
			t.Fatal("concurrent failure restored retired state or retained handles")
		}
	}
}

func TestBranchRecoveryBudgetSurvivesReplacementAndExhausts(t *testing.T) {
	for _, child := range []bool{false, true} {
		name := "root"
		if child {
			name = "child"
		}
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
			defer cancel()
			feed := &recoveryFeed{failTable: "bad", requests: make(chan q.QuerySubscribeRequest, 100)}
			feed.fail.Store(true)
			root := recoveryNode("bad")
			if child {
				root = recoveryNode("root", root, recoveryNode("good"))
			}
			p, err := BeginProjection(ctx, feed, root, StartOptions{AllowPartialErrors: true, QueueLimit: 8, Limits: Limits{RecoveryInitialDelay: time.Millisecond, RecoveryMaxDelay: 2 * time.Millisecond, RecoveryMaxAttempts: 2}})
			if err != nil {
				t.Fatal(err)
			}
			defer p.Close()
			if _, _, err := p.TakeInitial(ctx); err != nil {
				t.Fatal(err)
			}
			if err := p.releaseOutput(p.queue.Offer); err != nil {
				t.Fatal(err)
			}
			// Child retries publish replacement error placeholders. Consume those
			// exactly as the production scheduler does, so the next one is eligible.
			if child {
				for range 2 {
					batch, err := p.Next(ctx)
					if err != nil {
						t.Fatal(err)
					}
					completeTestPublication(t, p, batch)
				}
			}
			attempts := 0
			for attempts < 3 {
				select {
				case r := <-feed.requests:
					if r.Query.Relation.Name == "bad" {
						attempts++
					}
				case <-ctx.Done():
					t.Fatal("expected retries did not run", attempts)
				}
			}
			deadline := time.NewTimer(50 * time.Millisecond)
			defer deadline.Stop()
		collect:
			for {
				select {
				case r := <-feed.requests:
					if r.Query.Relation.Name == "bad" {
						attempts++
					}
				case <-deadline.C:
					break collect
				}
			}
			if attempts != 3 || p.Err() != nil {
				t.Fatalf("attempts=%d err=%v", attempts, p.Err())
			}
			p.mu.Lock()
			for _, failed := range p.failed {
				if !failed.recovery.Next().IsZero() {
					t.Error("exhausted branch retained a timer")
				}
			}
			p.mu.Unlock()
		})
	}
}

func TestFailedBranchCannotAcceptLateWindowResultOrData(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	feed := &recoveryFeed{requests: make(chan q.QuerySubscribeRequest, 8)}
	p, err := BeginProjection(ctx, feed, recoveryNode("root"), StartOptions{AllowPartialErrors: true, Limits: Limits{RecoveryInitialDelay: time.Hour, RecoveryMaxDelay: time.Hour}})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if _, _, err := p.TakeInitial(ctx); err != nil {
		t.Fatal(err)
	}
	if err := p.releaseOutput(p.queue.Offer); err != nil {
		t.Fatal(err)
	}
	p.mu.Lock()
	id := p.rootID
	bounds := p.instances[id].initial.Bounds
	p.mu.Unlock()
	if _, _, err := p.ApplyEvent(q.Event{Failure: &q.SubscriptionFailure{SubscriptionID: id, Cause: errors.New("failed")}}); err != nil {
		t.Fatal(err)
	}
	result, publish, err := p.ApplyDelivery(q.DeliveryEnvelope{Delivery: q.DeliveryMetadata{Kind: q.DeliveryWindow, Revision: 1}, Effects: []q.SubscriptionEffect{{SubscriptionID: id, Batch: q.Batch{Window: &q.WindowTransition{Token: "late", Bounds: q.Bounds{}}}}}})
	if err != nil || !publish || result.Window != nil || result.WindowFailure == nil || result.WindowFailure.Token != "late" {
		t.Fatal("failed branch accepted a window result", result, err)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if !reflect.DeepEqual(p.instances[id].initial.Bounds, bounds) {
		t.Fatal("late window changed recovery bounds")
	}
}

func TestRebindingFailedBranchStartsNewRecoveryBudget(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	feed := &recoveryFeed{failTable: "bad", requests: make(chan q.QuerySubscribeRequest, 100)}
	feed.fail.Store(true)
	child := recoveryNode("bad")
	child.BuildQuery = func(parent storage.Record) (storage.QuerySpec, bool, error) {
		group := "old"
		if value, ok := parent["group"]; ok {
			group = string(value.Data)
		}
		return storage.QuerySpec{Relation: storage.RelationName{Name: "bad"}, Filter: &storage.Predicate{Comparison: &storage.Comparison{Field: "group", Operator: storage.ComparisonEqual, Value: storage.Literal{Type: storage.TypeText, Data: []byte(group)}}}}, true, nil
	}
	p, err := BeginProjection(ctx, feed, recoveryNode("root", child), StartOptions{AllowPartialErrors: true, QueueLimit: 8, Limits: Limits{RecoveryInitialDelay: time.Millisecond, RecoveryMaxDelay: time.Millisecond, RecoveryMaxAttempts: 1}})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if _, _, err := p.TakeInitial(ctx); err != nil {
		t.Fatal(err)
	}
	if err := p.releaseOutput(p.queue.Offer); err != nil {
		t.Fatal(err)
	}
	batch, err := p.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	completeTestPublication(t, p, batch)
	for range 3 {
		<-feed.requests
	} // Root, initial child, single exhausted retry.
	p.mu.Lock()
	root := p.instances[p.rootID]
	key := root.order[0]
	record := storage.CloneRecord(root.members[key].record)
	record["group"] = storage.Value{Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("new")}
	id := root.id
	p.mu.Unlock()
	if _, _, err := p.ApplyDelivery(q.DeliveryEnvelope{Delivery: q.DeliveryMetadata{Kind: q.DeliveryCommit, Checkpoint: &q.Position{SourceID: "test", Value: 11}, Revision: 1, Atomic: true}, Effects: []q.SubscriptionEffect{{SubscriptionID: id, Batch: q.Batch{Changes: []q.Change{{Kind: q.Updated, Row: q.Row{Key: key, Record: record}}}}}}}); err != nil {
		t.Fatal(err)
	}
	for range 2 { // New selector's initial attempt, then its own retry.
		batch, err := p.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		completeTestPublication(t, p, batch)
		select {
		case request := <-feed.requests:
			if string(request.Query.Filter.Comparison.Value.Data) != "new" {
				t.Fatal("old selector retried")
			}
		case <-ctx.Done():
			t.Fatal("new selector did not load")
		}
	}
}

func TestPartialChildLoadFailureKeepsSiblingsAndRecoversAsACompleteSubtree(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	feed := &recoveryFeed{failTable: "bad", requests: make(chan q.QuerySubscribeRequest, 100)}
	feed.fail.Store(true)
	p, err := BeginProjection(ctx, feed, recoveryNode("root", recoveryNode("bad"), recoveryNode("good")), StartOptions{AllowPartialErrors: true, QueueLimit: 8, Limits: Limits{MaxActiveInstances: 3, RecoveryInitialDelay: 10 * time.Millisecond}})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	initial, _, err := p.TakeInitial(ctx)
	if err != nil {
		t.Fatal(err)
	}
	failedID, goodID := "", ""
	for _, event := range initial {
		if event.Type == ProjectionBranchChanged && event.Failure != nil {
			failedID = event.InstanceID
		}
		if event.Type == ProjectionInstanceOpened && event.Node == "good" {
			goodID = event.InstanceID
		}
	}
	if failedID == "" || goodID == "" || p.Err() != nil {
		t.Fatalf("child failure closed healthy projection: %v", initial)
	}
	if err := p.releaseOutput(p.queue.Offer); err != nil {
		t.Fatal(err)
	}
	feed.fail.Store(false)
	next, err := p.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	next = completeTestPublication(t, p, next)
	closed, replaced, loaded := false, false, false
	for _, event := range next.Events {
		if event.Node == "good" || event.Node == "root" {
			t.Fatal("recovery rebuilt an unaffected branch")
		}
		closed = closed || event.Type == ProjectionInstanceClosed && event.InstanceID == failedID
		replaced = replaced || event.Type == ProjectionInstanceOpened && event.Node == "bad" && event.InstanceID != failedID
		loaded = loaded || event.Type == ProjectionRowChanged && event.Node == "bad"
	}
	if !closed || !replaced || !loaded {
		t.Fatalf("incomplete recovery: %v", next.Events)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.instances) != 3 || p.activeSlots != 3 || p.instances[goodID] == nil || p.instances[failedID] != nil {
		t.Fatal("recovery leaked or retired unrelated instances")
	}
}

func TestPartialRootFailureRetriesAtTheExactSavedWindow(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	feed := &recoveryFeed{failTable: "root", requests: make(chan q.QuerySubscribeRequest, 100)}
	p, err := BeginProjection(ctx, feed, recoveryNode("root"), StartOptions{AllowPartialErrors: true, QueueLimit: 8, Limits: Limits{MaxActiveInstances: 1, RecoveryInitialDelay: 10 * time.Millisecond}})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if _, _, err := p.TakeInitial(ctx); err != nil {
		t.Fatal(err)
	}
	if err := p.releaseOutput(p.queue.Offer); err != nil {
		t.Fatal(err)
	}
	<-feed.requests
	old := p.rootID
	saved := q.Bounds{Left: q.Boundary{Kind: q.BoundaryCursor, Cursor: []byte("m")}, Right: q.Boundary{Kind: q.BoundaryCursor, Cursor: []byte("t")}}
	_, _, err = p.ApplyDelivery(q.DeliveryEnvelope{Delivery: q.DeliveryMetadata{Kind: q.DeliveryWindow, Revision: 1, Checkpoint: &q.Position{SourceID: "test", Value: 10}}, Effects: []q.SubscriptionEffect{{SubscriptionID: old, Batch: q.Batch{Window: &q.WindowTransition{Token: "resize", Bounds: saved}}}}})
	if err != nil {
		t.Fatal(err)
	}
	feed.fail.Store(true)
	failed, out, err := p.ApplyEvent(q.Event{Failure: &q.SubscriptionFailure{SubscriptionID: old, Cause: errors.New("disconnected selector")}})
	if err != nil || !out || len(failed.Events) == 0 || p.Err() != nil {
		t.Fatalf("failure was terminal: %v %v", failed, err)
	}
	select {
	case retry := <-feed.requests:
		if retry.Window.Count != 0 || !reflect.DeepEqual(retry.Window.Bounds, &saved) {
			t.Fatalf("recovery changed window: %+v", retry.Window)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	feed.fail.Store(false)
	batch, err := p.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	batch = completeTestPublication(t, p, batch)
	if p.rootID == old || len(batch.Events) < 3 {
		t.Fatal("root did not recover with a complete replacement")
	}
	for _, event := range batch.Events {
		if event.Type == ProjectionInstanceOpened && !reflect.DeepEqual(event.Bounds, saved) {
			t.Fatalf("published changed window: %+v", event.Bounds)
		}
	}
}

func TestPartialInitialRootFailureCanCompleteWithoutSourceEvidence(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	feed := &recoveryFeed{failTable: "root", requests: make(chan q.QuerySubscribeRequest, 10)}
	feed.fail.Store(true)
	p, err := BeginProjection(ctx, feed, recoveryNode("root"), StartOptions{AllowPartialErrors: true, WaitForSync: true, Limits: Limits{RecoveryInitialDelay: time.Hour, RecoveryMaxDelay: time.Hour}})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	image, _, err := p.TakeInitial(ctx)
	if err != nil || len(image) != 2 || image[1].Type != ProjectionBranchChanged || image[1].Failure == nil {
		t.Fatalf("initial error did not settle: %v %v", image, err)
	}
}

func TestPartialLiveChildFailureRecoversAtInstanceLimit(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	feed := &recoveryFeed{requests: make(chan q.QuerySubscribeRequest, 100)}
	p, err := BeginProjection(ctx, feed, recoveryNode("root", recoveryNode("bad"), recoveryNode("good")), StartOptions{AllowPartialErrors: true, QueueLimit: 8, Limits: Limits{MaxActiveInstances: 3, RecoveryInitialDelay: 10 * time.Millisecond}})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	initial, _, err := p.TakeInitial(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := p.releaseOutput(p.queue.Offer); err != nil {
		t.Fatal(err)
	}
	var failedID, healthyID string
	for _, event := range initial {
		if event.Type == ProjectionInstanceOpened {
			if event.Node == "bad" {
				failedID = event.InstanceID
			}
			if event.Node == "good" {
				healthyID = event.InstanceID
			}
		}
	}
	if _, changed, err := p.ApplyEvent(q.Event{Failure: &q.SubscriptionFailure{SubscriptionID: failedID, Cause: errors.New("temporarily unavailable")}}); err != nil || !changed {
		t.Fatalf("failure did not remain branch-local: %t, %v", changed, err)
	}
	batch, err := p.Next(ctx)
	if err != nil {
		t.Fatal("child recovery failed the projection:", err)
	}
	batch = completeTestPublication(t, p, batch)
	closed, loaded := false, false
	for _, event := range batch.Events {
		if event.Node != "bad" {
			t.Fatal("recovery rebuilt a healthy branch")
		}
		closed = closed || event.Type == ProjectionInstanceClosed && event.InstanceID == failedID
		loaded = loaded || event.Type == ProjectionRowChanged && event.InstanceID != failedID
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if !closed || !loaded || p.activeSlots != 3 || len(p.instances) != 3 || p.instances[healthyID] == nil {
		t.Fatal("recovery did not preserve healthy instances and the capacity limit")
	}
}

type siblingRecoveryFeed struct {
	recoveryFeed
	mu        sync.Mutex
	loads     map[string]int
	installed chan string
	resume    chan struct{}
}

func (f *siblingRecoveryFeed) SubscribeQuery(ctx context.Context, request q.QuerySubscribeRequest, install q.QuerySnapshotInstaller) error {
	name := request.Query.Relation.Name
	f.mu.Lock()
	f.loads[name]++
	attempt := f.loads[name]
	f.mu.Unlock()
	return f.recoveryFeed.SubscribeQuery(ctx, request, func(handle q.QuerySubscription, snapshot q.QuerySnapshot) error {
		if err := install(handle, snapshot); err != nil {
			return err
		}
		if name == "first" && attempt == 2 {
			f.installed <- request.ID
			select {
			case <-f.resume:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	})
}

func TestSiblingRecoveryDuringLoadPreservesCoverageAndDoesNotDuplicateQueries(t *testing.T) {
	for _, limit := range []int{3, 10} {
		for _, parentUpdate := range []bool{false, true} {
			name := fmt.Sprintf("limit-%d/parent-update-%t", limit, parentUpdate)
			t.Run(name, func(t *testing.T) {
				ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
				defer cancel()
				feed := &siblingRecoveryFeed{recoveryFeed: recoveryFeed{requests: make(chan q.QuerySubscribeRequest, 100)}, loads: map[string]int{}, installed: make(chan string, 1), resume: make(chan struct{})}
				p, err := BeginProjection(ctx, feed, recoveryNode("root", recoveryNode("first"), recoveryNode("second")), StartOptions{AllowPartialErrors: true, QueueLimit: 8, Limits: Limits{MaxActiveInstances: limit, MaxConcurrentExpansions: 2, RecoveryInitialDelay: 5 * time.Millisecond, RecoveryMaxDelay: 10 * time.Millisecond}})
				if err != nil {
					t.Fatal(err)
				}
				defer p.Close()
				initial, _, err := p.TakeInitial(ctx)
				if err != nil {
					t.Fatal(err)
				}
				if err := p.releaseOutput(p.queue.Offer); err != nil {
					t.Fatal(err)
				}
				ids := map[string]string{}
				for _, event := range initial {
					if event.Type == ProjectionInstanceOpened {
						ids[event.Node] = event.InstanceID
					}
				}
				fail := func(id string) {
					t.Helper()
					if _, changed, err := p.ApplyEvent(q.Event{Failure: &q.SubscriptionFailure{SubscriptionID: id, Cause: errors.New("temporarily unavailable")}}); err != nil || !changed {
						t.Fatalf("branch failure was not isolated: changed=%t err=%v", changed, err)
					}
				}
				fail(ids["first"])
				var firstReplacement string
				select {
				case firstReplacement = <-feed.installed:
				case <-ctx.Done():
					t.Fatal("first recovery did not install")
				}
				if parentUpdate {
					p.mu.Lock()
					root := p.instances[p.rootID]
					key := root.order[0]
					record := storage.CloneRecord(root.members[key].record)
					p.mu.Unlock()
					record["title"] = storage.Value{Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("updated parent")}
					if _, _, err := p.ApplyDelivery(q.DeliveryEnvelope{Delivery: q.DeliveryMetadata{Kind: q.DeliveryCommit, Checkpoint: &q.Position{SourceID: "test", Value: 11}, Revision: 1, Atomic: true}, Effects: []q.SubscriptionEffect{{SubscriptionID: ids["root"], Batch: q.Batch{Changes: []q.Change{{Kind: q.Updated, Row: q.Row{Key: key, Record: record}}}}}}}); err != nil {
						t.Fatal(err)
					}
				}
				fail(ids["second"])
				// Wait for the second retry to remove its old coverage while the
				// first pass is still blocked. No arbitrary sleep decides the race.
				tick := time.NewTicker(time.Millisecond)
				defer tick.Stop()
				for {
					p.mu.Lock()
					_, pending := p.failed[ids["second"]]
					p.mu.Unlock()
					if !pending {
						break
					}
					select {
					case <-tick.C:
					case <-p.ctx.Done():
						t.Fatalf("overlapping recovery terminated projection: %v", p.Err())
					}
				}
				close(feed.resume)
				batch, err := p.Next(ctx)
				if err != nil {
					t.Fatal("overlapping recovery failed:", err)
				}
				completeTestPublication(t, p, batch)
				if err := p.expansions.waitIdle(ctx); err != nil {
					t.Fatal(err)
				}
				p.mu.Lock()
				root := p.instances[p.rootID]
				member := root.members[root.order[0]]
				covered := p.coveredChildrenLocked(member, nil)
				valid := p.rootID == ids["root"] && member.expanded && len(member.children) == 2 && len(covered) == 2 && p.instances[firstReplacement] != nil && p.instances[ids["first"]] == nil && p.instances[ids["second"]] == nil && len(p.instances) == 3 && p.activeSlots == 3 && len(p.failed) == 0
				p.mu.Unlock()
				if !valid || p.Err() != nil {
					t.Fatal("recovery lost a sibling, retained a predecessor, or leaked capacity:", p.Err())
				}
				feed.mu.Lock()
				loads := maps.Clone(feed.loads)
				feed.mu.Unlock()
				if !reflect.DeepEqual(loads, map[string]int{"root": 1, "first": 2, "second": 2}) {
					t.Fatalf("recovery loaded duplicate or unrelated queries: %v", loads)
				}
			})
		}
	}
}
