package transactionmessages

import (
	"context"
	"errors"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/adiom-data/weave/internal/querysubscription/model"
	"github.com/adiom-data/weave/internal/storage"
	"github.com/adiom-data/weave/internal/transactionmessage"
	reactivev1 "github.com/adiom-data/weave/pkg/gen/reactive/v1"
)

func position(value uint64) *querysubscription.Position {
	return &querysubscription.Position{SourceID: "test", Value: value}
}
func testRouter() *Router {
	return &Router{options: Options{MaxQueuedTransactions: 2, MaxQueuedBytes: 1024, ReconnectDelay: time.Millisecond}, ready: true, connections: make(map[string]*Connection)}
}
func routed(t *testing.T, context, payload string) storage.TransactionMessage {
	t.Helper()
	bytes, err := proto.Marshal(&reactivev1.RoutedTransactionMessage{ConnectionContext: context, Kind: "receipt", Payload: []byte(payload)})
	if err != nil {
		t.Fatal(err)
	}
	return storage.TransactionMessage{Kind: RoutedKind, Payload: bytes}
}
func publish(t *testing.T, r *Router, p uint64, messages ...storage.TransactionMessage) {
	t.Helper()
	batch := transactionmessage.Batch{Through: position(p)}
	if len(messages) > 0 {
		batch.Transactions = []*transactionmessage.Transaction{{Position: position(p), Messages: messages}}
	}
	if err := r.publish(batch); err != nil {
		t.Fatal(err)
	}
}

func TestContextsRouteOnlyToTheirConnectionAndDoNotReplay(t *testing.T) {
	r := testRouter()
	a, _ := r.Open()
	b, _ := r.Open()
	defer a.Close()
	defer b.Close()
	if len(a.Context) != 43 || a.Context == b.Context {
		t.Fatal("connection capabilities are not independently random")
	}
	publish(t, r, 10, routed(t, a.Context, "a"), routed(t, b.Context, "b"), routed(t, "unknown", "ignored"), storage.TransactionMessage{}, storage.TransactionMessage{Kind: RoutedKind, Payload: []byte{255}})
	for _, item := range []struct {
		c    *Connection
		want string
	}{{a, "a"}, {b, "b"}} {
		transactions, err := item.c.TakeThrough(context.Background(), position(10))
		if err != nil || len(transactions) != 1 || len(transactions[0].Messages) != 1 || string(transactions[0].Messages[0].Payload) != item.want {
			t.Fatalf("route = %+v, %v", transactions, err)
		}
		if again, _ := item.c.TakeThrough(context.Background(), position(10)); len(again) != 0 {
			t.Fatal("message delivered twice")
		}
	}
	a.Close()
	fresh, _ := r.Open()
	defer fresh.Close()
	publish(t, r, 20, routed(t, a.Context, "stale"))
	if transactions, err := fresh.TakeThrough(context.Background(), position(20)); err != nil || len(transactions) != 0 {
		t.Fatal("old context was reused by a new connection")
	}
}

func TestEmptyMailboxDoesNotEstablishCoverage(t *testing.T) {
	r := testRouter()
	c, _ := r.Open()
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := c.TakeThrough(ctx, position(20)); done <- err }()
	publish(t, r, 10)
	select {
	case <-done:
		t.Fatal("empty mailbox incorrectly established coverage")
	default:
	}
	publish(t, r, 20)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("covered position did not release waiter")
	}
}

func TestOnlyAddressedConnectionsReceiveMessageNotifications(t *testing.T) {
	r := testRouter()
	recipient, _ := r.Open()
	idle, _ := r.Open()
	defer recipient.Close()
	defer idle.Close()
	publish(t, r, 10, routed(t, recipient.Context, "receipt"))
	select {
	case <-recipient.Wake():
	default:
		t.Fatal("recipient was not notified")
	}
	for _, c := range []*Connection{recipient, idle} {
		select {
		case <-c.Wake():
			t.Fatal("unaddressed connection was notified")
		default:
		}
	}
	// Neither unrelated metadata nor progress-only traffic needs to wake the
	// connection's message scheduler, even if it already has pending messages.
	publish(t, r, 20, routed(t, "another-server", "ignored"))
	publish(t, r, 30)
	for _, c := range []*Connection{recipient, idle} {
		select {
		case <-c.Wake():
			t.Fatal("source progress woke a message scheduler")
		default:
		}
	}
}

func waitForCoverageRegistration(t *testing.T, r *Router) {
	t.Helper()
	deadline := time.After(time.Second)
	for {
		r.mu.Lock()
		registered := r.coverage != nil
		r.mu.Unlock()
		if registered {
			return
		}
		select {
		case <-deadline:
			t.Fatal("coverage waiter did not register")
		case <-time.After(time.Millisecond):
		}
	}
}

func TestCoverageWaitsDoNotConsumeMessageNotifications(t *testing.T) {
	r := testRouter()
	a, _ := r.Open()
	b, _ := r.Open()
	defer a.Close()
	defer b.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	done := make(chan error, 2)
	for _, c := range []*Connection{a, b} {
		go func(c *Connection) { _, err := c.TakeThrough(ctx, position(20)); done <- err }(c)
	}
	waitForCoverageRegistration(t, r)
	publish(t, r, 10, routed(t, a.Context, "receipt"))
	select {
	case <-a.Wake():
	case <-ctx.Done():
		t.Fatal("coverage wait consumed addressed-message notification")
	}
	publish(t, r, 20)
	for range 2 {
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
		case <-ctx.Done():
			t.Fatal("lost coverage wakeup")
		}
	}
}

func TestCoverageWaitStopsOnCancellationAndConnectionClose(t *testing.T) {
	for _, closeConnection := range []bool{false, true} {
		t.Run(map[bool]string{false: "cancel", true: "close"}[closeConnection], func(t *testing.T) {
			r := testRouter()
			c, _ := r.Open()
			defer c.Close()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() { _, err := c.TakeThrough(ctx, position(20)); done <- err }()
			waitForCoverageRegistration(t, r)
			want := context.Canceled
			if closeConnection {
				c.Close()
				want = ErrUnavailable
			} else {
				cancel()
			}
			select {
			case err := <-done:
				if !errors.Is(err, want) {
					t.Fatalf("wait error = %v", err)
				}
			case <-time.After(time.Second):
				t.Fatal("coverage wait did not stop")
			}
			// A canceled waiter leaves no connection registration to retire, and
			// later source progress remains safe to publish.
			publish(t, r, 20)
		})
	}
}

func TestSlowConnectionIsIsolated(t *testing.T) {
	r := testRouter()
	r.options.MaxQueuedTransactions = 1
	a, _ := r.Open()
	b, _ := r.Open()
	defer a.Close()
	defer b.Close()
	publish(t, r, 10, routed(t, a.Context, "a"), routed(t, b.Context, "b"))
	if _, err := b.TakeThrough(context.Background(), position(10)); err != nil {
		t.Fatal(err)
	}
	publish(t, r, 20, routed(t, a.Context, "a"), routed(t, b.Context, "b"))
	if !errors.Is(a.Err(), ErrBackpressure) {
		t.Fatal("slow connection did not fail")
	}
	if transactions, err := b.TakeThrough(context.Background(), position(20)); err != nil || len(transactions) != 1 {
		t.Fatal("slow connection affected another recipient")
	}
}

func TestInvalidBatchDoesNotPublishPartialRouting(t *testing.T) {
	r := testRouter()
	c, _ := r.Open()
	defer c.Close()
	batch := transactionmessage.Batch{Through: position(10), Transactions: []*transactionmessage.Transaction{
		{Position: position(10), Messages: []storage.TransactionMessage{routed(t, c.Context, "a")}},
		{Position: position(9)},
	}}
	if !errors.Is(r.publish(batch), errProtocol) || c.HasPending() || r.through != nil {
		t.Fatal("invalid batch partially mutated routing state")
	}
}

type sourceResult struct {
	batch transactionmessage.Batch
	err   error
}
type testStream struct{ results chan sourceResult }

func (s *testStream) Receive(ctx context.Context) (transactionmessage.Batch, error) {
	select {
	case result := <-s.results:
		return result.batch, result.err
	case <-ctx.Done():
		return transactionmessage.Batch{}, ctx.Err()
	}
}
func (*testStream) Close() error { return nil }

type testClient struct {
	streams chan transactionmessage.Stream
}

func (c testClient) Open(ctx context.Context) (transactionmessage.Stream, error) {
	select {
	case s := <-c.streams:
		return s, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func TestSourceGapRetiresContextsAndReconnectsForNewConnections(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	a, b := &testStream{make(chan sourceResult, 1)}, &testStream{make(chan sourceResult, 1)}
	client := testClient{make(chan transactionmessage.Stream, 2)}
	client.streams <- a
	r, err := Start(ctx, client, Options{MaxQueuedTransactions: 4, MaxQueuedBytes: 1024, ReconnectDelay: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	old, _ := r.Open()
	a.results <- sourceResult{err: storage.ErrResumeRequired}
	select {
	case <-old.Done():
	case <-ctx.Done():
		t.Fatal("gap did not retire context")
	}
	if !errors.Is(old.Err(), storage.ErrResumeRequired) {
		t.Fatal(old.Err())
	}
	if _, err := r.Open(); !errors.Is(err, ErrUnavailable) {
		t.Fatal("accepted connection while source disconnected")
	}
	client.streams <- b
	var fresh *Connection
	for fresh == nil {
		fresh, err = r.Open()
		if err != nil && !errors.Is(err, ErrUnavailable) {
			t.Fatal(err)
		}
		if fresh == nil {
			select {
			case <-time.After(time.Millisecond):
			case <-ctx.Done():
				t.Fatal("source did not reconnect")
			}
		}
	}
	defer fresh.Close()
	if fresh.Context == old.Context {
		t.Fatal("reused retired capability")
	}
	b.results <- sourceResult{batch: transactionmessage.Batch{Through: position(20), Transactions: []*transactionmessage.Transaction{{Position: position(20), Messages: []storage.TransactionMessage{routed(t, old.Context, "stale"), routed(t, fresh.Context, "fresh")}}}}}
	transactions, err := fresh.TakeThrough(ctx, position(20))
	if err != nil || len(transactions) != 1 || len(transactions[0].Messages) != 1 || string(transactions[0].Messages[0].Payload) != "fresh" {
		t.Fatalf("recovered route = %+v, %v", transactions, err)
	}
}
