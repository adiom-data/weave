package projectionserver

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/adiom-data/weave/internal/engine"
	"github.com/adiom-data/weave/internal/postgres"
	"github.com/adiom-data/weave/internal/postgres/cdc"
	"github.com/adiom-data/weave/internal/projection/transactionmessages"
	querylocal "github.com/adiom-data/weave/internal/querysubscription/local"
	"github.com/adiom-data/weave/internal/querysubscription/model"
	"github.com/adiom-data/weave/internal/transactionmessage"
	reactivev1 "github.com/adiom-data/weave/pkg/gen/reactive/v1"
)

func messageRouter(t *testing.T) (*cdc.Broker, *transactionmessages.Router) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	broker := cdc.NewBroker()
	broker.MarkReady()
	source := postgres.NewPostgresDataSourceWithOptions(nil, broker, postgres.DataSourceOptions{})
	service, err := transactionmessage.Start(ctx, source, transactionmessage.Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { service.Close() })
	router, err := transactionmessages.Start(ctx, transactionmessage.NewLocalClient(service), transactionmessages.Options{MaxQueuedTransactions: 16, MaxQueuedBytes: 1 << 20, ReconnectDelay: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { router.Close() })
	return broker, router
}

func commitReceipt(t *testing.T, broker *cdc.Broker, p uint64, connectionContext string) {
	t.Helper()
	payload, err := proto.Marshal(&reactivev1.RoutedTransactionMessage{ConnectionContext: connectionContext, Kind: "receipt", Payload: []byte("mutation-1")})
	if err != nil {
		t.Fatal(err)
	}
	if err := broker.Publish(context.Background(), cdc.Transaction{CommitLSN: pglogrepl.LSN(p), Messages: []cdc.Message{{Prefix: transactionmessages.RoutedKind, Content: payload}}}); err != nil {
		t.Fatal(err)
	}
}

func messagePosition(p uint64) *querysubscription.Position {
	return &querysubscription.Position{SourceID: postgres.SourceID, Value: p}
}

type joinTestFeed struct {
	querysubscription.Feed
	events   chan querysubscription.Event
	progress func(context.Context) error
}

func (f *joinTestFeed) Next(ctx context.Context) (querysubscription.Event, error) {
	select {
	case e := <-f.events:
		return e, nil
	case <-ctx.Done():
		return querysubscription.Event{}, ctx.Err()
	}
}
func (f *joinTestFeed) RequestProgress(ctx context.Context) error {
	if f.progress != nil {
		return f.progress(ctx)
	}
	return nil
}

func TestJoinWaitsForMessageCoverageAndPairsExactTransaction(t *testing.T) {
	broker, router := messageRouter(t)
	route, _ := router.Open()
	defer route.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	feed := &joinTestFeed{events: make(chan querysubscription.Event, 1)}
	out := make(chan outboundMessage, 4)
	go forwardJoinedFeed(ctx, feed, route, out, time.Millisecond)
	feed.events <- querysubscription.Event{Delivery: &querysubscription.DeliveryEnvelope{Delivery: querysubscription.DeliveryMetadata{Kind: querysubscription.DeliveryCommit, Checkpoint: messagePosition(10), Atomic: true, Revision: 1}}}
	select {
	case <-out:
		t.Fatal("query advanced before message coverage")
	case <-time.After(10 * time.Millisecond):
	}
	commitReceipt(t, broker, 10, route.Context)
	select {
	case raw := <-out:
		item := raw.(queryEventMessage)
		if item.err != nil || len(item.messages) != 1 || item.event.Delivery.Delivery.Checkpoint.Value != 10 {
			t.Fatalf("joined delivery = %+v", item)
		}
	case <-ctx.Done():
		t.Fatal("join stalled")
	}
}

func TestJoinRequestsProgressForReceiptWithoutSubscriptions(t *testing.T) {
	broker, router := messageRouter(t)
	route, _ := router.Open()
	defer route.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	feed := &joinTestFeed{events: make(chan querysubscription.Event, 2)}
	attempts := 0
	feed.progress = func(ctx context.Context) error {
		attempts++
		if attempts == 1 {
			return nil // No known progress yet; no event is emitted.
		}
		select {
		case feed.events <- querysubscription.Event{Progress: &querysubscription.Progress{Position: messagePosition(10), Revision: 1}}:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	out := make(chan outboundMessage, 4)
	go forwardJoinedFeed(ctx, feed, route, out, time.Millisecond)
	commitReceipt(t, broker, 10, route.Context)
	select {
	case raw := <-out:
		message, ok := raw.(transactionMessage)
		if !ok || message.transaction.Position.Order() != 10 {
			t.Fatalf("message-only = %+v", raw)
		}
	case <-ctx.Done():
		t.Fatal("receipt-only join stalled")
	}
	select {
	case raw := <-out:
		if raw.(queryEventMessage).event.Progress == nil {
			t.Fatal("lost query progress")
		}
	case <-ctx.Done():
		t.Fatal("lost query progress")
	}
}

func TestProjectionConnectionWithNoSubscriptionsReceivesMessageOnlyCommit(t *testing.T) {
	broker, router := messageRouter(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	source := postgres.NewPostgresDataSourceWithOptions(nil, broker, postgres.DataSourceOptions{})
	coordinator, err := engine.StartCoordinator(ctx, source)
	if err != nil {
		t.Fatal(err)
	}
	stream := &authorizationCommitStream{ctx: ctx, received: make(chan *reactivev1.ClientMessage), sent: make(chan *reactivev1.ServerMessage, 8)}
	result := make(chan error, 1)
	go func() {
		result <- newServerWithRouter(querylocal.New(coordinator), router).Open(stream)
	}()
	opened := receiveAuthorizationMessage(t, stream.sent).GetConnectionOpened()
	if opened == nil || opened.ConnectionContext == "" {
		t.Fatal("missing routing context")
	}
	commitReceipt(t, broker, 10, opened.ConnectionContext)
	message := receiveAuthorizationMessage(t, stream.sent)
	batch := message.GetModelBatch()
	if batch == nil || batch.Kind != reactivev1.ModelBatch_COMMIT || len(batch.Messages) != 1 || len(batch.Entities) != 0 || message.ConnectionSequence != 1 || batch.Checkpoint != "10" {
		t.Fatalf("message-only commit = %+v", message)
	}
	cancel()
	select {
	case err := <-result:
		if err != nil && !errors.Is(err, context.Canceled) && status.Code(err) != codes.Canceled {
			t.Fatalf("connection close: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("connection did not stop")
	}
}

func TestReceiptAndVisibleEntityUpdateShareModelBatch(t *testing.T) {
	broker, router := messageRouter(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	key := modelKey(snapshotTestRelation(), "task")
	queries := newProjectionLifecycleClient(querysubscription.QuerySnapshot{
		Bounds: snapshotTestBounds(), Revision: 1, Checkpoint: messagePosition(1),
		Rows: []querysubscription.QueryRow{{Row: querysubscription.Row{Key: key, Record: modelRecord("task", "1", "original")}}},
	})
	stream := &authorizationCommitStream{ctx: ctx, received: make(chan *reactivev1.ClientMessage, 1), sent: make(chan *reactivev1.ServerMessage, 8)}
	done := make(chan error, 1)
	go func() {
		done <- newServerWithRouter(queries, router).Open(stream)
	}()
	defer func() {
		cancel()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Error("service did not stop")
		}
	}()
	opened := receiveAuthorizationMessage(t, stream.sent).GetConnectionOpened()
	stream.received <- &reactivev1.ClientMessage{SubscriptionId: "direct", RequestId: "subscribe", Operation: &reactivev1.ClientMessage_Subscribe{Subscribe: &reactivev1.Subscribe{
		Query: snapshotTestRequest(false, false).Root.GetList().Query, Window: &reactivev1.InitialWindow{From: reactivev1.InitialWindow_START, Count: 10},
	}}}
	if receiveAuthorizationMessage(t, stream.sent).GetModelBatch() == nil || receiveAuthorizationMessage(t, stream.sent).GetReady() == nil {
		t.Fatal("subscription did not initialize")
	}
	sub := <-queries.opened
	// Metadata arrives first. It must not escape before this feed proves query
	// processing has reached the commit, including its entity update.
	commitReceipt(t, broker, 10, opened.ConnectionContext)
	if err := sub.offer(querysubscription.DeliveryEnvelope{Delivery: querysubscription.DeliveryMetadata{Kind: querysubscription.DeliveryCommit, Checkpoint: messagePosition(10), Atomic: true, Revision: 2}, Effects: []querysubscription.SubscriptionEffect{{SubscriptionID: sub.id, Batch: querysubscription.Batch{Changes: []querysubscription.Change{{Kind: querysubscription.Updated, Row: querysubscription.Row{Key: key, Record: modelRecord("task", "1", "updated")}}}}}}}); err != nil {
		t.Fatal(err)
	}
	message := receiveAuthorizationMessage(t, stream.sent)
	batch := message.GetModelBatch()
	if batch == nil || batch.Kind != reactivev1.ModelBatch_COMMIT || !batch.Atomic || len(batch.Messages) != 1 || len(batch.Entities) != 1 || batch.Checkpoint != "10" {
		t.Fatalf("not one atomic update/receipt: %v", message)
	}
	if got := batch.Entities[0].GetUpsert().GetValues()["title"].GetTextValue(); got != "updated" {
		t.Fatalf("update = %q", got)
	}
}
