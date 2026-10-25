package transactionmessage

import (
	"context"
	"errors"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/adiom-data/weave/internal/messagebudget"
	"github.com/adiom-data/weave/internal/storage"
)

type testPosition uint64

func (testPosition) Source() storage.SourceID { return "test" }
func (p testPosition) Order() uint64          { return uint64(p) }
func (testPosition) StableID() string         { return "test" }
func (testPosition) StorageCheckpoint()       {}

type foreignPosition testPosition

func (foreignPosition) Source() storage.SourceID { return "foreign" }
func (p foreignPosition) Order() uint64          { return uint64(p) }
func (foreignPosition) StableID() string         { return "foreign" }
func (foreignPosition) StorageCheckpoint()       {}

type namedPosition struct {
	testPosition
	source storage.SourceID
}

func (p namedPosition) Source() storage.SourceID { return p.source }

func newService(t *testing.T, options Options) *service {
	t.Helper()
	options, err := options.normalized()
	if err != nil {
		t.Fatal(err)
	}
	options, err = options.forSource("test")
	if err != nil {
		t.Fatal(err)
	}
	return &service{options: options, sourceID: "test", feeds: make(map[*messageFeed]struct{})}
}

func openFeed(t *testing.T, s *service) *messageFeed {
	t.Helper()
	f, err := s.Open()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { f.Close() })
	return f.(*messageFeed)
}

func receive(t *testing.T, feed Feed) Batch {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	batch, err := feed.Receive(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return batch
}

func messageEvent(order testPosition, payload string) storage.MessageEvent {
	return storage.MessageEvent{Checkpoint: order, Messages: []storage.TransactionMessage{{Kind: "mutation", Payload: []byte(payload)}}}
}

func TestConsumerPositionsDoNotExposeNativeCheckpointFormatting(t *testing.T) {
	s := newService(t, Options{})
	f := openFeed(t, s)
	// This source's native StableID is deliberately unrelated to its ordered
	// coordinate. Consumers must see the same canonical position as remote RPCs.
	publish(t, s, messageEvent(42, "receipt"))
	batch := receive(t, f)
	if batch.Through.SourceID != "test" || batch.Through.Value != 42 || batch.Through.StableID() != "42" || len(batch.Transactions) != 1 || batch.Transactions[0].Position.StableID() != "42" {
		t.Fatalf("native checkpoint leaked into consumer batch: %+v", batch)
	}
}

func publish(t *testing.T, s *service, event storage.MessageEvent) {
	t.Helper()
	if err := s.publish(event); err != nil {
		t.Fatal(err)
	}
}

func TestBroadcastSharesPayloadAndCoalescesProgress(t *testing.T) {
	s := newService(t, Options{})
	a, b := openFeed(t, s), openFeed(t, s)
	event := messageEvent(10, "first")
	publish(t, s, event)
	event.Messages[0].Payload[0] = 'X'
	publish(t, s, storage.MessageEvent{Checkpoint: testPosition(20)})
	for index := 21; index <= 10000; index++ {
		publish(t, s, storage.MessageEvent{Checkpoint: testPosition(index)})
	}
	left, right := receive(t, a), receive(t, b)
	if left.Through.Order() != 10000 || len(left.Transactions) != 1 || string(left.Transactions[0].Messages[0].Payload) != "first" {
		t.Fatalf("batch = %+v", left)
	}
	if left.Transactions[0] != right.Transactions[0] {
		t.Fatal("broadcast duplicated immutable transaction payloads")
	}
	if len(a.transactions) != 0 || a.queuedBytes != 0 {
		t.Fatal("drained queue retained payloads")
	}
}

func TestBatchWatermarkNeverCoversUnsentMessages(t *testing.T) {
	s := newService(t, Options{MaxBatchTransactions: 1})
	f := openFeed(t, s)
	publish(t, s, messageEvent(10, "one"))
	publish(t, s, messageEvent(20, "two"))
	publish(t, s, storage.MessageEvent{Checkpoint: testPosition(30)})
	first := receive(t, f)
	if first.Through.Order() != 10 || len(first.Transactions) != 1 {
		t.Fatalf("first batch = %+v", first)
	}
	second := receive(t, f)
	if second.Through.Order() != 30 || len(second.Transactions) != 1 || second.Transactions[0].Position.Order() != 20 {
		t.Fatalf("second batch = %+v", second)
	}
}

func TestBatchByteLimitPreservesWholeTransactions(t *testing.T) {
	s := newService(t, Options{MaxBatchBytes: 256})
	f := openFeed(t, s)
	for _, order := range []testPosition{10, 20, 30} {
		publish(t, s, messageEvent(order, "payload"))
	}
	for _, order := range []uint64{10, 20, 30} {
		batch := receive(t, f)
		if len(batch.Transactions) != 1 || batch.Through.Order() != order {
			t.Fatalf("byte-limited batch = %+v", batch)
		}
	}
}

func TestOptionsRejectInvalidBudgetsBeforeOpeningSource(t *testing.T) {
	source := &fakeSource{}
	for _, options := range []Options{
		{MaxQueuedTransactions: -1}, {MaxQueuedBytes: -1},
		{MaxBatchTransactions: -1}, {MaxBatchBytes: -1},
		{MaxQueuedBytes: 256, MaxBatchBytes: 512},
	} {
		if _, err := start(context.Background(), source, options); err == nil {
			t.Fatalf("accepted limits: %+v", options)
		}
	}
	if source.opens.Load() != 0 {
		t.Fatal("invalid configuration opened the source")
	}
}

func TestSourceAwareBatchLimitIsValidatedBeforeOpening(t *testing.T) {
	source := &fakeSource{id: "postgres", feed: &fakeFeed{events: make(chan storage.MessageEvent)}}
	if _, err := start(context.Background(), source, Options{MaxBatchBytes: 200}); err == nil {
		t.Fatal("accepted batch too small for the PostgreSQL source identifier")
	}
	if source.opens.Load() != 0 {
		t.Fatal("invalid source-aware configuration opened a feed")
	}
	// Exact boundary: two position envelopes plus identifiers and two empty
	// messages. Clamping happens once at startup, not on the first transaction.
	minimum := positionOverheadBytes + batchOverheadBytes + 2*len(source.ID()) + 2*messagebudget.MessageOverhead
	s, err := start(context.Background(), source, Options{MaxBatchBytes: minimum})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if s.options.MessageLimits.MaxTransactionBytes != 2*messagebudget.MessageOverhead {
		t.Fatal("transaction budget was not clamped at startup")
	}
	opened := openFeed(t, s)
	source.feed.events <- storage.MessageEvent{Checkpoint: namedPosition{testPosition: 10, source: source.ID()}, Messages: []storage.TransactionMessage{{Kind: "large", Payload: make([]byte, 1024)}}}
	batch := receive(t, opened)
	if batch.Through.Order() != 10 || len(batch.Transactions) != 1 || len(batch.Transactions[0].Messages) != 1 || batch.Transactions[0].Messages[0].Kind != "" || len(batch.Transactions[0].Messages[0].Payload) != 0 || opened.Err() != nil {
		t.Fatal("minimum valid batch failed on its first message")
	}
	if _, err := (Options{MaxBatchBytes: 1024}).forSource(""); err == nil {
		t.Fatal("accepted empty source identifier")
	}
}

func TestQueueOverflowIsTerminalAndDoesNotAffectHealthyFeed(t *testing.T) {
	for _, options := range []Options{
		{MaxQueuedTransactions: 1},
		{MaxBatchBytes: 256, MaxQueuedBytes: 256},
	} {
		t.Run("bounded", func(t *testing.T) {
			s := newService(t, options)
			slow, healthy := openFeed(t, s), openFeed(t, s)
			for _, order := range []testPosition{10, 20, 30} {
				publish(t, s, messageEvent(order, "one"))
				if batch := receive(t, healthy); batch.Through.Order() != order.Order() {
					t.Fatal("healthy consumer stalled")
				}
			}
			if _, err := slow.Receive(context.Background()); !errors.Is(err, ErrSlowConsumer) {
				t.Fatalf("overflow = %v", err)
			}
			publish(t, s, storage.MessageEvent{Checkpoint: testPosition(40)})
			if _, err := slow.Receive(context.Background()); !errors.Is(err, ErrSlowConsumer) {
				t.Fatalf("overflow was not terminal: %v", err)
			}
		})
	}
}

func TestOversizedMessageBecomesEmptyWithoutFailingFeeds(t *testing.T) {
	s := newService(t, Options{MaxBatchBytes: 200})
	f := openFeed(t, s)
	publish(t, s, messageEvent(10, string(make([]byte, 200))))
	batch := receive(t, f)
	if batch.Through.Order() != 10 || len(batch.Transactions) != 1 || len(batch.Transactions[0].Messages) != 1 || batch.Transactions[0].Messages[0].Kind != "" || len(batch.Transactions[0].Messages[0].Payload) != 0 {
		t.Fatalf("placeholder = %+v", batch)
	}
	publish(t, s, storage.MessageEvent{Checkpoint: testPosition(20)})
	if receive(t, f).Through.Order() != 20 || f.Err() != nil {
		t.Fatal("oversize terminated the stream")
	}
}

func TestCloseAndCancellationUnblockReceivers(t *testing.T) {
	s := newService(t, Options{})
	f := openFeed(t, s)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := f.Receive(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { _, err := f.Receive(context.Background()); done <- err }()
	f.Close()
	select {
	case err := <-done:
		if !errors.Is(err, io.EOF) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("close did not unblock receiver")
	}
}

func TestRejectInvalidSourceEvents(t *testing.T) {
	for _, event := range []storage.MessageEvent{
		{},
		{Messages: []storage.TransactionMessage{{Kind: "x"}}},
		{Checkpoint: foreignPosition(11)},
		{Checkpoint: testPosition(9)},
		messageEvent(10, "late"),
	} {
		s := newService(t, Options{})
		publish(t, s, storage.MessageEvent{Checkpoint: testPosition(10)})
		if err := s.publish(event); err == nil {
			t.Fatalf("accepted invalid event: %+v", event)
		}
	}
}

type fakeSource struct {
	id    storage.SourceID
	feed  *fakeFeed
	opens atomic.Int32
}

func (s *fakeSource) ID() storage.SourceID {
	if s.id != "" {
		return s.id
	}
	return "test"
}
func (s *fakeSource) OpenMessageFeed(context.Context) (storage.MessageFeed, error) {
	s.opens.Add(1)
	return s.feed, nil
}

type fakeFeed struct {
	events  chan storage.MessageEvent
	failure error
	closed  atomic.Bool
}

func (f *fakeFeed) Receive(ctx context.Context) (storage.MessageEvent, error) {
	select {
	case event, ok := <-f.events:
		if !ok {
			if f.failure != nil {
				return storage.MessageEvent{}, f.failure
			}
			return storage.MessageEvent{}, io.EOF
		}
		return event, nil
	case <-ctx.Done():
		return storage.MessageEvent{}, ctx.Err()
	}
}
func (f *fakeFeed) Close() error { f.closed.Store(true); return nil }

func TestServiceSharesOneReaderAndNeverAcknowledgesWAL(t *testing.T) {
	source := &fakeSource{feed: &fakeFeed{events: make(chan storage.MessageEvent, 1)}}
	s, err := start(context.Background(), source, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	a, b := openFeed(t, s), openFeed(t, s)
	source.feed.events <- messageEvent(10, "one")
	if receive(t, a).Through.Order() != 10 || receive(t, b).Through.Order() != 10 {
		t.Fatal("missing broadcast")
	}
	close(source.feed.events)
	<-s.done
	if err := s.Wait(context.Background()); !errors.Is(err, io.EOF) {
		t.Fatalf("owner did not observe source failure: %v", err)
	}
	if source.opens.Load() != 1 || !source.feed.closed.Load() {
		t.Fatal("source ownership violated")
	}
	if _, err := s.Open(); !errors.Is(err, io.EOF) {
		t.Fatalf("new feed accepted after source failure: %v", err)
	}
}

type recoveringSource struct {
	first, next *fakeFeed
	opening     chan struct{}
	resume      chan struct{}
	opens       atomic.Int32
}

func (*recoveringSource) ID() storage.SourceID { return "test" }
func (s *recoveringSource) OpenMessageFeed(ctx context.Context) (storage.MessageFeed, error) {
	if s.opens.Add(1) == 1 {
		return s.first, nil
	}
	close(s.opening)
	select {
	case <-s.resume:
		return s.next, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func TestCaptureGapRetiresOldFeedsAndAllowsFreshStreams(t *testing.T) {
	source := &recoveringSource{
		first:   &fakeFeed{events: make(chan storage.MessageEvent, 1), failure: storage.ErrResumeRequired},
		next:    &fakeFeed{events: make(chan storage.MessageEvent, 1)},
		opening: make(chan struct{}), resume: make(chan struct{}),
	}
	s, err := start(context.Background(), source, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	old := openFeed(t, s)
	source.first.events <- messageEvent(10, "old")
	if receive(t, old).Through.Order() != 10 {
		t.Fatal("missing initial message")
	}
	close(source.first.events)
	select {
	case <-source.opening:
	case <-time.After(time.Second):
		t.Fatal("source did not reopen")
	}
	select {
	case <-old.Done():
	default:
		t.Fatal("old stream not retired")
	}
	if _, err := old.Receive(context.Background()); !errors.Is(err, storage.ErrResumeRequired) {
		t.Fatal(err)
	}
	if _, err := s.Open(); !errors.Is(err, storage.ErrResumeRequired) {
		t.Fatalf("opened stream while capture was unavailable: %v", err)
	}
	close(source.resume)
	deadline := time.After(time.Second)
	var fresh Feed
	for fresh == nil {
		fresh, err = s.Open()
		if err != nil && !errors.Is(err, storage.ErrResumeRequired) {
			t.Fatal(err)
		}
		if fresh == nil {
			select {
			case <-deadline:
				t.Fatal("fresh streams remained unavailable")
			case <-time.After(time.Millisecond):
			}
		}
	}
	defer fresh.Close()
	source.next.events <- messageEvent(20, "new")
	if batch := receive(t, fresh); len(batch.Transactions) != 1 || batch.Transactions[0].Position.Order() != 20 {
		t.Fatalf("fresh stream = %+v", batch)
	}
	if !errors.Is(old.Err(), storage.ErrResumeRequired) {
		t.Fatal("recovery resurrected the old stream")
	}
	if source.opens.Load() != 2 {
		t.Fatal("incorrect recovery ownership")
	}
	close(source.next.events)
	if err := s.Wait(context.Background()); !errors.Is(err, io.EOF) {
		t.Fatalf("fatal source failure hidden: %v", err)
	}
}

func TestDiscardedMessageTransactionsDoNotAllocatePayloadCopies(t *testing.T) {
	for _, withRecipient := range []bool{false, true} {
		allocations := func(oversized bool) float64 {
			s := newService(t, Options{MaxBatchBytes: 200})
			var f *messageFeed
			if withRecipient {
				f = openFeed(t, s)
			}
			event := messageEvent(10, "")
			event.Messages[0] = storage.TransactionMessage{}
			if oversized {
				event.Messages[0].Payload = make([]byte, 1<<20)
			}
			return testing.AllocsPerRun(20, func() {
				s.position = nil
				if err := s.publish(event); err != nil {
					panic(err)
				}
				if f != nil {
					if _, err := f.Receive(context.Background()); err != nil {
						panic(err)
					}
				}
			})
		}
		baseline, actual := allocations(false), allocations(true)
		if actual != baseline {
			t.Fatalf("oversized payload added allocations: recipient=%v baseline=%v actual=%v", withRecipient, baseline, actual)
		}
		if !withRecipient && actual != 0 {
			t.Fatalf("unobserved metadata allocated: %v", actual)
		}
	}
}

func TestPlaceholderPreservesOtherMetadataAndBroadcastCoverage(t *testing.T) {
	s := newService(t, Options{MessageLimits: MessageLimits{MaxMessageBytes: 16, MaxTransactionBytes: 256, MaxMessages: 10}})
	a, b := openFeed(t, s), openFeed(t, s)
	event := messageEvent(10, "")
	event.Messages = []storage.TransactionMessage{{Kind: "a", Payload: []byte("before")}, {Kind: "big", Payload: make([]byte, 1024)}, {Kind: "b", Payload: []byte("after")}}
	publish(t, s, event)
	for _, feed := range []*messageFeed{a, b} {
		batch := receive(t, feed)
		messages := batch.Transactions[0].Messages
		if len(messages) != 3 || messages[0].Kind != "a" || messages[1].Kind != "" || len(messages[1].Payload) != 0 || messages[2].Kind != "b" || batch.Through.Order() != 10 || feed.Err() != nil {
			t.Fatalf("metadata = %+v", messages)
		}
	}
	publish(t, s, messageEvent(20, "next"))
	if receive(t, a).Through.Order() != 20 || receive(t, b).Through.Order() != 20 {
		t.Fatal("metadata omission interrupted broadcast")
	}
}

func TestCloseInterruptsSourceReattachment(t *testing.T) {
	source := &recoveringSource{
		first:   &fakeFeed{events: make(chan storage.MessageEvent), failure: storage.ErrResumeRequired},
		next:    &fakeFeed{events: make(chan storage.MessageEvent)},
		opening: make(chan struct{}), resume: make(chan struct{}),
	}
	service, err := start(context.Background(), source, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer service.Close()
	close(source.first.events)
	select {
	case <-source.opening:
	case <-time.After(time.Second):
		t.Fatal("source did not enter reattachment")
	}
	closed := make(chan struct{})
	go func() { service.Close(); close(closed) }()
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("close blocked on source reattachment")
	}
	if err := service.Wait(context.Background()); !errors.Is(err, context.Canceled) {
		t.Fatalf("shutdown result = %v", err)
	}
}
