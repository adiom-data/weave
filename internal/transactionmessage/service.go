// Package transactionmessage broadcasts committed, opaque transaction metadata
// independently of row subscriptions. It never acknowledges the source feed:
// durable WAL ownership remains with the reactive engine.
package transactionmessage

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync"

	"github.com/adiom-data/weave/internal/messagebudget"
	"github.com/adiom-data/weave/internal/querysubscription/model"
	sourceposition "github.com/adiom-data/weave/internal/querysubscription/position"
	"github.com/adiom-data/weave/internal/storage"
)

var ErrSlowConsumer = errors.New("transaction message consumer fell behind; resynchronize")

// Conservative accounting includes payloads, kinds, and envelope overhead,
// rather than letting arbitrarily many empty messages evade byte limits. These
// bounds also exceed protobuf framing costs without coupling the core to it.
const (
	positionOverheadBytes = 64
	batchOverheadBytes    = 64
)

type Options struct {
	MessageLimits         MessageLimits
	MaxQueuedTransactions int
	MaxQueuedBytes        int
	MaxBatchTransactions  int
	MaxBatchBytes         int
}

// DefaultOptions returns the service's queue, batch, and payload limits.
func DefaultOptions() Options {
	return Options{
		MessageLimits:         MessageLimits(messagebudget.Defaults()),
		MaxQueuedTransactions: 1024, MaxQueuedBytes: 8 << 20,
		MaxBatchTransactions: 128, MaxBatchBytes: 1 << 20,
	}
}

func (o Options) normalized() (Options, error) {
	d := DefaultOptions()
	if o.MessageLimits == (MessageLimits{}) {
		o.MessageLimits = d.MessageLimits
	}
	if err := messagebudget.Limits(o.MessageLimits).Validate(); err != nil {
		return o, err
	}
	if o.MaxQueuedTransactions == 0 {
		o.MaxQueuedTransactions = d.MaxQueuedTransactions
	}
	if o.MaxQueuedBytes == 0 {
		o.MaxQueuedBytes = d.MaxQueuedBytes
	}
	if o.MaxBatchTransactions == 0 {
		o.MaxBatchTransactions = d.MaxBatchTransactions
	}
	if o.MaxBatchBytes == 0 {
		o.MaxBatchBytes = d.MaxBatchBytes
	}
	if o.MaxQueuedTransactions < 1 || o.MaxBatchTransactions < 1 || o.MaxBatchBytes < positionOverheadBytes+batchOverheadBytes+2*messagebudget.MessageOverhead || o.MaxQueuedBytes < o.MaxBatchBytes {
		return o, errors.New("invalid transaction message queue/batch limits")
	}
	return o, nil
}

func (o Options) forSource(source storage.SourceID) (Options, error) {
	if source == "" {
		return o, errors.New("transaction message source ID is required")
	}
	available := o.MaxBatchBytes - batchOverheadBytes - positionOverheadBytes - 2*len(source)
	if available < 2*messagebudget.MessageOverhead {
		return o, errors.New("message batch limit cannot fit placeholders for this source")
	}
	if o.MessageLimits.MaxTransactionBytes > available {
		o.MessageLimits.MaxTransactionBytes = available
	}
	return o, nil
}

// Transaction and Batch are immutable after delivery. Positions share the same
// source-global ordering as query subscriptions, but prove message coverage only.
// Datastore-native checkpoints and resume representations stay inside service.
type Transaction struct {
	Position *querysubscription.Position
	Messages []storage.TransactionMessage
	size     int
}

type Batch struct {
	Through      *querysubscription.Position
	Transactions []*Transaction
}

type service struct {
	mu       sync.Mutex
	options  Options
	sourceID storage.SourceID
	position storage.Checkpoint
	feeds    map[*messageFeed]struct{}
	err      error
	cancel   context.CancelFunc
	done     chan struct{}
}

// Start attaches once to the datastore. All downstream feeds share this reader
// and the immutable message payloads; row payloads are never retained here.
func Start(ctx context.Context, source storage.MessageSource, options Options) (Service, error) {
	return start(ctx, source, options)
}

func start(ctx context.Context, source storage.MessageSource, options Options) (*service, error) {
	options, err := options.normalized()
	if err != nil {
		return nil, err
	}
	if source == nil {
		return nil, errors.New("transaction message source is required")
	}
	sourceID := source.ID()
	options, err = options.forSource(sourceID)
	if err != nil {
		return nil, err
	}
	runCtx, cancel := context.WithCancel(ctx)
	feed, err := source.OpenMessageFeed(runCtx)
	if err != nil {
		cancel()
		return nil, err
	}
	s := &service{options: options, sourceID: sourceID, feeds: make(map[*messageFeed]struct{}), cancel: cancel, done: make(chan struct{})}
	go func() {
		defer close(s.done)
		defer cancel()
		for {
			err := s.consume(runCtx, feed)
			_ = feed.Close()
			if runCtx.Err() != nil {
				s.fail(runCtx.Err())
				return
			}
			s.fail(err)
			if !errors.Is(err, storage.ErrResumeRequired) {
				return
			}
			// A gap is terminal for every old downstream stream. A newly attached
			// source feed can serve fresh streams, but must not silently continue
			// any consumer whose history contains that gap.
			feed, err = source.OpenMessageFeed(runCtx)
			if err != nil {
				s.fail(err)
				return
			}
			s.mu.Lock()
			s.position = nil
			s.err = nil
			s.mu.Unlock()
		}
	}()
	return s, nil
}

func (s *service) consume(ctx context.Context, feed storage.MessageFeed) error {
	for {
		event, err := feed.Receive(ctx)
		if err != nil {
			return err
		}
		if err := s.publish(event); err != nil {
			return err
		}
	}
}

// Wait reports an unrecoverable source/protocol failure to the owner. Capture
// gaps fail existing streams and reopen the source instead of ending Wait.
func (s *service) Wait(ctx context.Context) error {
	select {
	case <-s.done:
		s.mu.Lock()
		defer s.mu.Unlock()
		return s.err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *service) Close() error {
	s.cancel()
	<-s.done
	return nil
}

// Open starts at the service's current position, not at historical messages.
// Its first progress batch (when available) establishes that starting point.
func (s *service) Open() (Feed, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return nil, s.err
	}
	f := &messageFeed{service: s, notify: make(chan struct{}, 1), done: make(chan struct{}), pending: s.position != nil}
	s.feeds[f] = struct{}{}
	return f, nil
}

func (s *service) fail(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.err = err
	for feed := range s.feeds {
		s.removeLocked(feed, err)
	}
}

func (s *service) removeLocked(f *messageFeed, err error) {
	delete(s.feeds, f)
	f.err = err
	f.transactions = nil
	f.queuedBytes = 0
	close(f.done)
	f.signal()
}

func (s *service) publish(event storage.MessageEvent) error {
	position, messages := event.Checkpoint, event.Messages
	if position == nil || position.Source() != s.sourceID {
		return errors.New("source event lacks a valid position")
	}
	// Apply the same bounded-placeholder policy as capture, also respecting the
	// transport's batch size. This protects sources without capture-side limits.
	limits := s.options.MessageLimits
	budget := messagebudget.Budget{Limits: messagebudget.Limits(limits)}
	for _, message := range messages {
		if budget.Add(len(message.Kind), len(message.Payload)) == messagebudget.Skip {
			break
		}
	}
	size := positionOverheadBytes + len(position.Source()) + budget.Bytes
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.position != nil {
		if position.Source() != s.position.Source() || position.Order() < s.position.Order() {
			return errors.New("transaction message source progress regressed or changed source")
		}
		if position.Order() == s.position.Order() {
			if len(messages) > 0 {
				return errors.New("transaction messages repeated an already covered position")
			}
			return nil
		}
	}
	s.position = position
	var transaction *Transaction
	for feed := range s.feeds {
		if len(messages) > 0 {
			if len(feed.transactions) >= s.options.MaxQueuedTransactions || size > s.options.MaxQueuedBytes-feed.queuedBytes {
				s.removeLocked(feed, ErrSlowConsumer)
				continue
			}
			// Only allocate for an accepted transaction with a live recipient.
			// One immutable copy is shared by every eligible downstream feed.
			if transaction == nil {
				transaction = &Transaction{Position: sourceposition.FromSource(position), size: size, Messages: make([]storage.TransactionMessage, 0, budget.Count+1)}
				copyBudget := messagebudget.Budget{Limits: messagebudget.Limits(limits)}
				for _, message := range messages {
					decision := copyBudget.Add(len(message.Kind), len(message.Payload))
					if decision == messagebudget.Skip {
						break
					}
					if decision == messagebudget.Empty {
						transaction.Messages = append(transaction.Messages, storage.TransactionMessage{})
					} else {
						transaction.Messages = append(transaction.Messages, storage.TransactionMessage{Kind: message.Kind, Payload: bytes.Clone(message.Payload)})
					}
				}
			}
			feed.transactions = append(feed.transactions, transaction)
			feed.queuedBytes += transaction.size
		}
		feed.pending = true
		feed.signal()
	}
	return nil
}

// feed has one receiver. Close may run concurrently with Receive. A terminal
// error discards buffered work: no later watermark can hide lost messages.
type messageFeed struct {
	service      *service
	notify       chan struct{}
	done         chan struct{}
	transactions []*Transaction
	queuedBytes  int
	pending      bool
	err          error
}

// Done closes on overflow, source failure, or Close, including while Receive's
// caller is busy sending an earlier batch. Err is stable once Done closes.
func (f *messageFeed) Done() <-chan struct{} { return f.done }
func (f *messageFeed) Err() error {
	f.service.mu.Lock()
	defer f.service.mu.Unlock()
	return f.err
}

func (f *messageFeed) signal() {
	select {
	case f.notify <- struct{}{}:
	default:
	}
}

func (f *messageFeed) Close() error {
	s := f.service
	s.mu.Lock()
	defer s.mu.Unlock()
	if f.err == nil {
		s.removeLocked(f, io.EOF)
	}
	return nil
}

func (f *messageFeed) Receive(ctx context.Context) (Batch, error) {
	s := f.service
	for {
		if err := ctx.Err(); err != nil {
			return Batch{}, err
		}
		s.mu.Lock()
		if f.err != nil {
			err := f.err
			s.mu.Unlock()
			return Batch{}, err
		}
		if f.pending {
			batch := Batch{Through: sourceposition.FromSource(s.position)}
			size := batchOverheadBytes + len(s.position.Source())
			count := 0
			for _, transaction := range f.transactions {
				if count == s.options.MaxBatchTransactions || size+transaction.size > s.options.MaxBatchBytes {
					break
				}
				count++
				size += transaction.size
			}
			batch.Transactions = append([]*Transaction(nil), f.transactions[:count]...)
			for index := 0; index < count; index++ {
				f.queuedBytes -= f.transactions[index].size
				f.transactions[index] = nil
			}
			f.transactions = f.transactions[count:]
			if len(f.transactions) > 0 {
				batch.Through = batch.Transactions[len(batch.Transactions)-1].Position
			} else {
				f.transactions = nil
				f.pending = false
			}
			s.mu.Unlock()
			return batch, nil
		}
		s.mu.Unlock()
		select {
		case <-f.notify:
		case <-ctx.Done():
			return Batch{}, ctx.Err()
		}
	}
}
