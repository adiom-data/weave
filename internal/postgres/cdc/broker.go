package cdc

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"

	"github.com/jackc/pglogrepl"
)

var ErrSlowConsumer = errors.New("CDC consumer fell behind")

type Cell struct {
	TypeOID uint32
	Null    bool
	Missing bool
	Text    []byte
}

type Row map[string]Cell

type Change struct {
	Operation   string
	RelationID  uint32
	Schema      string
	Table       string
	Identity    string
	NewIdentity string
	Old         Row
	New         Row
}

type Transaction struct {
	ID        uint32
	CommitLSN pglogrepl.LSN
	EndLSN    pglogrepl.LSN
	Changes   []Change
	Messages  []Message
}

// Message is a transactional PostgreSQL logical message. Prefix and Content
// remain opaque; only committed messages may enter the broker.
type Message struct {
	Prefix  string
	Content []byte
}

// StreamEvent is one ordered replication-feed unit. Transaction is nil for an
// authoritative progress-only watermark, such as an in-band logical barrier
// or an empty decoded transaction. Physical-WAL keepalives are never published.
type StreamEvent struct {
	Transaction *Transaction
	Position    pglogrepl.LSN
}

type Subscription struct {
	Events <-chan StreamEvent
	Err    <-chan error
	cancel func()
}

func (s Subscription) Close() { s.cancel() }

type consumer struct {
	events chan StreamEvent
	err    chan error
}

type Broker struct {
	options    BrokerOptions
	mu         sync.Mutex
	ready      chan struct{}
	readyOnce  sync.Once
	failed     chan struct{}
	failOnce   sync.Once
	failure    error
	nextID     uint64
	consumers  map[uint64]*consumer
	lastCommit pglogrepl.LSN
	progress   atomic.Uint64
	acked      atomic.Uint64
}

func NewBroker() *Broker {
	return newBroker(DefaultBrokerOptions())
}

func NewBrokerWithOptions(options BrokerOptions) (*Broker, error) {
	if options.ConsumerBufferLimit == 0 {
		options = DefaultBrokerOptions()
	}
	if options.ConsumerBufferLimit < 1 {
		return nil, errors.New("CDC consumer buffer limit must be positive")
	}
	return newBroker(options), nil
}

func newBroker(options BrokerOptions) *Broker {
	return &Broker{options: options, ready: make(chan struct{}), failed: make(chan struct{}), consumers: make(map[uint64]*consumer)}
}

// MarkReady records that the durable capture point exists. It remains ready
// across replication-connection retries because the PostgreSQL slot persists.
func (b *Broker) MarkReady() { b.readyOnce.Do(func() { close(b.ready) }) }

func (b *Broker) WaitReady(ctx context.Context) error {
	select {
	case <-b.ready:
		b.mu.Lock()
		defer b.mu.Unlock()
		return b.failure
	case <-b.failed:
		b.mu.Lock()
		defer b.mu.Unlock()
		return b.failure
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (b *Broker) Subscribe() (Subscription, error) {
	b.mu.Lock()
	if b.failure != nil {
		err := b.failure
		b.mu.Unlock()
		return Subscription{}, err
	}
	id := b.nextID
	b.nextID++
	c := &consumer{events: make(chan StreamEvent, b.options.ConsumerBufferLimit), err: make(chan error, 1)}
	if position := pglogrepl.LSN(b.progress.Load()); position != 0 {
		// A new engine has no pre-existing cache. Its later snapshots include all
		// commits before this subscription, so the broker's current position is a
		// valid ordered starting watermark for subsequent snapshot fences.
		c.events <- StreamEvent{Position: position}
	}
	b.consumers[id] = c
	b.mu.Unlock()
	var once sync.Once
	return Subscription{Events: c.events, Err: c.err, cancel: func() {
		once.Do(func() { b.remove(id, nil) })
	}}, nil
}

// Fail terminates every consumer after a non-recoverable decoding error and
// prevents new feeds from silently attaching to a permanently stalled source.
// Transient replication connection failures are retried without calling Fail.
func (b *Broker) Fail(cause error) {
	if cause == nil {
		return
	}
	b.failOnce.Do(func() {
		b.mu.Lock()
		b.failure = cause
		for id, c := range b.consumers {
			delete(b.consumers, id)
			c.err <- cause
			close(c.err)
		}
		close(b.failed)
		b.mu.Unlock()
	})
}

func (b *Broker) remove(id uint64, cause error) {
	b.mu.Lock()
	c, ok := b.consumers[id]
	if ok {
		delete(b.consumers, id)
		if cause != nil {
			c.err <- cause
		}
		close(c.events)
		close(c.err)
	}
	b.mu.Unlock()
}

func (b *Broker) Publish(ctx context.Context, transaction Transaction) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	// A replication connection resumes from PostgreSQL's last confirmed slot
	// position, which can lag transactions already delivered to this in-memory
	// broker. Suppress that replay so an older commit can never be appended
	// after a newer one during a reconnect.
	if transaction.CommitLSN <= b.lastCommit {
		return nil
	}
	position := transaction.EndLSN
	if position == 0 {
		position = transaction.CommitLSN
	}
	event := StreamEvent{Transaction: &transaction, Position: position}
	for id, c := range b.consumers {
		select {
		case c.events <- event:
		case <-ctx.Done():
			return ctx.Err()
		default:
			delete(b.consumers, id)
			c.err <- ErrSlowConsumer
			close(c.events)
			close(c.err)
		}
	}
	b.lastCommit = transaction.CommitLSN
	b.advanceLocked(position)
	return nil
}

func (b *Broker) Advance(lsn pglogrepl.LSN) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if uint64(lsn) <= b.progress.Load() {
		return
	}
	event := StreamEvent{Position: lsn}
	for id, c := range b.consumers {
		select {
		case c.events <- event:
		default:
			delete(b.consumers, id)
			c.err <- ErrSlowConsumer
			close(c.events)
			close(c.err)
		}
	}
	b.advanceLocked(lsn)
}

func (b *Broker) advanceLocked(lsn pglogrepl.LSN) {
	for {
		current := b.progress.Load()
		if uint64(lsn) <= current {
			return
		}
		if b.progress.CompareAndSwap(current, uint64(lsn)) {
			return
		}
	}
}

func (b *Broker) Progress() pglogrepl.LSN {
	return pglogrepl.LSN(b.progress.Load())
}

func (b *Broker) Acknowledge(lsn pglogrepl.LSN) {
	for {
		current := b.acked.Load()
		if uint64(lsn) <= current || b.acked.CompareAndSwap(current, uint64(lsn)) {
			return
		}
	}
}

func (b *Broker) Acknowledged() pglogrepl.LSN {
	return pglogrepl.LSN(b.acked.Load())
}
