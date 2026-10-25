// Package transactionmessages routes one shared source feed to connection
// generations. It interprets routing envelopes, never application payloads.
package transactionmessages

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/adiom-data/weave/internal/querysubscription/model"
	"github.com/adiom-data/weave/internal/transactionmessage"
	reactivev1 "github.com/adiom-data/weave/pkg/gen/reactive/v1"
)

const RoutedKind = "weave.client"

var ErrUnavailable = errors.New("transaction message routing is unavailable; reconnect")
var ErrBackpressure = errors.New("connection transaction messages exceeded queue limits; reconnect")
var errProtocol = errors.New("invalid transaction message source ordering")

type Options struct {
	MaxQueuedTransactions int
	MaxQueuedBytes        int
	ReconnectDelay        time.Duration
}

type Transaction struct {
	Position *querysubscription.Position
	Messages []*reactivev1.TransactionMessage
	size     int
}

type Router struct {
	mu          sync.Mutex
	options     Options
	connections map[string]*Connection
	through     *querysubscription.Position
	// Created lazily by coverage waiters, then closed on the next source
	// batch. Separate from addressed-message notifications so idle connections
	// do not wake for unrelated source traffic.
	coverage chan struct{}
	ready    bool
	err      error
	cancel   context.CancelFunc
	done     chan struct{}
}

// Start confirms source attachment before any connection context is issued.
func Start(ctx context.Context, client transactionmessage.Client, options Options) (*Router, error) {
	if client == nil || options.MaxQueuedTransactions < 1 || options.MaxQueuedBytes < 1 || options.ReconnectDelay <= 0 {
		return nil, errors.New("message router requires a client and positive limits")
	}
	ctx, cancel := context.WithCancel(ctx)
	feed, err := client.Open(ctx)
	if err != nil {
		cancel()
		return nil, err
	}
	r := &Router{options: options, connections: make(map[string]*Connection), ready: true, cancel: cancel, done: make(chan struct{})}
	go r.run(ctx, client, feed)
	return r, nil
}

func (r *Router) run(ctx context.Context, client transactionmessage.Client, feed transactionmessage.Stream) {
	defer close(r.done)
	defer r.cancel()
	for {
		err := r.consume(ctx, feed)
		_ = feed.Close()
		r.mu.Lock()
		r.ready = false
		r.err = err
		for _, c := range r.connections {
			r.remove(c, err)
		}
		r.mu.Unlock()
		if ctx.Err() != nil || errors.Is(err, errProtocol) {
			return
		}
		// Old contexts never survive a source disconnect or gap. Only fresh
		// connections may attach after this independent message feed recovers.
		for {
			timer := time.NewTimer(r.options.ReconnectDelay)
			select {
			case <-timer.C:
			case <-ctx.Done():
				timer.Stop()
				return
			}
			feed, err = client.Open(ctx)
			if err == nil {
				break
			}
		}
		r.mu.Lock()
		r.through = nil
		r.ready = true
		r.err = nil
		r.mu.Unlock()
	}
}

func (r *Router) consume(ctx context.Context, feed transactionmessage.Stream) error {
	for {
		batch, err := feed.Receive(ctx)
		if err != nil {
			return err
		}
		if err := r.publish(batch); err != nil {
			return err
		}
	}
}

func (r *Router) Close() error { r.cancel(); <-r.done; return nil }
func (r *Router) Wait(ctx context.Context) error {
	select {
	case <-r.done:
		r.mu.Lock()
		defer r.mu.Unlock()
		return r.err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (r *Router) Open() (*Connection, error) {
	var random [32]byte
	if _, err := rand.Read(random[:]); err != nil {
		return nil, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.ready {
		return nil, ErrUnavailable
	}
	c := &Connection{router: r, Context: base64.RawURLEncoding.EncodeToString(random[:]), wake: make(chan struct{}, 1), done: make(chan struct{})}
	r.connections[c.Context] = c
	return c, nil
}

// decodeRouting runs on the single source consumer, outside the router lock.
// It has no access to live connections; publication resolves contexts only
// after all envelopes have been decoded and ordering has been validated.
func decodeRouting(batch transactionmessage.Batch) ([]map[string]*Transaction, error) {
	if batch.Through == nil || batch.Through.Source() == "" {
		return nil, errProtocol
	}
	var previous *querysubscription.Position
	decoded := make([]map[string]*Transaction, 0, len(batch.Transactions))
	for _, txn := range batch.Transactions {
		if txn == nil || txn.Position == nil || txn.Position.Source() != batch.Through.Source() || txn.Position.Order() > batch.Through.Order() || (previous != nil && txn.Position.Order() <= previous.Order()) {
			return nil, errProtocol
		}
		previous = txn.Position
		var grouped map[string]*Transaction
		for _, message := range txn.Messages {
			if message.Kind != RoutedKind {
				continue
			} // Includes empty placeholders.
			var envelope reactivev1.RoutedTransactionMessage
			if err := (proto.UnmarshalOptions{DiscardUnknown: true, RecursionLimit: 16}).Unmarshal(message.Payload, &envelope); err != nil {
				continue
			}
			if envelope.ConnectionContext == "" || envelope.Kind == "" {
				continue
			}
			if grouped == nil {
				grouped = make(map[string]*Transaction)
			}
			target := grouped[envelope.ConnectionContext]
			if target == nil {
				target = &Transaction{Position: txn.Position}
				grouped[envelope.ConnectionContext] = target
			}
			target.Messages = append(target.Messages, &reactivev1.TransactionMessage{Kind: envelope.Kind, Payload: envelope.Payload})
			target.size += len(envelope.Kind) + len(envelope.Payload) + 64
		}
		decoded = append(decoded, grouped)
	}
	return decoded, nil
}

func (r *Router) publish(batch transactionmessage.Batch) error {
	decoded, err := decodeRouting(batch)
	if err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.through != nil {
		if batch.Through.Source() != r.through.Source() || batch.Through.Order() < r.through.Order() {
			return errProtocol
		}
		if len(batch.Transactions) > 0 && batch.Transactions[0].Position.Order() <= r.through.Order() {
			return errProtocol
		}
	}
	for _, grouped := range decoded {
		for connectionContext, transaction := range grouped {
			c := r.connections[connectionContext]
			if c == nil {
				continue
			}
			if len(c.pending) >= r.options.MaxQueuedTransactions || transaction.size > r.options.MaxQueuedBytes-c.bytes {
				r.remove(c, ErrBackpressure)
				continue
			}
			c.pending = append(c.pending, transaction)
			c.bytes += transaction.size
			select {
			case c.wake <- struct{}{}:
			default:
			}
		}
	}
	r.through = batch.Through
	if r.coverage != nil {
		close(r.coverage)
		r.coverage = nil
	}
	return nil
}

func (r *Router) remove(c *Connection, err error) {
	delete(r.connections, c.Context)
	c.err = err
	c.pending = nil
	c.bytes = 0
	close(c.done)
}

type Connection struct {
	Context string
	router  *Router
	pending []*Transaction
	bytes   int
	err     error
	wake    chan struct{}
	done    chan struct{}
}

func (c *Connection) Done() <-chan struct{} { return c.done }
func (c *Connection) Err() error            { c.router.mu.Lock(); defer c.router.mu.Unlock(); return c.err }
func (c *Connection) Wake() <-chan struct{} { return c.wake }
func (c *Connection) HasPending() bool {
	c.router.mu.Lock()
	defer c.router.mu.Unlock()
	return len(c.pending) > 0
}
func (c *Connection) Close() error {
	c.router.mu.Lock()
	defer c.router.mu.Unlock()
	if c.err == nil {
		c.router.remove(c, ErrUnavailable)
	}
	return nil
}

// TakeThrough waits for authoritative message coverage and consumes addressed
// transactions through position. It never treats an empty queue as coverage.
func (c *Connection) TakeThrough(ctx context.Context, position *querysubscription.Position) ([]*Transaction, error) {
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		r := c.router
		r.mu.Lock()
		if c.err != nil {
			err := c.err
			r.mu.Unlock()
			return nil, err
		}
		if position == nil || (r.through != nil && position.Source() != r.through.Source()) {
			r.mu.Unlock()
			return nil, errProtocol
		}
		if r.through != nil && r.through.Order() >= position.Order() {
			count := 0
			for _, txn := range c.pending {
				if txn.Position.Order() > position.Order() {
					break
				}
				count++
			}
			result := append([]*Transaction(nil), c.pending[:count]...)
			for index := 0; index < count; index++ {
				c.bytes -= c.pending[index].size
				c.pending[index] = nil
			}
			c.pending = c.pending[count:]
			if len(c.pending) == 0 {
				c.pending = nil
			}
			r.mu.Unlock()
			return result, nil
		}
		if r.coverage == nil {
			r.coverage = make(chan struct{})
		}
		coverage := r.coverage
		r.mu.Unlock()
		select {
		case <-coverage:
		case <-c.done:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}
