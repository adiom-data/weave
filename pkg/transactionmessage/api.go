// Package transactionmessage exposes supported contracts and forwards construction to internal implementations.
package transactionmessage

import (
	"context"

	impl "github.com/adiom-data/weave/internal/transactionmessage"
	"github.com/adiom-data/weave/pkg/storage"
)

// Client is the consumer boundary for one selected query server's message
// service. Open confirms attachment before returning. Streams have no replay.
type Client = impl.Client

// Stream has one logical receiver. Close must unblock a pending Receive.
// Delivered batches and their payloads are immutable and may be shared.
type Stream = impl.Stream

// Feed adds the terminal-state notifications required by a serving transport.
type Feed = impl.Feed

// Service is the lifecycle and subscription handle of a message broadcaster.
// Queue ownership and source-consumption state are private implementations.
type Service = impl.Service

func NewLocalClient(service Service) Client { return impl.NewLocalClient(service) }

// MessageLimits bounds opaque message payloads. An all-zero struct selects defaults.
type MessageLimits = impl.MessageLimits

var ErrSlowConsumer = impl.ErrSlowConsumer

type Options = impl.Options

func DefaultOptions() Options { return impl.DefaultOptions() }

// Transaction and Batch are immutable after delivery. Positions share the same
// source-global ordering as query subscriptions, but prove message coverage only.
// Datastore-native checkpoints and resume representations stay inside service.
type Transaction = impl.Transaction

type Batch = impl.Batch

// Start attaches once to the datastore. All downstream feeds share this reader
// and the immutable message payloads; row payloads are never retained here.
func Start(ctx context.Context, source storage.MessageSource, options Options) (Service, error) {
	return impl.Start(ctx, source, options)
}
