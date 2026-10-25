package transactionmessage

import "context"

// Client is the consumer boundary for one selected query server's message
// service. Open confirms attachment before returning. Streams have no replay.
type Client interface {
	Open(context.Context) (Stream, error)
}

// Stream has one logical receiver. Close must unblock a pending Receive.
// Delivered batches and their payloads are immutable and may be shared.
type Stream interface {
	Receive(context.Context) (Batch, error)
	Close() error
}

// Feed adds the terminal-state notifications required by a serving transport.
type Feed interface {
	Stream
	Done() <-chan struct{}
	Err() error
}

// Service is the lifecycle and subscription handle of a message broadcaster.
// Queue ownership and source-consumption state are private implementations.
type Service interface {
	Open() (Feed, error)
	Close() error
	Wait(context.Context) error
}

type localClient struct{ service Service }

func NewLocalClient(service Service) Client { return localClient{service: service} }
func (c localClient) Open(ctx context.Context) (Stream, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return c.service.Open()
}
