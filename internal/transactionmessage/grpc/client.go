package messagegrpc

import (
	"context"
	"errors"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/adiom-data/weave/internal/querysubscription/model"
	"github.com/adiom-data/weave/internal/storage"
	"github.com/adiom-data/weave/internal/transactionmessage"
	messagev1 "github.com/adiom-data/weave/pkg/gen/transactionmessage/v1"
)

type client struct {
	wire messagev1.TransactionMessagesClient
}

func NewClient(connection grpc.ClientConnInterface) transactionmessage.Client {
	return &client{wire: messagev1.NewTransactionMessagesClient(connection)}
}
func (c *client) Open(ctx context.Context) (transactionmessage.Stream, error) {
	ctx, cancel := context.WithCancel(ctx)
	stream, err := c.wire.Open(ctx, &emptypb.Empty{})
	if err != nil {
		cancel()
		return nil, err
	}
	if _, err := stream.Header(); err != nil {
		cancel()
		return nil, err
	}
	feed := &remoteStream{cancel: cancel, results: make(chan receivedBatch, 1)}
	go func() {
		defer close(feed.results)
		defer cancel()
		for {
			wire, err := stream.Recv()
			var batch transactionmessage.Batch
			if err == nil {
				batch, err = decodeBatch(wire)
			}
			select {
			case feed.results <- receivedBatch{batch: batch, err: err}:
			case <-ctx.Done():
				return
			}
			if err != nil {
				return
			}
		}
	}()
	return feed, nil
}

func decodeBatch(wire *messagev1.MessageBatch) (transactionmessage.Batch, error) {
	if wire == nil || wire.Through == nil || wire.Through.SourceId == "" {
		return transactionmessage.Batch{}, errors.New("message feed has no source position")
	}
	batch := transactionmessage.Batch{Through: &querysubscription.Position{SourceID: storage.SourceID(wire.Through.SourceId), Value: wire.Through.Value}}
	var previous uint64
	for index, item := range wire.Transactions {
		if item == nil || item.Position == nil || item.Position.SourceId != wire.Through.SourceId || item.Position.Value > wire.Through.Value || (index > 0 && item.Position.Value <= previous) {
			return transactionmessage.Batch{}, errors.New("invalid message transaction position")
		}
		previous = item.Position.Value
		transaction := &transactionmessage.Transaction{Position: &querysubscription.Position{SourceID: storage.SourceID(item.Position.SourceId), Value: item.Position.Value}}
		for _, message := range item.Messages {
			if message == nil {
				return transactionmessage.Batch{}, errors.New("invalid nil transaction message")
			}
			transaction.Messages = append(transaction.Messages, storage.TransactionMessage{Kind: message.Kind, Payload: message.Payload})
		}
		batch.Transactions = append(batch.Transactions, transaction)
	}
	return batch, nil
}

type receivedBatch struct {
	batch transactionmessage.Batch
	err   error
}
type remoteStream struct {
	cancel  context.CancelFunc
	once    sync.Once
	results chan receivedBatch
}

func (f *remoteStream) Close() error { f.once.Do(f.cancel); return nil }
func (f *remoteStream) Receive(ctx context.Context) (transactionmessage.Batch, error) {
	select {
	case result, ok := <-f.results:
		if !ok {
			return transactionmessage.Batch{}, errors.New("message feed closed")
		}
		return result.batch, result.err
	case <-ctx.Done():
		f.Close()
		return transactionmessage.Batch{}, ctx.Err()
	}
}
