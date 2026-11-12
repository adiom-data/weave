package messagegrpc

import (
	"context"
	"io"
	"net"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/adiom-data/weave/internal/postgres"
	"github.com/adiom-data/weave/internal/postgres/cdc"
	"github.com/adiom-data/weave/internal/transactionmessage"
	messagev1 "github.com/adiom-data/weave/pkg/gen/transactionmessage/v1"
)

func TestGRPCBroadcastMessagesAndOrderedProgress(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	broker := cdc.NewBroker()
	broker.MarkReady(0)
	source := postgres.NewPostgresDataSourceWithOptions(nil, broker, postgres.DataSourceOptions{})
	service, err := transactionmessage.Start(ctx, source, transactionmessage.Options{MaxBatchBytes: 1024})
	if err != nil {
		t.Fatal(err)
	}
	defer service.Close()
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer()
	NewServer(service).Register(server)
	go server.Serve(listener)
	defer server.Stop()
	connection, err := grpc.NewClient("passthrough:///messages", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	client := messagev1.NewTransactionMessagesClient(connection)
	a, err := client.Open(ctx, &emptypb.Empty{})
	if err != nil {
		t.Fatal(err)
	}
	b, err := client.Open(ctx, &emptypb.Empty{})
	if err != nil {
		t.Fatal(err)
	}
	// Confirm attachment without requiring database activity.
	if _, err := a.Header(); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Header(); err != nil {
		t.Fatal(err)
	}
	remote, err := NewClient(connection).Open(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer remote.Close()
	if err := broker.Publish(ctx, cdc.Transaction{CommitLSN: 10, EndLSN: 11, Changes: []cdc.Change{{Operation: "insert"}}, Messages: []cdc.Message{
		{Prefix: "app.receipt", Content: []byte{0, 255, 128}},
		{Prefix: "app.other", Content: []byte("opaque")},
	}}); err != nil {
		t.Fatal(err)
	}
	if err := broker.Publish(ctx, cdc.Transaction{CommitLSN: 20, EndLSN: 21, Messages: []cdc.Message{{Prefix: "app.receipt", Content: []byte("second")}}}); err != nil {
		t.Fatal(err)
	}
	if err := broker.Publish(ctx, cdc.Transaction{CommitLSN: 25, EndLSN: 26, Messages: []cdc.Message{{Prefix: "oversized", Content: make([]byte, 2048)}}}); err != nil {
		t.Fatal(err)
	}
	if err := broker.Publish(ctx, cdc.Transaction{CommitLSN: 30, EndLSN: 31}); err != nil {
		t.Fatal(err)
	}
	broker.Advance(40)
	for _, stream := range []grpc.ServerStreamingClient[messagev1.MessageBatch]{a, b} {
		var transactions []*messagev1.MessageTransaction
		var last uint64
		for last < 40 {
			batch, err := stream.Recv()
			if err != nil {
				t.Fatal(err)
			}
			if proto.Size(batch) > 1024 {
				t.Fatal("batch exceeded configured wire limit")
			}
			if batch.Through.SourceId != "postgres" || batch.Through.Value <= last {
				t.Fatalf("invalid progress: %+v", batch)
			}
			for _, transaction := range batch.Transactions {
				if transaction.Position.Value <= last || transaction.Position.Value > batch.Through.Value {
					t.Fatal("transaction outside claimed progress")
				}
				transactions = append(transactions, transaction)
			}
			last = batch.Through.Value
		}
		if len(transactions) != 3 || transactions[0].Position.Value != 11 || len(transactions[0].Messages) != 2 || string(transactions[0].Messages[0].Payload) != string([]byte{0, 255, 128}) || transactions[1].Position.Value != 21 || len(transactions[2].Messages) != 1 || transactions[2].Messages[0].Kind != "" || len(transactions[2].Messages[0].Payload) != 0 {
			t.Fatalf("transactions = %+v", transactions)
		}
	}
	if broker.Acknowledged() != 0 {
		t.Fatal("message service acknowledged WAL independently")
	}
	var remoteTransactions []*transactionmessage.Transaction
	for through := uint64(0); through < 40; {
		batch, err := remote.Receive(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if batch.Through.Source() != postgres.SourceID || batch.Through.Order() <= through {
			t.Fatal("invalid remote source progress")
		}
		through = batch.Through.Order()
		remoteTransactions = append(remoteTransactions, batch.Transactions...)
	}
	if len(remoteTransactions) != 3 || string(remoteTransactions[0].Messages[0].Payload) != string([]byte{0, 255, 128}) || remoteTransactions[2].Messages[0].Kind != "" {
		t.Fatalf("remote decoded transactions = %+v", remoteTransactions)
	}
	broker.Fail(io.ErrUnexpectedEOF)
	if _, err := a.Recv(); status.Code(err) != codes.Unavailable {
		t.Fatalf("fatal source error = %v", err)
	}
}

func TestFeedErrorRequiresResynchronization(t *testing.T) {
	for _, err := range []error{transactionmessage.ErrSlowConsumer} {
		if status.Code(feedError(err)) != codes.ResourceExhausted {
			t.Fatal(feedError(err))
		}
	}
	if status.Code(feedError(context.Canceled)) != codes.Canceled {
		t.Fatal("lost cancellation")
	}
}

type blockedSendStream struct {
	grpc.ServerStream
	ctx      context.Context
	attached chan struct{}
	sending  chan struct{}
	exited   chan struct{}
}

func (s *blockedSendStream) Context() context.Context     { return s.ctx }
func (s *blockedSendStream) SendHeader(metadata.MD) error { close(s.attached); return nil }
func (s *blockedSendStream) Send(*messagev1.MessageBatch) error {
	close(s.sending)
	defer close(s.exited)
	<-s.ctx.Done()
	return s.ctx.Err()
}

func TestOverflowTerminatesRPCWhileSendIsBlocked(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	broker := cdc.NewBroker()
	broker.MarkReady(0)
	source := postgres.NewPostgresDataSourceWithOptions(nil, broker, postgres.DataSourceOptions{})
	service, err := transactionmessage.Start(ctx, source, transactionmessage.Options{MaxQueuedTransactions: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer service.Close()
	streamCtx, closeStream := context.WithCancel(ctx)
	defer closeStream()
	stream := &blockedSendStream{ctx: streamCtx, attached: make(chan struct{}), sending: make(chan struct{}), exited: make(chan struct{})}
	result := make(chan error, 1)
	go func() {
		err := NewServer(service).(*server).Open(&emptypb.Empty{}, stream)
		// Like the gRPC runtime, cancel transport I/O after the handler returns.
		closeStream()
		result <- err
	}()
	select {
	case <-stream.attached:
	case <-ctx.Done():
		t.Fatal("stream did not attach")
	}
	commit := func(order uint64) {
		if err := broker.Publish(ctx, cdc.Transaction{CommitLSN: pglogrepl.LSN(order), Messages: []cdc.Message{{Prefix: "receipt", Content: []byte("small")}}}); err != nil {
			t.Fatal(err)
		}
	}
	commit(10)
	select {
	case <-stream.sending:
	case <-ctx.Done():
		t.Fatal("send did not begin")
	}
	commit(20)
	commit(30)
	select {
	case err := <-result:
		if status.Code(err) != codes.ResourceExhausted {
			t.Fatalf("blocked RPC result = %v", err)
		}
	case <-ctx.Done():
		t.Fatal("overflow did not terminate blocked RPC")
	}
	select {
	case <-stream.exited:
	case <-ctx.Done():
		t.Fatal("sender leaked after RPC termination")
	}
}
