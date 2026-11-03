package projectionserver

import (
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/protobuf/proto"

	"github.com/adiom-data/weave/internal/engine"
	"github.com/adiom-data/weave/internal/messagebudget"
	"github.com/adiom-data/weave/internal/postgres"
	"github.com/adiom-data/weave/internal/postgres/cdc"
	"github.com/adiom-data/weave/internal/projection/transactionmessages"
	querylocal "github.com/adiom-data/weave/internal/querysubscription/local"
	"github.com/adiom-data/weave/internal/transactionmessage"
	reactivev1 "github.com/adiom-data/weave/pkg/gen/reactive/v1"
)

func TestPostgresReceiptReachesProjectionWithoutSubscriptionsIntegration(t *testing.T) {
	databaseURL := os.Getenv("WEAVE_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("set WEAVE_TEST_DATABASE_URL for PostgreSQL integration")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	var nonce [8]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		t.Fatal(err)
	}
	slot := fmt.Sprintf("weave_receipt_%x", nonce)
	broker := cdc.NewBroker()
	replicator, err := cdc.NewReplicator(&pool.Config().ConnConfig.Config, "reactive_queries", slot, broker, slog.New(slog.NewTextHandler(io.Discard, nil)), cdc.ReplicatorOptions{MessageLimits: messagebudget.Defaults(), MessageLogInterval: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	runCtx, stop := context.WithCancel(ctx)
	stopped := make(chan struct{})
	go func() { defer close(stopped); replicator.Run(runCtx) }()
	t.Cleanup(func() {
		stop()
		<-stopped
		cleanup, done := context.WithTimeout(context.Background(), 5*time.Second)
		defer done()
		if _, err := pool.Exec(cleanup, `SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = $1 AND NOT active`, slot); err != nil {
			t.Error(err)
		}
	})
	source := postgres.NewPostgresDataSourceWithOptions(pool, broker, postgres.DataSourceOptions{})
	coordinator, err := engine.StartCoordinator(ctx, source)
	if err != nil {
		t.Fatal(err)
	}
	core, err := transactionmessage.Start(ctx, source, transactionmessage.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer core.Close()
	router, err := transactionmessages.Start(ctx, transactionmessage.NewLocalClient(core), transactionmessages.Options{MaxQueuedTransactions: 16, MaxQueuedBytes: 1 << 20, ReconnectDelay: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer router.Close()
	stream := &authorizationCommitStream{ctx: ctx, received: make(chan *reactivev1.ClientMessage), sent: make(chan *reactivev1.ServerMessage, 4)}
	result := make(chan error, 1)
	go func() {
		result <- newServerWithRouter(querylocal.New(coordinator), router).Open(stream)
	}()
	opened := receiveAuthorizationMessage(t, stream.sent).GetConnectionOpened()
	if opened == nil {
		t.Fatal("missing context")
	}
	payload, err := proto.Marshal(&reactivev1.RoutedTransactionMessage{ConnectionContext: opened.ConnectionContext, Kind: "receipt", Payload: []byte{0, 255, 128}})
	if err != nil {
		t.Fatal(err)
	}
	// No table writes or active subscriptions: the transactional message itself
	// must drive both source consumers and demand-driven query progress.
	if _, err := pool.Exec(ctx, `SELECT pg_logical_emit_message(true, 'weave.client', $1::bytea)`, payload); err != nil {
		t.Fatal(err)
	}
	select {
	case message := <-stream.sent:
		batch := message.GetModelBatch()
		if batch == nil || batch.Kind != reactivev1.ModelBatch_KIND_COMMIT || !batch.Atomic || len(batch.Messages) != 1 || string(batch.Messages[0].Payload) != string([]byte{0, 255, 128}) || len(batch.Entities) != 0 {
			t.Fatalf("receipt = %v", message)
		}
	case <-ctx.Done():
		t.Fatal("committed PostgreSQL message never reached projection connection")
	}
	cancel()
	select {
	case <-result:
	case <-time.After(time.Second):
		t.Fatal("projection did not stop")
	}
}
