package postgres

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pglogrepl"

	"github.com/adiom-data/weave/internal/postgres/cdc"
	"github.com/adiom-data/weave/internal/storage"
)

var _ storage.MessageSource = (*PostgresDataSource)(nil)

func TestMessageFeedPreservesMessagesProgressAndNoAckOwnership(t *testing.T) {
	broker := cdc.NewBroker()
	broker.MarkReady()
	source := NewPostgresDataSourceWithOptions(nil, broker, DataSourceOptions{})
	metadata, err := source.OpenMessageFeed(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer metadata.Close()
	full, err := source.OpenFeed(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer full.Close()
	changes := make([]cdc.Change, 10000)
	if err := broker.Publish(context.Background(), cdc.Transaction{CommitLSN: 100, EndLSN: 110, Changes: changes, Messages: []cdc.Message{{Prefix: "receipt", Content: []byte{0, 255}}, {}}}); err != nil {
		t.Fatal(err)
	}
	event, err := metadata.Receive(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if event.Checkpoint.Source() != SourceID || event.Checkpoint.Order() != 110 || len(event.Messages) != 2 || event.Messages[0].Kind != "receipt" || len(event.Messages[0].Payload) != 2 || event.Messages[1].Kind != "" || len(event.Messages[1].Payload) != 0 {
		t.Fatalf("metadata = %+v", event)
	}
	rows, err := full.Receive(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if rows.Transaction == nil || len(rows.Transaction.Changes) != len(changes) || rows.Transaction.Checkpoint.Order() != event.Checkpoint.Order() {
		t.Fatal("metadata feed changed the normal row feed")
	}
	if _, ok := metadata.(interface {
		Ack(context.Context, storage.Checkpoint) error
	}); ok {
		t.Fatal("metadata feed exposes WAL acknowledgment")
	}
	if err := broker.Publish(context.Background(), cdc.Transaction{CommitLSN: 120, Changes: changes}); err != nil {
		t.Fatal(err)
	}
	rowOnly, err := metadata.Receive(context.Background())
	if err != nil || rowOnly.Checkpoint.Order() != 120 || len(rowOnly.Messages) != 0 {
		t.Fatalf("row-only progress = %+v, %v", rowOnly, err)
	}
	broker.Advance(130)
	progress, err := metadata.Receive(context.Background())
	if err != nil || progress.Checkpoint.Order() != 130 || len(progress.Messages) != 0 {
		t.Fatalf("barrier = %+v, %v", progress, err)
	}
	if broker.Acknowledged() != 0 {
		t.Fatal("reading metadata acknowledged WAL")
	}
}

func TestMessageFeedAllocationsDoNotScaleWithRows(t *testing.T) {
	allocations := func(rowCount int) float64 {
		broker := cdc.NewBroker()
		broker.MarkReady()
		feed, err := newFeedSource(broker, newPostgresTypeAdapter()).OpenMessageFeed(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		defer feed.Close()
		changes := make([]cdc.Change, rowCount)
		position := pglogrepl.LSN(1000)
		return testing.AllocsPerRun(20, func() {
			position++
			if err := broker.Publish(context.Background(), cdc.Transaction{CommitLSN: position, Changes: changes}); err != nil {
				panic(err)
			}
			event, err := feed.Receive(context.Background())
			if err != nil || event.Checkpoint.Order() != uint64(position) || len(event.Messages) != 0 {
				panic("incorrect row-only progress")
			}
		})
	}
	small, large := allocations(0), allocations(10000)
	if large != small {
		t.Fatalf("metadata allocations scaled with rows: zero=%v ten-thousand=%v", small, large)
	}
}

func TestMessageFeedCaptureGapAndCancellation(t *testing.T) {
	broker := cdc.NewBroker()
	broker.MarkReady()
	feed, err := newFeedSource(broker, newPostgresTypeAdapter()).OpenMessageFeed(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer feed.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := feed.Receive(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	for index := 1; index <= 1025; index++ {
		broker.Advance(pglogrepl.LSN(index))
	}
	for {
		if _, err := feed.Receive(context.Background()); err != nil {
			if !errors.Is(err, storage.ErrResumeRequired) {
				t.Fatalf("lost capture-gap cause: %v", err)
			}
			break
		}
	}
}
