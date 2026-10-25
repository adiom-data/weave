package projectionserver

import (
	"context"
	"time"

	"github.com/adiom-data/weave/internal/projection/transactionmessages"
	"github.com/adiom-data/weave/internal/querysubscription/model"
	"github.com/adiom-data/weave/internal/querysubscription/position"
)

// forwardJoinedFeed preserves query-feed order while waiting independently for
// message coverage. Snapshot installers still run on the connection actor;
// neither source CDC consumer waits for this connection to make progress.
func forwardJoinedFeed(ctx context.Context, feed querysubscription.Feed, route *transactionmessages.Connection, outbound chan<- outboundMessage, progressInterval time.Duration) {
	if err := joinFeed(ctx, feed, route, outbound, progressInterval); err != nil {
		select {
		case outbound <- queryEventMessage{err: err}:
		case <-ctx.Done():
		}
	}
}

func joinFeed(ctx context.Context, feed querysubscription.Feed, route *transactionmessages.Connection, outbound chan<- outboundMessage, progressInterval time.Duration) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	queryEvents := make(chan outboundMessage, 1)
	go forwardQueryFeed(ctx, feed, queryEvents)
	progressResult := make(chan error, 1)
	requesting := false
	var timer *time.Timer
	var timerC <-chan time.Time
	defer func() {
		if timer != nil {
			timer.Stop()
		}
	}()
	schedule := func() {
		if !route.HasPending() {
			if timer != nil {
				timer.Stop()
			}
			timerC = nil
			return
		}
		if !requesting && timerC == nil {
			timer = time.NewTimer(progressInterval)
			timerC = timer.C
		}
	}
	send := func(message outboundMessage) error {
		select {
		case outbound <- message:
			return nil
		case <-route.Done():
			return route.Err()
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	var positions position.Tracker
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-route.Done():
			return route.Err()
		case <-route.Wake():
			schedule()
		case <-timerC:
			timerC = nil
			if route.HasPending() {
				requesting = true
				go func() {
					err := feed.RequestProgress(ctx)
					select {
					case progressResult <- err:
					case <-ctx.Done():
					}
				}()
			}
		case err := <-progressResult:
			requesting = false
			if err != nil {
				return err
			}
			schedule()
		case raw := <-queryEvents:
			item := raw.(queryEventMessage)
			if item.err != nil {
				return item.err
			}
			if err := item.event.Validate(); err != nil {
				return err
			}
			var position *querysubscription.Position
			commit := false
			if progress := item.event.Progress; progress != nil {
				position = progress.Position
				if err := positions.ObserveReport(position); err != nil {
					return err
				}
			} else if delivery := item.event.Delivery; delivery != nil && delivery.Delivery.Kind == querysubscription.DeliveryCommit {
				position = delivery.Delivery.Checkpoint
				commit = true
				if err := positions.Observe(position); err != nil {
					return err
				}
			}
			if position != nil {
				transactions, err := route.TakeThrough(ctx, position)
				if err != nil {
					return err
				}
				for _, transaction := range transactions {
					if commit && transaction.Position.Order() == position.Value {
						item.messages = transaction.Messages
					} else if err := send(transactionMessage{transaction: transaction}); err != nil {
						return err
					}
				}
			}
			if err := send(item); err != nil {
				return err
			}
			schedule()
		}
	}
}
