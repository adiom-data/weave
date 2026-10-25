package engine

import (
	"context"

	"github.com/adiom-data/weave/internal/datastore"
	"github.com/adiom-data/weave/internal/storage"
)

// EntitySubscribe batches independent entity interests on one delivery group.
// Each result has its own snapshot/floor and lifecycle; batching changes only
// registration scheduling, never the query or consistency model.
type EntitySubscribe struct {
	ID    string
	Query storage.QuerySpec
	From  *ReferenceSource
}

type SubscribeResult struct {
	Opened *OpenSubscription
	Err    error
}

// SubscribeEntities calls install exactly once per request, serially and outside
// the coordinator actor. Cached results may precede earlier requests that need
// IO. The callback owns successful registrations and must arrange their cleanup.
func (c *Coordinator) SubscribeEntities(ctx context.Context, group string, requests []EntitySubscribe, install func(int, SubscribeResult)) {
	for offset := 0; offset < len(requests); {
		count := min(len(requests)-offset, c.options.SubscriptionBatchSize)
		type started struct {
			operation initialWindowOperation
			start     initialStart
			err       error
		}
		done := make(chan []started, 1)
		chunk := requests[offset : offset+count]
		command := coordinatorCommand{run: func() {
			out := make([]started, len(chunk))
			for i, request := range chunk {
				if err := ctx.Err(); err != nil {
					out[i].err = err
					continue
				}
				plan := c.prepareFromReferenceActor(request.Query, request.From)
				if plan == nil {
					continue // No reusable preparation; ordinary async path below.
				}
				if preparer, ok := plan.(datastore.IdentityPreparer); ok {
					identity, matched, err := preparer.PrepareIdentity()
					if err != nil {
						out[i].err = err
						continue
					}
					if matched {
						plan = identity
					}
				}
				operation := initialWindowOperation{subscriptionID: request.ID, group: group, plan: plan}
				start, err := c.startSubscribeActor(operation, InitialWindow{Count: 1})
				out[i] = started{operation: operation, start: start, err: err}
			}
			done <- out
		}}
		select {
		case c.commands <- command:
		case <-ctx.Done():
			for i := offset; i < len(requests); i++ {
				install(i, SubscribeResult{Err: ctx.Err()})
			}
			return
		case <-c.done:
			for i := offset; i < len(requests); i++ {
				install(i, SubscribeResult{Err: c.terminalError()})
			}
			return
		}
		// Once accepted, consume all results to retain ownership of every
		// registered operation, even if cancellation races the actor.
		var starts []started
		select {
		case starts = <-done:
		case <-c.done:
			for i := offset; i < len(requests); i++ {
				install(i, SubscribeResult{Err: c.terminalError()})
			}
			return
		}
		// Publish all completed registrations before performing any slow read.
		// Otherwise a native delivery for a cached sibling could wait behind
		// an unrelated hydration merely because both used the same batch.
		completed := make([]bool, len(starts))
		for i, started := range starts {
			if started.err != nil {
				install(offset+i, SubscribeResult{Err: started.err})
				completed[i] = true
				continue
			}
			if err := ctx.Err(); err != nil {
				if started.operation.plan != nil {
					c.cancelSubscription(started.operation)
				}
				install(offset+i, SubscribeResult{Err: err})
				completed[i] = true
				continue
			}
			if started.start.opened != nil {
				install(offset+i, SubscribeResult{Opened: started.start.opened})
				completed[i] = true
			}
		}
		for i, started := range starts {
			if completed[i] {
				continue
			}
			var result SubscribeResult
			if started.operation.plan == nil {
				request := chunk[i]
				result.Opened, result.Err = c.SubscribeGroupedWithFetch(ctx, request.ID, group, request.Query, InitialWindow{Count: 1}, FetchOptions{Entity: true, From: request.From})
			} else {
				result.Opened, result.Err = c.completeSubscribe(ctx, started.operation, InitialWindow{Count: 1}, started.start)
			}
			install(offset+i, result)
		}
		offset += count
	}
}
