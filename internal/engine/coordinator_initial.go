package engine

import (
	"context"

	"github.com/adiom-data/weave/internal/datastore"
	"github.com/adiom-data/weave/internal/engine/queryregistry"
)

// initialStart runs the in-memory initialization phases together. Only missing
// bounds/data or reference prefetch require handing the operation back to the
// caller. No datastore IO or waits for source progress run inside the actor.
type initialStart struct {
	cached          queryregistry.InitialWindowPlan
	staged          bool
	materialization *materializationRequest
	opened          *OpenSubscription
}

// cachedInitialWindow resolves bounds using only prepared identity metadata or
// proven cache coverage. Identity bounds are fixed, but installing them can
// still require a fenced materialization if the entity is not current.
func cachedInitialWindow(plan datastore.PreparedQuery, cached queryregistry.InitialWindowPlan) (resolvedWindow, bool) {
	if _, identity := plan.(datastore.IdentityQuery); identity {
		full := datastore.Bounds{Left: datastore.Boundary{Kind: datastore.BoundaryStart}, Right: datastore.Boundary{Kind: datastore.BoundaryEnd}}
		return resolvedWindow{bounds: full, source: windowResolvedFromCache}, true
	}
	return resolvedWindow{bounds: cached.Bounds, source: windowResolvedFromCache}, cached.Resolved
}

func (c *Coordinator) startSubscribeActor(operation initialWindowOperation, initial InitialWindow) (_ initialStart, err error) {
	if err := c.beginSubscribeActor(operation); err != nil {
		return initialStart{}, err
	}
	defer func() {
		if err != nil && c.subscriptions.CancelInitialization(operation.subscriptionID, operation.plan.Fingerprint()) {
			c.releaseQuery(operation.subscriptionID, operation.plan.Fingerprint())
		}
	}()
	start := initialStart{}
	start.cached, err = c.planCachedInitialWindowActor(operation, initial.Count, initial.FromEnd)
	if err != nil {
		return start, err
	}
	resolved, ok := cachedInitialWindow(operation.plan, start.cached)
	if !ok {
		return start, nil
	}
	start.materialization, err = c.installInitializingWindowActor(operation, resolved)
	if err != nil {
		return start, err
	}
	start.staged = true
	if start.materialization == nil && operation.reference == nil {
		start.opened, err = c.finishSubscribeActor(operation)
	}
	return start, err
}

func (c *Coordinator) startSubscribe(ctx context.Context, operation initialWindowOperation, initial InitialWindow) (initialStart, error) {
	type response struct {
		start initialStart
		err   error
	}
	result := make(chan response, 1)
	command := coordinatorCommand{run: func() {
		if err := ctx.Err(); err != nil {
			result <- response{err: err}
			return
		}
		start, err := c.startSubscribeActor(operation, initial)
		result <- response{start: start, err: err}
	}}
	select {
	case c.commands <- command:
	case <-c.done:
		return initialStart{}, c.terminalError()
	case <-ctx.Done():
		return initialStart{}, ctx.Err()
	}
	// Once accepted, consume this bounded actor operation's result. Cancellation
	// must clean up only an operation we actually created, not an existing
	// subscription whose duplicate ID caused beginSubscribeActor to reject it.
	select {
	case response := <-result:
		if response.err != nil {
			return initialStart{}, response.err
		}
		if err := ctx.Err(); err != nil {
			c.cancelSubscription(operation)
			return initialStart{}, err
		}
		return response.start, nil
	case <-c.done:
		return initialStart{}, c.terminalError()
	}
}
