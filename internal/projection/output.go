package projection

import "errors"

// Output is a ready-to-consume projection batch. Err fails only its owner;
// completion errors must not discard other owners' output.
type Output[T any] struct {
	Owner T
	Batch ProjectionBatch
	Err   error
}

type pendingOutput[T any] struct {
	projection *Projection
	owner      T
	batch      ProjectionBatch
}

// OutputScheduler owns asynchronous subtree publication for projections sharing
// one ordered query feed. Its zero value is ready to use. All calls, output
// consumption, and projection ApplyEvent calls must run in the same serialized
// consumer. It does not read the feed or assemble connection-wide transactions.
//
// Advance is called only after source effects have been consumed (or ordered
// progress has been processed). It releases subtrees whose parent effects are
// now public, capturing their latest image rather than a queued stale snapshot.
type OutputScheduler[T any] struct {
	emittedRevision uint64
	pending         []pendingOutput[T]
	advancing       bool
}

// Submit returns nil when publication must wait for its causal transaction.
// Otherwise the returned output must be consumed before another feed event.
func (s *OutputScheduler[T]) Submit(p *Projection, owner T, batch ProjectionBatch) *Output[T] {
	item := pendingOutput[T]{projection: p, owner: owner, batch: batch}
	if publication := batch.publication; publication != nil && publication.CausalRevision > s.emittedRevision {
		s.pending = append(s.pending, item)
		return nil
	}
	output := s.complete(item)
	return &output
}

// Advance completes and consumes eligible outputs one at a time, in arrival
// order. Only the current output is materialized. A completion failure belongs
// to its owner and is passed to consume; a consumer error stops the drain,
// leaving unvisited publications queued for a later call.
//
// consume may Drop a failed or closed projection. It must not apply feed events
// or reenter Advance before this drain returns.
func (s *OutputScheduler[T]) Advance(revision uint64, consume func(Output[T]) error) error {
	if consume == nil {
		return errors.New("projection output consumer is required")
	}
	if s.advancing {
		return errors.New("projection output advance cannot be reentered")
	}
	if revision > s.emittedRevision {
		s.emittedRevision = revision
	}
	s.advancing = true
	defer func() {
		s.advancing = false
		s.compactPending()
	}()
	// Keep indexes stable while callbacks may Drop other entries. New queued
	// submissions belong to the next drain, not to this drain's initial work.
	count := len(s.pending)
	for index := 0; index < count; index++ {
		item := s.pending[index]
		if item.projection == nil || item.batch.publication.CausalRevision > s.emittedRevision {
			continue
		}
		// Remove before calling out: Drop can retire this owner's remaining
		// publications without invalidating traversal or affecting other owners.
		s.pending[index] = pendingOutput[T]{}
		if err := consume(s.complete(item)); err != nil {
			return err
		}
	}
	return nil
}

// Drop releases queued state for this exact projection lifetime, never a newer
// projection that happens to reuse an application identifier.
func (s *OutputScheduler[T]) Drop(p *Projection) {
	for index := range s.pending {
		if s.pending[index].projection == p {
			s.pending[index] = pendingOutput[T]{}
		}
	}
	if !s.advancing {
		s.compactPending()
	}
}

// compactPending releases consumed or dropped tokens. During Advance, Drop
// only clears slots so callbacks cannot move entries under the drain's cursor.
func (s *OutputScheduler[T]) compactPending() {
	retained := s.pending[:0]
	for _, item := range s.pending {
		if item.projection != nil {
			retained = append(retained, item)
		}
	}
	clear(s.pending[len(retained):])
	s.pending = retained
}

// Release drains output staged behind the initial image through the same
// publication machinery as live output. Call only after installing that image.
// Transaction batches remain intact for the consumer's transaction assembler.
func (s *OutputScheduler[T]) Release(p *Projection, owner T, consume func(Output[T]) error) error {
	if consume == nil {
		return errors.New("projection output consumer is required")
	}
	return p.releaseOutput(func(batch ProjectionBatch) error {
		if output := s.Submit(p, owner, batch); output != nil {
			return consume(*output)
		}
		return nil
	})
}

func (s *OutputScheduler[T]) complete(item pendingOutput[T]) Output[T] {
	output := Output[T]{Owner: item.owner, Batch: item.batch}
	if publication := item.batch.publication; publication != nil {
		output.Batch, output.Err = item.projection.completePublication(*publication)
	}
	return output
}
