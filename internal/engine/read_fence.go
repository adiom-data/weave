package engine

import (
	"context"
	"errors"

	"github.com/adiom-data/weave/internal/datastore"
	"github.com/adiom-data/weave/internal/storage"
)

type readFenceWait struct {
	ctx    context.Context
	fence  storage.Fence
	result chan error
}

// WaitForFence performs adapter IO before handing a bounded registration to
// the actor. The normal actor loop owns progress, barrier coalescing and cleanup.
func (c *Coordinator) WaitForFence(ctx context.Context, token string) (storage.Fence, error) {
	decoder, ok := c.source.(datastore.ReadFenceDecoder)
	if !ok {
		return nil, errors.New("storage does not support read fences")
	}
	fence, err := decoder.DecodeReadFence(ctx, token)
	if err != nil {
		return nil, err
	}
	if fence == nil || fence.Source() != c.source.ID() {
		return nil, errors.New("read fence belongs to another source")
	}
	wait := &readFenceWait{ctx: ctx, fence: fence, result: make(chan error, 1)}
	command := coordinatorCommand{run: func() {
		if c.readFences == nil {
			c.readFences = make(map[*readFenceWait]struct{})
		}
		c.readFences[wait] = struct{}{}
	}}
	select {
	case c.commands <- command:
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-c.done:
		return nil, c.terminalError()
	}
	select {
	case err := <-wait.result:
		if err != nil {
			return nil, err
		}
		return fence, nil
	case <-c.done:
		return nil, c.terminalError()
	case <-ctx.Done():
		// Ensure a canceled wait cannot keep a quiet source barrier alive.
		select {
		case c.commands <- coordinatorCommand{run: func() { delete(c.readFences, wait) }}:
		case <-c.done:
		}
		return nil, ctx.Err()
	}
}

func (c *Coordinator) completeReadFences(cause error) {
	for wait := range c.readFences {
		err := cause
		if err == nil {
			err = wait.ctx.Err()
		}
		if err == nil && !c.appliedThrough(wait.fence) {
			continue
		}
		delete(c.readFences, wait)
		wait.result <- err
	}
}
