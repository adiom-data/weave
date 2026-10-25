package engine

import "context"

// Wait joins the coordinator, including its source reader and subscription
// shutdown. Cancellation of this wait does not stop the coordinator.
func (c *Coordinator) Wait(ctx context.Context) error {
	select {
	case <-c.done:
		return c.terminalError()
	case <-ctx.Done():
		return ctx.Err()
	}
}
