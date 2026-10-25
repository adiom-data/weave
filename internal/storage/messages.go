package storage

import "context"

// MessageEvent contains only committed transaction metadata and source progress.
// Checkpoint covers every message through that position after the feed's start;
// an empty Messages slice also advances progress for row-only transactions and
// barriers. Messages (including empty placeholders) retain transaction order.
// No row changes, snapshot state, or acknowledgment capability are exposed.
type MessageEvent struct {
	Checkpoint Checkpoint
	Messages   []TransactionMessage
}

type MessageFeed interface {
	// Receive is called by one consumer, returns ordered complete source units,
	// and must unblock promptly on cancellation. Payload bytes are immutable.
	Receive(context.Context) (MessageEvent, error)
	Close() error
}

// MessageSource is an independent, non-acking view of a source's metadata.
// OpenMessageFeed attaches at the current capture point, not historical replay,
// and must establish capture before returning. A lost interval terminates the
// feed with ErrResumeRequired; reopening starts a new history, never silently
// resumes an old downstream consumer. ID is stable for the source's lifetime.
type MessageSource interface {
	ID() SourceID
	OpenMessageFeed(context.Context) (MessageFeed, error)
}
