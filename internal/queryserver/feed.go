package queryserver

import (
	"context"
	"time"

	q "github.com/adiom-data/weave/internal/querysubscription/model"
)

// boundedFeed applies the service's operation budget equally to embedded and
// remote callers. It wraps the complete query-side wait, including reference
// materialization, without changing the feed or subscription lifetime.
type boundedFeed struct {
	q.Feed
	fenceWaitTimeout time.Duration
}

func (f *boundedFeed) WaitForFence(ctx context.Context, token string) (*q.Position, error) {
	waitCtx, cancel := context.WithTimeout(ctx, f.fenceWaitTimeout)
	defer cancel()
	return f.Feed.WaitForFence(waitCtx, token)
}
