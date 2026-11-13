package projectionserver

import q "github.com/adiom-data/weave/internal/querysubscription/model"

// connectionProgress retains the cut already consumed on this connection's
// query feed, including events received before any projection exists. Like the
// scope maps, it is owned exclusively by the connection consumer.
type connectionProgress struct {
	checkpoint *q.Position
	revision   uint64
}

func (p *connectionProgress) observe(event q.Event) {
	var position *q.Position
	var revision uint64
	if event.Progress != nil {
		position, revision = event.Progress.Position, event.Progress.Revision
	} else if event.Delivery != nil {
		metadata := event.Delivery.Delivery
		revision = metadata.Revision
		if metadata.Kind == q.DeliveryCommit {
			position = metadata.Checkpoint
		}
	}
	if revision > p.revision {
		p.revision = revision
	}
	if position != nil {
		if p.checkpoint == nil {
			copy := *position
			p.checkpoint = &copy
		} else {
			*p.checkpoint = *position
		}
	}
}

// current detaches the seed from both future feed events and caller mutation.
func (p *connectionProgress) current() (*q.Position, uint64) {
	if p.checkpoint == nil {
		return nil, p.revision
	}
	copy := *p.checkpoint
	return &copy, p.revision
}
