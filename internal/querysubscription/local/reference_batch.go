package local

import (
	"context"
	"errors"

	"github.com/adiom-data/weave/internal/engine"
	q "github.com/adiom-data/weave/internal/querysubscription/model"
)

// subscribeReferences reserves local handles before native registration, and
// installs each result as soon as it is available. No per-entity queue or
// watcher is created; the group consumer can drain ready siblings while another
// child takes the ordinary asynchronous snapshot/hydration path.
func (s *localFeed) subscribeReferences(ctx context.Context, parentID string, children []*ownedReferenceChild, complete func(*ownedReferenceChild, error)) {
	if len(children) == 0 {
		return
	}
	ctx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(s.ctx, cancel)
	defer func() { stop(); cancel() }()
	requests := make([]engine.EntitySubscribe, len(children))
	reserved := 0
	defer func() {
		for range reserved {
			s.finishPending()
		}
	}()
	for i, child := range children {
		if err := s.reserveSubscription(child.id); err != nil {
			for _, prior := range children[:i] {
				s.removeReservation(prior.id)
			}
			for _, child := range children {
				complete(child, err)
			}
			return
		}
		reserved++
		r := child.owner
		r.mu.Lock()
		key := child.first
		for parent := range child.parents {
			key = parent
			break
		}
		r.mu.Unlock()
		requests[i] = engine.EntitySubscribe{ID: s.nativeID(child.id), Query: child.query, From: &engine.ReferenceSource{SubscriptionID: s.nativeID(parentID), ParentKey: key}}
	}
	s.coordinator.SubscribeEntities(ctx, s.group, requests, func(index int, result engine.SubscribeResult) {
		child := children[index]
		err := result.Err
		if err == nil && ctx.Err() != nil {
			err = ctx.Err()
			if result.Opened != nil {
				_, closeErr := s.coordinator.Unsubscribe(context.WithoutCancel(ctx), s.nativeID(child.id))
				err = errors.Join(err, closeErr)
			}
		}
		if err == nil {
			request := q.QuerySubscribeRequest{ID: child.id, Query: child.query, Window: q.InitialWindow{Count: 1}}
			err = s.installOpened(ctx, request, true, child, result.Opened, func(native *localSubscription, snapshot q.QuerySnapshot) error {
				r := child.owner
				r.mu.Lock()
				defer r.mu.Unlock()
				if r.closed || r.failed || child.retired {
					return q.ErrClosed
				}
				child.native = native
				child.adoptInitial(entityImage(snapshot))
				return nil
			})
		} else {
			s.removeReservation(child.id)
		}
		complete(child, err)
	})
}
