package projectionserver

import (
	"context"
	"errors"
	"sync"

	"github.com/adiom-data/weave/internal/accesscontrol"
	proj "github.com/adiom-data/weave/internal/projection"
	q "github.com/adiom-data/weave/internal/querysubscription/model"
	"github.com/adiom-data/weave/internal/storage"
)

// policySnapshot is the one-shot form of initial admission. Feed application,
// evidence capture, and final visibility evaluation use one serialized consumer
// just like a live connection. Authorization discovery never reads ahead of it.
func (s *server) policySnapshot(ctx context.Context, feed q.Feed, p *proj.Projection, prepared accesscontrol.Prepared) ([]proj.ProjectionEvent, map[storage.SourceID]*q.Position, error) {
	run, cancel := context.WithCancel(ctx)
	var workers sync.WaitGroup
	defer func() { cancel(); workers.Wait() }()
	outbound := make(chan outboundMessage, s.options.OutboundQueueLimit)
	workers.Add(3)
	go func() { defer workers.Done(); forwardQueryFeed(run, feed, outbound) }()
	go func() {
		defer workers.Done()
		if err := s.policyAccess.watch(run); err != nil && run.Err() == nil {
			select {
			case outbound <- queryEventMessage{err: err}:
			case <-run.Done():
			}
		}
	}()
	ready, retry := false, false
	go func() {
		defer workers.Done()
		err := p.WaitReady(run)
		select {
		case outbound <- completionMessage{run: func() error { ready = true; retry = true; return err }}:
		case <-run.Done():
		}
	}()
	image := newInitialPolicyImage()
	var waiting <-chan struct{}
	for {
		select {
		case <-run.Done():
			return nil, nil, run.Err()
		case message := <-outbound:
			switch item := message.(type) {
			case queryEventMessage:
				if item.err != nil {
					return nil, nil, item.err
				}
				affected, err := s.policyAccess.session.Observe(item.event)
				if err != nil {
					return nil, nil, err
				}
				if _, _, err = p.ApplyEvent(item.event); err != nil {
					return nil, nil, err
				}
				if item.event.Progress != nil || len(affected) > 0 {
					retry = true
				}
				if item.event.Delivery != nil {
					for effect := range item.event.Delivery.SubscriptionEffects() {
						if p.OwnsSubscription(effect.SubscriptionID) {
							retry = true
						}
					}
				}
			case completionMessage:
				if err := item.run(); err != nil {
					return nil, nil, err
				}
			default:
				return nil, nil, errors.New("unexpected authorized snapshot work item")
			}
		}
		if !ready || !retry {
			continue
		}
		retry = false
		wait, err := p.TryAdmitInitial(func(batch proj.ProjectionBatch) (bool, error) {
			return image.apply(run, s, prepared, batch)
		})
		if err != nil {
			return nil, nil, err
		}
		if wait.Accepted {
			filtered := image.borrowEvents()
			positions := make(map[storage.SourceID]*q.Position)
			for _, event := range filtered {
				if at := event.Checkpoint; at != nil {
					if prior := positions[at.SourceID]; prior == nil || prior.Value < at.Value {
						copy := *at
						positions[at.SourceID] = &copy
					}
				}
			}
			return filtered, positions, nil
		}
		if wait.Progress {
			s.policyAccess.requestProgress()
		}
		if wait.Wait != nil && waiting != wait.Wait {
			waiting = wait.Wait
			signal := wait.Wait
			workers.Add(1)
			go func() {
				defer workers.Done()
				select {
				case <-signal:
					select {
					case outbound <- completionMessage{run: func() error { retry = true; waiting = nil; return nil }}:
					case <-run.Done():
					}
				case <-run.Done():
				}
			}()
		}
	}
}
