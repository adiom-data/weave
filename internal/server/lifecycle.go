package server

import (
	"context"
	"errors"
	"io"
	"sync"

	"github.com/adiom-data/framework/httpapp"
	"github.com/adiom-data/weave/pkg/projectionserver"
	"github.com/adiom-data/weave/pkg/queryserver"
)

type host struct {
	ctx             context.Context
	cancel          context.CancelFunc
	source          io.Closer
	queries         queryserver.Backend
	projections     projectionserver.Server
	app             httpapp.Service
	mu              sync.Mutex
	started, closed bool
	runDone         chan struct{}
	closeOnce       sync.Once
	closeErr        error
}

func (h *host) Run(parent context.Context) error {
	h.mu.Lock()
	if h.closed || h.started {
		h.mu.Unlock()
		return errors.New("host is closed or has already run")
	}
	h.started = true
	h.mu.Unlock()
	defer close(h.runDone)
	defer h.cancel()
	stop := context.AfterFunc(parent, h.cancel)
	defer stop()
	if parent.Err() != nil {
		h.cancel()
	}
	if h.ctx.Err() != nil {
		return nil
	}
	result := make(chan error, 2)
	go func() { result <- h.queries.Wait(h.ctx) }()
	go func() { result <- h.projections.Wait(h.ctx) }()
	serving := make(chan error, 1)
	go func() { serving <- h.app.Run(h.ctx) }()
	var err error
	select {
	case <-h.ctx.Done():
	case err = <-result:
		if h.ctx.Err() == nil && err == nil {
			err = errors.New("server component stopped unexpectedly")
		}
	case err = <-serving:
		if h.ctx.Err() != nil {
			return err
		}
		if err == nil {
			err = errors.New("HTTP server stopped unexpectedly")
		}
		return err
	}
	cancelled := h.ctx.Err() != nil
	h.cancel()
	shutdownErr := <-serving
	if cancelled {
		return shutdownErr
	}
	return errors.Join(err, shutdownErr)
}

func (h *host) Close() error {
	h.closeOnce.Do(func() {
		h.mu.Lock()
		h.closed = true
		h.cancel()
		started := h.started
		h.mu.Unlock()
		if started {
			<-h.runDone
		}
		if h.projections != nil {
			h.closeErr = errors.Join(h.closeErr, h.projections.Close())
		}
		if h.queries != nil {
			h.closeErr = errors.Join(h.closeErr, h.queries.Close())
		}
		if h.source != nil {
			h.closeErr = errors.Join(h.closeErr, h.source.Close())
		}
	})
	return h.closeErr
}
