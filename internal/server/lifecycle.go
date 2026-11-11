package server

import (
	"context"
	"errors"
	"io"
	"slices"
	"sync"

	"github.com/adiom-data/framework/httpapp"
	"github.com/adiom-data/weave/pkg/projectionserver"
	"github.com/adiom-data/weave/pkg/queryserver"
)

type server struct {
	ctx         context.Context
	cancel      context.CancelFunc
	source      io.Closer
	queries     queryserver.Backend
	projections projectionserver.Server
	services    []httpapp.ConnectService
	routes      []httpapp.Route
	done        chan struct{}
	err         error // immutable once done closes
	closeOnce   sync.Once
	closeErr    error
}

func (s *server) Services() []httpapp.ConnectService {
	return cloneServices(s.services)
}

func cloneServices(source []httpapp.ConnectService) []httpapp.ConnectService {
	services := slices.Clone(source)
	for i := range services {
		services[i].Interceptors = slices.Clone(services[i].Interceptors)
		services[i].ConnectOptions = slices.Clone(services[i].ConnectOptions)
		services[i].Middleware = slices.Clone(services[i].Middleware)
	}
	return services
}

func (s *server) Routes() []httpapp.Route {
	routes := slices.Clone(s.routes)
	for i := range routes {
		routes[i].Middleware = slices.Clone(routes[i].Middleware)
	}
	return routes
}

func (s *server) supervise() {
	defer close(s.done)
	results := make(chan error, 2)
	go func() { results <- s.queries.Wait(s.ctx) }()
	go func() { results <- s.projections.Wait(s.ctx) }()
	remaining := 2
	select {
	case <-s.ctx.Done():
		s.err = s.ctx.Err()
	case s.err = <-results:
		remaining--
		if s.err == nil {
			s.err = s.ctx.Err()
			if s.err == nil {
				s.err = errors.New("reactive component stopped unexpectedly")
			}
		}
	}
	s.cancel()
	for range remaining {
		<-results
	}
}

// Wait reports a terminal reactive component failure or context cancellation.
// A canceled wait does not stop the server; Close owns resource cleanup.
func (s *server) Wait(ctx context.Context) error {
	select {
	case <-s.done:
		return s.err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *server) Close() error {
	s.closeOnce.Do(func() {
		s.cancel()
		if s.projections != nil {
			s.closeErr = errors.Join(s.closeErr, s.projections.Close())
		}
		if s.queries != nil {
			s.closeErr = errors.Join(s.closeErr, s.queries.Close())
		}
		if s.source != nil {
			s.closeErr = errors.Join(s.closeErr, s.source.Close())
		}
		if s.done != nil {
			<-s.done
		}
	})
	return s.closeErr
}
