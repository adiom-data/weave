package projection

import (
	"context"
	"errors"
	"testing"
	"time"

	q "github.com/adiom-data/weave/internal/querysubscription/model"
	"github.com/adiom-data/weave/internal/storage"
)

type fencedProjectionFeed struct {
	boundarySession
	started chan struct{}
	release chan struct{}
	err     error
}

func (f *fencedProjectionFeed) WaitForFence(ctx context.Context, token string) (*q.Position, error) {
	if token != "opaque" {
		return nil, errors.New("wrong token")
	}
	close(f.started)
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-f.release:
	}
	return &q.Position{SourceID: "test", Value: 20}, f.err
}

func TestProjectionWaitsForFenceBeforeLoadingAndFailsClosed(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(map[bool]string{false: "success", true: "invalid-fence"}[fail], func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			f := &fencedProjectionFeed{started: make(chan struct{}), release: make(chan struct{})}
			if fail {
				f.err = errors.New("foreign fence")
			}
			loaded := make(chan struct{}, 1)
			node := &ProjectionNode{Name: "root", BuildQuery: func(storage.Record) (storage.QuerySpec, bool, error) {
				loaded <- struct{}{}
				return storage.QuerySpec{}, false, nil
			}}
			p, err := BeginProjection(ctx, f, node, StartOptions{AtLeastFence: "opaque", AllowPartialErrors: true})
			if err != nil {
				t.Fatal(err)
			}
			defer p.Close()
			select {
			case <-f.started:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			select {
			case <-loaded:
				t.Fatal("query bound before fence")
			default:
			}
			close(f.release)
			err = p.WaitReady(ctx)
			if fail {
				if !errors.Is(err, f.err) {
					t.Fatalf("fence failure became partial data: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			select {
			case <-loaded:
			default:
				t.Fatal("query never loaded")
			}
		})
	}
}
