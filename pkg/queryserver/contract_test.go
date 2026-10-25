package queryserver_test

import (
	"context"
	"errors"
	"testing"

	"github.com/adiom-data/weave/pkg/queryserver"
	"github.com/adiom-data/weave/pkg/storage"
)

// Implementing the storage boundary must not require importing internal types.
type applicationStorage struct {
	closed  bool
	failure error
}

var _ storage.Storage = (*applicationStorage)(nil)

func (*applicationStorage) ID() storage.SourceID { return "application" }
func (s *applicationStorage) PrepareQuery(context.Context, storage.QuerySpec) (storage.PreparedQuery, error) {
	return nil, s.failure
}
func (s *applicationStorage) BeginSnapshot(context.Context, storage.PreparedQuery) (storage.SnapshotSession, error) {
	return nil, s.failure
}
func (s *applicationStorage) OpenFeed(context.Context) (storage.ChangeFeed, error) {
	return nil, s.failure
}
func (s *applicationStorage) OpenMessageFeed(context.Context) (storage.MessageFeed, error) {
	return nil, s.failure
}
func (s *applicationStorage) Close() error                   { s.closed = true; return nil }
func (s *applicationStorage) Wait(ctx context.Context) error { <-ctx.Done(); return ctx.Err() }

func TestQueryConstructionBorrowsApplicationStorage(t *testing.T) {
	sentinel := errors.New("application source unavailable")
	source := &applicationStorage{failure: sentinel}
	backend, err := queryserver.New(context.Background(), source, queryserver.Options{})
	if !errors.Is(err, sentinel) || backend != nil {
		t.Fatalf("construction returned %v, %v", backend, err)
	}
	if source.closed {
		t.Fatal("query construction closed application-owned storage")
	}
}
