package postgres

import (
	"os"
	"testing"

	"github.com/adiom-data/weave/internal/readfence"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestReadFenceRejectsUnreachablePositionsBeforeRegistration(t *testing.T) {
	dsn := os.Getenv("WEAVE_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set WEAVE_TEST_DATABASE_URL to test read fences")
	}
	pool, err := pgxpool.New(t.Context(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	source := NewPostgresDataSourceWithOptions(pool, nil, DataSourceOptions{})
	token, err := CaptureReadFence(t.Context(), pool)
	if err != nil {
		t.Fatal(err)
	}
	fence, err := source.DecodeReadFence(t.Context(), token)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := source.DecodeReadFence(t.Context(), readfence.Encode(^uint64(0))); err == nil {
		t.Fatal("accepted a future fence")
	}
	// Rejection is caller-local: the original valid fence still decodes.
	if again, err := source.DecodeReadFence(t.Context(), token); err != nil || again.Order() != fence.Order() {
		t.Fatalf("valid fence affected by rejection: %+v %v", again, err)
	}
}
