package bench

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// BenchmarkLogicalBarrierFlush isolates WAL durability from query assembly.
// Use only a disposable PostgreSQL 17+ benchmark DB: each operation writes one
// nontransactional logical message, then observes the flush frontier. No tables
// or replication slots are created. Polling adds roughly millisecond precision.
func BenchmarkLogicalBarrierFlush(b *testing.B) {
	url := os.Getenv("WEAVE_TEST_DATABASE_URL")
	if url == "" {
		b.Skip("set WEAVE_TEST_DATABASE_URL to a disposable benchmark database")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	conn, err := pgx.Connect(ctx, url)
	if err != nil {
		b.Fatal(err)
	}
	defer conn.Close(context.Background())
	var database string
	var version int
	if err := conn.QueryRow(ctx, "SELECT current_database(), current_setting('server_version_num')::integer").Scan(&database, &version); err != nil {
		b.Fatal(err)
	}
	if database != "weave_query_benchmark" {
		b.Fatal("refusing non-benchmark database")
	}
	if version < 170000 {
		b.Skip("flush argument requires PostgreSQL 17+")
	}
	for _, flush := range []bool{false, true} {
		b.Run(fmt.Sprintf("flush=%v", flush), func(b *testing.B) {
			for range b.N {
				var lsn string
				if err := conn.QueryRow(ctx, "SELECT pg_logical_emit_message(false, 'weave_bench_flush_probe', '', $1)::text", flush).Scan(&lsn); err != nil {
					b.Fatal(err)
				}
				for {
					var covered bool
					if err := conn.QueryRow(ctx, "SELECT pg_current_wal_flush_lsn() >= $1::pg_lsn", lsn).Scan(&covered); err != nil {
						b.Fatal(err)
					}
					if covered {
						break
					}
					timer := time.NewTimer(time.Millisecond)
					select {
					case <-ctx.Done():
						timer.Stop()
						b.Fatal(ctx.Err())
					case <-timer.C:
					}
				}
			}
		})
	}
}
