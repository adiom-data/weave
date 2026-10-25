package diagnostics

import (
	"bufio"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestDiagnosticsAreOptInAndStopIsIdempotent(t *testing.T) {
	t.Setenv("BENCH_DIAGNOSTICS_DIR", "")
	stop, err := StartFromEnv(context.Background(), "server")
	if err != nil {
		t.Fatal(err)
	}
	stop()
	stop()
	dir := t.TempDir()
	t.Setenv("BENCH_DIAGNOSTICS_DIR", dir)
	t.Setenv("BENCH_DIAGNOSTICS_TRACE", "0")
	t.Setenv("BENCH_DIAGNOSTICS_CPU", "0")
	t.Setenv("BENCH_DIAGNOSTICS_PERIOD", "100ms")
	stop, err = StartFromEnv(context.Background(), "server")
	if err != nil {
		t.Fatal(err)
	}
	stop()
	stop()
	file, err := os.Open(filepath.Join(dir, "server.samples.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	count := 0
	for scanner.Scan() {
		var sample struct {
			UnixNS  int64          `json:"unix_ns"`
			Runtime map[string]any `json:"runtime"`
		}
		if err := json.Unmarshal(scanner.Bytes(), &sample); err != nil {
			t.Fatal(err)
		}
		if sample.UnixNS == 0 || len(sample.Runtime) == 0 {
			t.Fatal("incomplete runtime sample")
		}
		count++
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Fatalf("expected initial/final samples, got %d", count)
	}
	if _, err := StartFromEnv(context.Background(), "server"); err == nil {
		t.Fatal("overwrote existing diagnostics")
	}
}

func TestDiagnosticsRejectInvalidSamplePeriod(t *testing.T) {
	t.Setenv("BENCH_DIAGNOSTICS_DIR", t.TempDir())
	t.Setenv("BENCH_DIAGNOSTICS_TRACE", "0")
	t.Setenv("BENCH_DIAGNOSTICS_CPU", "0")
	t.Setenv("BENCH_DIAGNOSTICS_PERIOD", "0s")
	if _, err := StartFromEnv(context.Background(), "runner"); err == nil {
		t.Fatal("accepted invalid period")
	}
}
