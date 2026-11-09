package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/adiom-data/weave/benchmarks/query/internal/bench"
	"github.com/adiom-data/weave/benchmarks/query/internal/diagnostics"
)

func main() {
	if err := mainCommand(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func mainCommand() error {
	if len(os.Args) < 2 {
		return fmt.Errorf("usage: bench-runner seed|run|wait [flags]")
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	command := os.Args[1]
	if command == "wait" {
		flags := flag.NewFlagSet("wait", flag.ContinueOnError)
		endpoint := flags.String("url", "", "readiness URL")
		timeout := flags.Duration("timeout", 2*time.Minute, "readiness timeout")
		if err := flags.Parse(os.Args[2:]); err != nil {
			return err
		}
		return wait(ctx, *endpoint, *timeout)
	}
	if command != "seed" && command != "run" {
		return fmt.Errorf("unknown command %q", command)
	}
	if command == "run" {
		stopDiagnostics, err := diagnostics.StartFromEnv(ctx, "runner")
		if err != nil {
			return err
		}
		defer stopDiagnostics()
	}
	flags := flag.NewFlagSet(command, flag.ContinueOnError)
	c := bench.Config{}
	flags.StringVar(&c.Workload, "workload", "flat", "flat (live writes), expansion, or windows (read only)")
	flags.BoolVar(&c.ReferenceFetch, "reference-fetch", false, "use Weave's joined loading hint for expansion")
	flags.StringVar(&c.Backend, "backend", "weave", "weave, electric, drasi, weave-embedded, or weave-remote")
	flags.StringVar(&c.Scenario, "scenario", "shared", "shared or independent")
	flags.IntVar(&c.Clients, "clients", 100, "independent network clients")
	flags.IntVar(&c.Projects, "projects", 1000, "seeded projects (>=clients for independent)")
	flags.IntVar(&c.Rows, "rows", 100, "rows per project, 1..999; full project on both backends")
	flags.IntVar(&c.PayloadBytes, "payload-bytes", 128, "bytes of unchanged text per row")
	flags.IntVar(&c.Rate, "rate", 100, "scheduled sequential SQL updates per second")
	flags.DurationVar(&c.Duration, "duration", 30*time.Second, "measurement duration")
	flags.DurationVar(&c.Warmup, "warmup", 2*time.Second, "unmeasured writes before measurement")
	flags.IntVar(&c.InitConcurrency, "init-concurrency", 32, "concurrent initial subscriptions")
	flags.DurationVar(&c.InitTimeout, "init-timeout", 2*time.Minute, "all-clients initialization deadline")
	flags.DurationVar(&c.DrainTimeout, "drain-timeout", 30*time.Second, "deadline to receive every committed update")
	grpcAddress := flags.String("grpc", "weave:9090", "query service address")
	electricURL := flags.String("electric", "http://electric:3000/v1/shape", "shape endpoint")
	drasiURL := flags.String("drasi", "http://drasi:8080", "Drasi management endpoint")
	drasiEvents := flags.String("drasi-events", "http://drasi:8081", "Drasi SSE reaction endpoint")
	if err := flags.Parse(os.Args[2:]); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("unexpected arguments: %v", flags.Args())
	}
	if err := c.Validate(); err != nil {
		return err
	}
	poolConfig, err := pgxpool.ParseConfig(os.Getenv("DATABASE_URL"))
	if err != nil {
		return err
	}
	poolConfig.MaxConns = 2
	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return err
	}
	defer pool.Close()
	if command == "seed" {
		return bench.Seed(ctx, pool, c)
	}
	if err := bench.CheckDatabase(ctx, pool); err != nil {
		return err
	}
	if err := bench.VerifySeed(ctx, pool, c); err != nil {
		return err
	}
	if c.Workload == "expansion" {
		report, runErr := bench.RunExpansion(ctx, c, *grpcAddress, *electricURL)
		if err := json.NewEncoder(os.Stdout).Encode(report); err != nil {
			return err
		}
		return runErr
	}
	if c.Workload == "windows" {
		report, runErr := bench.RunWindows(ctx, c, *grpcAddress, *electricURL)
		if err := json.NewEncoder(os.Stdout).Encode(report); err != nil {
			return err
		}
		return runErr
	}
	var adapter bench.Adapter = &bench.Weave{Address: *grpcAddress, Rows: c.Rows}
	if c.Backend == "weave-embedded" || c.Backend == "weave-remote" {
		adapter = &bench.WeaveProjection{Weave: bench.Weave{Address: *grpcAddress, Rows: c.Rows}}
	}
	if c.Backend == "electric" {
		adapter = &bench.Electric{URL: *electricURL}
	}
	if c.Backend == "drasi" {
		adapter = &bench.Drasi{URL: *drasiURL, EventsURL: *drasiEvents, Rows: c.Rows, CaptureReady: func(ctx context.Context) error { return bench.WaitDrasiCapture(ctx, pool) }}
	}
	report, runErr := bench.Run(ctx, pool, c, adapter)
	if runErr != nil {
		report.Error = runErr.Error()
	}
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(report); err != nil {
		return err
	}
	return runErr
}
func wait(parent context.Context, endpoint string, timeout time.Duration) error {
	if endpoint == "" || timeout <= 0 {
		return fmt.Errorf("wait requires URL and positive timeout")
	}
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()
	client := &http.Client{Timeout: 2 * time.Second}
	for {
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
		if err != nil {
			return err
		}
		response, err := client.Do(request)
		if err == nil {
			response.Body.Close()
			if response.StatusCode == http.StatusOK {
				return nil
			}
		}
		timer := time.NewTimer(100 * time.Millisecond)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return fmt.Errorf("readiness %s: %w", endpoint, ctx.Err())
		}
	}
}
