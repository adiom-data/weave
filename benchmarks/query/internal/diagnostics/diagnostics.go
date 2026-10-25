// Package diagnostics provides opt-in process profiling for the benchmark only.
// It adds no hooks, endpoints, or dependencies to the production engine.
package diagnostics

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"runtime/metrics"
	"runtime/pprof"
	"runtime/trace"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/adiom-data/weave/pkg/querysubscription"
)

// StartFromEnv is disabled unless BENCH_DIAGNOSTICS_DIR is set. Each process
// has a distinct role/file prefix. The caller must stop before process exit.
func StartFromEnv(parent context.Context, role string) (func(), error) {
	dir := os.Getenv("BENCH_DIAGNOSTICS_DIR")
	if dir == "" {
		return func() {}, nil
	}
	if role != "server" && role != "runner" {
		return nil, errors.New("unknown benchmark diagnostic role")
	}
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, err
	}
	var files []*os.File
	var cpu, tracing bool
	var heap *os.File
	ctx, cancel := context.WithCancel(parent)
	var done chan struct{}
	var once sync.Once
	stop := func() {
		once.Do(func() {
			cancel()
			if done != nil {
				<-done
			}
			if tracing {
				trace.Stop()
			}
			if cpu {
				pprof.StopCPUProfile()
			}
			if heap != nil {
				_ = pprof.WriteHeapProfile(heap)
			}
			for _, file := range files {
				_ = file.Close()
			}
		})
	}
	open := func(suffix string) (*os.File, error) {
		file, err := os.OpenFile(filepath.Join(dir, role+suffix), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if err == nil {
			files = append(files, file)
		}
		return file, err
	}
	if os.Getenv("BENCH_DIAGNOSTICS_TRACE") == "1" {
		file, err := open(".trace")
		if err != nil {
			stop()
			return nil, err
		}
		if err := trace.Start(file); err != nil {
			stop()
			return nil, err
		}
		tracing = true
	}
	if os.Getenv("BENCH_DIAGNOSTICS_CPU") == "1" {
		file, err := open(".cpu")
		if err != nil {
			stop()
			return nil, err
		}
		if err := pprof.StartCPUProfile(file); err != nil {
			stop()
			return nil, err
		}
		cpu = true
	}
	if os.Getenv("BENCH_DIAGNOSTICS_HEAP") == "1" {
		var err error
		heap, err = open(".heap")
		if err != nil {
			stop()
			return nil, err
		}
	}
	file, err := open(".samples.jsonl")
	if err != nil {
		stop()
		return nil, err
	}
	period := 100 * time.Millisecond
	if value := os.Getenv("BENCH_DIAGNOSTICS_PERIOD"); value != "" {
		period, err = time.ParseDuration(value)
		if err != nil || period <= 0 {
			stop()
			return nil, errors.New("invalid BENCH_DIAGNOSTICS_PERIOD")
		}
	}
	done = make(chan struct{})
	go func() {
		defer close(done)
		samples := []metrics.Sample{
			{Name: "/gc/cycles/total:gc-cycles"},
			{Name: "/gc/heap/live:bytes"},
			{Name: "/memory/classes/heap/objects:bytes"},
			{Name: "/cpu/classes/gc/total:cpu-seconds"},
			{Name: "/cpu/classes/gc/mark/assist:cpu-seconds"},
			{Name: "/cpu/classes/total:cpu-seconds"},
			{Name: "/sched/goroutines:goroutines"},
			{Name: "/sched/gomaxprocs:threads"},
		}
		encoder := json.NewEncoder(file)
		capture := func() {
			now := time.Now()
			// Anchors Go's monotonic trace clock to cross-process wall timestamps.
			trace.Log(ctx, "wall_unix_ns", strconv.FormatInt(now.UnixNano(), 10))
			metrics.Read(samples)
			values := make(map[string]any, len(samples))
			for _, sample := range samples {
				switch sample.Value.Kind() {
				case metrics.KindUint64:
					values[sample.Name] = sample.Value.Uint64()
				case metrics.KindFloat64:
					values[sample.Name] = sample.Value.Float64()
				}
			}
			_ = encoder.Encode(struct {
				UnixNS  int64             `json:"unix_ns"`
				Runtime map[string]any    `json:"runtime"`
				CPU     map[string]uint64 `json:"cgroup_cpu,omitempty"`
			}{now.UnixNano(), values, cgroupCPU()})
		}
		ticker := time.NewTicker(period)
		defer ticker.Stop()
		capture()
		for {
			select {
			case <-ctx.Done():
				capture()
				return
			case <-ticker.C:
				capture()
			}
		}
	}()
	return stop, nil
}

func cgroupCPU() map[string]uint64 {
	data, err := os.ReadFile("/sys/fs/cgroup/cpu.stat")
	if err != nil {
		data, err = os.ReadFile("/sys/fs/cgroup/cpu/cpu.stat")
	}
	if err != nil {
		return nil
	}
	fields := strings.Fields(string(data))
	values := map[string]uint64{}
	for i := 0; i+1 < len(fields); i += 2 {
		if value, err := strconv.ParseUint(fields[i+1], 10, 64); err == nil {
			values[fields[i]] = value
		}
	}
	return values
}

// WrapClient labels server-side subscription lifetimes in the runtime trace.
func WrapClient(client querysubscription.Client) querysubscription.Client {
	if os.Getenv("BENCH_DIAGNOSTICS_DIR") == "" {
		return client
	}
	return tracedClient{client}
}

type tracedClient struct{ querysubscription.Client }

func (c tracedClient) OpenFeed(ctx context.Context, options querysubscription.FeedOptions) (querysubscription.Feed, error) {
	feed, err := c.Client.OpenFeed(ctx, options)
	if err != nil {
		return nil, err
	}
	return tracedFeed{feed}, nil
}

type tracedFeed struct{ querysubscription.Feed }

func (f tracedFeed) SubscribeQuery(ctx context.Context, request querysubscription.QuerySubscribeRequest, install querysubscription.QuerySnapshotInstaller) error {
	ctx, task := trace.NewTask(ctx, "SubscribeQuery")
	defer task.End()
	trace.Log(ctx, "subscription", request.ID)
	return f.Feed.SubscribeQuery(ctx, request, install)
}

func (f tracedFeed) SubscribeEntity(ctx context.Context, request querysubscription.EntitySubscribeRequest, install querysubscription.EntitySnapshotInstaller) error {
	ctx, task := trace.NewTask(ctx, "SubscribeEntity")
	defer task.End()
	trace.Log(ctx, "subscription", request.ID)
	return f.Feed.SubscribeEntity(ctx, request, install)
}
