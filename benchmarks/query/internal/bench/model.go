// Package bench owns only workload generation, observation, and verification.
// No benchmark hooks are added to the reactive engine.
package bench

import (
	"context"
	"fmt"
	"math"
	"sort"
	"sync"
	"time"
)

type Config struct {
	Workload        string        `json:"workload,omitempty"`
	ReferenceFetch  bool          `json:"reference_fetch,omitempty"`
	Backend         string        `json:"backend"`
	Scenario        string        `json:"scenario"`
	Clients         int           `json:"clients"`
	Projects        int           `json:"projects"`
	Rows            int           `json:"rows_per_project"`
	PayloadBytes    int           `json:"payload_bytes"`
	Rate            int           `json:"requested_writes_per_second"`
	Duration        time.Duration `json:"duration_ns"`
	Warmup          time.Duration `json:"warmup_ns"`
	InitConcurrency int           `json:"initialization_concurrency"`
	InitTimeout     time.Duration `json:"initialization_timeout_ns"`
	DrainTimeout    time.Duration `json:"drain_timeout_ns"`
}

func (c Config) Validate() error {
	if c.Workload != "" && c.Workload != "flat" && c.Workload != "expansion" && c.Workload != "windows" {
		return fmt.Errorf("workload must be flat, expansion, or windows")
	}
	if (c.Workload == "expansion" || c.Workload == "windows") && (c.Backend != "weave" && c.Backend != "electric" || c.Clients > c.Projects || c.InitConcurrency != 1) {
		return fmt.Errorf("read workloads require weave/electric, projects >= clients, and init-concurrency=1 (sequential samples)")
	}
	if c.Workload == "windows" && (c.Rows < 4 || c.ReferenceFetch) {
		return fmt.Errorf("windows requires at least four rows and no reference-fetch hint")
	}
	if c.Backend != "weave" && c.Backend != "electric" && c.Backend != "drasi" && c.Backend != "weave-embedded" && c.Backend != "weave-remote" {
		return fmt.Errorf("backend must be weave, electric, drasi, weave-embedded, or weave-remote")
	}
	if c.Scenario != "shared" && c.Scenario != "independent" {
		return fmt.Errorf("scenario must be shared or independent")
	}
	if c.Clients < 1 || c.Clients > 10000 || c.Projects < 1 || c.Projects > 100000 || c.Rows < 1 || c.Rows > 999 || c.PayloadBytes < 1 || c.PayloadBytes > 65536 || c.Rate < 1 || c.Rate > 100000 || c.InitConcurrency < 1 || c.InitTimeout <= 0 || c.DrainTimeout <= 0 || c.Duration <= 0 || c.Duration > time.Hour || c.Warmup < 0 || c.Warmup > time.Hour-c.Duration {
		return fmt.Errorf("invalid configuration: clients 1..10000, projects 1..100000, rows 1..999, payload 1..65536, rate 1..100000; positive timeouts/duration/concurrency; total write duration <=1h")
	}
	if c.Scenario == "independent" && c.Clients > c.Projects {
		return fmt.Errorf("independent scenario needs at least one project per client")
	}
	if (c.Duration+c.Warmup).Seconds()*float64(c.Rate) > 1000000 {
		return fmt.Errorf("initial harness supports at most one million writes per run")
	}
	return nil
}

func (c Config) Project(client int) int64 {
	if c.Scenario == "shared" {
		return 1
	}
	return int64(client + 1)
}
func (c Config) Mutation(sequence int64) (project, id int64) {
	projects := 1
	if c.Scenario == "independent" {
		projects = c.Clients
	}
	project = (sequence-1)%int64(projects) + 1
	row := (sequence-1)/int64(projects)%int64(c.Rows) + 1
	return project, (project-1)*int64(c.Rows) + row
}

type Task struct {
	ID        int64
	ProjectID int64
	Revision  int64
	Payload   string
}
type Change struct {
	Deleted bool
	Task    Task
}

// Adapter runs one real, independent network subscription until cancellation.
// Ready installs a complete initial set; Apply supplies received row changes.
type Adapter interface {
	Run(context.Context, int64, *Observer) error
	Bytes() uint64
}

// Histogram uses bounded storage with logarithmic buckets <=2% apart.
// Percentiles are bucket upper bounds; min/max are exact. Sum uses float64
// nanoseconds to avoid time.Duration overflow across many deliveries.
type Histogram struct {
	bounds   []time.Duration
	counts   []uint64
	count    uint64
	min, max time.Duration
	total    float64
}
type Distribution struct {
	Count uint64  `json:"count"`
	Min   float64 `json:"min_ms"`
	Mean  float64 `json:"mean_ms"`
	P50   float64 `json:"p50_ms"`
	P95   float64 `json:"p95_ms"`
	P99   float64 `json:"p99_ms"`
	Max   float64 `json:"max_ms"`
}

func (h *Histogram) Add(d time.Duration) {
	if d < 0 {
		d = 0
	}
	if h.bounds == nil {
		for b := time.Microsecond; b < time.Hour; b = b + b/50 + 1 {
			h.bounds = append(h.bounds, b)
		}
		h.bounds = append(h.bounds, time.Duration(math.MaxInt64))
		h.counts = make([]uint64, len(h.bounds))
	}
	i := sort.Search(len(h.bounds), func(i int) bool { return h.bounds[i] >= d })
	h.counts[i]++
	h.count++
	if h.count == 1 || d < h.min {
		h.min = d
	}
	if d > h.max {
		h.max = d
	}
	h.total += float64(d)
}
func (h *Histogram) Summary() Distribution {
	if h.count == 0 {
		return Distribution{}
	}
	percentile := func(p float64) float64 {
		target := uint64(math.Ceil(float64(h.count) * p))
		var n uint64
		for i, c := range h.counts {
			n += c
			if n >= target {
				return float64(min(h.bounds[i], h.max)) / float64(time.Millisecond)
			}
		}
		return 0
	}
	return Distribution{Count: h.count, Min: float64(h.min) / 1e6, Mean: float64(h.total) / float64(h.count) / 1e6, P50: percentile(.5), P95: percentile(.95), P99: percentile(.99), Max: float64(h.max) / 1e6}
}

type sample struct {
	start       time.Time
	project, id int64
	measured    bool
	seen        []uint64
	count       int
}
type Recorder struct {
	mu                   sync.Mutex
	config               Config
	samples              map[int64]*sample
	remaining            uint64
	expected, received   uint64
	latency, first, last Histogram
	notify               chan struct{}
}

func NewRecorder(c Config) *Recorder {
	return &Recorder{config: c, samples: make(map[int64]*sample), notify: make(chan struct{}, 1)}
}
func (r *Recorder) Register(sequence int64, start time.Time, measured bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	project, id := r.config.Mutation(sequence)
	n := 1
	if r.config.Scenario == "shared" {
		n = r.config.Clients
	}
	r.samples[sequence] = &sample{start: start, project: project, id: id, measured: measured, seen: make([]uint64, (n+63)/64)}
	r.remaining += uint64(n)
	if measured {
		r.expected += uint64(n)
	}
}
func (r *Recorder) Observe(client int, row Task) error {
	if row.Revision == 0 {
		return nil
	}
	now := time.Now()
	r.mu.Lock()
	defer r.mu.Unlock()
	s := r.samples[row.Revision]
	if s == nil || s.id != row.ID || s.project != row.ProjectID || r.config.Project(client) != row.ProjectID {
		return fmt.Errorf("client %d received unexpected row/revision: %+v", client, row)
	}
	index := 0
	if r.config.Scenario == "shared" {
		index = client
	}
	word, bit := index/64, uint(index%64)
	if s.seen[word]&(1<<bit) != 0 {
		return fmt.Errorf("client %d received duplicate revision %d", client, row.Revision)
	}
	s.seen[word] |= 1 << bit
	s.count++
	r.remaining--
	if s.measured {
		r.received++
		latency := now.Sub(s.start)
		r.latency.Add(latency)
		if s.count == 1 {
			r.first.Add(latency)
		}
		n := 1
		if r.config.Scenario == "shared" {
			n = r.config.Clients
		}
		if s.count == n {
			r.last.Add(latency)
		}
	}
	select {
	case r.notify <- struct{}{}:
	default:
	}
	return nil
}
func (r *Recorder) Wait(ctx context.Context) error {
	for {
		r.mu.Lock()
		n := r.remaining
		r.mu.Unlock()
		if n == 0 {
			return nil
		}
		select {
		case <-r.notify:
		case <-ctx.Done():
			return fmt.Errorf("%d expected deliveries missing: %w", n, ctx.Err())
		}
	}
}

type Observer struct {
	mu          sync.Mutex
	client      int
	project     int64
	rows        map[int64]Task
	recorder    *Recorder
	ready       chan error
	initialized bool
}

func NewObserver(client int, r *Recorder) *Observer {
	return &Observer{client: client, project: r.config.Project(client), recorder: r, rows: make(map[int64]Task), ready: make(chan error, 1)}
}
func (o *Observer) Ready(rows []Task) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.initialized {
		return fmt.Errorf("client %d unexpectedly resynchronized", o.client)
	}
	if len(rows) != o.recorder.config.Rows {
		return fmt.Errorf("client %d initial rows %d, expected %d", o.client, len(rows), o.recorder.config.Rows)
	}
	for _, row := range rows {
		if row.ProjectID != o.project || row.Revision != 0 || len(row.Payload) != o.recorder.config.PayloadBytes {
			return fmt.Errorf("invalid initial row %+v", row)
		}
		if _, exists := o.rows[row.ID]; exists {
			return fmt.Errorf("duplicate initial row %d", row.ID)
		}
		o.rows[row.ID] = row
	}
	o.initialized = true
	o.ready <- nil
	return nil
}
func (o *Observer) Apply(changes []Change) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if !o.initialized {
		return fmt.Errorf("changes before initial synchronization")
	}
	for _, change := range changes {
		if change.Deleted {
			delete(o.rows, change.Task.ID)
			continue
		}
		row := change.Task
		if row.ProjectID != o.project {
			return fmt.Errorf("row outside subscribed project")
		}
		if old, exists := o.rows[row.ID]; exists && row.Revision <= old.Revision {
			return fmt.Errorf("non-advancing row revision %d", row.Revision)
		}
		o.rows[row.ID] = row
		if err := o.recorder.Observe(o.client, row); err != nil {
			return err
		}
	}
	return nil
}
func (o *Observer) Verify(want map[int64]Task) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if len(o.rows) != len(want) {
		return fmt.Errorf("client %d final row count mismatch: got %d want %d", o.client, len(o.rows), len(want))
	}
	for id, row := range want {
		if got, ok := o.rows[id]; !ok || got != row {
			return fmt.Errorf("client %d final row %d does not match PostgreSQL", o.client, id)
		}
	}
	return nil
}
