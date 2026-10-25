package bench

import (
	"context"
	"math"
	"testing"
	"time"
)

func testConfig() Config {
	return Config{Backend: "weave", Scenario: "shared", Clients: 2, Projects: 2, Rows: 2, PayloadBytes: 1, Rate: 10, Duration: time.Second, InitConcurrency: 2, InitTimeout: time.Second, DrainTimeout: time.Second}
}

func TestConfigValidation(t *testing.T) {
	c := testConfig()
	if err := c.Validate(); err != nil {
		t.Fatal(err)
	}
	for _, edit := range []func(*Config){func(c *Config) { c.Rows = 1000 }, func(c *Config) { c.Clients = 0 }, func(c *Config) { c.Rate = 0 }, func(c *Config) { c.Duration = time.Duration(math.MaxInt64); c.Warmup = time.Duration(math.MaxInt64) }, func(c *Config) { c.Scenario = "independent"; c.Clients = 3 }, func(c *Config) { c.Duration = time.Hour; c.Rate = 100000 }} {
		bad := c
		edit(&bad)
		if err := bad.Validate(); err == nil {
			t.Fatalf("accepted invalid config %+v", bad)
		}
	}
}
func TestMutationSequenceIsDeterministicAndWithinSubscribedRows(t *testing.T) {
	for _, scenario := range []string{"shared", "independent"} {
		c := testConfig()
		c.Scenario = scenario
		for seq := int64(1); seq <= 20; seq++ {
			project, id := c.Mutation(seq)
			if project < 1 || project > int64(c.Clients) || id < (project-1)*int64(c.Rows)+1 || id > project*int64(c.Rows) {
				t.Fatalf("invalid target %d,%d", project, id)
			}
			if scenario == "shared" && project != 1 {
				t.Fatal("shared scenario targets another project")
			}
		}
	}
}
func TestHistogramBoundedQuantilesAndLargeSums(t *testing.T) {
	var h Histogram
	for i := 1; i <= 1000; i++ {
		h.Add(time.Duration(i) * time.Microsecond)
	}
	s := h.Summary()
	if s.Count != 1000 || s.P50 < .5 || s.P50 > .511 || s.P99 < .99 || s.P99 > 1 || s.Max != 1 {
		t.Fatalf("histogram = %+v", s)
	}
	if len(h.counts) > 2000 {
		t.Fatal("histogram uses unbounded sample storage")
	}
	var big Histogram
	for range 3 {
		big.Add(time.Duration(math.MaxInt64 / 2))
	}
	if big.Summary().Mean <= 0 {
		t.Fatal("histogram sum overflowed")
	}
}
func TestRecorderTracksEveryExpectedClientWithoutDuplicateReceipts(t *testing.T) {
	c := testConfig()
	r := NewRecorder(c)
	r.Register(1, time.Now().Add(-time.Millisecond), true)
	row := Task{ID: 1, ProjectID: 1, Revision: 1, Payload: "x"}
	if err := r.Observe(0, row); err != nil {
		t.Fatal(err)
	}
	if err := r.Observe(0, row); err == nil {
		t.Fatal("accepted duplicate delivery")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	if err := r.Wait(ctx); err == nil {
		t.Fatal("completed before all clients received update")
	}
	if err := r.Observe(1, row); err != nil {
		t.Fatal(err)
	}
	if err := r.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	if r.latency.Summary().Count != 2 || r.first.Summary().Count != 1 || r.last.Summary().Count != 1 || r.remaining != 0 {
		t.Fatal("incorrect delivery accounting")
	}
}
func TestIndependentRoutingAndWarmupAreNotCountedAsFanout(t *testing.T) {
	c := testConfig()
	c.Scenario = "independent"
	r := NewRecorder(c)
	r.Register(2, time.Now(), false)
	row := Task{ID: 3, ProjectID: 2, Revision: 2, Payload: "x"}
	if err := r.Observe(0, row); err == nil {
		t.Fatal("accepted cross-project leak")
	}
	if err := r.Observe(1, row); err != nil {
		t.Fatal(err)
	}
	if r.remaining != 0 || r.latency.Summary().Count != 0 {
		t.Fatal("warmup contaminated measurement")
	}
}
func TestObserverValidatesInitialAndFinalState(t *testing.T) {
	c := testConfig()
	c.Rows = 1
	r := NewRecorder(c)
	o := NewObserver(0, r)
	row := Task{ID: 1, ProjectID: 1, Payload: "x"}
	if err := o.Ready([]Task{row}); err != nil {
		t.Fatal(err)
	}
	if err := o.Ready([]Task{row}); err == nil {
		t.Fatal("accepted an unexpected resnapshot")
	}
	r.Register(1, time.Now(), true)
	row.Revision = 1
	if err := o.Apply([]Change{{Task: row}}); err != nil {
		t.Fatal(err)
	}
	if err := o.Verify(map[int64]Task{1: row}); err != nil {
		t.Fatal(err)
	}
	row.Payload = "wrong"
	if err := o.Verify(map[int64]Task{1: row}); err == nil {
		t.Fatal("missed incorrect final data")
	}
	if err := o.Apply([]Change{{Task: row}}); err == nil {
		t.Fatal("accepted non-advancing revision")
	}
}
