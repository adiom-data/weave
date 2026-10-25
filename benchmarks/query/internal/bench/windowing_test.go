package bench

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestWindowConfigAndOracle(t *testing.T) {
	c := testConfig()
	c.Workload, c.InitConcurrency, c.Rows = "windows", 1, 8
	if err := c.Validate(); err != nil {
		t.Fatal(err)
	}
	for _, edit := range []func(*Config){
		func(c *Config) { c.Rows = 3 },
		func(c *Config) { c.ReferenceFetch = true },
		func(c *Config) { c.Backend = "zero" },
		func(c *Config) { c.InitConcurrency = 2 },
		func(c *Config) { c.Clients = 3 },
	} {
		bad := c
		edit(&bad)
		if err := bad.Validate(); err == nil {
			t.Fatalf("accepted %+v", bad)
		}
	}
	step := windowSteps(c.Rows)[1]
	tasks := []Task{{ID: 9, ProjectID: 2, Payload: "x"}, {ID: 10, ProjectID: 2, Payload: "x"}, {ID: 11, ProjectID: 2, Payload: "x"}, {ID: 12, ProjectID: 2, Payload: "x"}}
	if err := verifyWindow(c, 2, step, tasks); err != nil {
		t.Fatal(err)
	}
	if err := verifyWindow(c, 2, step, tasks[:3]); err == nil {
		t.Fatal("accepted missing row")
	}
	tasks[3] = tasks[2]
	if err := verifyWindow(c, 2, step, tasks); err == nil {
		t.Fatal("accepted duplicate row")
	}
	tasks[3].ID = 13
	if err := verifyWindow(c, 2, step, tasks); err == nil {
		t.Fatal("accepted out-of-window row")
	}
}

func TestElectricWindowReplacementRequestsAndMeasurements(t *testing.T) {
	c := testConfig()
	c.Workload, c.Backend, c.InitConcurrency, c.Rows = "windows", "electric", 1, 8
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		var project, left, right int64
		if r.URL.Query().Get("table") != "bench.tasks" {
			t.Error("wrong table")
		}
		if _, err := fmt.Sscanf(r.URL.Query().Get("where"), "project_id = %d AND id > %d AND id <= %d", &project, &left, &right); err != nil {
			t.Error(err)
			w.WriteHeader(400)
			return
		}
		fmt.Fprint(w, "[")
		for id := left + 1; id <= right; id++ {
			fmt.Fprintf(w, `{"key":"%d","headers":{"operation":"insert"},"value":{"id":"%d","project_id":"%d","revision":"0","payload":"x"}},`, id, id, project)
		}
		fmt.Fprint(w, `{"headers":{"control":"up-to-date"}}]`)
	}))
	defer server.Close()
	report, err := RunWindows(context.Background(), c, "", server.URL)
	if err != nil {
		t.Fatal(err)
	}
	if report.Status != "ok" || report.Mode != "bounded_shape_replacements" || requests != 16 || len(report.Steps) != 8 || len(report.Samples) != 16 {
		t.Fatalf("report=%+v requests=%d", report, requests)
	}
	for name, distribution := range report.Steps {
		if distribution.Count != 2 || distribution.P99 < distribution.P50 {
			t.Fatalf("%s: %+v", name, distribution)
		}
	}
}
