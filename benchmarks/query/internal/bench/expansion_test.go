package bench

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestExpansionConfigAndOracle(t *testing.T) {
	c := testConfig()
	c.Workload = "expansion"
	c.InitConcurrency = 1
	if err := c.Validate(); err != nil {
		t.Fatal(err)
	}
	bad := c
	bad.Backend = "zero"
	if err := bad.Validate(); err == nil {
		t.Fatal("accepted unsupported expansion backend")
	}
	bad = c
	bad.InitConcurrency = 2
	if err := bad.Validate(); err == nil {
		t.Fatal("accepted ambiguous concurrent cold/warm sampling")
	}
	bad = c
	bad.Clients = 3
	if err := bad.Validate(); err == nil {
		t.Fatal("not enough distinct cold selectors")
	}
	links := []expansionLink{{id: 3, project: 2, task: 3}, {id: 4, project: 2, task: 4}}
	if err := validateExpansionLinks(c, 2, links); err != nil {
		t.Fatal(err)
	}
	links[1] = links[0]
	if err := validateExpansionLinks(c, 2, links); err == nil {
		t.Fatal("accepted duplicate link")
	}
	tasks := []Task{{ID: 3, ProjectID: 2, Payload: "x"}, {ID: 4, ProjectID: 2, Payload: "x"}}
	if err := verifyExpansionTasks(c, 2, tasks); err != nil {
		t.Fatal(err)
	}
	tasks[1].Revision = 1
	if err := verifyExpansionTasks(c, 2, tasks); err == nil {
		t.Fatal("accepted mutated seed")
	}
}

func TestElectricExpansionDiscoversIDsAndBatchesTargets(t *testing.T) {
	c := testConfig()
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		q := r.URL.Query()
		w.Header().Set("electric-offset", "0_0")
		switch q.Get("table") {
		case "bench.links":
			if q.Get("where") != "project_id = 2" {
				t.Error("wrong association filter")
			}
			fmt.Fprint(w, `[{"key":"a","headers":{"operation":"insert"},"value":{"id":"4","project_id":"2","task_id":"4"}},{"key":"b","headers":{"operation":"insert"},"value":{"id":3,"project_id":2,"task_id":3}},{"headers":{"control":"up-to-date"}}]`)
		case "bench.tasks":
			if q.Get("where") != "id = ANY('{3,4}'::bigint[])" {
				t.Error("did not batch discovered IDs")
			}
			fmt.Fprint(w, `[{"key":"c","headers":{"operation":"insert"},"value":{"id":"3","project_id":"2","revision":0,"payload":"x"}},{"key":"d","headers":{"operation":"insert"},"value":{"id":4,"project_id":2,"revision":"0","payload":"x"}},{"headers":{"control":"up-to-date"}}]`)
		default:
			t.Error("unexpected table")
		}
	}))
	defer server.Close()
	tasks, bytes, err := readElectricExpansion(context.Background(), server.URL, c, 2)
	if err != nil {
		t.Fatal(err)
	}
	if err := verifyExpansionTasks(c, 2, tasks); err != nil {
		t.Fatal(err)
	}
	if requests != 2 || bytes == 0 {
		t.Fatalf("requests=%d bytes=%d", requests, bytes)
	}
}

func TestElectricInitialShapeContinuationAndFailures(t *testing.T) {
	for _, mode := range []string{"pages", "duplicate", "resync", "stalled", "trailing", "http"} {
		t.Run(mode, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				q := r.URL.Query()
				w.Header().Set("electric-offset", "0_0")
				w.Header().Set("electric-handle", "shape")
				if q.Get("offset") == "-1" {
					if mode == "resync" {
						fmt.Fprint(w, `[{"headers":{"control":"must-refetch"}}]`)
						return
					}
					if mode == "http" {
						w.WriteHeader(429)
						return
					}
					fmt.Fprint(w, `[{"key":"a","headers":{"operation":"insert"},"value":{"id":9007199254740993}}]`)
					if mode == "trailing" {
						fmt.Fprint(w, `{}`)
					}
					return
				}
				if q.Get("handle") != "shape" {
					t.Error("missing continuation handle")
				}
				switch mode {
				case "duplicate":
					fmt.Fprint(w, `[{"key":"a","headers":{"operation":"insert"},"value":{}},{"headers":{"control":"up-to-date"}}]`)
				case "stalled":
					fmt.Fprint(w, `[]`)
				default:
					fmt.Fprint(w, `[{"headers":{"control":"snapshot-end"}},{"headers":{"control":"up-to-date"}}]`)
				}
			}))
			defer server.Close()
			rows, _, err := electricInitialShape(context.Background(), server.Client(), server.URL, "bench.tasks", "id > 0")
			if mode == "pages" {
				if err != nil || string(rows["a"]["id"]) != "9007199254740993" {
					t.Fatalf("rows=%v err=%v", rows, err)
				}
			} else if err == nil {
				t.Fatal("accepted malformed snapshot")
			}
		})
	}
}
