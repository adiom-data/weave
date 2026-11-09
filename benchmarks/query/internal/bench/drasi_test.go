package bench

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestDrasiSetupSharesQueriesAndUsesProjectFilters(t *testing.T) {
	for _, scenario := range []string{"shared", "independent"} {
		t.Run(scenario, func(t *testing.T) {
			var queries []map[string]any
			var reaction map[string]any
			var started bool
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				switch {
				case r.Method == http.MethodGet && !strings.HasSuffix(r.URL.Path, "/results"):
					fmt.Fprint(w, `{"success":true,"data":{"status":"Running"}}`)
				case r.Method == http.MethodPost && r.URL.Path == "/api/v1/queries":
					var query map[string]any
					if err := json.NewDecoder(r.Body).Decode(&query); err != nil {
						t.Error(err)
					}
					queries = append(queries, query)
					fmt.Fprint(w, `{"success":true}`)
				case r.Method == http.MethodPost && r.URL.Path == "/api/v1/reactions":
					if err := json.NewDecoder(r.Body).Decode(&reaction); err != nil {
						t.Error(err)
					}
					fmt.Fprint(w, `{"success":true}`)
				case r.Method == http.MethodPost && r.URL.Path == "/api/v1/reactions/viewers/start":
					if started {
						t.Error("reaction started more than once")
					}
					started = true
					fmt.Fprint(w, `{"success":true}`)
				case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/results"):
					project := len(queries)
					fmt.Fprintf(w, `{"success":true,"data":[{"id":%d,"project_id":%d,"revision":0,"payload":"x"}]}`, project, project)
				default:
					t.Error("unexpected management request", r.Method, r.URL)
					http.NotFound(w, r)
				}
			}))
			defer server.Close()
			c := testConfig()
			c.Backend, c.Scenario, c.Clients, c.Rows = "drasi", scenario, 3, 1
			captureReady := false
			d := &Drasi{URL: server.URL, Rows: 1, CaptureReady: func(context.Context) error {
				want := 1
				if scenario == "independent" {
					want = c.Clients
				}
				if len(queries) != want {
					return fmt.Errorf("capture was awaited before subscribing queries")
				}
				captureReady = true
				return nil
			}}
			if err := d.Setup(t.Context(), c); err != nil {
				t.Fatal(err)
			}
			count := 1
			if scenario == "independent" {
				count = 3
			}
			if len(queries) != count {
				t.Fatal("query sharing was lost")
			}
			if !captureReady {
				t.Fatal("capture readiness was skipped")
			}
			if !started || reaction["autoStart"] != false || len(reaction["queries"].([]any)) != count {
				t.Fatal("reaction startup or query selection changed")
			}
			for i, query := range queries {
				if !strings.Contains(query["query"].(string), fmt.Sprintf("WHERE t.project_id = %d ", i+1)) {
					t.Fatal("incorrect query filter")
				}
				routes := reaction["routes"].(map[string]any)[drasiQueryID(int64(i+1))].(map[string]any)
				for _, op := range []string{"added", "updated", "deleted"} {
					if routes[op].(map[string]any)["path"] != drasiPath(int64(i+1)) {
						t.Fatal("foreign project traffic would be broadcast")
					}
				}
			}
			if d.Bytes() == 0 {
				t.Fatal("management payload accounting missing")
			}
		})
	}
}

func TestDrasiStreamsEveryRevisionAndDoesNotRoundIntegers(t *testing.T) {
	c := testConfig()
	c.Backend, c.Clients, c.Rows, c.PayloadBytes = "drasi", 1, 1, 1
	recorder := NewRecorder(c)
	observer := NewObserver(0, recorder)
	const revision = int64(9007199254740993)
	emit := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v1/queries/project-1/results" {
			fmt.Fprint(w, `{"success":true,"data":[{"id":1,"project_id":1,"revision":0,"payload":"x"}]}`)
			return
		}
		if r.URL.Path != "/projects/1" {
			t.Error("unexpected stream", r.URL)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.(http.Flusher).Flush()
		select {
		case <-emit:
		case <-r.Context().Done():
			return
		}
		fmt.Fprint(w, ": keep-alive\n\n")
		fmt.Fprint(w, "data: {\"type\":\"heartbeat\"}\n\n")
		fmt.Fprint(w, `data: {"query_id":"project-1","results":[{"type":"ADD","data":{"id":1,"project_id":1,"revision":0,"payload":"x"}}]}`+"\n\n")
		for _, seq := range []int64{revision, revision + 1} {
			fmt.Fprintf(w, "data: {\"query_id\":\"project-1\",\"results\":[{\"type\":\"UPDATE\",\"data\":{\"id\":1,\"project_id\":1,\"revision\":%d,\"payload\":\"x\"}}]}\n\n", seq)
		}
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))
	defer server.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	d := &Drasi{URL: server.URL, EventsURL: server.URL, Rows: 1}
	done := make(chan error, 1)
	go func() { done <- d.Run(ctx, 1, observer) }()
	select {
	case <-observer.ready:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	recorder.Register(revision, time.Now(), true)
	recorder.Register(revision+1, time.Now(), true)
	close(emit)
	if err := recorder.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	if err := observer.Verify(map[int64]Task{1: {ID: 1, ProjectID: 1, Revision: revision + 1, Payload: "x"}}); err != nil {
		t.Fatal(err)
	}
	if d.Bytes() == 0 {
		t.Fatal("byte accounting missing")
	}
	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("stream cancellation hidden")
		}
	case <-time.After(time.Second):
		t.Fatal("adapter did not cancel")
	}
}

func TestDrasiSSEFramingAndStreamLoss(t *testing.T) {
	var count atomic.Uint64
	var received []string
	err := readDrasiSSE(strings.NewReader(": comment\r\n\r\ndata: {\r\ndata: \"a\":1}\r\n\r\n"), &count, func(b []byte) error { received = append(received, string(b)); return nil })
	if !errors.Is(err, io.ErrUnexpectedEOF) || len(received) != 1 || received[0] != "{\n\"a\":1}" || count.Load() == 0 {
		t.Fatal(received, err)
	}
	if err := readDrasiSSE(strings.NewReader("event: reset\n\n"), &count, func([]byte) error { return nil }); err == nil {
		t.Fatal("unknown SSE control hidden")
	}
	sentinel := errors.New("oracle rejected update")
	if err := readDrasiSSE(strings.NewReader("data: {}\n\n"), &count, func([]byte) error { return sentinel }); !errors.Is(err, sentinel) {
		t.Fatal(err)
	}
}

func TestDrasiRejectsMalformedRowsAndRESTFailures(t *testing.T) {
	for _, raw := range []string{`{}`, `{"id":1.5,"project_id":1,"revision":0,"payload":"x"}`, `{"id":9223372036854775808,"project_id":1,"revision":0,"payload":"x"}`, `{"id":1,"project_id":1,"revision":0,"payload":null}`} {
		if _, err := drasiTask(json.RawMessage(raw)); err == nil {
			t.Fatal("invalid row accepted", raw)
		}
	}
	for _, status := range []int{http.StatusOK, http.StatusConflict, http.StatusInternalServerError} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(status)
				fmt.Fprint(w, `{"status":"error"}`)
			}))
			defer server.Close()
			if err := (&Drasi{URL: server.URL}).api(t.Context(), server.Client(), http.MethodGet, "/test", nil, nil); err == nil {
				t.Fatal("REST failure hidden")
			}
		})
	}
}
