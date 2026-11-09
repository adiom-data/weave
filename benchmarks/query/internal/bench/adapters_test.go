package bench

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestElectricLongPollingMergesPartialUpdatesAndContinuesOffsets(t *testing.T) {
	c := testConfig()
	c.Clients = 1
	c.Rows = 1
	r := NewRecorder(c)
	o := NewObserver(0, r)
	const revision = int64(9007199254740993)
	r.Register(revision, time.Now(), true)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		q := request.URL.Query()
		if q.Get("table") != "public.tasks" || q.Get("where") != "project_id = 1" {
			t.Error("incorrect Electric shape")
		}
		w.Header().Set("electric-handle", "shape-1")
		switch q.Get("offset") {
		case "-1":
			w.Header().Set("electric-offset", "0_0")
			fmt.Fprint(w, `[{"key":"opaque","value":{"id":"1","project_id":"1","revision":"0","payload":"x"},"headers":{"operation":"insert"}},{"headers":{"control":"snapshot-end"}},{"headers":{"control":"up-to-date"}}]`)
		case "0_0":
			if q.Get("handle") != "shape-1" || q.Get("live") != "true" {
				t.Error("missing continuation parameters")
			}
			w.Header().Set("electric-offset", "1_0")
			fmt.Fprintf(w, `[{"key":"opaque","value":{"revision":"%d"},"headers":{"operation":"update"}},{"headers":{"control":"up-to-date"}}]`, revision)
		default:
			<-request.Context().Done()
		}
	}))
	defer server.Close()
	adapter := &Electric{URL: server.URL}
	done := make(chan error, 1)
	go func() { done <- adapter.Run(ctx, 1, o) }()
	if err := r.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	if err := o.Verify(map[int64]Task{1: {ID: 1, ProjectID: 1, Revision: revision, Payload: "x"}}); err != nil {
		t.Fatal(err)
	}
	if adapter.Bytes() == 0 {
		t.Fatal("payload accounting missing")
	}
	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected cancellation")
		}
	case <-time.After(time.Second):
		t.Fatal("HTTP adapter did not stop")
	}
}
func TestElectricDoesNotHideResyncOrHTTPFailures(t *testing.T) {
	for _, status := range []int{200, 409, 429, 500} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("electric-offset", "0_0")
				w.WriteHeader(status)
				fmt.Fprint(w, `[{"headers":{"control":"must-refetch"}}]`)
			}))
			defer server.Close()
			c := testConfig()
			c.Clients = 1
			c.Rows = 1
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			err := (&Electric{URL: server.URL}).Run(ctx, 1, NewObserver(0, NewRecorder(c)))
			if err == nil || errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("failure hidden: %v", err)
			}
		})
	}
}
func TestElectricNumbersAreNotRoundedThroughFloat64(t *testing.T) {
	var values map[string]json.RawMessage
	if err := json.Unmarshal([]byte(`{"id":9007199254740993,"project_id":"1","revision":"9223372036854775807","payload":"x"}`), &values); err != nil {
		t.Fatal(err)
	}
	row, err := electricTask(Task{}, values, true)
	if err != nil || row.ID != 9007199254740993 || row.Revision != 9223372036854775807 {
		t.Fatalf("row=%+v err=%v", row, err)
	}
	values["id"] = json.RawMessage(`1.5`)
	if _, err := electricTask(Task{}, values, true); err == nil {
		t.Fatal("accepted fractional primary key")
	}
	delete(values, "id")
	if _, err := electricTask(Task{}, values, true); err == nil || !strings.Contains(err.Error(), "missing") {
		t.Fatal("accepted incomplete insert")
	}
}
