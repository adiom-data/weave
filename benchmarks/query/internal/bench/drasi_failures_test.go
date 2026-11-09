package bench

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestDrasiDoesNotHideInvalidationsOrDuplicateUpdates(t *testing.T) {
	for _, tc := range []struct{ name, event, want string }{
		{"foreign query", `{"query_id":"project-2","results":[]}`, "unexpected Drasi event"},
		{"foreign row", `{"query_id":"project-1","results":[{"type":"UPDATE","data":{"id":1,"project_id":2,"revision":1,"payload":"x"}}]}`, "foreign project"},
		{"unknown operation", `{"query_id":"project-1","results":[{"type":"RESET","data":{"id":1,"project_id":1,"revision":0,"payload":"x"}}]}`, "unknown Drasi diff"},
		{"duplicate update", `{"query_id":"project-1","results":[{"type":"UPDATE","data":{"id":1,"project_id":1,"revision":0,"payload":"x"}}]}`, "non-advancing row revision"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if strings.HasSuffix(r.URL.Path, "/results") {
					fmt.Fprint(w, `{"success":true,"data":[{"id":1,"project_id":1,"revision":0,"payload":"x"}]}`)
					return
				}
				w.Header().Set("Content-Type", "text/event-stream")
				fmt.Fprintf(w, "data: %s\n\n", tc.event)
			}))
			defer server.Close()
			c := testConfig()
			c.Clients, c.Rows, c.PayloadBytes = 1, 1, 1
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			err := (&Drasi{URL: server.URL, EventsURL: server.URL, Rows: 1}).Run(ctx, 1, NewObserver(0, NewRecorder(c)))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("want %q, got %v", tc.want, err)
			}
		})
	}
}
