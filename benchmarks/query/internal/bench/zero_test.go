package bench

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func zeroJSON(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func zeroInitialPart() zeroPokePart {
	return zeroPokePart{ID: "initial", Queries: []zeroQueryPatch{{Op: "put", Hash: zeroQueryHash(1)}}, Rows: []zeroRowPatch{{Op: "put", Table: "bench.tasks", Value: map[string]json.RawMessage{"id": json.RawMessage(`1`), "project_id": json.RawMessage(`1`), "revision": json.RawMessage(`0`), "payload": json.RawMessage(`"x"`)}}}}
}

func TestZeroQueryIdentityIncludesArguments(t *testing.T) {
	if zeroQueryHash(1) == zeroQueryHash(2) {
		t.Fatal("different project queries share a transform cache key")
	}
	if zeroQueryHash(1) != zeroQueryHash(1) {
		t.Fatal("query cache key is not deterministic")
	}
}

func TestZeroStagesChunksUntilMatchingEndAndDiscardsCancelledPokes(t *testing.T) {
	c := testConfig()
	c.Rows = 1
	r := NewRecorder(c)
	o := NewObserver(0, r)
	s := zeroState{rows: make(map[int64]Task), queryHash: zeroQueryHash(1)}
	if err := s.start([]byte(`{"pokeID":"initial","baseCookie":null}`)); err != nil {
		t.Fatal(err)
	}
	data := zeroJSON(t, []zeroPokePart{zeroInitialPart()})
	for _, b := range data {
		if err := s.chunk([]byte{0, b}); err != nil {
			t.Fatal(err)
		}
	}
	select {
	case <-o.ready:
		t.Fatal("published incomplete poke")
	default:
	}
	if err := s.end([]byte(`{"pokeID":"initial","cookie":"01"}`), o); err != nil {
		t.Fatal(err)
	}
	if !s.initialized || *s.cookie != "01" {
		t.Fatal("initial query not installed")
	}
	if err := s.start([]byte(`{"pokeID":"cancelled","baseCookie":"01"}`)); err != nil {
		t.Fatal(err)
	}
	if err := s.chunk([]byte{0, '['}); err != nil {
		t.Fatal(err)
	}
	if err := s.end([]byte(`{"pokeID":"cancelled","cookie":"","cancel":true}`), o); err != nil {
		t.Fatal(err)
	}
	if *s.cookie != "01" || s.rows[1].Revision != 0 {
		t.Fatal("cancel advanced state")
	}
	r.Register(1, time.Now(), true)
	if err := s.start([]byte(`{"pokeID":"next","baseCookie":"01"}`)); err != nil {
		t.Fatal(err)
	}
	part := zeroPokePart{ID: "next", Rows: []zeroRowPatch{{Op: "update", Table: "bench.tasks", ID: map[string]json.RawMessage{"id": json.RawMessage(`1`)}, Merge: map[string]json.RawMessage{"revision": json.RawMessage(`1`)}, Constrain: []string{"id", "project_id", "revision", "payload"}}}}
	if err := s.chunk(append([]byte{0}, zeroJSON(t, []zeroPokePart{part})...)); err != nil {
		t.Fatal(err)
	}
	if err := s.end([]byte(`{"pokeID":"next","cookie":"02"}`), o); err != nil {
		t.Fatal(err)
	}
	if r.received != 1 || s.rows[1].Payload != "x" {
		t.Fatal("partial update lost fields or receipt")
	}
	if err := s.start([]byte(`{"pokeID":"old","baseCookie":"01"}`)); err == nil {
		t.Fatal("accepted stale cookie")
	}
}

func TestZeroRejectsMalformedOrUnsupportedState(t *testing.T) {
	for _, tc := range []struct {
		name  string
		patch zeroRowPatch
	}{
		{"unknown row", zeroRowPatch{Op: "update", Table: "bench.tasks", ID: map[string]json.RawMessage{"id": json.RawMessage(`2`)}}},
		{"removed column", zeroRowPatch{Op: "update", Table: "bench.tasks", ID: map[string]json.RawMessage{"id": json.RawMessage(`1`)}, Constrain: []string{"id"}}},
		{"wrong table", zeroRowPatch{Op: "put", Table: "other"}},
		{"unknown operation", zeroRowPatch{Op: "surprise", Table: "bench.tasks"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := zeroState{rows: map[int64]Task{1: {ID: 1, ProjectID: 1, Payload: "x"}}, initialized: true}
			if err := s.start([]byte(`{"pokeID":"bad","baseCookie":null}`)); err != nil {
				t.Fatal(err)
			}
			if err := s.chunk(append([]byte{0}, zeroJSON(t, []zeroPokePart{{ID: "bad", Rows: []zeroRowPatch{tc.patch}}})...)); err != nil {
				t.Fatal(err)
			}
			if err := s.end([]byte(`{"pokeID":"bad","cookie":"01"}`), nil); err == nil {
				t.Fatal("accepted invalid patch")
			}
		})
	}
	var s zeroState
	if err := s.chunk([]byte{0, '['}); err == nil {
		t.Fatal("accepted chunk without start")
	}
	if err := s.start([]byte(`{"pokeID":"a","baseCookie":null}`)); err != nil {
		t.Fatal(err)
	}
	if err := s.start([]byte(`{"pokeID":"b","baseCookie":null}`)); err == nil {
		t.Fatal("accepted overlapping poke")
	}
	if err := s.end([]byte(`{"pokeID":"b","cookie":"01"}`), nil); err == nil {
		t.Fatal("accepted mismatched end")
	}
	for _, raw := range []string{`9007199254740992`, `null`, `1.5`, `"1"`} {
		if _, err := zeroInteger(json.RawMessage(raw)); err == nil {
			t.Fatalf("accepted %s", raw)
		}
	}
}

func TestZeroQueryEndpointUsesQualifiedTableAndBoundProject(t *testing.T) {
	for _, body := range []string{`["transform",[{"id":"project","name":"benchmark.projectTasks","args":[3]}]]`, `["transform",[]]`} {
		w := httptest.NewRecorder()
		ZeroQueryHandler(w, httptest.NewRequest(http.MethodPost, "/query", strings.NewReader(body)))
		if w.Code != 200 {
			t.Fatalf("response %d: %s", w.Code, w.Body.String())
		}
		var result struct {
			Kind    string
			Queries []struct {
				AST struct {
					Table string
					Where struct{ Right struct{ Value int64 } }
				}
			}
		}
		if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
			t.Fatal(err)
		}
		if result.Kind != "QueryResponse" {
			t.Fatal(result.Kind)
		}
		if len(result.Queries) > 0 && (result.Queries[0].AST.Table != "bench.tasks" || result.Queries[0].AST.Where.Right.Value != 3) {
			t.Fatal(w.Body.String())
		}
	}
	for _, body := range []string{`["transform",[{"id":"p","name":"other","args":[1]}]]`, `["transform",[{"id":"p","name":"benchmark.projectTasks","args":[-1]}]]`, `{}`} {
		w := httptest.NewRecorder()
		ZeroQueryHandler(w, httptest.NewRequest(http.MethodPost, "/query", strings.NewReader(body)))
		if w.Code != 400 {
			t.Fatal("accepted invalid transform")
		}
	}
}

func TestZeroWebSocketHandshakeBinaryUpdatesAndCancellation(t *testing.T) {
	c := testConfig()
	c.Rows = 1
	c.Clients = 1
	r := NewRecorder(c)
	o := NewObserver(0, r)
	allowUpdate := make(chan struct{})
	serverErrors := make(chan error, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		serve := func() error {
			if req.URL.Path != "/sync/v53/connect" || req.URL.Query().Get("clientID") == "" || req.URL.Query().Get("baseCookie") != "" {
				return fmt.Errorf("invalid connect URL %s", req.URL)
			}
			upgrader := websocket.Upgrader{Subprotocols: websocket.Subprotocols(req)}
			ws, err := upgrader.Upgrade(w, req, nil)
			if err != nil {
				return err
			}
			defer ws.Close()
			if err := ws.WriteJSON([]any{"connected", map[string]any{"wsid": "server"}}); err != nil {
				return err
			}
			var init []json.RawMessage
			if err := ws.ReadJSON(&init); err != nil {
				return err
			}
			if len(init) != 2 || string(init[0]) != `"initConnection"` || !strings.Contains(string(init[1]), zeroQueryName) {
				return fmt.Errorf("invalid initialization: %s", init)
			}
			send := func(id string, base any, cookie string, part zeroPokePart) error {
				if err := ws.WriteJSON([]any{"pokeStart", map[string]any{"pokeID": id, "baseCookie": base}}); err != nil {
					return err
				}
				data, err := json.Marshal([]zeroPokePart{part})
				if err != nil {
					return err
				}
				mid := len(data) / 2
				for _, chunk := range [][]byte{data[:mid], data[mid:]} {
					if err := ws.WriteMessage(websocket.BinaryMessage, append([]byte{0}, chunk...)); err != nil {
						return err
					}
				}
				return ws.WriteJSON([]any{"pokeEnd", map[string]any{"pokeID": id, "cookie": cookie}})
			}
			if err := send("initial", nil, "01", zeroInitialPart()); err != nil {
				return err
			}
			select {
			case <-allowUpdate:
			case <-time.After(5 * time.Second):
				return fmt.Errorf("test timed out waiting to send update")
			}
			part := zeroPokePart{ID: "next", Rows: []zeroRowPatch{{Op: "update", Table: "bench.tasks", ID: map[string]json.RawMessage{"id": json.RawMessage(`1`)}, Merge: map[string]json.RawMessage{"revision": json.RawMessage(`1`)}}}}
			if err := send("next", "01", "02", part); err != nil {
				return err
			}
			_, _, _ = ws.ReadMessage() // closes when the client's context is cancelled
			return nil
		}
		serverErrors <- serve()
	}))
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	z := &Zero{URL: "ws" + strings.TrimPrefix(server.URL, "http")}
	done := make(chan error, 1)
	go func() { done <- z.Run(ctx, 1, o) }()
	select {
	case <-o.ready:
	case err := <-done:
		t.Fatal(err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	r.Register(1, time.Now(), true)
	close(allowUpdate)
	if err := r.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	if err := o.Verify(map[int64]Task{1: {ID: 1, ProjectID: 1, Revision: 1, Payload: "x"}}); err != nil {
		t.Fatal(err)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("cancellation did not interrupt socket read")
	}
	if err := <-serverErrors; err != nil {
		t.Fatal(err)
	}
	if z.Bytes() == 0 {
		t.Fatal("no measured protocol bytes")
	}
}
