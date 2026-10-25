package bench

// This is intentionally a benchmark client, not a general Zero SDK. The wire
// contract is pinned to rocicorp/mono zero/v1.11.3 (e91e55f05b7a2c631f77238e1dd6fadf1029a538),
// packages/zero-protocol/src and zero-cache/src/types/poke-chunk.ts.
import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

const zeroProtocol = 53
const zeroMaxPokeBytes = 64 << 20
const zeroQueryName = "benchmark.projectTasks"

// Zero uses this opaque ID as a cross-client transform-cache key. It must
// identify the name AND arguments, not merely the connection-local query slot.
func zeroQueryHash(project int64) string { return fmt.Sprintf("%s:%d", zeroQueryName, project) }

type Zero struct {
	URL      string
	received atomic.Uint64
}

func (z *Zero) Bytes() uint64 { return z.received.Load() }

func (z *Zero) Run(parent context.Context, project int64, observer *Observer) error {
	endpoint, err := url.Parse(z.URL)
	if err != nil {
		return err
	}
	if endpoint.Scheme != "ws" && endpoint.Scheme != "wss" {
		return fmt.Errorf("Zero endpoint must use ws or wss")
	}
	endpoint.Path = fmt.Sprintf("%s/sync/v%d/connect", endpoint.Path, zeroProtocol)
	id := fmt.Sprintf("%x", randomZeroID())
	q := endpoint.Query()
	for key, value := range map[string]string{"clientID": id, "clientGroupID": id, "profileID": id, "baseCookie": "", "ts": "0", "lmid": "0", "wsid": id} {
		q.Set(key, value)
	}
	endpoint.RawQuery = q.Encode()
	dialer := websocket.Dialer{HandshakeTimeout: 10 * time.Second, Subprotocols: []string{url.QueryEscape(base64.StdEncoding.EncodeToString([]byte(`{}`)))}}
	conn, response, err := dialer.DialContext(parent, endpoint.String(), nil)
	if response != nil && response.Body != nil {
		defer response.Body.Close()
	}
	if err != nil {
		return err
	}
	defer conn.Close()
	conn.SetReadLimit(zeroMaxPokeBytes)
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	// Cancellation must interrupt a blocked socket read. There is no reconnect:
	// a failed/expired feed invalidates this measurement rather than hiding loss.
	go func() { <-ctx.Done(); conn.Close() }()
	var writeMu sync.Mutex
	write := func(kind string, body any) error {
		writeMu.Lock()
		defer writeMu.Unlock()
		if err := conn.SetWriteDeadline(time.Now().Add(5 * time.Second)); err != nil {
			return err
		}
		return conn.WriteJSON([]any{kind, body})
	}
	state := zeroState{rows: make(map[int64]Task), queryHash: zeroQueryHash(project)}
	connected := false
	for {
		typeID, data, err := conn.ReadMessage()
		if err != nil {
			return err
		}
		z.received.Add(uint64(len(data)))
		if typeID == websocket.BinaryMessage {
			if err := state.chunk(data); err != nil {
				return err
			}
			continue
		}
		if typeID != websocket.TextMessage {
			return fmt.Errorf("unexpected Zero frame type %d", typeID)
		}
		var message []json.RawMessage
		if err := json.Unmarshal(data, &message); err != nil {
			return err
		}
		if len(message) != 2 {
			return fmt.Errorf("invalid Zero message envelope")
		}
		var kind string
		if err := json.Unmarshal(message[0], &kind); err != nil {
			return err
		}
		if !connected && kind != "connected" && kind != "error" {
			return fmt.Errorf("Zero message %q before connected", kind)
		}
		switch kind {
		case "connected":
			if connected {
				return fmt.Errorf("duplicate Zero connection initialization")
			}
			connected = true
			init := map[string]any{
				"desiredQueriesPatch": []any{map[string]any{"op": "put", "hash": zeroQueryHash(project), "name": zeroQueryName, "args": []any{project}}},
				"clientSchema": map[string]any{"tables": map[string]any{"bench.tasks": map[string]any{
					"columns": map[string]any{"id": map[string]string{"type": "number"}, "project_id": map[string]string{"type": "number"}, "revision": map[string]string{"type": "number"}, "payload": map[string]string{"type": "string"}}, "primaryKey": []string{"id"},
				}}}, "activeClients": []string{id},
			}
			if err := write("initConnection", init); err != nil {
				return err
			}
			go func() {
				ticker := time.NewTicker(5 * time.Second)
				defer ticker.Stop()
				for {
					select {
					case <-ctx.Done():
						return
					case <-ticker.C:
						if write("ping", struct{}{}) != nil {
							conn.Close()
							return
						}
					}
				}
			}()
		case "pokeStart":
			if err := state.start(message[1]); err != nil {
				return err
			}
		case "pokeEnd":
			if err := state.end(message[1], observer); err != nil {
				return err
			}
		case "pong":
		case "error", "transformError":
			return fmt.Errorf("Zero %s: %s", kind, message[1])
		default:
			return fmt.Errorf("unsupported Zero message %q", kind)
		}
	}
}

func randomZeroID() [16]byte {
	var id [16]byte
	// crypto/rand.Read either fills the buffer or terminates the process on
	// entropy failure in Go 1.24. No timestamps/counters shared across runs.
	rand.Read(id[:])
	return id
}

type zeroQueryPatch struct {
	Op   string `json:"op"`
	Hash string `json:"hash"`
}
type zeroRowPatch struct {
	Op        string                     `json:"op"`
	Table     string                     `json:"tableName"`
	Value     map[string]json.RawMessage `json:"value"`
	ID        map[string]json.RawMessage `json:"id"`
	Merge     map[string]json.RawMessage `json:"merge"`
	Constrain []string                   `json:"constrain"`
}
type zeroPokePart struct {
	ID        string            `json:"pokeID"`
	Queries   []zeroQueryPatch  `json:"gotQueriesPatch"`
	Rows      []zeroRowPatch    `json:"rowsPatch"`
	Mutations []json.RawMessage `json:"mutationsPatch"`
}
type zeroState struct {
	queryHash               string
	cookie                  *string
	pokeID                  string
	chunks                  bytes.Buffer
	rows                    map[int64]Task
	initialized, queryReady bool
}

func (s *zeroState) start(data []byte) error {
	var start struct {
		ID   string  `json:"pokeID"`
		Base *string `json:"baseCookie"`
	}
	if err := json.Unmarshal(data, &start); err != nil {
		return err
	}
	if s.pokeID != "" || start.ID == "" {
		return fmt.Errorf("overlapping/invalid Zero poke")
	}
	if (start.Base == nil) != (s.cookie == nil) || (start.Base != nil && *start.Base != *s.cookie) {
		return fmt.Errorf("Zero base cookie does not match applied state")
	}
	s.pokeID = start.ID
	s.chunks.Reset()
	return nil
}
func (s *zeroState) chunk(data []byte) error {
	if s.pokeID == "" || len(data) < 2 || data[0] != 0 {
		return fmt.Errorf("unexpected Zero binary poke chunk")
	}
	if s.chunks.Len()+len(data)-1 > zeroMaxPokeBytes {
		return fmt.Errorf("Zero poke exceeds benchmark buffer limit")
	}
	s.chunks.Write(data[1:])
	return nil
}
func (s *zeroState) end(data []byte, observer *Observer) error {
	var end struct {
		ID     string `json:"pokeID"`
		Cookie string `json:"cookie"`
		Cancel bool   `json:"cancel"`
	}
	if err := json.Unmarshal(data, &end); err != nil {
		return err
	}
	if s.pokeID == "" || end.ID != s.pokeID {
		return fmt.Errorf("unmatched Zero poke end")
	}
	defer func() { s.pokeID = ""; s.chunks.Reset() }()
	if end.Cancel {
		return nil
	}
	if end.Cookie == "" {
		return fmt.Errorf("Zero poke omitted cookie")
	}
	var parts []zeroPokePart
	if err := json.Unmarshal(s.chunks.Bytes(), &parts); err != nil {
		return err
	}
	var changes []Change
	for _, part := range parts {
		if part.ID != s.pokeID {
			return fmt.Errorf("mismatched Zero poke part")
		}
		if len(part.Mutations) != 0 {
			return fmt.Errorf("unexpected mutation result in read-only Zero benchmark")
		}
		for _, query := range part.Queries {
			if query.Op != "put" || query.Hash != s.queryHash {
				return fmt.Errorf("unexpected Zero query invalidation: %+v", query)
			}
			s.queryReady = true
		}
		for _, patch := range part.Rows {
			if patch.Op == "clear" {
				if s.initialized {
					return fmt.Errorf("unexpected Zero resnapshot")
				}
				clear(s.rows)
				continue
			}
			if patch.Table != "bench.tasks" {
				return fmt.Errorf("unexpected Zero table %q", patch.Table)
			}
			var row Task
			switch patch.Op {
			case "del":
				id, err := zeroInteger(patch.ID["id"])
				if err != nil {
					return err
				}
				delete(s.rows, id)
				changes = append(changes, Change{Deleted: true, Task: Task{ID: id}})
				continue
			case "put":
				var err error
				row, err = zeroTask(patch.Value)
				if err != nil {
					return err
				}
			case "update":
				id, err := zeroInteger(patch.ID["id"])
				if err != nil {
					return err
				}
				old, ok := s.rows[id]
				if !ok {
					return fmt.Errorf("Zero patch for unknown row %d", id)
				}
				if patch.Constrain != nil {
					keep := make(map[string]bool, len(patch.Constrain))
					for _, field := range patch.Constrain {
						keep[field] = true
					}
					for _, field := range []string{"id", "project_id", "revision", "payload"} {
						if !keep[field] && patch.Merge[field] == nil {
							return fmt.Errorf("Zero patch removed required field %s", field)
						}
					}
				}
				row, err = decodeZeroTask(old, patch.Merge, false)
				if err != nil {
					return err
				}
				if row.ID != id {
					return fmt.Errorf("Zero update changed primary key")
				}
			default:
				return fmt.Errorf("unsupported Zero row operation %q", patch.Op)
			}
			if old, ok := s.rows[row.ID]; ok && old == row {
				continue
			}
			s.rows[row.ID] = row
			changes = append(changes, Change{Task: row})
		}
	}
	s.cookie = &end.Cookie
	if !s.initialized {
		if !s.queryReady {
			return nil
		}
		rows := make([]Task, 0, len(s.rows))
		for _, row := range s.rows {
			rows = append(rows, row)
		}
		if err := observer.Ready(rows); err != nil {
			return err
		}
		s.initialized = true
		return nil
	}
	return observer.Apply(changes)
}

func zeroInteger(raw json.RawMessage) (int64, error) {
	n, err := strconv.ParseInt(string(raw), 10, 64)
	if err != nil {
		return 0, err
	}
	if n < -(1<<53-1) || n > 1<<53-1 {
		return 0, fmt.Errorf("Zero benchmark integer exceeds JS safe range")
	}
	return n, nil
}

func zeroTask(value map[string]json.RawMessage) (Task, error) {
	return decodeZeroTask(Task{}, value, true)
}

func decodeZeroTask(row Task, value map[string]json.RawMessage, complete bool) (Task, error) {
	for field, ptr := range map[string]*int64{"id": &row.ID, "project_id": &row.ProjectID, "revision": &row.Revision} {
		if !complete && value[field] == nil {
			continue
		}
		n, err := zeroInteger(value[field])
		if err != nil {
			return row, fmt.Errorf("Zero %s: %w", field, err)
		}
		*ptr = n
	}
	if raw := value["payload"]; complete || raw != nil {
		if bytes.Equal(raw, []byte("null")) {
			return row, fmt.Errorf("Zero payload is null")
		}
		if err := json.Unmarshal(raw, &row.Payload); err != nil {
			return row, err
		}
	}
	return row, nil
}

// Zero's normal custom-query API, implemented in Go too. It only transforms
// this benchmark's one named query; there is no query evaluation in this service.
func ZeroQueryHandler(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/ready" {
		w.WriteHeader(http.StatusOK)
		return
	}
	if r.URL.Path != "/query" || r.Method != http.MethodPost {
		http.NotFound(w, r)
		return
	}
	var envelope []json.RawMessage
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&envelope); err != nil || len(envelope) != 2 || string(envelope[0]) != `"transform"` {
		http.Error(w, "invalid transform request", 400)
		return
	}
	var queries []struct {
		ID   string  `json:"id"`
		Name string  `json:"name"`
		Args []int64 `json:"args"`
	}
	if err := json.Unmarshal(envelope[1], &queries); err != nil {
		http.Error(w, "invalid query arguments", 400)
		return
	}
	results := make([]any, 0, len(queries))
	for _, query := range queries {
		if query.Name != zeroQueryName || len(query.Args) != 1 || query.Args[0] < 1 || query.Args[0] > 100000 {
			http.Error(w, "unsupported benchmark query", 400)
			return
		}
		ast := map[string]any{"table": "bench.tasks", "orderBy": [][]string{{"id", "asc"}}, "where": map[string]any{"type": "simple", "op": "=", "left": map[string]string{"type": "column", "name": "project_id"}, "right": map[string]any{"type": "literal", "value": query.Args[0]}}}
		results = append(results, map[string]any{"id": query.ID, "name": query.Name, "ast": ast})
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"kind": "QueryResponse", "queries": results})
}
