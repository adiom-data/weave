package bench

// The standalone Drasi adapter is pinned to server 0.2.4 and source plugins
// recorded in drasi/plugins-*.lock. It uses only public REST and SSE interfaces.
// Initialization is intentionally limited to this harness's quiescent seed phase:
// these endpoints do not provide a general atomic snapshot/stream handoff.
import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const maxDrasiPayload = 128 << 20

type Drasi struct {
	URL       string
	EventsURL string
	Rows      int
	// Drasi reports Running before its replication slot exists. The harness
	// checks durable capture after subscribing queries but before allowing writes.
	CaptureReady func(context.Context) error
	received     atomic.Uint64
}

func (d *Drasi) Bytes() uint64 { return d.received.Load() }

func drasiQueryID(project int64) string { return fmt.Sprintf("project-%d", project) }
func drasiPath(project int64) string    { return fmt.Sprintf("/projects/%d", project) }

func (d *Drasi) api(ctx context.Context, client *http.Client, method, path string, input, output any) error {
	var body io.Reader
	if input != nil {
		raw, err := json.Marshal(input)
		if err != nil {
			return err
		}
		body = bytes.NewReader(raw)
	}
	req, err := http.NewRequestWithContext(ctx, method, strings.TrimRight(d.URL, "/")+path, body)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	response, err := client.Do(req)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(response.Body, maxDrasiPayload+1))
	d.received.Add(uint64(len(raw)))
	if err != nil {
		return err
	}
	if len(raw) > maxDrasiPayload {
		return fmt.Errorf("Drasi REST response exceeds benchmark bound")
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return fmt.Errorf("Drasi %s %s: HTTP %d: %.1000s", method, path, response.StatusCode, raw)
	}
	var status struct {
		Success bool `json:"success"`
	}
	if err := json.Unmarshal(raw, &status); err != nil {
		return err
	}
	if !status.Success {
		return fmt.Errorf("Drasi REST returned unsuccessful status: %.1000s", raw)
	}
	if output != nil {
		return json.Unmarshal(raw, output)
	}
	return nil
}

// Setup creates one persistent query per distinct project and one routed SSE
// reaction. Shared viewers do not duplicate query compute; independent viewers
// do not receive and discard other projects' traffic.
func (d *Drasi) Setup(ctx context.Context, c Config) error {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport}
	if err := d.waitRunning(ctx, client, "/api/v1/sources/db"); err != nil {
		return err
	}
	if d.CaptureReady == nil {
		return fmt.Errorf("Drasi requires a replication capture readiness check")
	}
	count := 1
	if c.Scenario == "independent" {
		count = c.Clients
	}
	queries := make([]string, 0, count)
	routes := make(map[string]any, count)
	for project := int64(1); project <= int64(count); project++ {
		id := drasiQueryID(project)
		query := map[string]any{
			"id": id, "autoStart": true, "queryLanguage": "Cypher", "enableBootstrap": true,
			"query":   fmt.Sprintf("MATCH (t:tasks) WHERE t.project_id = %d RETURN t.id AS id, t.project_id AS project_id, t.revision AS revision, t.payload AS payload", project),
			"sources": []any{map[string]any{"sourceId": "db"}},
		}
		if err := d.api(ctx, client, http.MethodPost, "/api/v1/queries", query, nil); err != nil {
			return err
		}
		if err := d.waitRunning(ctx, client, "/api/v1/queries/"+id); err != nil {
			return err
		}
		if _, err := d.snapshot(ctx, client, project, true); err != nil {
			return err
		}
		queries = append(queries, id)
		route := make(map[string]any, 3)
		for _, operation := range []struct{ name, kind, image string }{
			{"added", "ADD", "after"}, {"updated", "UPDATE", "after"}, {"deleted", "DELETE", "before"},
		} {
			route[operation.name] = map[string]any{
				"path":     drasiPath(project),
				"template": fmt.Sprintf(`{"query_id":%q,"results":[{"type":%q,"data":{{json %s}}}]}`, id, operation.kind, operation.image),
			}
		}
		routes[id] = route
	}
	// The PostgreSQL source starts replication lazily on the first query
	// subscription. Waiting before creating queries would never start capture.
	if err := d.CaptureReady(ctx); err != nil {
		return err
	}
	// Start explicitly, once. The release's native /attach endpoint combines
	// auto-start and explicit start and was observed replaying identical events.
	if err := d.api(ctx, client, http.MethodPost, "/api/v1/reactions", map[string]any{
		"kind": "sse", "id": "viewers", "autoStart": false, "queries": queries,
		"host": "0.0.0.0", "port": 8081, "routes": routes, "heartbeatIntervalMs": 1000,
	}, nil); err != nil {
		return err
	}
	if err := d.api(ctx, client, http.MethodPost, "/api/v1/reactions/viewers/start", nil, nil); err != nil {
		return err
	}
	return d.waitRunning(ctx, client, "/api/v1/reactions/viewers")
}

// WaitDrasiCapture observes source readiness rather than sleeping or issuing a
// benchmark write. The freshly seeded database has no pre-existing slot, so a
// confirmed position and active consumer prove this run established capture.
func WaitDrasiCapture(ctx context.Context, pool *pgxpool.Pool) error {
	for {
		var ready bool
		if err := pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_replication_slots WHERE slot_name='bench_drasi' AND database=current_database() AND active AND confirmed_flush_lsn IS NOT NULL)`).Scan(&ready); err != nil {
			return err
		}
		if ready {
			return nil
		}
		if err := drasiPause(ctx); err != nil {
			return err
		}
	}
}

func (d *Drasi) waitRunning(ctx context.Context, client *http.Client, path string) error {
	for {
		var state struct {
			Data struct {
				Status string `json:"status"`
				Error  string `json:"error_message"`
			} `json:"data"`
		}
		if err := d.api(ctx, client, http.MethodGet, path, nil, &state); err != nil {
			return err
		}
		switch state.Data.Status {
		case "Running":
			return nil
		case "Added", "Starting":
		default:
			return fmt.Errorf("Drasi component %s: %s: %s", path, state.Data.Status, state.Data.Error)
		}
		if err := drasiPause(ctx); err != nil {
			return err
		}
	}
}

func drasiPause(ctx context.Context) error {
	timer := time.NewTimer(10 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (d *Drasi) snapshot(ctx context.Context, client *http.Client, project int64, wait bool) ([]Task, error) {
	for {
		var response struct {
			Data []json.RawMessage `json:"data"`
		}
		if err := d.api(ctx, client, http.MethodGet, "/api/v1/queries/"+drasiQueryID(project)+"/results", nil, &response); err != nil {
			return nil, err
		}
		if len(response.Data) > d.Rows {
			return nil, fmt.Errorf("Drasi returned inconsistent initial result count")
		}
		if len(response.Data) == d.Rows {
			rows := make([]Task, 0, d.Rows)
			for _, raw := range response.Data {
				row, err := drasiTask(raw)
				if err != nil {
					return nil, err
				}
				rows = append(rows, row)
			}
			return rows, nil
		}
		if !wait {
			return nil, fmt.Errorf("Drasi query lost seeded rows")
		}
		if err := drasiPause(ctx); err != nil {
			return nil, err
		}
	}
}

func drasiTask(raw json.RawMessage) (Task, error) {
	var row struct {
		ID       json.Number `json:"id"`
		Project  json.Number `json:"project_id"`
		Revision json.Number `json:"revision"`
		Payload  *string     `json:"payload"`
	}
	if err := json.Unmarshal(raw, &row); err != nil {
		return Task{}, err
	}
	id, err := row.ID.Int64()
	if err != nil {
		return Task{}, err
	}
	project, err := row.Project.Int64()
	if err != nil {
		return Task{}, err
	}
	revision, err := row.Revision.Int64()
	if err != nil {
		return Task{}, err
	}
	if row.Payload == nil {
		return Task{}, fmt.Errorf("Drasi row has no payload")
	}
	return Task{ID: id, ProjectID: project, Revision: revision, Payload: *row.Payload}, nil
}

func (d *Drasi) Run(ctx context.Context, project int64, observer *Observer) error {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(d.EventsURL, "/")+drasiPath(project), nil)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "text/event-stream")
	response, err := client.Do(req)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK || !strings.HasPrefix(response.Header.Get("Content-Type"), "text/event-stream") {
		return fmt.Errorf("Drasi SSE: HTTP %d, content type %q", response.StatusCode, response.Header.Get("Content-Type"))
	}
	// The writer starts only after every observer is ready. This is a quiescent
	// snapshot handoff, not a claim that Drasi exposes a resumable snapshot fence.
	rows, err := d.snapshot(ctx, client, project, false)
	if err != nil {
		return err
	}
	seed := make(map[int64]Task, len(rows))
	for _, row := range rows {
		seed[row.ID] = row
	}
	if err := observer.Ready(rows); err != nil {
		return err
	}
	return readDrasiSSE(response.Body, &d.received, func(raw []byte) error {
		var event struct {
			Type    string `json:"type"`
			QueryID string `json:"query_id"`
			Results []struct {
				Type string          `json:"type"`
				Data json.RawMessage `json:"data"`
			} `json:"results"`
		}
		if err := json.Unmarshal(raw, &event); err != nil {
			return err
		}
		if event.Type == "heartbeat" && event.QueryID == "" {
			return nil
		}
		if event.QueryID != drasiQueryID(project) {
			return fmt.Errorf("unexpected Drasi event for %q", event.QueryID)
		}
		changes := make([]Change, 0, len(event.Results))
		for _, diff := range event.Results {
			if diff.Type == "noop" {
				continue
			}
			row, err := drasiTask(diff.Data)
			if err != nil {
				return err
			}
			if row.ProjectID != project {
				return fmt.Errorf("Drasi routed a foreign project")
			}
			switch diff.Type {
			case "ADD":
				// A bootstrap ADD may still be queued when the snapshot is read.
				// Deduplicate only exact revision-zero seed images. Positive-revision
				// duplicates remain errors in the unchanged benchmark oracle.
				if baseline, ok := seed[row.ID]; ok && row.Revision == 0 && row == baseline {
					continue
				}
			case "UPDATE", "DELETE":
			default:
				return fmt.Errorf("unknown Drasi diff %q", diff.Type)
			}
			changes = append(changes, Change{Deleted: diff.Type == "DELETE", Task: row})
		}
		if err := observer.Apply(changes); err != nil {
			return fmt.Errorf("Drasi event rejected: %w; event=%.2000s", err, raw)
		}
		return nil
	})
}

// No reconnects, retries, or polling for updates: stream loss fails the run.
func readDrasiSSE(reader io.Reader, received *atomic.Uint64, apply func([]byte) error) error {
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 4096), maxDrasiPayload)
	var data []byte
	for scanner.Scan() {
		line := scanner.Text()
		received.Add(uint64(len(line) + 1))
		if line == "" {
			if len(data) > 0 {
				if err := apply(data); err != nil {
					return err
				}
				data = data[:0]
			}
			continue
		}
		if strings.HasPrefix(line, "data:") {
			value := strings.TrimPrefix(strings.TrimPrefix(line, "data:"), " ")
			if len(data)+len(value)+1 > maxDrasiPayload {
				return fmt.Errorf("Drasi SSE event exceeds benchmark bound")
			}
			if len(data) > 0 {
				data = append(data, '\n')
			}
			data = append(data, value...)
		} else if !strings.HasPrefix(line, ":") {
			return fmt.Errorf("unsupported Drasi SSE field: %.100s", line)
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	return io.ErrUnexpectedEOF
}
