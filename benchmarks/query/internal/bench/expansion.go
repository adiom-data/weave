package bench

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"runtime/trace"
	"sort"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/adiom-data/weave/pkg/querysubscription"
	querygrpc "github.com/adiom-data/weave/pkg/querysubscription/grpc"
	"github.com/adiom-data/weave/pkg/storage"
)

// ExpansionReport deliberately does not reuse the write-load report: this
// workload measures only complete initial reads, not CDC throughput or latency.
type ExpansionReport struct {
	Status     string            `json:"status"`
	Error      string            `json:"error,omitempty"`
	Config     Config            `json:"config"`
	Cold       Distribution      `json:"cold_initial_read"`
	Warm       Distribution      `json:"warm_initial_read"`
	ColdParent Distribution      `json:"cold_parent_read"`
	WarmParent Distribution      `json:"warm_parent_read"`
	Samples    []ExpansionSample `json:"samples"`
}

type ExpansionSample struct {
	Project int64   `json:"project"`
	ColdMS  float64 `json:"cold_ms"`
	WarmMS  float64 `json:"warm_ms"`
	// Weave-only timing: parent response received and validated, before any
	// child subscription request. Full task validation still runs afterward.
	ColdParentMS  float64          `json:"cold_parent_ms,omitempty"`
	WarmParentMS  float64          `json:"warm_parent_ms,omitempty"`
	ColdBytes     uint64           `json:"cold_bytes"`
	WarmBytes     uint64           `json:"warm_bytes"`
	VerifiedTasks int              `json:"verified_tasks"`
	ColdTiming    *ExpansionTiming `json:"cold_timing,omitempty"`
	WarmTiming    *ExpansionTiming `json:"warm_timing,omitempty"`
}

// Diagnostic timings are collected only for instrumented runs. Absolute wall
// times correlate client requests with server/runtime/cgroup samples; traces
// also contain explicit wall-clock anchors. JSON encoding is outside timing.
type ExpansionTiming struct {
	StartedUnixNS      int64         `json:"started_unix_ns"`
	FeedDoneUnixNS     int64         `json:"feed_done_unix_ns"`
	ParentDoneUnixNS   int64         `json:"parent_done_unix_ns"`
	ChildrenDoneUnixNS int64         `json:"children_done_unix_ns"`
	Children           []ChildTiming `json:"children"`
}

type ChildTiming struct {
	ID            int64 `json:"id"`
	StartedUnixNS int64 `json:"started_unix_ns"`
	ElapsedNS     int64 `json:"elapsed_ns"`
}

func RunExpansion(parent context.Context, c Config, address, electricURL string) (report ExpansionReport, runErr error) {
	report = ExpansionReport{Status: "failed", Config: c}
	defer func() {
		if runErr != nil {
			report.Error = runErr.Error()
		}
	}()
	if err := c.Validate(); err != nil {
		return report, err
	}
	if c.Workload != "expansion" {
		return report, fmt.Errorf("expansion workload required")
	}
	ctx, cancel := context.WithTimeout(parent, c.InitTimeout)
	defer cancel()
	var cold, warm, coldParent, warmParent Histogram
	for n := 0; n < c.Clients; n++ {
		project := int64(n + 1) // Each cold read has distinct selectors and rows.
		sample := ExpansionSample{Project: project}
		var closes []func()
		err := func() error {
			defer func() {
				for i := len(closes) - 1; i >= 0; i-- {
					closes[i]()
				}
			}()
			for _, warmed := range []bool{false, true} {
				start := time.Now()
				var tasks []Task
				var bytes uint64
				var parentElapsed time.Duration
				var err error
				if c.Backend == "weave" {
					var closeRead func()
					var timing *ExpansionTiming
					readCtx := ctx
					var task *trace.Task
					if os.Getenv("BENCH_DIAGNOSTICS_DIR") != "" {
						timing = &ExpansionTiming{}
						readCtx, task = trace.NewTask(ctx, "Expansion")
						trace.Log(readCtx, "project", strconv.FormatInt(project, 10))
						trace.Log(readCtx, "warm", strconv.FormatBool(warmed))
						if warmed {
							sample.WarmTiming = timing
						} else {
							sample.ColdTiming = timing
						}
					}
					closeRead, tasks, bytes, err = readWeaveExpansion(readCtx, address, c, project, func() { parentElapsed = time.Since(start) }, timing)
					if task != nil {
						task.End()
					}
					if closeRead != nil {
						closes = append(closes, closeRead)
					}
				} else {
					tasks, bytes, err = readElectricExpansion(ctx, electricURL, c, project)
				}
				if err != nil {
					return err
				}
				if err := verifyExpansionTasks(c, project, tasks); err != nil {
					return err
				}
				elapsed := time.Since(start)
				if warmed {
					warm.Add(elapsed)
					if parentElapsed > 0 {
						warmParent.Add(parentElapsed)
					}
					sample.WarmMS = float64(elapsed) / 1e6
					sample.WarmParentMS = float64(parentElapsed) / 1e6
					sample.WarmBytes = bytes
				} else {
					cold.Add(elapsed)
					if parentElapsed > 0 {
						coldParent.Add(parentElapsed)
					}
					sample.ColdMS = float64(elapsed) / 1e6
					sample.ColdParentMS = float64(parentElapsed) / 1e6
					sample.ColdBytes = bytes
				}
			}
			return nil
		}()
		if err != nil {
			return report, fmt.Errorf("project %d: %w", project, err)
		}
		sample.VerifiedTasks = c.Rows
		report.Samples = append(report.Samples, sample)
	}
	report.Cold, report.Warm = cold.Summary(), warm.Summary()
	report.ColdParent, report.WarmParent = coldParent.Summary(), warmParent.Summary()
	report.Status = "ok"
	return report, nil
}

func integerEqual(field string, value int64) *storage.Predicate {
	return &storage.Predicate{Comparison: &storage.Comparison{Field: storage.MustFieldPath(field), Operator: storage.ComparisonEqual, Value: storage.Literal{Type: storage.TypeInteger, Data: []byte(strconv.FormatInt(value, 10))}}}
}

type expansionLink struct {
	key               storage.EntityKey
	id, project, task int64
}

func validateExpansionLinks(c Config, project int64, links []expansionLink) error {
	if len(links) != c.Rows {
		return fmt.Errorf("got %d links, expected %d", len(links), c.Rows)
	}
	seen := map[int64]bool{}
	for _, link := range links {
		if link.project != project || link.id != link.task || link.id < (project-1)*int64(c.Rows)+1 || link.id > project*int64(c.Rows) || seen[link.id] {
			return fmt.Errorf("invalid/duplicate link %+v", link)
		}
		seen[link.id] = true
	}
	return nil
}

func verifyExpansionTasks(c Config, project int64, tasks []Task) error {
	if len(tasks) != c.Rows {
		return fmt.Errorf("got %d tasks, expected %d", len(tasks), c.Rows)
	}
	seen := map[int64]bool{}
	payload := strings.Repeat("x", c.PayloadBytes)
	for _, task := range tasks {
		if task.ProjectID != project || task.ID < (project-1)*int64(c.Rows)+1 || task.ID > project*int64(c.Rows) || task.Revision != 0 || task.Payload != payload || seen[task.ID] {
			return fmt.Errorf("invalid/duplicate task %d", task.ID)
		}
		seen[task.ID] = true
	}
	return nil
}

func readWeaveExpansion(ctx context.Context, address string, c Config, project int64, parentReady func(), timing *ExpansionTiming) (func(), []Task, uint64, error) {
	if timing != nil {
		timing.StartedUnixNS = time.Now().UnixNano()
		timing.Children = make([]ChildTiming, 0, c.Rows)
	}
	meter := &Weave{}
	connection, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithStatsHandler(meter))
	if err != nil {
		return nil, nil, 0, err
	}
	feed, err := querygrpc.NewClient(connection).OpenFeed(ctx, querysubscription.FeedOptions{BufferLimit: 1024})
	if err != nil {
		connection.Close()
		return nil, nil, 0, err
	}
	if timing != nil {
		timing.FeedDoneUnixNS = time.Now().UnixNano()
	}
	closeRead := func() {
		cleanup, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = feed.Close(cleanup)
		_ = connection.Close()
	}
	request := querysubscription.QuerySubscribeRequest{ID: "links", Query: storage.QuerySpec{Relation: storage.RelationName{Namespace: "public", Name: "links"}, Filter: integerEqual("project_id", project)}, Window: querysubscription.InitialWindow{Count: c.Rows + 1}}
	if c.ReferenceFetch {
		request.Reference = &storage.ReferenceFetch{Query: storage.QuerySpec{Relation: storage.RelationName{Namespace: "public", Name: "tasks"}}, Bindings: []storage.ReferenceBinding{{Source: storage.MustFieldPath("task_id"), Target: storage.MustFieldPath("id")}}}
	}
	var links []expansionLink
	err = feed.SubscribeQuery(ctx, request, func(_ querysubscription.QuerySubscription, snapshot querysubscription.QuerySnapshot) error {
		if snapshot.Bounds.Left.Kind != querysubscription.BoundaryStart || snapshot.Bounds.Right.Kind != querysubscription.BoundaryEnd {
			return fmt.Errorf("incomplete link window")
		}
		for _, row := range snapshot.Rows {
			link := expansionLink{key: row.Key}
			for name, target := range map[string]*int64{"id": &link.id, "project_id": &link.project, "task_id": &link.task} {
				value, ok := row.Record[name]
				if !ok || value.Type != storage.TypeInteger || value.State != storage.ValuePresent {
					return fmt.Errorf("invalid link field %s", name)
				}
				var err error
				*target, err = strconv.ParseInt(string(value.Data), 10, 64)
				if err != nil {
					return err
				}
			}
			links = append(links, link)
		}
		return validateExpansionLinks(c, project, links)
	})
	if err != nil {
		return closeRead, nil, meter.Bytes(), err
	}
	if parentReady != nil {
		parentReady()
	}
	if timing != nil {
		timing.ParentDoneUnixNS = time.Now().UnixNano()
	}
	var tasks []Task
	for _, link := range links {
		child := querysubscription.EntitySubscribeRequest{ID: fmt.Sprintf("task-%d", link.id), Query: storage.QuerySpec{Relation: storage.RelationName{Namespace: "public", Name: "tasks"}, Filter: integerEqual("id", link.task)}}
		if c.ReferenceFetch {
			child.FetchFrom = &querysubscription.ReferenceSource{SubscriptionID: "links", ParentKey: link.key}
		}
		var childStarted time.Time
		if timing != nil {
			childStarted = time.Now()
		}
		err = feed.SubscribeEntity(ctx, child, func(_ querysubscription.EntitySubscription, snapshot querysubscription.EntitySnapshot) error {
			if snapshot.Row == nil {
				return fmt.Errorf("missing task %d", link.task)
			}
			task, err := canonicalTask(snapshot.Row.Record)
			if err != nil {
				return err
			}
			tasks = append(tasks, task)
			return nil
		})
		if timing != nil {
			timing.Children = append(timing.Children, ChildTiming{ID: link.task, StartedUnixNS: childStarted.UnixNano(), ElapsedNS: time.Since(childStarted).Nanoseconds()})
		}
		if err != nil {
			return closeRead, nil, meter.Bytes(), err
		}
	}
	if timing != nil {
		timing.ChildrenDoneUnixNS = time.Now().UnixNano()
	}
	return closeRead, tasks, meter.Bytes(), nil
}

func readElectricExpansion(ctx context.Context, endpoint string, c Config, project int64) ([]Task, uint64, error) {
	transport := &http.Transport{DisableCompression: true, MaxIdleConnsPerHost: 1, MaxConnsPerHost: 1, ResponseHeaderTimeout: 70 * time.Second}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport}
	linkRows, bytes, err := electricInitialShape(ctx, client, endpoint, "public.links", fmt.Sprintf("project_id = %d", project))
	if err != nil {
		return nil, bytes, err
	}
	var links []expansionLink
	for _, row := range linkRows {
		var link expansionLink
		for name, target := range map[string]*int64{"id": &link.id, "project_id": &link.project, "task_id": &link.task} {
			raw, ok := row[name]
			if !ok {
				return nil, bytes, fmt.Errorf("missing link field %s", name)
			}
			text := string(raw)
			if len(raw) > 0 && raw[0] == '"' {
				if err := json.Unmarshal(raw, &text); err != nil {
					return nil, bytes, err
				}
			}
			*target, err = strconv.ParseInt(text, 10, 64)
			if err != nil {
				return nil, bytes, err
			}
		}
		links = append(links, link)
	}
	if err := validateExpansionLinks(c, project, links); err != nil {
		return nil, bytes, err
	}
	sort.Slice(links, func(i, j int) bool { return links[i].task < links[j].task })
	ids := make([]string, len(links))
	for i, link := range links {
		ids[i] = strconv.FormatInt(link.task, 10)
	}
	// A single typed array keeps the filter AST compact for large expansions,
	// rather than constructing hundreds of scalar IN operands. IDs were parsed
	// as int64 above; no application-provided SQL is interpolated here.
	rows, taskBytes, err := electricInitialShape(ctx, client, endpoint, "public.tasks", "id = ANY('{"+strings.Join(ids, ",")+"}'::bigint[])")
	bytes += taskBytes
	if err != nil {
		return nil, bytes, err
	}
	var tasks []Task
	for _, row := range rows {
		task, err := electricTask(Task{}, row, true)
		if err != nil {
			return nil, bytes, err
		}
		tasks = append(tasks, task)
	}
	return tasks, bytes, nil
}

// Initial-only shape reader. Handles chunked snapshots and continuation until
// up-to-date, but never silently retries a resync. The workload performs no
// writes; changes and unexpected control messages are rejected.
func electricInitialShape(ctx context.Context, client *http.Client, endpoint, table, where string) (map[string]map[string]json.RawMessage, uint64, error) {
	u, err := url.Parse(endpoint)
	if err != nil {
		return nil, 0, err
	}
	query := u.Query()
	query.Set("table", table)
	query.Set("where", where)
	query.Set("offset", "-1")
	rows := map[string]map[string]json.RawMessage{}
	var bytes uint64
	for {
		u.RawQuery = query.Encode()
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
		if err != nil {
			return nil, bytes, err
		}
		response, err := client.Do(request)
		if err != nil {
			return nil, bytes, err
		}
		body, err := io.ReadAll(io.LimitReader(response.Body, (64<<20)+1))
		response.Body.Close()
		bytes += uint64(len(body))
		if err != nil {
			return nil, bytes, err
		}
		if response.StatusCode != http.StatusOK || len(body) > 64<<20 {
			return nil, bytes, fmt.Errorf("Electric initial shape HTTP %d or oversized response: %.1024s", response.StatusCode, body)
		}
		var messages []electricMessage
		if err := json.Unmarshal(body, &messages); err != nil {
			return nil, bytes, err
		}
		caughtUp := false
		for _, message := range messages {
			switch message.Headers.Control {
			case "up-to-date":
				caughtUp = true
				continue
			case "snapshot-end":
				continue
			case "":
			default:
				return nil, bytes, fmt.Errorf("unexpected Electric control %q", message.Headers.Control)
			}
			if message.Headers.Operation != "insert" || message.Key == "" || rows[message.Key] != nil {
				return nil, bytes, fmt.Errorf("unexpected/duplicate Electric initial row")
			}
			rows[message.Key] = message.Value
		}
		if caughtUp {
			return rows, bytes, nil
		}
		offset := response.Header.Get("electric-offset")
		if offset == "" || offset == query.Get("offset") {
			return nil, bytes, fmt.Errorf("Electric snapshot did not advance")
		}
		query.Set("offset", offset)
		for _, name := range []string{"handle", "cursor"} {
			if value := response.Header.Get("electric-" + name); value != "" {
				query.Set(name, value)
			}
		}
	}
}
