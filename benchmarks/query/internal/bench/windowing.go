package bench

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	q "github.com/adiom-data/weave/pkg/querysubscription"
	querygrpc "github.com/adiom-data/weave/pkg/querysubscription/grpc"
	"github.com/adiom-data/weave/pkg/storage"
)

// WindowsReport compares native window resizing with bounded-shape replacement,
// not equivalent server operations. Neither implementation is timed under writes.
type WindowsReport struct {
	Status  string                  `json:"status"`
	Error   string                  `json:"error,omitempty"`
	Config  Config                  `json:"config"`
	Mode    string                  `json:"mode"`
	Steps   map[string]Distribution `json:"steps"`
	Samples []WindowSample          `json:"samples"`
}

type WindowSample struct {
	Project      int64   `json:"project"`
	Step         string  `json:"step"`
	Milliseconds float64 `json:"ms"`
	Rows         int     `json:"rows"`
	Bytes        uint64  `json:"bytes"`
}

type windowStep struct {
	name        string
	left, right int // Project-relative (left, right].
	changes     []q.BoundaryChange
}

func windowSteps(rows int) []windowStep {
	n := rows / 4
	change := func(side q.BoundarySide, op q.BoundaryOperation) q.BoundaryChange {
		return q.BoundaryChange{Side: side, Operation: op, Count: n}
	}
	return []windowStep{
		{name: "initial", right: n},
		{name: "extend_right_cold", right: 2 * n, changes: []q.BoundaryChange{change(q.BoundaryRight, q.BoundaryExtend)}},
		{name: "contract_right", right: n, changes: []q.BoundaryChange{change(q.BoundaryRight, q.BoundaryContract)}},
		{name: "reextend_right_warm", right: 2 * n, changes: []q.BoundaryChange{change(q.BoundaryRight, q.BoundaryExtend)}},
		{name: "contract_left", left: n, right: 2 * n, changes: []q.BoundaryChange{change(q.BoundaryLeft, q.BoundaryContract)}},
		{name: "reextend_left_warm", right: 2 * n, changes: []q.BoundaryChange{change(q.BoundaryLeft, q.BoundaryExtend)}},
		{name: "shift_both", left: n, right: 3 * n, changes: []q.BoundaryChange{change(q.BoundaryLeft, q.BoundaryContract), change(q.BoundaryRight, q.BoundaryExtend)}},
		{name: "return_both_warm", right: 2 * n, changes: []q.BoundaryChange{change(q.BoundaryLeft, q.BoundaryExtend), change(q.BoundaryRight, q.BoundaryContract)}},
	}
}

func RunWindows(parent context.Context, c Config, address, endpoint string) (report WindowsReport, runErr error) {
	report = WindowsReport{Status: "failed", Config: c, Mode: "native_window_updates", Steps: map[string]Distribution{}}
	if c.Backend == "electric" {
		report.Mode = "bounded_shape_replacements"
	}
	defer func() {
		if runErr != nil {
			report.Error = runErr.Error()
		}
	}()
	if err := c.Validate(); err != nil {
		return report, err
	}
	if c.Workload != "windows" {
		return report, fmt.Errorf("windows workload required")
	}
	ctx, cancel := context.WithTimeout(parent, c.InitTimeout)
	defer cancel()
	histograms := map[string]*Histogram{}
	for i := 0; i < c.Clients; i++ {
		project := int64(i + 1)
		var samples []WindowSample
		var err error
		if c.Backend == "weave" {
			samples, err = readWeaveWindows(ctx, address, c, project)
		} else {
			samples, err = readElectricWindows(ctx, endpoint, c, project)
		}
		if err != nil {
			return report, fmt.Errorf("project %d: %w", project, err)
		}
		for _, sample := range samples {
			if histograms[sample.Step] == nil {
				histograms[sample.Step] = &Histogram{}
			}
			histograms[sample.Step].Add(time.Duration(sample.Milliseconds * 1e6))
		}
		report.Samples = append(report.Samples, samples...)
	}
	for name, histogram := range histograms {
		report.Steps[name] = histogram.Summary()
	}
	report.Status = "ok"
	return report, nil
}

func verifyWindow(c Config, project int64, step windowStep, tasks []Task) error {
	if len(tasks) != step.right-step.left {
		return fmt.Errorf("%s: got %d rows, want %d", step.name, len(tasks), step.right-step.left)
	}
	first := (project-1)*int64(c.Rows) + int64(step.left) + 1
	payload := strings.Repeat("x", c.PayloadBytes)
	seen := map[int64]bool{}
	for _, task := range tasks {
		if task.ID < first || task.ID >= first+int64(len(tasks)) || task.ProjectID != project || task.Revision != 0 || task.Payload != payload || seen[task.ID] {
			return fmt.Errorf("%s: wrong or duplicate row %d", step.name, task.ID)
		}
		seen[task.ID] = true
	}
	return nil
}

func readWeaveWindows(ctx context.Context, address string, c Config, project int64) ([]WindowSample, error) {
	meter := &Weave{}
	connection, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithStatsHandler(meter))
	if err != nil {
		return nil, err
	}
	defer connection.Close()
	feed, err := querygrpc.NewClient(connection).OpenFeed(ctx, q.FeedOptions{BufferLimit: 1024})
	if err != nil {
		return nil, err
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = feed.Close(cleanup)
	}()
	rows := map[storage.EntityKey]Task{}
	var order []storage.EntityKey
	var subscription q.QuerySubscription
	var samples []WindowSample
	for index, step := range windowSteps(c.Rows) {
		before := meter.Bytes()
		start := time.Now()
		if index == 0 {
			err = feed.SubscribeQuery(ctx, q.QuerySubscribeRequest{ID: "window", Query: storage.QuerySpec{Relation: storage.RelationName{Namespace: "bench", Name: "tasks"}, Filter: integerEqual("project_id", project)}, Window: q.InitialWindow{Count: step.right}}, func(s q.QuerySubscription, snapshot q.QuerySnapshot) error {
				subscription = s
				for _, row := range snapshot.Rows {
					if row.Pending {
						return fmt.Errorf("pending initial row")
					}
					task, err := canonicalTask(row.Record)
					if err != nil {
						return err
					}
					rows[row.Key] = task
					order = append(order, row.Key)
				}
				return nil
			})
		} else {
			err = subscription.UpdateWindow(ctx, step.changes, step.name)
			if err == nil {
				err = receiveWindow(ctx, feed, step.name, rows, &order)
			}
		}
		if err != nil {
			return nil, fmt.Errorf("%s: %w", step.name, err)
		}
		tasks := make([]Task, 0, len(order))
		for i, key := range order {
			task, ok := rows[key]
			if !ok {
				return nil, fmt.Errorf("membership references missing row")
			}
			if i > 0 && task.ID <= tasks[i-1].ID {
				return nil, fmt.Errorf("unordered window")
			}
			tasks = append(tasks, task)
		}
		if err := verifyWindow(c, project, step, tasks); err != nil {
			return nil, err
		}
		samples = append(samples, WindowSample{Project: project, Step: step.name, Milliseconds: float64(time.Since(start)) / 1e6, Rows: len(tasks), Bytes: meter.Bytes() - before})
	}
	return samples, nil
}

func receiveWindow(ctx context.Context, feed q.Feed, token string, rows map[storage.EntityKey]Task, order *[]storage.EntityKey) error {
	for {
		event, err := feed.Next(ctx)
		if err != nil {
			return err
		}
		if event.Failure != nil {
			return event.Failure.Cause
		}
		if event.Delivery == nil {
			continue
		}
		for _, effect := range event.Delivery.Effects {
			if effect.SubscriptionID != "window" {
				return fmt.Errorf("unexpected subscription effect")
			}
			batch := effect.Batch
			if batch.Clear {
				return fmt.Errorf("unexpected query clear")
			}
			for _, change := range batch.Changes {
				if change.Kind == q.Removed {
					delete(rows, change.Key)
					continue
				}
				if change.Pending {
					return fmt.Errorf("pending window row")
				}
				task, err := canonicalTask(change.Record)
				if err != nil {
					return err
				}
				rows[change.Key] = task
			}
			*order, err = q.ApplyMembership(*order, batch.Membership)
			if err != nil {
				return err
			}
			if batch.Window != nil {
				if batch.Window.Token != token {
					return fmt.Errorf("unexpected window token %q", batch.Window.Token)
				}
				return nil
			}
		}
	}
}

func readElectricWindows(ctx context.Context, endpoint string, c Config, project int64) ([]WindowSample, error) {
	transport := &http.Transport{DisableCompression: true, MaxIdleConnsPerHost: 1, MaxConnsPerHost: 1, ResponseHeaderTimeout: 70 * time.Second}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport}
	var samples []WindowSample
	for _, step := range windowSteps(c.Rows) {
		start := time.Now()
		base := (project - 1) * int64(c.Rows)
		where := fmt.Sprintf("project_id = %d AND id > %d AND id <= %d", project, base+int64(step.left), base+int64(step.right))
		rows, bytes, err := electricInitialShape(ctx, client, endpoint, "bench.tasks", where)
		if err != nil {
			return nil, err
		}
		tasks := make([]Task, 0, len(rows))
		for _, row := range rows {
			task, err := electricTask(Task{}, row, true)
			if err != nil {
				return nil, err
			}
			tasks = append(tasks, task)
		}
		if err := verifyWindow(c, project, step, tasks); err != nil {
			return nil, err
		}
		samples = append(samples, WindowSample{Project: project, Step: step.name, Milliseconds: float64(time.Since(start)) / 1e6, Rows: len(tasks), Bytes: bytes})
	}
	return samples, nil
}
