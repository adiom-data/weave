package bench

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const DatabaseName = "weave_query_benchmark"

func CheckDatabase(ctx context.Context, pool *pgxpool.Pool) error {
	var name string
	if err := pool.QueryRow(ctx, "SELECT current_database()").Scan(&name); err != nil {
		return err
	}
	if name != DatabaseName {
		return fmt.Errorf("refusing to write to non-benchmark database %q", name)
	}
	return nil
}

// Seed only creates fresh objects; it never truncates/replaces existing data.
func Seed(ctx context.Context, pool *pgxpool.Pool, c Config) error {
	if err := c.Validate(); err != nil {
		return err
	}
	if err := CheckDatabase(ctx, pool); err != nil {
		return err
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(context.Background())
	for _, sql := range []string{
		"CREATE TABLE public.tasks (id bigint PRIMARY KEY, project_id bigint NOT NULL, revision bigint NOT NULL, payload text NOT NULL)",
		"CREATE INDEX tasks_project_id_id ON public.tasks(project_id,id)",
		"CREATE PUBLICATION bench_publication FOR TABLE public.tasks",
	} {
		if _, err := tx.Exec(ctx, sql); err != nil {
			return err
		}
	}
	_, err = tx.Exec(ctx, `INSERT INTO public.tasks SELECT (p-1)*$2+r,p,0,repeat('x',$3) FROM generate_series(1,$1::bigint) p CROSS JOIN generate_series(1,$2::bigint) r`, c.Projects, c.Rows, c.PayloadBytes)
	if err != nil {
		return err
	}
	if c.Workload == "expansion" {
		for _, sql := range []string{
			"CREATE TABLE public.links(id bigint PRIMARY KEY, project_id bigint NOT NULL, task_id bigint NOT NULL)",
			"CREATE INDEX links_project_id_id ON public.links(project_id,id)",
			"INSERT INTO public.links SELECT id,project_id,id FROM public.tasks",
			"ALTER PUBLICATION bench_publication ADD TABLE public.links",
			"ANALYZE public.links",
		} {
			if _, err := tx.Exec(ctx, sql); err != nil {
				return err
			}
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	_, err = pool.Exec(ctx, "ANALYZE public.tasks")
	return err
}

type Report struct {
	FormatVersion         int          `json:"format_version"`
	Status                string       `json:"status"`
	Error                 string       `json:"error,omitempty"`
	Config                Config       `json:"config"`
	StartedAt             time.Time    `json:"started_at"`
	MeasurementStartedAt  time.Time    `json:"measurement_started_at"`
	WritesFinishedAt      time.Time    `json:"writes_finished_at"`
	FinishedAt            time.Time    `json:"finished_at"`
	InitialWallSeconds    float64      `json:"initial_wall_seconds"`
	BackendSetupSeconds   float64      `json:"backend_setup_seconds,omitempty"`
	Initialization        Distribution `json:"initial_sync"`
	Latency               Distribution `json:"write_start_to_client_apply"`
	First                 Distribution `json:"write_start_to_first_client"`
	Last                  Distribution `json:"write_start_to_last_client"`
	DatabaseAck           Distribution `json:"write_start_to_database_ack"`
	SchedulerLag          Distribution `json:"writer_schedule_lag"`
	MeasuredWrites        int64        `json:"measured_committed_writes"`
	WarmupWrites          int64        `json:"warmup_committed_writes"`
	ActualWritesPerSecond float64      `json:"actual_writes_per_second"`
	Expected              uint64       `json:"expected_measured_deliveries"`
	Received              uint64       `json:"received_measured_deliveries"`
	Missing               uint64       `json:"missing_deliveries_including_warmup"`
	Bytes                 uint64       `json:"received_application_payload_bytes"`
	VerifiedClients       int          `json:"verified_clients"`
	WriterLimited         bool         `json:"writer_limited"`
	WriterConcurrency     int          `json:"writer_concurrency"`
}

func Run(parent context.Context, pool *pgxpool.Pool, c Config, adapter Adapter) (report Report, runErr error) {
	report = Report{FormatVersion: 1, Status: "failed", Config: c, StartedAt: time.Now().UTC(), WriterConcurrency: 1}
	if err := c.Validate(); err != nil {
		return report, err
	}
	if err := CheckDatabase(parent, pool); err != nil {
		return report, err
	}
	ctx, cancel := context.WithCancelCause(parent)
	recorder := NewRecorder(c)
	var clients sync.WaitGroup
	var initial, ack, lag Histogram
	defer func() {
		cancel(context.Canceled)
		clients.Wait()
		report.FinishedAt = time.Now().UTC()
		report.Bytes = adapter.Bytes()
		report.Initialization = initial.Summary()
		report.DatabaseAck = ack.Summary()
		report.SchedulerLag = lag.Summary()
		recorder.mu.Lock()
		defer recorder.mu.Unlock()
		report.Latency = recorder.latency.Summary()
		report.First = recorder.first.Summary()
		report.Last = recorder.last.Summary()
		report.Expected = recorder.expected
		report.Received = recorder.received
		report.Missing = recorder.remaining
		if !report.MeasurementStartedAt.IsZero() && !report.WritesFinishedAt.IsZero() {
			report.ActualWritesPerSecond = float64(report.MeasuredWrites) / report.WritesFinishedAt.Sub(report.MeasurementStartedAt).Seconds()
		}
		report.WriterLimited = report.ActualWritesPerSecond < float64(c.Rate)*.95 || report.SchedulerLag.P99 > 1000/float64(c.Rate)
		if runErr != nil {
			report.Error = runErr.Error()
		} else {
			report.Status = "ok"
		}
	}()
	observers := make([]*Observer, c.Clients)
	initialized := make(chan time.Duration, c.Clients)
	semaphore := make(chan struct{}, c.InitConcurrency)
	initCtx, stopInit := context.WithTimeout(ctx, c.InitTimeout)
	defer stopInit()
	// Persistent-query backends may need setup before individual viewers attach.
	// Include it in total initialization wall time, but report it separately from
	// per-viewer attachment distributions. It shares the initialization deadline.
	if setup, ok := adapter.(interface {
		Setup(context.Context, Config) error
	}); ok {
		started := time.Now()
		err := setup.Setup(initCtx, c)
		report.BackendSetupSeconds = time.Since(started).Seconds()
		if err != nil {
			return report, fmt.Errorf("backend setup: %w", err)
		}
	}
	for i := range c.Clients {
		o := NewObserver(i, recorder)
		observers[i] = o
		clients.Add(1)
		go func() {
			defer clients.Done()
			select {
			case semaphore <- struct{}{}:
			case <-initCtx.Done():
				return
			}
			start := time.Now()
			result := make(chan error, 1)
			go func() { result <- adapter.Run(ctx, o.project, o) }()
			select {
			case <-o.ready:
				<-semaphore
				initialized <- time.Since(start)
			case err := <-result:
				<-semaphore
				cancel(fmt.Errorf("client %d initialization: %w", o.client, err))
				return
			case <-initCtx.Done():
				<-semaphore
				cancel(initCtx.Err())
				<-result
				return
			}
			if err := <-result; ctx.Err() == nil {
				cancel(fmt.Errorf("client %d stream ended: %w", o.client, err))
			}
		}()
	}
	for range c.Clients {
		select {
		case duration := <-initialized:
			initial.Add(duration)
		case <-initCtx.Done():
			return report, fmt.Errorf("initialize clients: %w", context.Cause(initCtx))
		}
	}
	stopInit()
	report.InitialWallSeconds = time.Since(report.StartedAt).Seconds()
	// One sequential writer preserves exact per-row revision order. Scheduling
	// lag and achieved rate are reported so writer saturation cannot look like
	// engine capacity. Register before Exec: CDC may arrive before its SQL ack.
	sequence := int64(0)
	writePhase := func(duration time.Duration, measured bool) error {
		start := time.Now()
		end := start.Add(duration)
		if measured {
			report.MeasurementStartedAt = start.UTC()
		}
		for index := int64(0); ; index++ {
			scheduled := start.Add(time.Duration(index) * time.Second / time.Duration(c.Rate))
			if !scheduled.Before(end) {
				break
			}
			if remaining := time.Until(scheduled); remaining > 0 {
				timer := time.NewTimer(remaining)
				select {
				case <-timer.C:
				case <-ctx.Done():
					timer.Stop()
					return context.Cause(ctx)
				}
			}
			if err := ctx.Err(); err != nil {
				return context.Cause(ctx)
			}
			sequence++
			begin := time.Now()
			if measured {
				lag.Add(begin.Sub(scheduled))
			}
			recorder.Register(sequence, begin, measured)
			_, id := c.Mutation(sequence)
			command, err := pool.Exec(ctx, "UPDATE public.tasks SET revision=$1 WHERE id=$2", sequence, id)
			if err != nil {
				return err
			}
			if command.RowsAffected() != 1 {
				return fmt.Errorf("mutation did not update one seeded row")
			}
			if measured {
				report.MeasuredWrites++
				ack.Add(time.Since(begin))
			} else {
				report.WarmupWrites++
			}
		}
		if remaining := time.Until(end); remaining > 0 {
			timer := time.NewTimer(remaining)
			select {
			case <-timer.C:
			case <-ctx.Done():
				timer.Stop()
				return context.Cause(ctx)
			}
		}
		return nil
	}
	if err := writePhase(c.Warmup, false); err != nil {
		return report, err
	}
	if err := writePhase(c.Duration, true); err != nil {
		return report, err
	}
	report.WritesFinishedAt = time.Now().UTC()
	drain, stopDrain := context.WithTimeout(ctx, c.DrainTimeout)
	defer stopDrain()
	if err := recorder.Wait(drain); err != nil {
		if ctx.Err() != nil {
			return report, context.Cause(ctx)
		}
		return report, err
	}
	oracle, err := readOracle(ctx, pool)
	if err != nil {
		return report, err
	}
	for _, o := range observers {
		if err := o.Verify(oracle[o.project]); err != nil {
			return report, err
		}
		report.VerifiedClients++
	}
	if ctx.Err() != nil {
		return report, context.Cause(ctx)
	}
	return report, nil
}

func readOracle(ctx context.Context, pool *pgxpool.Pool) (map[int64]map[int64]Task, error) {
	rows, err := pool.Query(ctx, "SELECT id,project_id,revision,payload FROM public.tasks")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make(map[int64]map[int64]Task)
	for rows.Next() {
		var row Task
		if err := rows.Scan(&row.ID, &row.ProjectID, &row.Revision, &row.Payload); err != nil {
			return nil, err
		}
		if result[row.ProjectID] == nil {
			result[row.ProjectID] = make(map[int64]Task)
		}
		result[row.ProjectID][row.ID] = row
	}
	return result, rows.Err()
}

// VerifySeed refuses runs against an already mutated/incompatible dataset.
func VerifySeed(ctx context.Context, pool *pgxpool.Pool, c Config) error {
	var total, invalid int64
	err := pool.QueryRow(ctx, `SELECT count(*), count(*) FILTER (WHERE revision<>0 OR octet_length(payload)<>$1 OR payload<>$2 OR project_id<1 OR project_id>$3 OR id<((project_id-1)*$4+1) OR id>project_id*$4) FROM public.tasks`, c.PayloadBytes, strings.Repeat("x", c.PayloadBytes), c.Projects, c.Rows).Scan(&total, &invalid)
	if err != nil {
		return err
	}
	if total != int64(c.Projects)*int64(c.Rows) || invalid != 0 {
		return fmt.Errorf("dataset is not a fresh seed for this configuration: rows=%d invalid=%d", total, invalid)
	}
	if c.Workload == "expansion" {
		if err := pool.QueryRow(ctx, `SELECT count(*),count(*) FILTER (WHERE l.id<>l.task_id OR t.id IS NULL OR l.project_id<>t.project_id) FROM public.links l LEFT JOIN public.tasks t ON t.id=l.task_id`).Scan(&total, &invalid); err != nil {
			return err
		}
		if total != int64(c.Projects)*int64(c.Rows) || invalid != 0 {
			return fmt.Errorf("invalid expansion seed: links=%d invalid=%d", total, invalid)
		}
	}
	return nil
}
