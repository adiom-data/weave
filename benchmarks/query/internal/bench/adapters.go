package bench

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/stats"

	"github.com/adiom-data/weave/pkg/querysubscription"
	querygrpc "github.com/adiom-data/weave/pkg/querysubscription/grpc"
	"github.com/adiom-data/weave/pkg/storage"
)

type Weave struct {
	Address  string
	Rows     int
	received atomic.Uint64
}

func (s *Weave) Bytes() uint64                                                     { return s.received.Load() }
func (s *Weave) TagRPC(ctx context.Context, _ *stats.RPCTagInfo) context.Context   { return ctx }
func (s *Weave) TagConn(ctx context.Context, _ *stats.ConnTagInfo) context.Context { return ctx }
func (s *Weave) HandleConn(context.Context, stats.ConnStats)                       {}
func (s *Weave) HandleRPC(_ context.Context, event stats.RPCStats) {
	if payload, ok := event.(*stats.InPayload); ok {
		s.received.Add(uint64(payload.WireLength))
	}
}
func (s *Weave) Run(ctx context.Context, project int64, o *Observer) error {
	// Each simulated client owns its connection and feed; no client-side sharing.
	connection, err := grpc.NewClient(s.Address, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithStatsHandler(s))
	if err != nil {
		return err
	}
	defer connection.Close()
	feed, err := querygrpc.NewClient(connection).OpenFeed(ctx, querysubscription.FeedOptions{BufferLimit: 1024})
	if err != nil {
		return err
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = feed.Close(cleanup)
	}()
	keys := make(map[storage.EntityKey]int64)
	err = feed.SubscribeQuery(ctx, querysubscription.QuerySubscribeRequest{
		ID: "project", Query: storage.QuerySpec{Relation: storage.RelationName{Namespace: "public", Name: "tasks"}, Filter: &storage.Predicate{Comparison: &storage.Comparison{Field: storage.MustFieldPath("project_id"), Operator: storage.ComparisonEqual, Value: storage.Literal{Type: storage.TypeInteger, Data: []byte(strconv.FormatInt(project, 10))}}}},
		Window: querysubscription.InitialWindow{Count: s.Rows + 1},
	}, func(_ querysubscription.QuerySubscription, snapshot querysubscription.QuerySnapshot) error {
		if snapshot.Bounds.Left.Kind != querysubscription.BoundaryStart || snapshot.Bounds.Right.Kind != querysubscription.BoundaryEnd {
			return fmt.Errorf("benchmark query must cover the entire filtered project")
		}
		rows := make([]Task, 0, len(snapshot.Rows))
		for _, row := range snapshot.Rows {
			task, err := canonicalTask(row.Record)
			if err != nil {
				return err
			}
			keys[row.Key] = task.ID
			rows = append(rows, task)
		}
		return o.Ready(rows)
	})
	if err != nil {
		return err
	}
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
		for effect := range event.Delivery.SubscriptionEffects() {
			if effect.Batch.Clear {
				return fmt.Errorf("unexpected query reset")
			}
			var changes []Change
			for _, change := range effect.Batch.Changes {
				if change.Kind == querysubscription.Removed {
					if id, ok := keys[change.Key]; ok {
						changes = append(changes, Change{Deleted: true, Task: Task{ID: id}})
						delete(keys, change.Key)
					}
					continue
				}
				if change.Pending {
					return fmt.Errorf("unexpected unresolved benchmark row")
				}
				task, err := canonicalTask(change.Record)
				if err != nil {
					return err
				}
				keys[change.Key] = task.ID
				changes = append(changes, Change{Task: task})
			}
			for _, key := range effect.Batch.Membership.Removed {
				if id, ok := keys[key]; ok {
					changes = append(changes, Change{Deleted: true, Task: Task{ID: id}})
					delete(keys, key)
				}
			}
			if err := o.Apply(changes); err != nil {
				return err
			}
		}
	}
}
func canonicalTask(record storage.Record) (Task, error) {
	var task Task
	for name, target := range map[string]*int64{"id": &task.ID, "project_id": &task.ProjectID, "revision": &task.Revision} {
		value, ok := record[name]
		if !ok || value.State != storage.ValuePresent || value.Type != storage.TypeInteger {
			return task, fmt.Errorf("invalid integer %s", name)
		}
		n, err := strconv.ParseInt(string(value.Data), 10, 64)
		if err != nil {
			return task, err
		}
		*target = n
	}
	payload, ok := record["payload"]
	if !ok || payload.State != storage.ValuePresent || payload.Type != storage.TypeText {
		return task, fmt.Errorf("invalid payload")
	}
	task.Payload = string(payload.Data)
	return task, nil
}

// Electric implements only the documented full-shape GET/long-poll protocol.
// It intentionally fails on resync/retry responses rather than masking loss.
type Electric struct {
	URL      string
	received atomic.Uint64
}

func (e *Electric) Bytes() uint64 { return e.received.Load() }

type electricMessage struct {
	Key     string                     `json:"key"`
	Value   map[string]json.RawMessage `json:"value"`
	Headers struct {
		Operation string `json:"operation"`
		Control   string `json:"control"`
	} `json:"headers"`
}
type countingReader struct {
	io.Reader
	count *atomic.Uint64
}

func (r countingReader) Read(p []byte) (int, error) {
	n, err := r.Reader.Read(p)
	r.count.Add(uint64(n))
	return n, err
}
func (e *Electric) Run(ctx context.Context, project int64, o *Observer) error {
	transport := &http.Transport{MaxIdleConns: 1, MaxIdleConnsPerHost: 1, MaxConnsPerHost: 1, DisableCompression: true, ResponseHeaderTimeout: 70 * time.Second}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport}
	endpoint, err := url.Parse(e.URL)
	if err != nil {
		return err
	}
	query := endpoint.Query()
	query.Set("table", "public.tasks")
	query.Set("where", fmt.Sprintf("project_id = %d", project))
	query.Set("offset", "-1")
	rows := make(map[string]Task)
	initialized := false
	for {
		endpoint.RawQuery = query.Encode()
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.String(), nil)
		if err != nil {
			return err
		}
		response, err := client.Do(request)
		if err != nil {
			return err
		}
		if response.StatusCode != http.StatusOK {
			message, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
			response.Body.Close()
			return fmt.Errorf("Electric HTTP %d: %s", response.StatusCode, message)
		}
		var messages []electricMessage
		decoder := json.NewDecoder(io.LimitReader(countingReader{response.Body, &e.received}, 64<<20))
		err = decoder.Decode(&messages)
		if err == nil {
			var extra any
			if trailing := decoder.Decode(&extra); trailing != io.EOF {
				err = fmt.Errorf("unexpected trailing Electric response: %v", trailing)
			}
		}
		response.Body.Close()
		if err != nil {
			return err
		}
		offset := response.Header.Get("electric-offset")
		if offset == "" {
			return fmt.Errorf("Electric response omitted continuation offset")
		}
		query.Set("offset", offset)
		if handle := response.Header.Get("electric-handle"); handle != "" {
			query.Set("handle", handle)
		}
		if cursor := response.Header.Get("electric-cursor"); cursor != "" {
			query.Set("cursor", cursor)
		}
		caughtUp := false
		var changes []Change
		for _, message := range messages {
			switch message.Headers.Control {
			case "must-refetch":
				return fmt.Errorf("Electric required resynchronization")
			case "up-to-date":
				caughtUp = true
				continue
			case "snapshot-end":
				if initialized {
					return fmt.Errorf("unexpected Electric snapshot after initialization")
				}
				continue
			case "":
			default:
				return fmt.Errorf("unsupported Electric control %q", message.Headers.Control)
			}
			old, exists := rows[message.Key]
			if message.Key == "" {
				return fmt.Errorf("Electric row has no key")
			}
			switch message.Headers.Operation {
			case "delete":
				if !exists {
					return fmt.Errorf("Electric deleted unknown key")
				}
				delete(rows, message.Key)
				changes = append(changes, Change{Deleted: true, Task: old})
			case "insert", "update":
				if message.Headers.Operation == "update" && !exists {
					return fmt.Errorf("Electric updated unknown key")
				}
				row, err := electricTask(old, message.Value, message.Headers.Operation == "insert")
				if err != nil {
					return err
				}
				rows[message.Key] = row
				changes = append(changes, Change{Task: row})
			default:
				return fmt.Errorf("unsupported Electric operation %q", message.Headers.Operation)
			}
		}
		if !initialized {
			if !caughtUp {
				continue
			}
			initial := make([]Task, 0, len(rows))
			for _, row := range rows {
				initial = append(initial, row)
			}
			if err := o.Ready(initial); err != nil {
				return err
			}
			initialized = true
			query.Set("live", "true")
		} else if err := o.Apply(changes); err != nil {
			return err
		}
	}
}
func electricTask(row Task, values map[string]json.RawMessage, complete bool) (Task, error) {
	for name, target := range map[string]*int64{"id": &row.ID, "project_id": &row.ProjectID, "revision": &row.Revision} {
		raw, ok := values[name]
		if !ok {
			if complete {
				return row, fmt.Errorf("Electric row missing %s", name)
			}
			continue
		}
		var text string
		if len(raw) > 0 && raw[0] == '"' {
			if err := json.Unmarshal(raw, &text); err != nil {
				return row, err
			}
		} else {
			text = string(raw)
		}
		n, err := strconv.ParseInt(text, 10, 64)
		if err != nil {
			return row, err
		}
		*target = n
	}
	if raw, ok := values["payload"]; ok {
		if err := json.Unmarshal(raw, &row.Payload); err != nil {
			return row, err
		}
	} else if complete {
		return row, fmt.Errorf("Electric row missing payload")
	}
	return row, nil
}
