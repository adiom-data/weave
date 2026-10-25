package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/adiom-data/framework/httpapp"
	"github.com/gorilla/websocket"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	healthv1 "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/adiom-data/weave/internal/server/config"
	"github.com/adiom-data/weave/pkg/accesscontrol"
	queryv1 "github.com/adiom-data/weave/pkg/gen/querysubscription/v1"
	reactivev1 "github.com/adiom-data/weave/pkg/gen/reactive/v1"
)

type readyAddresses struct{ address string }
type readyLogHandler struct{ ready chan readyAddresses }

func (h readyLogHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h readyLogHandler) WithAttrs([]slog.Attr) slog.Handler       { return h }
func (h readyLogHandler) WithGroup(string) slog.Handler            { return h }
func (h readyLogHandler) Handle(_ context.Context, r slog.Record) error {
	if r.Message == "http server listening" {
		var a readyAddresses
		r.Attrs(func(v slog.Attr) bool {
			if v.Key == "addr" {
				a.address = v.Value.String()
			}
			return true
		})
		h.ready <- a
	}
	return nil
}

// This test only runs with an explicitly supplied disposable PostgreSQL database.
// It owns a uniquely named table, publication, and slot, and removes them after
// stopping the host. It never changes an existing application's objects.
func TestStandalonePostgres(t *testing.T) {
	databaseURL := os.Getenv("WEAVE_SERVER_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("set WEAVE_SERVER_TEST_DATABASE_URL to a disposable logical-replication database")
	}
	// Exercise the framework's real stream instrumentation and provider shutdown
	// without requiring a collector or sending test data outside this process.
	var exports atomic.Int32
	collector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		exports.Add(1)
		w.Header().Set("Content-Type", "application/x-protobuf")
		w.WriteHeader(http.StatusOK) // Empty protobuf ExportResponse is valid OTLP.
	}))
	defer collector.Close()
	t.Setenv("OTEL_SDK_DISABLED", "false")
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", collector.URL)
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", collector.URL+"/v1/traces")
	t.Setenv("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT", collector.URL+"/v1/metrics")
	t.Setenv("OTEL_TRACES_EXPORTER", "otlp")
	t.Setenv("OTEL_METRICS_EXPORTER", "otlp")
	t.Setenv("OTEL_TRACES_SAMPLER_ARG", "1")
	ctx, cancel := context.WithTimeout(t.Context(), 45*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	name := fmt.Sprintf("weave_host_%d", time.Now().UnixNano()) // fixed prefix + digits only
	if _, err := pool.Exec(ctx, fmt.Sprintf(`CREATE TABLE public.%s(id bigint PRIMARY KEY, title text, organization_id text COLLATE "C"); INSERT INTO public.%s VALUES (1,'visible','one'),(2,'hidden','two'); CREATE PUBLICATION %s FOR TABLE public.%s`, name, name, name, name)); err != nil {
		t.Fatal(err)
	}
	defer func() {
		cleanup, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		if _, err := pool.Exec(cleanup, `SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name=$1`, name); err != nil {
			t.Error(err)
		}
		if _, err := pool.Exec(cleanup, fmt.Sprintf(`DROP PUBLICATION %s; DROP TABLE public.%s`, name, name)); err != nil {
			t.Error(err)
		}
	}()
	c := validConfig()
	c.Storage.Postgres.URL = databaseURL
	c.Storage.Postgres.Publication = name
	c.Storage.Postgres.Slot = name
	c.Storage.Postgres.AllowedTables = []string{"public." + name}
	c.Auth.FixedClaims = config.JSONClaims{"organization_id": "one"}
	c.Address = "127.0.0.1:0"
	c.Visibility.Mode = "policy"
	c.Visibility.PolicyFile = writeTestFile(t, "policy.yaml", fmt.Sprintf(`relations:
  - schema: public
    table: %s
    allow:
      - id: organization
        when: facts.organization_grant.exists(grant, true)
    facts:
      - id: organization_grant
        projection:
          name: grants
          kind: list
          relation: {schema: public, table: %s}
          bindings:
            - field: [id]
              operator: eq
              parameter: [item, id]
            - field: [organization_id]
              operator: eq
              parameter: [claims, organization_id]
`, name, name))
	start := func() (readyAddresses, func()) {
		ready := make(chan readyAddresses, 1)
		done := make(chan error, 1)
		runCtx, stop := context.WithCancel(ctx)
		claimsService := func(name string) httpapp.ConnectService {
			return httpapp.Connect(name, func(options ...connect.HandlerOption) (string, http.Handler) {
				return "/" + name + "/", connect.NewUnaryHandler("/"+name+"/Claims", func(ctx context.Context, _ *connect.Request[emptypb.Empty]) (*connect.Response[wrapperspb.StringValue], error) {
					claims, err := accesscontrol.ClaimsFromContext(ctx)
					value := "no host admission"
					if err == nil {
						value = string(claims["organization_id"].Data)
					}
					return connect.NewResponse(wrapperspb.String(value)), nil
				}, options...)
			})
		}
		host, err := New(runCtx, c, Options{
			Logger:                slog.New(readyLogHandler{ready}),
			Services:              []httpapp.ConnectService{claimsService("app.v1.OwnAdmission")},
			AuthenticatedServices: []httpapp.ConnectService{claimsService("app.v1.HostAdmission")},
			Routes:                []httpapp.Route{httpapp.Handle("GET /demo-status", http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) }))},
		})
		if err != nil {
			stop()
			t.Fatal(err)
		}
		go func() { err := host.Run(runCtx); done <- errors.Join(err, host.Close()) }()
		var once sync.Once
		shutdown := func() {
			once.Do(func() {
				stop()
				select {
				case err := <-done:
					if err != nil {
						t.Error(err)
					}
				case <-ctx.Done():
					t.Error("host failed to stop")
				}
			})
		}
		t.Cleanup(shutdown)
		select {
		case a := <-ready:
			return a, shutdown
		case err := <-done:
			stop()
			t.Fatalf("host failed startup: %v", err)
		case <-ctx.Done():
			stop()
			t.Fatal("host startup timed out")
		}
		return readyAddresses{}, shutdown
	}
	a, stop := start()
	defer stop()
	for _, test := range []struct{ name, want string }{
		{"app.v1.OwnAdmission", "no host admission"},
		{"app.v1.HostAdmission", "one"},
	} {
		client := connect.NewClient[emptypb.Empty, wrapperspb.StringValue](http.DefaultClient, "http://"+a.address+"/"+test.name+"/Claims")
		response, err := client.CallUnary(ctx, connect.NewRequest(&emptypb.Empty{}))
		if err != nil {
			t.Fatal(err)
		}
		if response.Msg.Value != test.want {
			t.Fatalf("%s claims: got %q, want %q", test.name, response.Msg.Value, test.want)
		}
	}
	response, err := http.Get("http://" + a.address + "/demo-status")
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusNoContent {
		t.Fatal("custom route was not mounted")
	}
	connection, err := grpc.NewClient(a.address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	client := reactivev1.NewReactiveQueriesClient(connection)
	root := &reactivev1.ProjectionNode{NodeId: "items", Selection: &reactivev1.ProjectionNode_List{List: &reactivev1.ProjectionList{Query: &reactivev1.Query{Schema: "public", Table: name, Columns: []*reactivev1.FieldPath{{Segments: []string{"id"}}, {Segments: []string{"title"}}, {Segments: []string{"organization_id"}}}}, Window: &reactivev1.InitialWindow{From: reactivev1.InitialWindow_START, Count: 10}}}}
	image, err := client.GetProjectionSnapshot(ctx, &reactivev1.ProjectionSnapshotRequest{Root: root, WaitForSync: true})
	if err != nil {
		t.Fatal(err)
	}
	assertVisibleSnapshot(t, image, "visible")
	raw, err := queryv1.NewQuerySubscriptionsClient(connection).Open(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := raw.Recv(); status.Code(err) != codes.Unimplemented {
		t.Fatalf("raw query service must not be public: %v", err)
	}
	resp, err := healthv1.NewHealthClient(connection).Check(ctx, &healthv1.HealthCheckRequest{Service: "readiness"})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Status != healthv1.HealthCheckResponse_SERVING {
		t.Fatal("not ready")
	}
	stream, err := client.Open(ctx)
	if err != nil {
		t.Fatal(err)
	}
	request := &reactivev1.ClientMessage{SubscriptionId: "items", RequestId: "subscribe", Operation: &reactivev1.ClientMessage_SubscribeProjection{SubscribeProjection: &reactivev1.SubscribeProjection{Root: root, WaitForSync: true}}}
	if err := stream.Send(request); err != nil {
		t.Fatal(err)
	}
	for {
		event, err := stream.Recv()
		if err != nil {
			t.Fatal(err)
		}
		if event.GetError() != nil {
			t.Fatal(event.GetError())
		}
		if batch := event.GetModelBatch(); batch != nil && batch.Kind == reactivev1.ModelBatch_SNAPSHOT {
			assertVisibleImage(t, batch, "visible")
		}
		if event.GetReady() != nil {
			break
		}
	}
	socket, _, err := websocket.DefaultDialer.DialContext(ctx, "ws://"+a.address+"/api/reactive", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer socket.Close()
	if err := socket.SetReadDeadline(time.Now().Add(15 * time.Second)); err != nil {
		t.Fatal(err)
	}
	frame, err := proto.Marshal(&reactivev1.WebSocketClientFrame{Payload: &reactivev1.WebSocketClientFrame_Message{Message: request}})
	if err != nil {
		t.Fatal(err)
	}
	if err := socket.WriteMessage(websocket.BinaryMessage, frame); err != nil {
		t.Fatal(err)
	}
	readSocket := func() *reactivev1.ServerMessage {
		kind, raw, err := socket.ReadMessage()
		if err != nil {
			t.Fatal(err)
		}
		if kind != websocket.BinaryMessage {
			t.Fatal("expected binary")
		}
		var frame reactivev1.WebSocketServerFrame
		if err := proto.Unmarshal(raw, &frame); err != nil {
			t.Fatal(err)
		}
		return frame.GetMessage()
	}
	for {
		event := readSocket()
		if event.GetError() != nil {
			t.Fatal(event.GetError())
		}
		if event.GetReady() != nil {
			break
		}
	}
	if _, err := pool.Exec(ctx, fmt.Sprintf(`UPDATE public.%s SET title='updated' WHERE id=1`, name)); err != nil {
		t.Fatal(err)
	}
	for {
		event, err := stream.Recv()
		if err != nil {
			t.Fatal(err)
		}
		if event.GetError() != nil {
			t.Fatal(event.GetError())
		}
		if batch := event.GetModelBatch(); batch != nil && len(batch.Entities) > 0 {
			assertVisibleImage(t, batch, "updated")
			break
		}
	}
	for {
		event := readSocket()
		if event.GetError() != nil {
			t.Fatal(event.GetError())
		}
		if batch := event.GetModelBatch(); batch != nil && len(batch.Entities) > 0 {
			assertVisibleImage(t, batch, "updated")
			break
		}
	}
	// Stop with both transports still open: host context cancellation must
	// release active RPC handlers and close hijacked WebSockets on its own.
	stop()
	if err := socket.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, _, err := socket.ReadMessage(); err == nil {
		t.Fatal("shutdown left WebSocket open")
	}
	for {
		if _, err := stream.Recv(); err != nil {
			break
		}
	}
	socket.Close()
	connection.Close()
	var retained bool
	if err := pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_replication_slots WHERE slot_name=$1)`, name).Scan(&retained); err != nil || !retained {
		t.Fatalf("slot not retained: %v", err)
	}
	// Reusing the same slot must successfully restart and reload visible state.
	a, stopAgain := start()
	defer stopAgain()
	connection2, err := grpc.NewClient(a.address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer connection2.Close()
	image, err = reactivev1.NewReactiveQueriesClient(connection2).GetProjectionSnapshot(ctx, &reactivev1.ProjectionSnapshotRequest{Root: root, WaitForSync: true})
	if err != nil {
		t.Fatal(err)
	}
	assertVisibleSnapshot(t, image, "updated")
	stopAgain()
	if exports.Load() == 0 {
		t.Fatal("framework telemetry never exported to the test collector")
	}
}

func assertVisibleSnapshot(t *testing.T, image *reactivev1.ProjectionSnapshotResponse, title string) {
	t.Helper()
	rows := image.GetData().GetFields()["items"].GetListValue().GetValues()
	if len(rows) != 1 {
		t.Fatalf("expected one visible row: %+v", image)
	}
	values := rows[0].GetStructValue().GetFields()
	if values["title"].GetStringValue() != title || values["organization_id"].GetStringValue() != "one" {
		t.Fatalf("wrong visible snapshot: %+v", values)
	}
}

func assertVisibleImage(t *testing.T, batch *reactivev1.ModelBatch, title string) {
	t.Helper()
	if batch == nil || len(batch.Entities) != 1 {
		t.Fatalf("expected one visible entity: %+v", batch)
	}
	values := batch.Entities[0].GetUpsert().GetValues()
	if values["title"].GetTextValue() != title || values["organization_id"].GetTextValue() != "one" {
		t.Fatalf("wrong visible state: %+v", values)
	}
}

func validConfig() config.Config {
	c := config.Default()
	c.Storage.Postgres.URL = "postgres://localhost/app"
	c.Storage.Postgres.Publication, c.Storage.Postgres.Slot = "publication", "slot"
	c.Auth.Mode, c.Visibility.Mode = "fixed", "allow_all"
	return c
}
func writeTestFile(t *testing.T, name, contents string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, []byte(contents), 0600); err != nil {
		t.Fatal(err)
	}
	return path
}
