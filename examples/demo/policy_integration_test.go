package demo_test

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	reactivev1 "github.com/adiom-data/weave/pkg/gen/reactive/v1"
	"github.com/adiom-data/weave/pkg/server"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestDeclarativeDemoPolicyWithPostgres(t *testing.T) {
	url := os.Getenv("WEAVE_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("set WEAVE_TEST_DATABASE_URL to a disposable logical-replication database")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	name := fmt.Sprintf("weave_policy_%d", time.Now().UnixNano())
	// The namespace/publication/slot name is generated here, never user SQL.
	ddl := fmt.Sprintf(`CREATE SCHEMA %[1]s;
CREATE TABLE %[1]s.projects(id uuid PRIMARY KEY, organization_id uuid NOT NULL,title text NOT NULL,deleted_at timestamptz);
CREATE TABLE %[1]s.tasks(id uuid PRIMARY KEY, organization_id uuid NOT NULL,title text NOT NULL,deleted_at timestamptz);
CREATE TABLE %[1]s.project_tasks(id uuid PRIMARY KEY,organization_id uuid NOT NULL,project_id uuid NOT NULL,task_id uuid NOT NULL,deleted_at timestamptz);
CREATE TABLE %[1]s.project_user_grants(id uuid PRIMARY KEY,organization_id uuid NOT NULL,project_id uuid NOT NULL,user_id uuid NOT NULL,deleted_at timestamptz);
CREATE TABLE %[1]s.project_team_grants(id uuid PRIMARY KEY,organization_id uuid NOT NULL,project_id uuid NOT NULL,team_id uuid NOT NULL,deleted_at timestamptz);
CREATE PUBLICATION %[1]s FOR ALL TABLES;`, name)
	if _, err = pool.Exec(ctx, ddl); err != nil {
		t.Fatal(err)
	}
	defer func() {
		cleanup, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		_, _ = pool.Exec(cleanup, `SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name=$1`, name)
		_, _ = pool.Exec(cleanup, fmt.Sprintf(`DROP PUBLICATION %s; DROP SCHEMA %s CASCADE`, name, name))
	}()
	const organization = "00000000-0000-0000-0000-000000000001"
	const user = "00000000-0000-0000-0000-0000000000b2"
	const project = "00000000-0000-0000-0000-000000000003"
	const task = "00000000-0000-0000-0000-000000000004"
	const link = "00000000-0000-0000-0000-000000000005"
	const grant = "00000000-0000-0000-0000-000000000006"
	const team = "00000000-0000-0000-0000-0000000000c7"
	const teamGrant = "00000000-0000-0000-0000-000000000008"
	const otherGrant = "00000000-0000-0000-0000-000000000009"
	const otherUser = "00000000-0000-0000-0000-00000000000a"
	if _, err = pool.Exec(ctx, fmt.Sprintf(`INSERT INTO %s.projects VALUES($1,$2,'Project',NULL)`, name), project, organization); err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, fmt.Sprintf(`INSERT INTO %s.tasks VALUES($1,$2,'Task',NULL)`, name), task, organization); err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, fmt.Sprintf(`INSERT INTO %s.project_tasks VALUES($1,$2,$3,$4,NULL)`, name), link, organization, project, task); err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, fmt.Sprintf(`INSERT INTO %s.project_user_grants VALUES($1,$2,$3,$4,NULL)`, name), grant, organization, project, user); err != nil {
		t.Fatal(err)
	}
	// A grant to someone else must not become an allow for this subject,
	// including after its real grants vanish.
	if _, err = pool.Exec(ctx, fmt.Sprintf(`INSERT INTO %s.project_user_grants VALUES($1,$2,$3,$4,NULL)`, name), otherGrant, organization, project, otherUser); err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, fmt.Sprintf(`INSERT INTO %s.project_team_grants VALUES($1,$2,$3,$4,now())`, name), teamGrant, organization, project, team); err != nil {
		t.Fatal(err)
	}

	cfg, err := server.LoadConfig("server.yaml", func(string) (string, bool) { return "", false })
	if err != nil {
		t.Fatal(err)
	}
	cfg.Address = "127.0.0.1:0"
	cfg.Storage.Postgres.URL, cfg.Storage.Postgres.Slot, cfg.Storage.Postgres.Publication = url, name, name
	// Admission must canonicalize both compact and uppercase UUID strings so
	// native fact bindings and policy comparisons agree with mutation IDs.
	cfg.Auth.FixedClaims = server.JSONClaims{"organization_id": strings.ReplaceAll(organization, "-", ""), "user_id": strings.ToUpper(strings.ReplaceAll(user, "-", "")), "team_ids": []any{strings.ToUpper(team)}}
	policyData, err := os.ReadFile("relations.yaml")
	if err != nil {
		t.Fatal(err)
	}
	cfg.Relations.File = filepath.Join(t.TempDir(), "relations.yaml")
	if err := os.WriteFile(cfg.Relations.File, []byte(strings.ReplaceAll(string(policyData), "tenant_001", name)), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("OTEL_SDK_DISABLED", "true")
	ready := make(chan string, 1)
	runCtx, stop := context.WithCancel(ctx)
	defer stop()
	host, err := server.New(runCtx, cfg, server.Options{Logger: slog.New(demoReadyHandler{ready})})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- host.Run(runCtx) }()
	resultRead := false
	defer func() {
		stop()
		if err := host.Close(); err != nil {
			t.Error(err)
		}
		if !resultRead {
			if err := <-done; err != nil {
				t.Error(err)
			}
		}
	}()
	var address string
	select {
	case address = <-ready:
	case err := <-done:
		resultRead = true
		t.Fatalf("host startup failed: %v", err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	root := &reactivev1.ProjectionNode{NodeId: "project", Selection: &reactivev1.ProjectionNode_Entity{Entity: &reactivev1.ProjectionEntity{Query: &reactivev1.Query{Schema: name, Table: "projects", Columns: policyColumns("id", "title"), Filter: policyEqual("id", project)}}}}
	root.Children = []*reactivev1.ProjectionNode{{NodeId: "links", Selection: &reactivev1.ProjectionNode_List{List: &reactivev1.ProjectionList{Query: &reactivev1.Query{Schema: name, Table: "project_tasks", Columns: policyColumns("id", "project_id", "task_id")}, Window: &reactivev1.InitialWindow{From: reactivev1.InitialWindow_FROM_START, Count: 10}, Bindings: []*reactivev1.ProjectionBinding{policyBinding("project_id", "id")}}}, Children: []*reactivev1.ProjectionNode{{NodeId: "task", Selection: &reactivev1.ProjectionNode_Entity{Entity: &reactivev1.ProjectionEntity{Query: &reactivev1.Query{Schema: name, Table: "tasks", Columns: policyColumns("id", "title")}, Bindings: []*reactivev1.ProjectionBinding{policyBinding("id", "task_id")}}}}}}}
	root.Computed = []string{"role", "canEdit"}
	root.Children[0].Children[0].Computed = []string{"role", "canEdit"}
	client := reactivev1.NewReactiveQueriesClient(conn)
	image, err := client.GetProjectionSnapshot(ctx, &reactivev1.ProjectionSnapshotRequest{Root: root, WaitForSync: true})
	if err != nil {
		t.Fatal(err)
	}
	projectData := image.GetData().GetFields()["project"].GetStructValue()
	links := projectData.GetFields()["links"].GetListValue().GetValues()
	if len(links) != 1 || links[0].GetStructValue().GetFields()["task"].GetStructValue() == nil {
		t.Fatalf("expected project with association and task; got %v", image)
	}
	for _, row := range []map[string]*structpb.Value{projectData.Fields, links[0].GetStructValue().Fields["task"].GetStructValue().Fields} {
		if row["role"].GetStringValue() != "2" || !row["canEdit"].GetBoolValue() {
			t.Fatalf("computed role did not propagate through the association: %v", image)
		}
	}
	live, err := client.Open(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err = live.Send(&reactivev1.ClientMessage{SubscriptionId: "project", RequestId: "open", Operation: &reactivev1.ClientMessage_SubscribeProjection{SubscribeProjection: &reactivev1.SubscribeProjection{Root: root, WaitForSync: true}}}); err != nil {
		t.Fatal(err)
	}
	for {
		message, err := live.Recv()
		if err != nil {
			t.Fatal(err)
		}
		if failure := message.GetError(); failure != nil {
			t.Fatal(failure)
		}
		if message.GetReady() != nil {
			break
		}
	}
	// Replace the known user grant with a previously undiscovered team
	// grant in one transaction. Lazy discovery must not flicker the old
	// projection, and its newer evidence must not be backdated.
	title := "TeamProject-configured-host"
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.Background())
	if _, err = tx.Exec(ctx, fmt.Sprintf(`UPDATE %s.project_user_grants SET deleted_at=now() WHERE id=$1`, name), grant); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Exec(ctx, fmt.Sprintf(`UPDATE %s.project_team_grants SET deleted_at=NULL WHERE id=$1`, name), teamGrant); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Exec(ctx, fmt.Sprintf(`UPDATE %s.projects SET title=$1 WHERE id=$2`, name), title, project); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	for {
		message, err := live.Recv()
		if err != nil {
			t.Fatal(err)
		}
		if failure := message.GetError(); failure != nil {
			t.Fatal(failure)
		}
		current := false
		if batch := message.GetModelBatch(); batch != nil {
			for _, entity := range batch.Entities {
				if entity.GetReleased() != nil {
					t.Fatalf("grant replacement flickered: %v", batch)
				}
				if entity.GetUpsert().GetValues()["title"].GetTextValue() == title {
					current = true
				}
			}
		}
		if current {
			break
		}
	}
	if _, err = pool.Exec(ctx, fmt.Sprintf(`UPDATE %s.project_team_grants SET deleted_at=now() WHERE id=$1`, name), teamGrant); err != nil {
		t.Fatal(err)
	}
	for {
		message, err := live.Recv()
		if err != nil {
			t.Fatal(err)
		}
		if failure := message.GetError(); failure != nil {
			t.Fatal(failure)
		}
		released := 0
		if batch := message.GetModelBatch(); batch != nil {
			for _, entity := range batch.Entities {
				if entity.GetReleased() != nil {
					released++
				}
			}
		}
		if released == 3 {
			break
		}
	}
	if _, err = pool.Exec(ctx, fmt.Sprintf(`UPDATE %s.project_user_grants SET deleted_at=NULL WHERE id=$1`, name), grant); err != nil {
		t.Fatal(err)
	}
	for {
		message, err := live.Recv()
		if err != nil {
			t.Fatal(err)
		}
		if failure := message.GetError(); failure != nil {
			t.Fatal(failure)
		}
		upserts := 0
		if batch := message.GetModelBatch(); batch != nil {
			for _, entity := range batch.Entities {
				if entity.GetUpsert() != nil {
					upserts++
				}
			}
		}
		if upserts == 3 {
			break
		}
	}
	_ = live.CloseSend()
}

type demoReadyHandler struct{ ready chan string }

func (h demoReadyHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h demoReadyHandler) WithAttrs([]slog.Attr) slog.Handler       { return h }
func (h demoReadyHandler) WithGroup(string) slog.Handler            { return h }
func (h demoReadyHandler) Handle(_ context.Context, record slog.Record) error {
	if record.Message == "http server listening" {
		record.Attrs(func(a slog.Attr) bool {
			if a.Key == "addr" {
				h.ready <- a.Value.String()
			}
			return true
		})
	}
	return nil
}
func policyColumns(names ...string) []*reactivev1.FieldPath {
	var columns []*reactivev1.FieldPath
	for _, name := range names {
		columns = append(columns, &reactivev1.FieldPath{Segments: []string{name}})
	}
	return columns
}
func policyEqual(column, value string) *reactivev1.Predicate {
	return &reactivev1.Predicate{Expression: &reactivev1.Predicate_Comparison{Comparison: &reactivev1.Comparison{Column: &reactivev1.FieldPath{Segments: []string{column}}, Operator: reactivev1.Comparison_OPERATOR_EQUAL, Value: &reactivev1.Value{Kind: &reactivev1.Value_UuidValue{UuidValue: value}}}}}
}
func policyBinding(column, parent string) *reactivev1.ProjectionBinding {
	return &reactivev1.ProjectionBinding{Column: &reactivev1.FieldPath{Segments: []string{column}}, Operator: reactivev1.Comparison_OPERATOR_EQUAL, Value: &reactivev1.ProjectionBinding_ParentColumn{ParentColumn: &reactivev1.FieldPath{Segments: []string{parent}}}}
}
