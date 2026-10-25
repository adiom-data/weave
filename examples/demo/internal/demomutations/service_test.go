package demomutations

import (
	"context"
	"encoding/base64"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"

	"connectrpc.com/connect"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"google.golang.org/protobuf/proto"

	demov1 "github.com/adiom-data/weave/examples/demo/gen/demo/v1"
	"github.com/adiom-data/weave/pkg/accesscontrol"
	reactivev1 "github.com/adiom-data/weave/pkg/gen/reactive/v1"
	"github.com/adiom-data/weave/pkg/mutation"
	"github.com/adiom-data/weave/pkg/storage"
)

const projectID = "10000000-0000-4000-8000-000000000001"
const mutationID = "40000000-0000-4000-8000-000000000001"

type testClaims struct{ err error }

func (a testClaims) Claims(context.Context) (accesscontrol.Claims, error) {
	text := func(s string) storage.Value {
		return storage.Value{Type: storage.TypeText, State: storage.ValuePresent, Data: []byte(s)}
	}
	return accesscontrol.Claims{
		"user_id":         text("20000000-0000-4000-8000-000000000001"),
		"organization_id": text("00000000-0000-4000-8000-000000000001"),
		"team_ids":        {Type: storage.TypeList, State: storage.ValuePresent, List: []storage.Value{text("30000000-0000-4000-8000-000000000001")}},
	}, a.err
}

type testDB struct {
	tx    *testTx
	calls int
}

func (d *testDB) Begin(context.Context) (pgx.Tx, error) { d.calls++; return d.tx, nil }

type testTx struct {
	pgx.Tx
	statements            []string
	payload               []byte
	failAt                int
	denied                bool
	noTask                bool
	committed, rolledBack bool
	commitError           error
}
type testRow struct{ err error }

func (r testRow) Scan(dest ...any) error {
	if r.err == nil {
		*dest[0].(*string) = projectID
	}
	return r.err
}
func (tx *testTx) QueryRow(_ context.Context, sql string, _ ...any) pgx.Row {
	tx.statements = append(tx.statements, sql)
	if tx.denied {
		return testRow{pgx.ErrNoRows}
	}
	return testRow{}
}
func (tx *testTx) Exec(_ context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	tx.statements = append(tx.statements, sql)
	if len(tx.statements) == tx.failAt {
		return pgconn.CommandTag{}, errors.New("injected database failure")
	}
	if strings.Contains(sql, "pg_logical_emit_message") {
		tx.payload = append([]byte(nil), args[0].([]byte)...)
	}
	if tx.noTask && strings.Contains(sql, "UPDATE tenant_001.tasks t") {
		return pgconn.NewCommandTag("UPDATE 0"), nil
	}
	return pgconn.NewCommandTag("INSERT 0 1"), nil
}
func (tx *testTx) Commit(context.Context) error {
	tx.committed = tx.commitError == nil
	return tx.commitError
}
func (tx *testTx) Rollback(context.Context) error { tx.rolledBack = !tx.committed; return nil }
func newTestService(tx *testTx, auth testClaims) (*Service, *testDB) {
	db := &testDB{tx: tx}
	return New(db, auth.Claims, slog.New(slog.NewTextHandler(io.Discard, nil))), db
}
func createRequest(correlated bool) *connect.Request[demov1.CreateTaskRequest] {
	r := connect.NewRequest(&demov1.CreateTaskRequest{ProjectId: projectID, Title: "  Test task  "})
	if correlated {
		r.Header().Set(mutation.ConnectionContextHeader, base64.RawURLEncoding.EncodeToString(make([]byte, 32)))
		r.Header().Set(mutation.MutationIDHeader, mutationID)
	}
	return r
}

func TestCreateTaskWritesAndReceiptShareTransaction(t *testing.T) {
	tx := &testTx{}
	service, _ := newTestService(tx, testClaims{})
	response, err := service.CreateTask(context.Background(), createRequest(true))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := storage.Canonicalize(storage.TypeUUID, []byte(response.Msg.TaskId)); err != nil {
		t.Fatal(err)
	}
	if !tx.committed || tx.rolledBack || len(tx.statements) != 4 {
		t.Fatalf("transaction = %+v", tx)
	}
	if !strings.Contains(tx.statements[0], "project_user_grants") || !strings.Contains(tx.statements[0], "project_team_grants") || !strings.Contains(tx.statements[0], "FOR UPDATE") {
		t.Fatal("missing project authorization/append lock")
	}
	var envelope reactivev1.RoutedTransactionMessage
	if err := proto.Unmarshal(tx.payload, &envelope); err != nil {
		t.Fatal(err)
	}
	if envelope.Kind != mutation.ReceiptKind || envelope.ConnectionContext != createRequest(true).Header().Get(mutation.ConnectionContextHeader) || string(envelope.Payload) != mutationID {
		t.Fatalf("receipt = %v", &envelope)
	}
}

func TestCreateTaskFailuresRollbackWithoutSuccess(t *testing.T) {
	for _, at := range []int{2, 3, 4} {
		t.Run(string(rune('0'+at)), func(t *testing.T) {
			tx := &testTx{failAt: at}
			service, _ := newTestService(tx, testClaims{})
			response, err := service.CreateTask(context.Background(), createRequest(true))
			if response != nil || connect.CodeOf(err) != connect.CodeInternal || tx.committed || !tx.rolledBack {
				t.Fatalf("response=%v err=%v tx=%+v", response, err, tx)
			}
		})
	}
	tx := &testTx{commitError: errors.New("connection lost")}
	service, _ := newTestService(tx, testClaims{})
	if _, err := service.CreateTask(context.Background(), createRequest(true)); connect.CodeOf(err) != connect.CodeUnknown {
		t.Fatalf("commit error = %v", err)
	}
}

func TestCreateTaskAuthorizationValidationAndOptionalCorrelation(t *testing.T) {
	service, db := newTestService(&testTx{}, testClaims{err: errors.New("claims unavailable")})
	if _, err := service.CreateTask(context.Background(), createRequest(true)); connect.CodeOf(err) != connect.CodeUnauthenticated || db.calls != 0 {
		t.Fatalf("authentication error = %v", err)
	}
	service, db = newTestService(&testTx{}, testClaims{})
	for _, change := range []func(*connect.Request[demov1.CreateTaskRequest]){
		func(r *connect.Request[demov1.CreateTaskRequest]) { r.Msg.Title = " " },
		func(r *connect.Request[demov1.CreateTaskRequest]) { r.Msg.ProjectId = "invalid" },
		func(r *connect.Request[demov1.CreateTaskRequest]) { r.Header().Del(mutation.MutationIDHeader) },
		func(r *connect.Request[demov1.CreateTaskRequest]) {
			r.Header().Set(mutation.ConnectionContextHeader, "not-a-context")
		},
	} {
		r := createRequest(true)
		change(r)
		if _, err := service.CreateTask(context.Background(), r); connect.CodeOf(err) != connect.CodeInvalidArgument || db.calls != 0 {
			t.Fatalf("validation error = %v", err)
		}
	}
	tx := &testTx{denied: true}
	service, _ = newTestService(tx, testClaims{})
	if _, err := service.CreateTask(context.Background(), createRequest(true)); connect.CodeOf(err) != connect.CodePermissionDenied || tx.committed || !tx.rolledBack {
		t.Fatalf("authorization error = %v", err)
	}
	tx = &testTx{}
	service, _ = newTestService(tx, testClaims{})
	if _, err := service.CreateTask(context.Background(), createRequest(false)); err != nil || !tx.committed || len(tx.payload) != 0 {
		t.Fatalf("nonreactive create: %v", err)
	}
}

func TestTaskChangesShareAuthorizationTransactionAndReceipt(t *testing.T) {
	for _, remove := range []bool{false, true} {
		name := "update"
		steps := 3
		if remove {
			name, steps = "delete", 4
		}
		t.Run(name, func(t *testing.T) {
			call := func(service *Service, id string) error {
				if remove {
					r := connect.NewRequest(&demov1.DeleteTaskRequest{ProjectId: projectID, TaskId: id})
					r.Header().Set(mutation.ConnectionContextHeader, createRequest(true).Header().Get(mutation.ConnectionContextHeader))
					r.Header().Set(mutation.MutationIDHeader, mutationID)
					_, err := service.DeleteTask(context.Background(), r)
					return err
				}
				r := connect.NewRequest(&demov1.UpdateTaskRequest{ProjectId: projectID, TaskId: id, Completed: true})
				r.Header().Set(mutation.ConnectionContextHeader, createRequest(true).Header().Get(mutation.ConnectionContextHeader))
				r.Header().Set(mutation.MutationIDHeader, mutationID)
				_, err := service.UpdateTask(context.Background(), r)
				return err
			}
			const taskID = "50000000-0000-4000-8000-000000000001"
			tx := &testTx{}
			service, _ := newTestService(tx, testClaims{})
			if err := call(service, taskID); err != nil {
				t.Fatal(err)
			}
			if !tx.committed || len(tx.statements) != steps || len(tx.payload) == 0 {
				t.Fatalf("transaction = %+v", tx)
			}
			if !strings.Contains(tx.statements[1], "pt.project_id = $2") || !strings.Contains(tx.statements[1], "pt.organization_id = t.organization_id") {
				t.Fatal("missing task membership/organization restriction")
			}
			if remove {
				if !strings.Contains(tx.statements[2], "UPDATE tenant_001.project_tasks") {
					t.Fatal("delete must soft-delete memberships")
				}
			} else if strings.Contains(tx.statements[1], "title =") {
				t.Fatal("completion changes must not overwrite title")
			}
			for at := 2; at <= steps; at++ {
				tx = &testTx{failAt: at}
				service, _ = newTestService(tx, testClaims{})
				if err := call(service, taskID); connect.CodeOf(err) != connect.CodeInternal || tx.committed || !tx.rolledBack {
					t.Fatalf("step %d: err=%v tx=%+v", at, err, tx)
				}
			}
			for _, failure := range []struct {
				tx   *testTx
				code connect.Code
			}{
				{&testTx{denied: true}, connect.CodePermissionDenied},
				{&testTx{noTask: true}, connect.CodeNotFound},
				{&testTx{commitError: errors.New("lost connection")}, connect.CodeUnknown},
			} {
				service, _ = newTestService(failure.tx, testClaims{})
				if err := call(service, taskID); connect.CodeOf(err) != failure.code || failure.tx.committed || !failure.tx.rolledBack {
					t.Fatalf("err=%v tx=%+v", err, failure.tx)
				}
			}
			service, db := newTestService(&testTx{}, testClaims{})
			if err := call(service, "invalid"); connect.CodeOf(err) != connect.CodeInvalidArgument || db.calls != 0 {
				t.Fatalf("validation = %v", err)
			}
		})
	}
}
