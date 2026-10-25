// Package demomutations owns application write commands, not reactive queries
// or read visibility. Its deliberately small write policy is demo-specific.
package demomutations

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"
	"unicode/utf8"

	"connectrpc.com/connect"
	"github.com/jackc/pgx/v5"

	demov1 "github.com/adiom-data/weave/examples/demo/gen/demo/v1"
	"github.com/adiom-data/weave/pkg/accesscontrol"
	"github.com/adiom-data/weave/pkg/mutation"
	"github.com/adiom-data/weave/pkg/storage"
	"github.com/adiom-data/weave/pkg/storage/postgres"
)

type Database interface {
	Begin(context.Context) (pgx.Tx, error)
}
type Service struct {
	db     Database
	claims func(context.Context) (accesscontrol.Claims, error)
	logger *slog.Logger
}

func New(db Database, claims func(context.Context) (accesscontrol.Claims, error), logger *slog.Logger) *Service {
	return &Service{db: db, claims: claims, logger: logger}
}

func (s *Service) CreateTask(ctx context.Context, request *connect.Request[demov1.CreateTaskRequest]) (*connect.Response[demov1.CreateTaskResponse], error) {
	title := strings.TrimSpace(request.Msg.Title)
	if title == "" || !utf8.ValidString(title) || strings.ContainsRune(title, 0) || len(title) > 4096 {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("title must contain 1–4096 UTF-8 bytes without NUL"))
	}
	var id string
	err := s.writeProject(ctx, request.Header(), request.Msg.ProjectId, func(tx pgx.Tx, organization, project string) error {
		var err error
		id, err = newUUID()
		if err != nil {
			return s.databaseError(err)
		}
		if _, err = tx.Exec(ctx, `INSERT INTO tenant_001.tasks (id, organization_id, title) VALUES ($1, $2, $3)`, id, organization, title); err != nil {
			return s.databaseError(err)
		}
		if _, err = tx.Exec(ctx, `
			INSERT INTO tenant_001.project_tasks (organization_id, project_id, task_id, position)
			SELECT $1, $2, $3, COALESCE(MAX(position), 0) + 1024
			FROM tenant_001.project_tasks WHERE organization_id = $1 AND project_id = $2 AND deleted_at IS NULL`, organization, project, id); err != nil {
			return s.databaseError(err)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(&demov1.CreateTaskResponse{TaskId: id}), nil
}

// Updates are deliberately field-specific: toggling completion must not write
// back a stale title from the caller's projection.
func (s *Service) UpdateTask(ctx context.Context, request *connect.Request[demov1.UpdateTaskRequest]) (*connect.Response[demov1.UpdateTaskResponse], error) {
	err := s.changeTask(ctx, request.Header(), request.Msg.ProjectId, request.Msg.TaskId, &request.Msg.Completed)
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(&demov1.UpdateTaskResponse{}), nil
}

func (s *Service) DeleteTask(ctx context.Context, request *connect.Request[demov1.DeleteTaskRequest]) (*connect.Response[demov1.DeleteTaskResponse], error) {
	err := s.changeTask(ctx, request.Header(), request.Msg.ProjectId, request.Msg.TaskId, nil)
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(&demov1.DeleteTaskResponse{}), nil
}

func (s *Service) changeTask(ctx context.Context, headers http.Header, project, task string, completed *bool) error {
	taskID, err := storage.Canonicalize(storage.TypeUUID, []byte(task))
	if err != nil {
		return connect.NewError(connect.CodeInvalidArgument, errors.New("task_id must be a UUID"))
	}
	return s.writeProject(ctx, headers, project, func(tx pgx.Tx, organization, project string) error {
		// A shared task may be changed through any authorized project containing
		// it. Deletion is global within its organization, not merely an unlink.
		assignment := "completed = $4, updated_at = now()"
		args := []any{organization, project, string(taskID)}
		if completed == nil {
			assignment = "deleted_at = now(), updated_at = now()"
		} else {
			args = append(args, *completed)
		}
		tag, err := tx.Exec(ctx, `UPDATE tenant_001.tasks t SET `+assignment+`
			WHERE t.organization_id = $1 AND t.id = $3 AND t.deleted_at IS NULL
			AND EXISTS (SELECT 1 FROM tenant_001.project_tasks pt
			 WHERE pt.organization_id = t.organization_id AND pt.project_id = $2
			 AND pt.task_id = t.id AND pt.deleted_at IS NULL)`, args...)
		if err != nil {
			return s.databaseError(err)
		}
		if tag.RowsAffected() != 1 {
			return connect.NewError(connect.CodeNotFound, errors.New("task not found in project"))
		}
		if completed == nil {
			_, err = tx.Exec(ctx, `UPDATE tenant_001.project_tasks SET deleted_at = now(), updated_at = now()
				WHERE organization_id = $1 AND task_id = $2 AND deleted_at IS NULL`, organization, string(taskID))
			if err != nil {
				return s.databaseError(err)
			}
		}
		return nil
	})
}

// Every demo command shares authorization, transaction ownership, correlation,
// and receipt emission. The callback never commits or emits a receipt itself.
func (s *Service) writeProject(ctx context.Context, headers http.Header, project string, write func(pgx.Tx, string, string) error) error {
	claims, err := s.claims(ctx)
	if err != nil {
		return connect.NewError(connect.CodeUnauthenticated, errors.New("policy claims unavailable"))
	}
	projectID, err := storage.Canonicalize(storage.TypeUUID, []byte(project))
	if err != nil {
		return connect.NewError(connect.CodeInvalidArgument, errors.New("project_id must be a UUID"))
	}
	correlation, err := mutation.ReadHeaders(headers.Get, mutation.HeaderNames{})
	if err != nil {
		return connect.NewError(connect.CodeInvalidArgument, err)
	}
	organization, organizationOK := claimUUID(claims["organization_id"])
	user, userOK := claimUUID(claims["user_id"])
	if !organizationOK || !userOK {
		return connect.NewError(connect.CodePermissionDenied, errors.New("organization and user claims required"))
	}
	teams := make([]string, 0)
	if value := claims["team_ids"]; value.State == storage.ValuePresent {
		if value.Type != storage.TypeList {
			return connect.NewError(connect.CodePermissionDenied, errors.New("team_ids must be a list of UUIDs"))
		}
		for _, team := range value.List {
			id, ok := claimUUID(team)
			if !ok {
				return connect.NewError(connect.CodePermissionDenied, errors.New("team_ids must be a list of UUIDs"))
			}
			teams = append(teams, id)
		}
	}
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return s.databaseError(err)
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		_ = tx.Rollback(cleanup)
	}()
	// A project lock serializes append-position allocation for this command.
	// Authorization is evaluated against committed database facts, not the
	// reactive cache or the caller-supplied connection routing token.
	var authorized string
	err = tx.QueryRow(ctx, `
		SELECT p.id::text FROM tenant_001.projects p
		WHERE p.id = $1 AND p.organization_id = $2 AND p.deleted_at IS NULL AND (
		 EXISTS (SELECT 1 FROM tenant_001.project_user_grants g
		  WHERE g.organization_id = p.organization_id AND g.project_id = p.id
		  AND g.user_id = $3 AND g.deleted_at IS NULL)
		 OR EXISTS (SELECT 1 FROM tenant_001.project_team_grants g
		  WHERE g.organization_id = p.organization_id AND g.project_id = p.id
		  AND g.team_id = ANY($4::uuid[]) AND g.deleted_at IS NULL)
		) FOR UPDATE OF p`, string(projectID), organization, user, teams).Scan(&authorized)
	if errors.Is(err, pgx.ErrNoRows) {
		return connect.NewError(connect.CodePermissionDenied, errors.New("project access required"))
	}
	if err != nil {
		return s.databaseError(err)
	}
	if err := write(tx, organization, string(projectID)); err != nil {
		return err
	}
	if err := postgres.EmitMutationReceipt(ctx, tx, correlation); err != nil {
		return s.databaseError(err)
	}
	if err := tx.Commit(ctx); err != nil {
		s.logger.Error("commit task mutation", "error", err)
		return connect.NewError(connect.CodeUnknown, errors.New("commit outcome unknown; do not automatically retry"))
	}
	return nil
}

// Claims from YAML/JWT are strings; library callers may also provide UUID values.
// Conversion belongs to this application, not the generic claims admission layer.
func claimUUID(value storage.Value) (string, bool) {
	if value.State != storage.ValuePresent || (value.Type != storage.TypeText && value.Type != storage.TypeUUID) {
		return "", false
	}
	id, err := storage.Canonicalize(storage.TypeUUID, value.Data)
	return string(id), err == nil
}

func (s *Service) databaseError(err error) error {
	s.logger.Error("task mutation", "error", err)
	return connect.NewError(connect.CodeInternal, errors.New("could not mutate task"))
}

func newUUID() (string, error) {
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return "", err
	}
	id[6], id[8] = id[6]&0x0f|0x40, id[8]&0x3f|0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", id[:4], id[4:6], id[6:8], id[8:10], id[10:]), nil
}
