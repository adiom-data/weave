package demomutations

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/adiom-data/weave/pkg/accesscontrol"
	"github.com/adiom-data/weave/pkg/storage"
)

func TestEndpointConsumesHostClaims(t *testing.T) {
	for _, admitted := range []bool{false, true} {
		t.Run(map[bool]string{false: "no claims", true: "host claims"}[admitted], func(t *testing.T) {
			tx := &testTx{}
			db := &testDB{tx: tx}
			endpoint := Endpoint(db, "http://localhost:5173", slog.New(slog.NewTextHandler(io.Discard, nil)))
			_, handler := endpoint.NewHandler(endpoint.ConnectOptions...)
			for i := len(endpoint.Middleware) - 1; i >= 0; i-- {
				handler = endpoint.Middleware[i](handler)
			}
			ctx := context.Background()
			if admitted {
				claims, _ := (testClaims{}).Claims(ctx)
				ctx = accesscontrol.ContextWithClaims(ctx, claims)
			}
			request := httptest.NewRequest(http.MethodPost, "/demo.v1.TaskMutationService/CreateTask", strings.NewReader(`{"projectId":"`+projectID+`","title":"Task"}`)).WithContext(ctx)
			request.Header.Set("Content-Type", "application/json")
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			want := http.StatusUnauthorized
			if admitted {
				want = http.StatusOK
			}
			if response.Code != want {
				t.Fatalf("status %d: %s", response.Code, response.Body.String())
			}
			if admitted != tx.committed || (!admitted && db.calls != 0) {
				t.Fatal("mutation did not honor host admission")
			}
			if response.Header().Get("Access-Control-Allow-Origin") != "http://localhost:5173" {
				t.Fatal("missing mutation CORS")
			}
		})
	}
}

func TestMutationPreflightDoesNotRequireClaims(t *testing.T) {
	endpoint := Endpoint(nil, "http://localhost:5173", slog.Default())
	_, handler := endpoint.NewHandler(endpoint.ConnectOptions...)
	for i := len(endpoint.Middleware) - 1; i >= 0; i-- {
		handler = endpoint.Middleware[i](handler)
	}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodOptions, "/demo.v1.TaskMutationService/CreateTask", nil))
	if response.Code != http.StatusNoContent {
		t.Fatal(response.Code)
	}
	if !strings.Contains(response.Header().Get("Access-Control-Allow-Headers"), "Authorization") {
		t.Fatal("preflight excludes authorization")
	}
}

func TestMutationRejectsMalformedConfiguredIDsBeforeDatabase(t *testing.T) {
	for _, key := range []string{"organization_id", "user_id", "team_ids"} {
		tx := &testTx{}
		db := &testDB{tx: tx}
		claims, _ := (testClaims{}).Claims(context.Background())
		invalid := storage.Value{Type: storage.TypeText, State: storage.ValuePresent, Data: []byte("not-a-uuid")}
		if key == "team_ids" {
			claims[key] = storage.Value{Type: storage.TypeList, State: storage.ValuePresent, List: []storage.Value{invalid}}
		} else {
			claims[key] = invalid
		}
		service := New(db, accesscontrol.ClaimsFromContext, slog.Default())
		_, err := service.CreateTask(accesscontrol.ContextWithClaims(context.Background(), claims), createRequest(false))
		if err == nil || db.calls != 0 {
			t.Fatalf("invalid %s reached the database", key)
		}
	}
}
