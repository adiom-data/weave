package management

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"connectrpc.com/connect"
	"github.com/adiom-data/weave/internal/storage"
	managementv1 "github.com/adiom-data/weave/pkg/gen/management/v1"
	"github.com/adiom-data/weave/pkg/gen/management/v1/managementv1connect"
)

type controls struct {
	invalidated   *storage.RelationName
	all           bool
	invalidations int
	err           error
}

func (c *controls) InvalidateMetadata(_ context.Context, name *storage.RelationName) error {
	c.invalidated, c.all = name, name == nil
	c.invalidations++
	return c.err
}

type handlerClient struct{ handler http.Handler }

func (c handlerClient) Do(req *http.Request) (*http.Response, error) {
	w := httptest.NewRecorder()
	c.handler.ServeHTTP(w, req)
	return w.Result(), nil
}

func TestManagementServiceProtocolAndOptionalTable(t *testing.T) {
	control := &controls{}
	s, err := NewServer(control)
	if err != nil {
		t.Fatal(err)
	}
	_, handler := s.Handler()
	client := managementv1connect.NewManagementClient(handlerClient{handler}, "http://management")
	ctx := context.Background()
	for _, json := range []bool{false, true} {
		var options []connect.ClientOption
		if json {
			options = append(options, connect.WithProtoJSON())
		}
		client := managementv1connect.NewManagementClient(handlerClient{handler}, "http://management", options...)
		before := control.invalidations
		for _, table := range []*managementv1.Table{{}, {Name: "tasks"}, {Namespace: "app"}} {
			if _, err := client.InvalidateMetadata(ctx, connect.NewRequest(&managementv1.InvalidateMetadataRequest{Table: table})); connect.CodeOf(err) != connect.CodeInvalidArgument {
				t.Fatal(err)
			}
		}
		if control.invalidations != before {
			t.Fatal("malformed table was treated as all")
		}
		name := &managementv1.Table{Namespace: "app", Name: "tasks"}
		if _, err := client.InvalidateMetadata(ctx, connect.NewRequest(&managementv1.InvalidateMetadataRequest{Table: name})); err != nil || control.invalidated == nil || control.invalidated.Name != "tasks" || control.all {
			t.Fatal(control, err)
		}
		if _, err := client.InvalidateMetadata(ctx, connect.NewRequest(&managementv1.InvalidateMetadataRequest{})); err != nil || !control.all || control.invalidations != before+2 {
			t.Fatal(control, err)
		}
	}
	control.err = errors.New("secret database URL")
	if _, err := client.InvalidateMetadata(ctx, connect.NewRequest(&managementv1.InvalidateMetadataRequest{})); connect.CodeOf(err) != connect.CodeUnavailable || strings.Contains(err.Error(), "secret") {
		t.Fatal(err)
	}
}
