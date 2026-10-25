package projectionserver

import (
	"context"
	"errors"
	"net/http/httptest"
	"testing"
	"time"

	"connectrpc.com/connect"
	q "github.com/adiom-data/weave/internal/querysubscription/model"
	reactivev1 "github.com/adiom-data/weave/pkg/gen/reactive/v1"
	"github.com/adiom-data/weave/pkg/gen/reactive/v1/reactivev1connect"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestConnectHandlerSupportsConnectAndGRPC(t *testing.T) {
	backend := newPolicyBackend(func(context.Context, q.QuerySubscribeRequest) q.QuerySnapshot {
		row := policyRow("projects", "one", "project")
		row.Record["extra"] = row.Record["title"]
		return policySnapshot(10, row)
	})
	service, err := New(t.Context(), backend, DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer service.Close()
	_, handler := service.Handler()
	host := httptest.NewUnstartedServer(handler)
	host.EnableHTTP2 = true
	host.StartTLS()
	defer host.Close()
	for _, test := range []struct {
		name      string
		grpc, all bool
	}{
		{"Connect", false, false}, {"ConnectAll", false, true},
		{"gRPC", true, false}, {"gRPCAll", true, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
			defer cancel()
			var options []connect.ClientOption
			if test.grpc {
				options = append(options, connect.WithGRPC())
			}
			client := reactivev1connect.NewReactiveQueriesClient(host.Client(), host.URL, options...)
			request := snapshotTestRequest(false, false)
			if test.all {
				request.Root.AllFields = true
				request.Root.GetList().Query.Columns = nil
			}
			image, err := client.GetProjectionSnapshot(ctx, connect.NewRequest(request))
			if err != nil {
				t.Fatal(err)
			}
			if len(image.Msg.Data.Fields["projects"].GetListValue().Values) != 1 {
				t.Fatal("lost snapshot data")
			}
			fields := image.Msg.Data.Fields["projects"].GetListValue().Values[0].GetStructValue().Fields
			if (fields["extra"] != nil) != test.all {
				t.Fatal("snapshot lost field selection mode")
			}
			stream := client.Open(ctx)
			defer stream.CloseResponse()
			if err := stream.Send(&reactivev1.ClientMessage{SubscriptionId: "projection", RequestId: "subscribe", Operation: &reactivev1.ClientMessage_SubscribeProjection{SubscribeProjection: &reactivev1.SubscribeProjection{Root: request.Root}}}); err != nil {
				t.Fatal(err)
			}
			imageSeen := false
			for {
				event, err := stream.Receive()
				if err != nil {
					t.Fatal(err)
				}
				if event.GetError() != nil {
					t.Fatal(event.GetError())
				}
				if batch := event.GetModelBatch(); batch != nil && batch.Kind == reactivev1.ModelBatch_SNAPSHOT {
					imageSeen = true
					if len(batch.Entities) != 1 || (batch.Entities[0].GetUpsert().Values["extra"] != nil) != test.all {
						t.Fatal("live snapshot lost field selection mode")
					}
				}
				if event.GetReady() != nil {
					break
				}
			}
			if !imageSeen {
				t.Fatal("Ready preceded snapshot")
			}
			cancel()
		})
	}
}
func TestConnectErrorCodes(t *testing.T) {
	for _, err := range []error{context.Canceled, context.DeadlineExceeded, status.Error(codes.InvalidArgument, "invalid query"), connect.NewError(connect.CodeUnauthenticated, errors.New("unauthenticated"))} {
		converted := connectError(err)
		if converted == nil || connect.CodeOf(converted) == connect.CodeUnknown {
			t.Fatalf("lost error code: %v", err)
		}
	}
	if connectError(nil) != nil {
		t.Fatal("nil became an error")
	}
}
