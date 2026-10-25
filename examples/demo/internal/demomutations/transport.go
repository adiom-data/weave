package demomutations

import (
	"log/slog"
	"net/http"

	"connectrpc.com/connect"
	"github.com/adiom-data/framework/httpapp"
	"github.com/adiom-data/weave/examples/demo/gen/demo/v1/demov1connect"
	"github.com/adiom-data/weave/pkg/accesscontrol"
)

// Endpoint is added to server.Options.AuthenticatedServices. Admission and
// trusted claims come from the config-driven host; write authorization stays here.
func Endpoint(db Database, origin string, logger *slog.Logger) httpapp.ConnectService {
	service := New(db, accesscontrol.ClaimsFromContext, logger)
	return httpapp.Connect(demov1connect.TaskMutationServiceName,
		func(options ...connect.HandlerOption) (string, http.Handler) {
			return demov1connect.NewTaskMutationServiceHandler(service, options...)
		},
		httpapp.WithConnectOptions(connect.WithReadMaxBytes(16<<10)),
		httpapp.WithConnectMiddleware(cors(origin)),
	)
}

func cors(origin string) httpapp.Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Access-Control-Allow-Origin", origin)
			w.Header().Add("Vary", "Origin")
			w.Header().Set("Access-Control-Allow-Headers", "Authorization, Content-Type, Connect-Protocol-Version, Connect-Timeout-Ms, X-Weave-Connection-Context, X-Weave-Mutation-Id")
			w.Header().Set("Access-Control-Allow-Methods", "POST, OPTIONS")
			if r.Method == http.MethodOptions {
				w.WriteHeader(http.StatusNoContent)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}
