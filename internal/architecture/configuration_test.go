package architecture

import (
	"reflect"
	"testing"

	"github.com/adiom-data/weave/pkg/projectionserver"
	"github.com/adiom-data/weave/pkg/projectionserver/websocket"
	"github.com/adiom-data/weave/pkg/queryserver"
	querygrpc "github.com/adiom-data/weave/pkg/querysubscription/grpc"
	relationdef "github.com/adiom-data/weave/pkg/relation"
	"github.com/adiom-data/weave/pkg/server"
	"github.com/adiom-data/weave/pkg/storage"
	"github.com/adiom-data/weave/pkg/transactionmessage"
)

// Forwarded configuration must not accidentally acquire exported internal
// helpers such as WithDefaults/Normalized when implementations are refactored.
func TestForwardedConfigurationExposesDataNotImplementationMethods(t *testing.T) {
	configs := []any{
		storage.PostgresOptions{}, storage.QueryLimits{}, storage.MessageLimits{}, storage.ReplicationOptions{},
		queryserver.Options{}, queryserver.Limits{},
		server.Options{},
		projectionserver.Options{}, projectionserver.ProjectionLimits{}, projectionserver.MessageOptions{},
		projectionserver.DependencyOptions{}, projectionserver.RelationOptions{},
		websocket.SocketConfig{}, querygrpc.TransportOptions{},
		transactionmessage.Options{}, transactionmessage.MessageLimits{},
		relationdef.Limits{},
	}
	for _, config := range configs {
		value := reflect.TypeOf(config)
		for _, kind := range []reflect.Type{value, reflect.PointerTo(value)} {
			if kind.NumMethod() != 0 {
				t.Errorf("configuration %v exposes implementation methods", kind)
			}
		}
	}
}
