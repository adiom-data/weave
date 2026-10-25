package projectionserver_test

import (
	"context"
	"fmt"
	"github.com/adiom-data/weave/pkg/accesscontrol"
	"github.com/adiom-data/weave/pkg/accesscontrol/policy"
	"github.com/adiom-data/weave/pkg/projectionserver"
	querygrpc "github.com/adiom-data/weave/pkg/querysubscription/grpc"
	"github.com/adiom-data/weave/pkg/storage"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// This example deliberately imports no internal packages: an application can
// embed the projection server over a remote query backend using only public APIs.
func ExampleServer_Register() {
	connection, err := grpc.NewClient("localhost:9091", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		panic(err)
	}
	defer connection.Close()
	queries := querygrpc.NewClient(connection)
	server, err := projectionserver.New(context.Background(), queries, projectionserver.Options{
		ProjectionLimits: projectionserver.ProjectionLimits{MaxDefinitionDepth: 8},
		AccessController: accesscontrol.NewAllowAll(),
		ClaimsProvider: func(context.Context) (accesscontrol.Claims, error) {
			return accesscontrol.Claims{}, nil
		},
	})
	if err != nil {
		panic(err)
	}
	defer server.Close()
	grpcServer := grpc.NewServer()
	defer grpcServer.Stop()
	server.Register(grpcServer)
	fmt.Println(len(grpcServer.GetServiceInfo()))
	// Output: 1
}

// Providers may load definitions from application storage. Return a new version
// whenever content changes, without mutating previously returned definitions.
func ExamplePolicyOptions() {
	projects := storage.RelationName{Namespace: "app", Name: "projects"}
	definition := &policy.Relation{Allow: []policy.Rule{{
		ID: "organization", When: `claims.organization_id != null && item.organization_id == claims.organization_id`,
	}}}
	provider := policy.ProviderFunc(func(_ context.Context, key policy.Key) (policy.Snapshot, error) {
		if key == projects {
			return policy.Snapshot{Version: "projects-v1", Policy: definition}, nil
		}
		return policy.Snapshot{Version: "missing-v1"}, nil // deny by default
	})
	options := projectionserver.DefaultPolicyOptions()
	options.Provider = provider
	options.MaxFactRows = 100
	fmt.Println(options.MaxFactRows)
	// Output: 100
}
