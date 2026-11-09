package projectionserver_test

import (
	"context"
	"fmt"
	"github.com/adiom-data/weave/pkg/accesscontrol"
	"github.com/adiom-data/weave/pkg/projectionserver"
	querygrpc "github.com/adiom-data/weave/pkg/querysubscription/grpc"
	relationdef "github.com/adiom-data/weave/pkg/relation"
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

// A Store owns one complete definition set and can atomically replace it while
// existing evaluations retain their old immutable snapshot.
func ExampleRelationOptions() {
	projects := storage.RelationName{Namespace: "app", Name: "projects"}
	definition := relationdef.Relation{Visibility: `claims.organization_id != null && item.organization_id == claims.organization_id`}
	store, err := relationdef.NewStore(relationdef.Definitions{projects: definition}, relationdef.DefaultLimits())
	if err != nil {
		panic(err)
	}
	options := projectionserver.DefaultRelationOptions()
	options.Store = store
	options.MaxFactRows = 100
	// A complete replacement validates before publication and automatically
	// invalidates dependents. Omitted relations deny by default.
	if err := store.Replace(relationdef.Definitions{projects: definition}); err != nil {
		panic(err)
	}
	fmt.Println(options.MaxFactRows)
	// Output: 100
}
