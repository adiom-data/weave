package queryserver_test

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/adiom-data/weave/pkg/projectionserver"
	"github.com/adiom-data/weave/pkg/queryserver"
	querygrpc "github.com/adiom-data/weave/pkg/querysubscription/grpc"
	"github.com/adiom-data/weave/pkg/storage"
	messagegrpc "github.com/adiom-data/weave/pkg/transactionmessage/grpc"
)

func ExampleNew_embedded() {
	ctx := context.Background()
	store, err := storage.NewPostgres(ctx, "postgres://localhost/app", storage.PostgresOptions{
		Publication: "app_publication", Slot: "app_reactive",
	})
	if err != nil {
		panic(err)
	}
	defer store.Close()
	backend, err := queryserver.New(ctx, store, queryserver.Options{})
	if err != nil {
		panic(err)
	}
	defer backend.Close()
	projections, err := projectionserver.New(ctx, backend, projectionserver.Options{TransactionMessages: backend.Messages()})
	if err != nil {
		panic(err)
	}
	defer projections.Close()
	host := grpc.NewServer() // The application configures authentication/TLS.
	projections.Register(host)
	// Optional: also serve query/message clients on this same listener.
	backend.Register(host)
	// The host application supplies a listener and calls host.Serve(listener).
}

func ExampleNew_remote() {
	ctx := context.Background()
	store, err := storage.NewPostgres(ctx, "postgres://localhost/app", storage.PostgresOptions{
		Publication: "app_publication", Slot: "app_reactive",
	})
	if err != nil {
		panic(err)
	}
	defer store.Close()
	backend, err := queryserver.New(ctx, store, queryserver.Options{})
	if err != nil {
		panic(err)
	}
	defer backend.Close()
	queryHost := grpc.NewServer() // Secure this trusted service-to-service endpoint.
	backend.Register(queryHost)
	// On the projection host, connect to the query host's application-owned listener.
	// Insecure transport is suitable only for a trusted local example; use TLS and
	// service credentials in a deployed application.
	connection, err := grpc.NewClient("localhost:9091", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		panic(err)
	}
	defer connection.Close()
	projections, err := projectionserver.New(ctx, querygrpc.NewClient(connection), projectionserver.Options{
		TransactionMessages: messagegrpc.NewClient(connection),
	})
	if err != nil {
		panic(err)
	}
	defer projections.Close()
	projectionHost := grpc.NewServer()
	projections.Register(projectionHost)
}
