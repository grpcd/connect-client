package client_test

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"connectrpc.com/connect/v2"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"git.sonicoriginal.software/logger"

	foundationotel "github.com/pbrpc/connect-foundation/otel"
	foundation "github.com/pbrpc/connect-foundation/server"
	"github.com/pbrpc/connect-service/diagnostics"
	"github.com/pbrpc/connect-service/health"
	"github.com/pbrpc/connect-service/service"

	grpcdclient "github.com/grpcd/connect-client/client"
	"github.com/grpcd/connect-client/discover"
	"github.com/grpcd/protos/grpcdconnect"
)

const cleanupTimeout = 5 * time.Second

// registerExampleService stands in for the caller's own service registration,
// which in practice is the generated RegisterXServiceHandler.
func registerExampleService(rpc *connect.Server) {
	rpc.Register(connect.Method{
		Spec: connect.Spec{
			StreamType: connect.StreamTypeUnary,
			Procedure:  "/example.ExampleService/Create",
		},
		Handler: func(_ context.Context, _ connect.Spec, stream connect.ServerStream) error {
			var request wrapperspb.StringValue
			if err := stream.Receive(&request); err != nil {
				return err
			}

			return stream.Send(&request)
		},
	})
}

// Example shows how a service registers with grpcd and reaches an upstream
// through it. The listener and the server are the caller's, as are the
// goroutines. It has no Output comment, so it is compiled but never run — its
// job is to keep this sequence type-checked.
func Example() {
	// Registered first so it runs last, after the teardown below has flushed.
	// Returning rather than calling os.Exit directly is what lets the defers run
	// at all.
	exitCode := 1
	defer func() { os.Exit(exitCode) }()

	// The process context. The shutdown builds its deadline on this one, which
	// is why the signal cancels a child of it rather than this.
	ctx := context.Background()

	serveCtx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()

	// The same name the otel resource and the grpcd registration are keyed by.
	serverName := foundation.Name("example")

	log, flush, err := foundationotel.Init(ctx, serverName, foundation.Version())
	if err != nil {
		slog.Default().Error("Failed to initialize telemetry", slog.Any("error", err))
		return
	}

	ctx = logger.ContextWithLogger(ctx, log)

	// New installs an interceptor that puts this logger into every request
	// context, so it has to be built after the logger is complete.
	srv := foundation.New(log)

	// Deferred before anything else can fail, so every path out of here stops
	// the server and exports what it logged on the way.
	defer foundation.HandleGracefulShutdown(ctx, log, srv.HTTP, flush, cleanupTimeout)

	// Checks for the services this one depends on, keyed by the name
	// diagnostics reports them under.
	checks := diagnostics.Checks{}

	// With no grpcd address there is nothing to register with and nothing to
	// discover through, and the server serves anyway.
	var grpcdService grpcdconnect.GRPCDServiceClient

	grpcdAddress := os.Getenv(grpcdclient.GRPCDAddressKey)

	if grpcdAddress != "" {
		// Connect builds the one connection every call to grpcd goes over:
		// the registration, every discovery, every watch.
		grpcdService = grpcdconnect.NewGRPCDServiceClient(grpcdclient.Connect(grpcdAddress))

		// One Discovery per process, shared by every upstream. The example
		// service has none; the method below stands in for a generated
		// procedure constant of a real one.
		discovery := discover.New(serveCtx, log, grpcdService, discover.NewProbe(nil))

		upstream := discovery.Upstream("/example.UpstreamService/Get", nil)

		// The upstream is the transport under its own client. The generated
		// client built on it calls the same URL for the life of the process
		// while the replica behind it changes:
		//
		//	upstreamService := upstreamconnect.NewUpstreamServiceClient(
		//	    foundationclient.New(upstream.HTTPClient(), upstream.BaseURL(), nil),
		//	)
		checks["upstream"] = diagnostics.NewUpstreamCheck(upstream.HTTPClient(), upstream)
	}

	healthSrv := health.NewServer()

	// The returned method list is what this server exposes beyond the
	// infrastructure endpoints, which is what it advertises to grpcd.
	methodList, err := service.Register(srv.RPC, srv.Mux, healthSrv, checks, registerExampleService)
	if err != nil {
		log.Error("Failed to register services", slog.Any("error", err))
		return
	}

	lis, err := foundation.Listen()
	if err != nil {
		log.Error("Failed to create listener", slog.Any("error", err))
		return
	}
	addr := lis.Addr()

	log = log.With(slog.String("address", addr.String()))

	if grpcdService != nil {
		registration := grpcdclient.New(log, serverName, addr, methodList, grpcdService, grpcdAddress)

		// The registration stream is the service's own evidence grpcd is up,
		// so it is what diagnostics report for grpcd. Register holds the
		// stream open; its ending is what removes the rows, so there is no
		// deregistration to wait for here.
		checks[grpcdclient.CheckName] = registration.Check

		go registration.Register(serveCtx)
	}

	// Serve mounts what was registered and blocks. A deferred teardown cannot
	// run while it does, so it goes to a goroutine and the select below decides
	// when this returns.
	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(lis) }()

	log.Info("Connect server listening")

	select {
	case err := <-serveErr:
		if err != nil {
			log.Error("Failed to serve", slog.Any("error", err))
		}
	case <-serveCtx.Done():
		exitCode = 0
	}
}
