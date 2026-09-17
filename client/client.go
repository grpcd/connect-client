//revive:disable:package-comments
package client

import (
	"log/slog"
	"net"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"

	"git.sonicoriginal.software/logger"

	"github.com/grpcd/protos/grpcdconnect"
)

const (
	component = "grpcd-client"
	// CheckName is the diagnostics name a service reports its grpcd
	// dependency under, so every service reports it under the same one.
	CheckName = "grpcd"
)

// Client holds this server's registration with grpcd.
type Client struct {
	tracer     trace.Tracer
	log        *slog.Logger
	service    grpcdconnect.GRPCDServiceClient
	serverName string
	methods    []string
	addr       net.Addr
}

// New returns a new grpcd client.
//
// The server name is what grpcd reports the registration under, and is the same
// name the server identifies itself by everywhere else.
//
// addr is the server's own listener address. grpcd takes the IP from the
// connection and cannot see the port the server is listening on, so the port
// half comes from here — read from the listener rather than from configuration,
// so a bind to :0 reports what it actually received.
//
// conn is the connection Connect answered with, or in a test any generated
// grpcd client.
func New(
	log *slog.Logger,
	serverName string,
	addr net.Addr,
	methods []string,
	conn grpcdconnect.GRPCDServiceClient,
) *Client {
	if log == nil {
		log = logger.NewNullLogger()
	}

	log = log.With(slog.String("component", component))

	return &Client{
		log:        log,
		tracer:     otel.Tracer(component),
		service:    conn,
		serverName: serverName,
		methods:    methods,
		addr:       addr,
	}
}
