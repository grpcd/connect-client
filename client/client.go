//revive:disable:package-comments
package client

import (
	"log/slog"
	"net"
	"sync/atomic"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"

	"git.sonicoriginal.software/logger"

	"github.com/grpcd/protos/grpcdconnect"
)

const (
	component = "grpcd-client"
	// GRPCDAddressKey is the env variable name
	// of what address to use for the grpcd connection
	GRPCDAddressKey = "GRPCD_ADDRESS"
	// CheckName is the diagnostics name a service reports its grpcd
	// dependency under, so every service reports it under the same one.
	CheckName = "grpcd"
)

// Client holds this server's registration with grpcd.
type Client struct {
	tracer       trace.Tracer
	log          *slog.Logger
	service      grpcdconnect.GRPCDServiceClient
	grpcdAddress string
	serverName   string
	methods      []string
	addr         net.Addr

	// held is whether the registration stream is open: the registration
	// goroutine writes it and every diagnostics request reads it.
	held atomic.Bool

	// onHeld runs on every change of held, with the new value. Nil in
	// production; a test substitutes one to know when the stream is held.
	onHeld func(held bool)
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
// service is the grpcd client the caller built, on the connection Connect
// answers with. Taking it rather than an address is what lets a test supply a
// fake. grpcdAddress is what that connection was built for, which is what the
// diagnostics check reports.
func New(
	log *slog.Logger,
	serverName string,
	addr net.Addr,
	methods []string,
	service grpcdconnect.GRPCDServiceClient,
	grpcdAddress string,
) *Client {
	if log == nil {
		log = logger.NewNullLogger()
	}

	log = log.With(slog.String("component", component))

	return &Client{
		log:          log,
		tracer:       otel.Tracer(component),
		service:      service,
		grpcdAddress: grpcdAddress,
		serverName:   serverName,
		methods:      methods,
		addr:         addr,
	}
}

// setHeld records whether the registration stream is open.
func (c *Client) setHeld(held bool) {
	c.held.Store(held)

	if c.onHeld != nil {
		c.onHeld(held)
	}
}
