package client

import (
	"os"

	"connectrpc.com/connect/v2"
	"connectrpc.com/connect/v2/connecthttp"

	foundationclient "github.com/pbrpc/connect-foundation/client"

	"github.com/grpcd/protos/grpcdconnect"
)

// Connection is the one connection every call to grpcd goes over: the
// registration, every discovery and watch, and the health check. It is the
// generated grpcd client, so it goes wherever one is taken, and it knows the
// address it was built for, which is what diagnostics report.
type Connection struct {
	grpcdconnect.GRPCDServiceClient

	client  *connect.Client
	address string
}

// Address answers with the grpcd address the connection was built for.
func (c *Connection) Address() string {
	return c.address
}

// Connect reads GRPCD_ADDRESS and answers with the connection to it, or nil
// when the variable is unset: nothing to register with and nothing to
// discover through, and the caller decides whether to serve anyway. This is
// the one place the variable is read.
//
// The connection is the foundation's standard HTTP client under the ready
// transport, so a grpcd that cannot be reached is retried on its schedule
// rather than on every call, speaking the gRPC protocol, which is the one
// grpcd serves. Building it dials nothing.
func Connect() *Connection {
	address := os.Getenv(GRPCDAddressKey)
	if address == "" {
		return nil
	}

	tp := foundationclient.NewReadyTransport(nil, nil, nil)
	httpClient := foundationclient.NewHTTPClient(tp)

	client := foundationclient.New(
		httpClient,
		foundationclient.BaseURL(address),
		nil,
		connecthttp.WithGRPC(),
	)

	return newConnection(client, address)
}

// newConnection wraps client as the connection to grpcd at address.
func newConnection(client *connect.Client, address string) *Connection {
	return &Connection{
		GRPCDServiceClient: grpcdconnect.NewGRPCDServiceClient(client),
		client:             client,
		address:            address,
	}
}
