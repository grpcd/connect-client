package client

import (
	"connectrpc.com/connect/v2"
	"connectrpc.com/connect/v2/connecthttp"

	foundationclient "github.com/pbrpc/connect-foundation/client"
)

// Connect answers with the client every call to grpcd goes through: the
// foundation's standard HTTP client under the ready transport, so a grpcd
// that cannot be reached is retried on its schedule rather than on every
// call, speaking the gRPC protocol, which is the one grpcd serves.
//
// The generated service client is built on it:
//
//	service := grpcdconnect.NewGRPCDServiceClient(client.Connect(grpcdAddress))
func Connect(grpcdAddress string) *connect.Client {
	httpClient := foundationclient.NewHTTPClient(foundationclient.NewReadyTransport(nil, nil, nil))

	return foundationclient.New(httpClient, foundationclient.BaseURL(grpcdAddress), nil, connecthttp.WithGRPC())
}
