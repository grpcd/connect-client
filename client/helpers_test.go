package client

import (
	"context"
	"sync"

	"connectrpc.com/connect/v2"
	"connectrpc.com/connect/v2/connectinprocess"
	"google.golang.org/grpc/health/grpc_health_v1"

	grpcd "github.com/grpcd/protos"
	"github.com/grpcd/protos/grpcdconnect"
)

// grpcdStub stands in for grpcd's Register handler. It records every request,
// acknowledges each unless err is set, and then holds the stream until the
// test releases it or the client's context ends. The embedded handler
// supplies Discover and Watch, which the registration path never calls.
type grpcdStub struct {
	grpcdconnect.UnimplementedGRPCDServiceHandler

	// err refuses every registration instead of acknowledging it.
	err error

	// release ends every held stream when closed; nil holds until ctx ends.
	release chan struct{}

	// onRegister runs on every Register with the call's ordinal, so a test
	// can end the client's loop once it has seen what it needs.
	onRegister func(n int)

	mu       sync.Mutex
	requests []*grpcd.RegisterRequest
}

func (s *grpcdStub) Register(
	ctx context.Context, request *grpcd.RegisterRequest, stream grpcdconnect.GRPCDServiceRegisterServerStream,
) error {
	s.mu.Lock()
	s.requests = append(s.requests, request)
	n := len(s.requests)
	s.mu.Unlock()

	if s.onRegister != nil {
		s.onRegister(n)
	}

	if s.err != nil {
		return s.err
	}

	if err := stream.Send(&grpcd.RegisterResponse{}); err != nil {
		return err
	}

	select {
	case <-s.release:
	case <-ctx.Done():
	}

	return nil
}

// registrations answers with how many Register calls the stub has seen.
func (s *grpcdStub) registrations() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	return len(s.requests)
}

// request answers with the nth request the stub saw.
func (s *grpcdStub) request(n int) *grpcd.RegisterRequest {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.requests[n]
}

// newService serves stub in-process and answers with a client on it, so every
// registration in these tests goes through the generated handler and nothing
// listens.
func newService(stub *grpcdStub) grpcdconnect.GRPCDServiceClient {
	return newConnection(connect.NewClient(connectinprocess.New(newServer(stub))), grpcdAddress)
}

// newServer registers stub on a dispatcher, which a test adds other methods
// to before building the connection.
func newServer(stub *grpcdStub) *connect.Server {
	rpc := connect.NewServer()
	grpcdconnect.RegisterGRPCDServiceHandler(rpc, stub)

	return rpc
}

// healthMethod answers grpcd's health procedure with status, the way grpc-go's
// health server does on grpcd.
func healthMethod(status grpc_health_v1.HealthCheckResponse_ServingStatus) connect.Method {
	return connect.Method{
		Spec: healthSpec,
		Handler: func(_ context.Context, _ connect.Spec, stream connect.ServerStream) error {
			var request grpc_health_v1.HealthCheckRequest
			if err := stream.Receive(&request); err != nil {
				return err
			}

			return stream.Send(&grpc_health_v1.HealthCheckResponse{Status: status})
		},
	}
}

// unopenableTransport stands in for a transport that cannot reach grpcd at
// all: every stream fails to open. onOpen runs on each attempt with its
// ordinal, so a test can end the client's loop.
type unopenableTransport struct {
	err    error
	onOpen func(n int)

	mu       sync.Mutex
	attempts int
}

func (u *unopenableTransport) NewClientStream(context.Context, connect.Spec) (connect.ClientStream, error) {
	u.mu.Lock()
	u.attempts++
	n := u.attempts
	u.mu.Unlock()

	if u.onOpen != nil {
		u.onOpen(n)
	}

	return nil, u.err
}

func (u *unopenableTransport) opened() int {
	u.mu.Lock()
	defer u.mu.Unlock()

	return u.attempts
}
