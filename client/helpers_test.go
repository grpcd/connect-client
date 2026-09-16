package client

import (
	"context"
	"sync"

	"connectrpc.com/connect/v2"
	"connectrpc.com/connect/v2/connectinprocess"

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
// listens. Nothing on the registration path probes, so the connection carries
// no HTTP client to send a probe over.
func newService(stub *grpcdStub) grpcdconnect.GRPCDServiceClient {
	return newConnection(connect.NewClient(connectinprocess.New(newServer(stub))), nil, grpcdAddress)
}

// newServer registers stub on a dispatcher.
func newServer(stub *grpcdStub) *connect.Server {
	rpc := connect.NewServer()
	grpcdconnect.RegisterGRPCDServiceHandler(rpc, stub)

	return rpc
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
