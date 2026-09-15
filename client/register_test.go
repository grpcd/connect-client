package client

import (
	"context"
	"errors"
	"log/slog"
	"testing"

	"connectrpc.com/connect/v2"

	"git.sonicoriginal.software/grpc-testing/mocks/addr"

	"github.com/grpcd/protos/grpcdconnect"
)

const (
	serverName   = "example"
	grpcdAddress = "grpcd.example:50051"
	listenAddr   = "10.0.0.1:50051"
	method       = "/pkg.S/M"
)

// newClient builds a client on stub for the example service.
func newClient(stub *grpcdStub, methods []string) *Client {
	return New(slog.New(slog.DiscardHandler), serverName, addr.New(listenAddr), methods, newService(stub), grpcdAddress)
}

func TestNew(t *testing.T) {
	t.Run("substitutes a logger when given none", func(t *testing.T) {
		client := New(nil, serverName, addr.New(listenAddr), []string{method}, nil, grpcdAddress)

		if client.log == nil {
			t.Fatal("expected a logger")
		}
	})
}

func TestRegister(t *testing.T) {
	t.Run("holds the stream and sends the listening port", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		// Cancelling from inside the handler ends the held stream and the loop
		// with it, once the registration has been seen.
		stub := &grpcdStub{onRegister: func(int) { cancel() }}

		newClient(stub, []string{method}).Register(ctx)

		if stub.registrations() != 1 {
			t.Fatalf("expected one registration, got %d", stub.registrations())
		}

		request := stub.request(0)

		if request.Port != 50051 {
			t.Errorf("expected port 50051, got %d", request.Port)
		}
		if request.ServerName != serverName {
			t.Errorf("expected server name %q, got %q", serverName, request.ServerName)
		}
		if len(request.Methods) != 1 || request.Methods[0] != method {
			t.Errorf("expected methods [%s], got %v", method, request.Methods)
		}
	})

	t.Run("reopens the stream after it ends", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		// Every stream is released as soon as it is held, so the first ends
		// and is reopened, which is what the loop exists for. The second call
		// ends the loop.
		stub := &grpcdStub{release: make(chan struct{})}
		close(stub.release)
		stub.onRegister = func(n int) {
			if n >= 2 {
				cancel()
			}
		}

		newClient(stub, []string{method}).Register(ctx)

		if stub.registrations() < 2 {
			t.Fatalf("expected the stream to be reopened, got %d attempts", stub.registrations())
		}
	})

	t.Run("registers nothing without methods", func(t *testing.T) {
		stub := &grpcdStub{}

		newClient(stub, nil).Register(t.Context())

		if stub.registrations() != 0 {
			t.Fatalf("expected no registration, got %d", stub.registrations())
		}
	})

	t.Run("registers nothing when the address has no port", func(t *testing.T) {
		stub := &grpcdStub{}

		New(slog.New(slog.DiscardHandler), serverName, addr.New("10.0.0.1"), []string{method}, newService(stub), grpcdAddress).
			Register(t.Context())

		if stub.registrations() != 0 {
			t.Fatalf("expected no registration, got %d", stub.registrations())
		}
	})

	t.Run("registers nothing when the port is not a number", func(t *testing.T) {
		stub := &grpcdStub{}

		New(slog.New(slog.DiscardHandler), serverName, addr.New("10.0.0.1:http"), []string{method}, newService(stub), grpcdAddress).
			Register(t.Context())

		if stub.registrations() != 0 {
			t.Fatalf("expected no registration, got %d", stub.registrations())
		}
	})

	t.Run("gives up the attempt when the registration is refused", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		stub := &grpcdStub{err: errors.New("invalid method name"), onRegister: func(int) { cancel() }}

		newClient(stub, []string{method}).Register(ctx)

		if stub.registrations() != 1 {
			t.Fatalf("expected one attempt, got %d", stub.registrations())
		}
	})

	t.Run("gives up the attempt when the stream cannot be opened", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		transport := &unopenableTransport{err: errors.New("unavailable"), onOpen: func(int) { cancel() }}
		service := grpcdconnect.NewGRPCDServiceClient(connect.NewClient(transport))

		New(slog.New(slog.DiscardHandler), serverName, addr.New(listenAddr), []string{method}, service, grpcdAddress).
			Register(ctx)

		if transport.opened() != 1 {
			t.Fatalf("expected one attempt, got %d", transport.opened())
		}
	})
}
