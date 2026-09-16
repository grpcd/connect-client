package client

import (
	"errors"
	"testing"

	"connectrpc.com/connect/v2"
	"connectrpc.com/connect/v2/connectinprocess"
	"google.golang.org/grpc/health/grpc_health_v1"

	"github.com/pbrpc/connect-service/diagnostics"
)

func TestCheck(t *testing.T) {
	t.Run("reports what grpcd says of itself", func(t *testing.T) {
		rpc := newServer(&grpcdStub{})
		rpc.Register(healthMethod(grpc_health_v1.HealthCheckResponse_SERVING))

		conn := newConnection(connect.NewClient(connectinprocess.New(rpc)), grpcdAddress)

		got, err := Check(conn)(t.Context())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got.GetAddress() != grpcdAddress {
			t.Errorf("address = %q, want %q", got.GetAddress(), grpcdAddress)
		}
		if got.GetState() != diagnostics.StateReachable {
			t.Errorf("state = %q, want reachable", got.GetState())
		}
		if got.GetServing() != grpc_health_v1.HealthCheckResponse_SERVING.String() {
			t.Errorf("serving = %q, want SERVING", got.GetServing())
		}
		if got.GetLastChecked() == 0 {
			t.Error("expected a last checked time")
		}
	})

	t.Run("reports grpcd unreachable when the call is not answered", func(t *testing.T) {
		transport := &unopenableTransport{err: errors.New("unavailable")}

		conn := newConnection(connect.NewClient(transport), grpcdAddress)

		got, err := Check(conn)(t.Context())
		if err == nil {
			t.Fatal("expected the failure to be reported")
		}
		if got.GetState() != diagnostics.StateUnreachable {
			t.Errorf("state = %q, want unreachable", got.GetState())
		}
		if got.GetServing() != grpc_health_v1.HealthCheckResponse_UNKNOWN.String() {
			t.Errorf("serving = %q, want UNKNOWN", got.GetServing())
		}
	})
}
