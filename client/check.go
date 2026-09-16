package client

import (
	"context"
	"time"

	"connectrpc.com/connect/v2"
	"google.golang.org/grpc/health/grpc_health_v1"

	diagpb "github.com/pbrpc/connect-protos/diagnostics"
	"github.com/pbrpc/connect-service/diagnostics"
)

// healthSpec names grpcd's health procedure, the one every gRPC server
// answers, for a call built without a generated client.
var healthSpec = connect.Spec{
	StreamType: connect.StreamTypeUnary,
	Procedure:  grpc_health_v1.Health_Check_FullMethodName,
}

// Check answers with the diagnostics check for grpcd: its health procedure,
// asked over conn, the same connection the registration and every discovery
// go over. Reachable is the call being answered; serving is what grpcd says
// of itself. It goes in a service's checks under CheckName.
func Check(conn *Connection) diagnostics.Check {
	return func(ctx context.Context) (*diagpb.ServiceDependency, error) {
		dependency := &diagpb.ServiceDependency{
			Address:     conn.address,
			Serving:     grpc_health_v1.HealthCheckResponse_UNKNOWN.String(),
			State:       diagnostics.StateUnreachable,
			LastChecked: time.Now().Unix(),
			Details:     map[string]string{},
		}

		var response grpc_health_v1.HealthCheckResponse

		if err := conn.client.CallUnary(ctx, healthSpec, &grpc_health_v1.HealthCheckRequest{}, &response); err != nil {
			return dependency, err
		}

		dependency.State = diagnostics.StateReachable
		dependency.Serving = response.GetStatus().String()

		return dependency, nil
	}
}
