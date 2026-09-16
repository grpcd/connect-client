package client

import (
	"context"
	"time"

	diagpb "github.com/pbrpc/connect-protos/diagnostics"
	"github.com/pbrpc/connect-service/diagnostics"
	"github.com/pbrpc/connect-service/health"
)

// Check answers with the diagnostics check for grpcd: its health probe,
// GET /healthz at the address the connection was built for, sent over the
// connection's own HTTP client, the one the registration and every discovery
// go over. Reachable is the probe being answered; serving is what grpcd says
// of itself. It goes in a service's checks under CheckName.
func Check(conn *Connection) diagnostics.Check {
	return func(ctx context.Context) (*diagpb.ServiceDependency, error) {
		status, err := health.Check(ctx, conn.httpClient, conn.address)

		state := diagnostics.StateReachable
		if err != nil {
			state = diagnostics.StateUnreachable
		}

		return &diagpb.ServiceDependency{
			Address:     conn.address,
			Serving:     string(status),
			State:       state,
			LastChecked: time.Now().Unix(),
			Details:     map[string]string{},
		}, err
	}
}
