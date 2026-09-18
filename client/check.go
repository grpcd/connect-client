//revive:disable:package-comments
package client

import (
	"context"
	"time"

	diagpb "github.com/pbrpc/connect-protos/diagnostics"
	"github.com/pbrpc/connect-service/diagnostics"
	"github.com/pbrpc/connect-service/health"

	"github.com/grpcd/protos/grpcdconnect"
)

// Check answers with the diagnostics check for grpcd: its health probe for
// the grpcd service, GET /healthz?service=grpcd.GRPCDService at the address
// the connection was built for, sent over the connection's own HTTP client,
// the one the registration and every discovery go over. Reachable is the
// probe being answered; serving is what grpcd says of the service, which is
// not serving while grpcd has lost its store though the process is up. It
// goes in a service's checks under CheckName.
func Check(conn *Connection) diagnostics.Check {
	return func(ctx context.Context) (*diagpb.ServiceDependency, error) {
		status, err := health.CheckService(ctx, conn.httpClient, conn.address, grpcdconnect.GRPCDServiceName)

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
