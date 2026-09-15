package client

import (
	"context"
	"errors"
	"time"

	diagpb "github.com/pbrpc/connect-protos/diagnostics"
	"github.com/pbrpc/connect-service/diagnostics"
	"github.com/pbrpc/connect-service/health"
)

// ErrNotRegistered is what Check answers with while the registration stream
// is not held.
var ErrNotRegistered = errors.New("registration stream is not held")

// Check reports grpcd as a dependency, from the registration stream: while it
// is held, grpcd accepted this server's registration and the connection is
// alive, which is more than a health call could say. It is a diagnostics.Check
// and goes in a service's checks under CheckName.
func (c *Client) Check(context.Context) (*diagpb.ServiceDependency, error) {
	dependency := &diagpb.ServiceDependency{
		Address:     c.grpcdAddress,
		Serving:     string(health.StatusUnknown),
		State:       diagnostics.StateUnreachable,
		LastChecked: time.Now().Unix(),
		Details:     map[string]string{},
	}

	err := ErrNotRegistered

	if c.held.Load() {
		dependency.Serving = string(health.StatusServing)
		dependency.State = diagnostics.StateReachable
		err = nil
	}

	return dependency, err
}
