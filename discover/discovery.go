//revive:disable:package-comments
package discover

import (
	"context"
	"log/slog"
	"net/http"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"

	"git.sonicoriginal.software/logger"

	foundationclient "github.com/pbrpc/connect-foundation/client"

	"github.com/grpcd/protos/grpcdconnect"
)

const component = "grpcd-discover"

// Discovery is what every upstream in a process shares: the grpcd client the
// lookups go through, the probe that decides whether a candidate is reachable,
// and the context the lookups run under.
type Discovery struct {
	ctx     context.Context
	log     *slog.Logger
	tracer  trace.Tracer
	service grpcdconnect.GRPCDServiceClient
	probe   Probe
}

// New returns a Discovery.
//
// ctx is the process context. The watch an upstream holds on its address runs
// under a child of this one and stops with it.
//
// service is the grpcd client the caller built, the same one its registration
// uses. Taking it rather than an address is what lets a test supply a fake.
//
// probe is what a candidate has to pass before it is used; nil means the one
// NewProbe builds on the foundation's standard HTTP client.
func New(
	ctx context.Context,
	log *slog.Logger,
	service grpcdconnect.GRPCDServiceClient,
	probe Probe,
) *Discovery {
	if log == nil {
		log = logger.NewNullLogger()
	}

	if probe == nil {
		probe = NewProbe(nil)
	}

	return &Discovery{
		ctx:     ctx,
		log:     log.With(slog.String("component", component)),
		tracer:  otel.Tracer(component),
		service: service,
		probe:   probe,
	}
}

// Upstream describes one dependency, named by one of its methods. A replica
// registers every method of its service, so the client built from this serves
// the whole service.
//
// base is the transport requests to the replica go over once the host is
// chosen; nil means the foundation's standard transport.
func (d *Discovery) Upstream(method string, base http.RoundTripper) *Upstream {
	if base == nil {
		base = foundationclient.NewTransport()
	}

	return &Upstream{
		discovery: d,
		method:    method,
		base:      base,
		log:       d.log.With(slog.String("method", method)),
	}
}
