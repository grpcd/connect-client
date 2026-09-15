//revive:disable:package-comments
package discover

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"

	"git.sonicoriginal.software/logger"

	foundationclient "github.com/pbrpc/connect-foundation/client"
	diagpb "github.com/pbrpc/connect-protos/diagnostics"
	"github.com/pbrpc/connect-service/diagnostics"
	"github.com/pbrpc/connect-service/health"

	"github.com/grpcd/protos/grpcdconnect"
)

const component = "grpcd-discover"

// Scheme is the URL scheme a dependency reached through grpcd is addressed
// by. A request whose URL carries it is routed by its path: the procedure,
// looked up through grpcd and sent to the replica held for it.
const Scheme = "grpcd"

// BaseURL is what every Connect client to a discovered dependency is built
// against: the scheme with no authority, so the request path is the whole
// address. grpcd:///package.Service/Method names the method as it was
// registered, and nothing in it is ever dialed as written.
const BaseURL = Scheme + ":///"

// ErrNoProcedure reports a grpcd URL whose path does not name a procedure, so
// there is nothing to look up.
var ErrNoProcedure = errors.New("path does not name a procedure")

// ErrNothingHeld is what Check answers with while no procedure holds a
// replica: grpcd has not been seen to answer.
var ErrNothingHeld = errors.New("no replica held")

// Discovery is the transport a process reaches its dependencies through. A
// request to a grpcd URL passes to the upstream for the procedure its path
// names, one held per procedure for the life of the process; a request to any
// other URL goes over the base transport as it is. It is built once and put
// under the HTTP client every dependency's Connect client is built on.
type Discovery struct {
	ctx     context.Context
	log     *slog.Logger
	tracer  trace.Tracer
	service grpcdconnect.GRPCDServiceClient
	probe   Probe

	// base carries every request once its host is known: a discovered one
	// after the replica is chosen, and any other as it arrived.
	base http.RoundTripper

	mu        sync.Mutex
	upstreams map[string]*Upstream
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
//
// base is the transport requests go over once their host is known; nil means
// the foundation's standard transport.
func New(
	ctx context.Context,
	log *slog.Logger,
	service grpcdconnect.GRPCDServiceClient,
	probe Probe,
	base http.RoundTripper,
) *Discovery {
	if log == nil {
		log = logger.NewNullLogger()
	}

	if probe == nil {
		probe = NewProbe(nil)
	}

	if base == nil {
		base = foundationclient.NewTransport()
	}

	return &Discovery{
		ctx:       ctx,
		log:       log.With(slog.String("component", component)),
		tracer:    otel.Tracer(component),
		service:   service,
		probe:     probe,
		base:      base,
		upstreams: map[string]*Upstream{},
	}
}

// RoundTrip sends req to the replica held for the procedure its path names
// when its URL carries the grpcd scheme, and over the base transport
// otherwise.
func (d *Discovery) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.URL.Scheme != Scheme {
		return d.base.RoundTrip(req)
	}

	procedure := req.URL.Path
	if !isProcedure(procedure) {
		return nil, ErrNoProcedure
	}

	return d.Upstream(procedure).RoundTrip(req)
}

// Upstream answers with the upstream for method, building it on first sight.
// Building dials nothing: discovery runs on the upstream's first request.
// Diagnostics take this, for the address the method is on.
func (d *Discovery) Upstream(method string) *Upstream {
	d.mu.Lock()
	defer d.mu.Unlock()

	upstream, ok := d.upstreams[method]
	if !ok {
		upstream = &Upstream{
			discovery: d,
			method:    method,
			log:       d.log.With(slog.String("method", method)),
		}
		d.upstreams[method] = upstream
	}

	return upstream
}

// Addresses reports the replica each procedure seen so far is on, "" for one
// not held right now.
func (d *Discovery) Addresses() map[string]string {
	d.mu.Lock()
	defer d.mu.Unlock()

	addresses := make(map[string]string, len(d.upstreams))
	for method, upstream := range d.upstreams {
		addresses[method] = upstream.Address()
	}

	return addresses
}

// Check reports grpcd as a dependency, from what discovery holds: while any
// procedure holds a replica, grpcd answered a lookup and the watch on that
// replica is alive. Nothing is dialed to find out. grpcdAddress is what the
// report names. It is a diagnostics.Check for a process that discovers
// without registering; one that registers reports the registration instead.
func (d *Discovery) Check(grpcdAddress string) diagnostics.Check {
	return func(context.Context) (*diagpb.ServiceDependency, error) {
		addresses := d.Addresses()

		held := 0

		for _, address := range addresses {
			if address != "" {
				held++
			}
		}

		dependency := &diagpb.ServiceDependency{
			Address:     grpcdAddress,
			Serving:     string(health.StatusUnknown),
			State:       diagnostics.StateUnreachable,
			LastChecked: time.Now().Unix(),
			Details: map[string]string{
				"procedures": strconv.Itoa(len(addresses)),
				"held":       strconv.Itoa(held),
			},
		}

		err := ErrNothingHeld

		if held > 0 {
			dependency.Serving = string(health.StatusServing)
			dependency.State = diagnostics.StateReachable
			err = nil
		}

		return dependency, err
	}
}

// isProcedure reports whether path has the "/package.Service/Method" shape:
// a leading slash, two non-empty segments, and nothing more.
func isProcedure(path string) bool {
	service, method, found := strings.Cut(strings.TrimPrefix(path, "/"), "/")

	return strings.HasPrefix(path, "/") && found && service != "" && method != "" && !strings.Contains(method, "/")
}
