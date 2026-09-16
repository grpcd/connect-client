//revive:disable:package-comments
package discover

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"sync"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"

	"git.sonicoriginal.software/logger"

	foundationclient "github.com/pbrpc/connect-foundation/client"

	"github.com/grpcd/protos/grpcdconnect"
)

const component = "grpcd-discover"

// Scheme is the URL scheme a method reached through grpcd is addressed by.
// Resolving it is the Discover loop: ask grpcd for the method in the path,
// probe the candidate, report it dead and take the next, close the stream on
// the one that answers, send there.
const Scheme = "grpcd"

// BaseURL is what every Connect client to a discovered dependency is built
// against: the scheme with no authority, so the request path is the whole
// address. grpcd:///package.Service/Method names the method as it was
// registered, and nothing in it is ever dialed as written.
const BaseURL = Scheme + ":///"

// ErrNoProcedure reports a grpcd URL whose path does not name a procedure, so
// there is nothing to look up.
var ErrNoProcedure = errors.New("path does not name a procedure")

// URL answers with the grpcd URL for a procedure, "/package.Service/Method",
// which is what a generated procedure constant holds.
func URL(procedure string) string {
	return BaseURL + strings.TrimPrefix(procedure, "/")
}

// Discovery resolves the grpcd scheme. It is an http.RoundTripper: a request
// to a grpcd URL is resolved on the spot and sent to the replica grpcd named,
// with nothing kept; a request to any other URL goes over the base transport
// as it is. Held answers with the transport that keeps what it resolves.
// One Discovery is built per process and put under the HTTP clients its
// dependencies are reached through.
type Discovery struct {
	ctx     context.Context
	log     *slog.Logger
	tracer  trace.Tracer
	service grpcdconnect.GRPCDServiceClient
	probe   Probe

	// base carries every request once its host is known: a resolved one
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
// service is the grpcd connection the caller built, the same one its
// registration uses. Taking it rather than an address is what lets a test
// supply a fake.
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

// RoundTrip resolves req's procedure through grpcd and sends req to the
// replica it names, keeping nothing, when req's URL carries the grpcd scheme;
// any other request goes over the base transport as it is. Every request
// resolves on its own, so each lands where grpcd sends it.
func (d *Discovery) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.URL.Scheme != Scheme {
		return d.base.RoundTrip(req)
	}

	if !isProcedure(req.URL.Path) {
		return nil, ErrNoProcedure
	}

	address, err := d.resolve(req.Context(), req.URL.Path, nil)
	if err != nil {
		return nil, err
	}

	return d.send(req, address, req.Body)
}

// Held answers with the transport for a caller that keeps what it resolves:
// the first request for a method resolves it and holds the replica, with a
// Watch on it, and every later request for that method goes there until the
// replica stops answering or grpcd moves the caller. Any other URL goes over
// the base transport as it is.
func (d *Discovery) Held() http.RoundTripper {
	return held{d}
}

// held is the holding transport over a Discovery.
type held struct {
	discovery *Discovery
}

// RoundTrip sends req to the replica held for its procedure, resolving one
// first when none is.
func (h held) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.URL.Scheme != Scheme {
		return h.discovery.base.RoundTrip(req)
	}

	if !isProcedure(req.URL.Path) {
		return nil, ErrNoProcedure
	}

	return h.discovery.upstream(req.URL.Path).RoundTrip(req)
}

// Upstream answers with the upstream for the method a grpcd URL names,
// building it on first sight; building dials nothing. A URL not on the
// scheme, or whose path is not a procedure, is ErrNoProcedure. Diagnostics
// take the upstream, for the address the method is on.
func (d *Discovery) Upstream(rawURL string) (*Upstream, error) {
	parsed, err := url.Parse(rawURL)
	if err != nil || parsed.Scheme != Scheme || !isProcedure(parsed.Path) {
		return nil, ErrNoProcedure
	}

	return d.upstream(parsed.Path), nil
}

// upstream answers with the upstream for method, building it on first sight.
func (d *Discovery) upstream(method string) *Upstream {
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

// send carries req to address over the base transport, with body in place of
// the one it arrived with. The request is cloned so the caller's is untouched:
// the clone is a cleartext HTTP request to the replica, whatever the caller
// addressed.
func (d *Discovery) send(req *http.Request, address string, body io.ReadCloser) (*http.Response, error) {
	attempt := req.Clone(req.Context())
	attempt.URL.Scheme = "http"
	attempt.URL.Host = address
	attempt.Body = body

	return d.base.RoundTrip(attempt)
}

// isProcedure reports whether path has the "/package.Service/Method" shape:
// a leading slash, two non-empty segments, and nothing more.
func isProcedure(path string) bool {
	service, method, found := strings.Cut(strings.TrimPrefix(path, "/"), "/")

	return strings.HasPrefix(path, "/") && found && service != "" && method != "" && !strings.Contains(method, "/")
}
