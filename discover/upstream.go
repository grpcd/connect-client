package discover

import (
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"

	foundationclient "github.com/pbrpc/connect-foundation/client"
)

// Upstream is one dependency reached through grpcd.
//
// It is the transport under its own HTTP client: every request bound for the
// dependency passes through RoundTrip, which sends it to the replica held
// right now, discovering one first when none is. The Connect client built on
// HTTPClient and BaseURL never sees an address; the replica it is on can change
// under it and it keeps calling the same URL.
type Upstream struct {
	discovery *Discovery
	method    string
	log       *slog.Logger

	// base carries the request once the replica is chosen.
	base http.RoundTripper

	// mu guards address and watcher, which change together, and serializes
	// discovery: the request that finds no address held runs it, and the ones
	// behind it wait for the result rather than each discovering on their own.
	mu      sync.Mutex
	address string
	watcher *watcher
}

// HTTPClient answers with the client to build the dependency's Connect client
// on: the foundation's tracing over this upstream's routing.
func (u *Upstream) HTTPClient() *http.Client {
	return foundationclient.NewHTTPClient(u)
}

// BaseURL is the URL to build the Connect client against. Its host is the
// service name, which RoundTrip replaces with the replica on every request, so
// it is never dialed as written.
func (u *Upstream) BaseURL() string {
	return foundationclient.BaseURL(serviceName(u.method))
}

// Address reports the replica the upstream is on right now, or "" while none
// is held. Diagnostics report this.
func (u *Upstream) Address() string {
	u.mu.Lock()
	defer u.mu.Unlock()

	return u.address
}

// RoundTrip sends req to the held replica, discovering one first when none is
// held. A request the replica does not answer at the transport is the signal
// the replica is gone: the address is dropped, and a request whose body can be
// sent again is sent once more to whatever is discovered next, so a unary call
// that lands on a dead replica succeeds on a live one without the caller
// seeing it. A request that cannot be resent, or whose own context has ended,
// gets the error.
func (u *Upstream) RoundTrip(req *http.Request) (*http.Response, error) {
	address, err := u.hold(req.Context())
	if err != nil {
		return nil, err
	}

	response, err := u.send(req, address, req.Body)
	if err == nil || req.Context().Err() != nil {
		return response, err
	}

	u.log.InfoContext(req.Context(), "Upstream did not answer, dropping it",
		slog.String("address", address), slog.Any("error", err))

	u.drop(address)

	if req.GetBody == nil {
		return nil, err
	}

	body, bodyErr := req.GetBody()
	if bodyErr != nil {
		return nil, err
	}

	address, holdErr := u.hold(req.Context())
	if holdErr != nil {
		return nil, err
	}

	return u.send(req, address, body)
}

// send carries req to address over the base transport, with body in place of
// the one it arrived with. The request is cloned so the caller's is untouched.
func (u *Upstream) send(req *http.Request, address string, body io.ReadCloser) (*http.Response, error) {
	attempt := req.Clone(req.Context())
	attempt.URL.Host = address
	attempt.Body = body

	return u.base.RoundTrip(attempt)
}

// serviceName reports the fully qualified service owning a method named in
// wire format, "/package.Service/Method".
func serviceName(method string) string {
	name, _, _ := strings.Cut(strings.TrimPrefix(method, "/"), "/")

	return name
}
