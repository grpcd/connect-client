package discover

import (
	"io"
	"log/slog"
	"net/http"
	"sync"
)

// Upstream is one method reached through grpcd.
//
// Discovery hands it every request whose path names its method, and it sends
// each to the replica held right now, discovering one first when none is. The
// Connect client the request came from never sees an address; the replica
// can change under it and it keeps calling the same URL.
type Upstream struct {
	discovery *Discovery
	method    string
	log       *slog.Logger

	// mu guards address and watcher, which change together, and serializes
	// discovery: the request that finds no address held runs it, and the ones
	// behind it wait for the result rather than each discovering on their own.
	mu      sync.Mutex
	address string
	watcher *watcher
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
// the one it arrived with. The request is cloned so the caller's is untouched:
// the clone is a cleartext HTTP request to the replica, whatever the caller
// addressed.
func (u *Upstream) send(req *http.Request, address string, body io.ReadCloser) (*http.Response, error) {
	attempt := req.Clone(req.Context())
	attempt.URL.Scheme = "http"
	attempt.URL.Host = address
	attempt.Body = body

	return u.discovery.base.RoundTrip(attempt)
}
