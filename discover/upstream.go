package discover

import (
	"context"
	"log/slog"
	"net/http"
	"sync"
)

// Upstream is one method held through grpcd: the replica resolved for it,
// kept and watched until it stops answering or grpcd moves the holder.
//
// The holding transport hands it every request whose path names its method,
// and it sends each to the replica held right now, resolving one first when
// none is. The Connect client the request came from never sees an address;
// the replica can change under it and it keeps calling the same URL.
type Upstream struct {
	discovery *Discovery
	method    string
	log       *slog.Logger

	// resolving serializes resolution: the request that finds no address held
	// runs it, and the ones behind it wait for the result rather than each
	// resolving on their own. mu guards address and watcher, which change
	// together, and is held only to read or write them, never across a
	// resolution, so a reader such as diagnostics answers while one runs.
	resolving sync.Mutex
	mu        sync.Mutex
	address   string
	watcher   *watcher
}

// Address reports the replica the upstream is on right now, or "" while none
// is held. Diagnostics report this.
func (u *Upstream) Address() string {
	u.mu.Lock()
	defer u.mu.Unlock()

	return u.address
}

// Resolve holds a replica now rather than on the first request, for a caller
// that wants the method's Discover and Watch open from startup. It is a no-op
// while one is held. The wait for a grpcd that cannot be reached, or for a
// method nothing has registered yet, is ctx's to end.
func (u *Upstream) Resolve(ctx context.Context) error {
	_, err := u.hold(ctx)

	return err
}

// RoundTrip sends req to the held replica, resolving one first when none is
// held. A request the replica does not answer at the transport is the signal
// the replica is gone: the address is dropped, and a request whose body can be
// sent again is sent once more to whatever is resolved next, so a unary call
// that lands on a dead replica succeeds on a live one without the caller
// seeing it. A request that cannot be resent, or whose own context has ended,
// gets the error.
func (u *Upstream) RoundTrip(req *http.Request) (*http.Response, error) {
	address, err := u.hold(req.Context())
	if err != nil {
		return nil, err
	}

	response, err := u.discovery.send(req, address, req.Body)
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

	return u.discovery.send(req, address, body)
}
