//revive:disable:package-comments
package discover

import (
	"context"
	"log/slog"
	"net/http"
	"sync"

	grpcd "github.com/grpcd/protos"
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

	// dropped is closed when the address held is dropped, and replaced when
	// one is held again, so a holder waits on it for the moment to resolve
	// once more. A move keeps it: the address changed, but one is still held.
	// Set whenever address is, under mu.
	dropped chan struct{}
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

// Hold keeps a replica held for as long as ctx lives: it resolves one now,
// and again whenever the one held is dropped. A caller that wants the
// method's Discover and Watch open from startup, and reopened after the
// replica stops answering, runs this on its own goroutine in place of one
// Resolve, so a dependency that could not be reached when the process started
// is reached once it can be, with or without a request arriving to ask.
//
// A resolution that fails is asked again. The wait between attempts while
// grpcd cannot be reached is the ready transport's under the connection the
// Discovery was built on, and the wait while nothing serves the method is
// grpcd's, which holds the Discover stream for a registration, so this holds
// no timer of its own. The address held when ctx ends stays held: it belongs
// to the process, as one Resolve leaves it.
func (u *Upstream) Hold(ctx context.Context) {
	for ctx.Err() == nil {
		if _, err := u.hold(ctx); err != nil {
			continue
		}

		u.mu.Lock()
		dropped := u.dropped
		u.mu.Unlock()

		select {
		case <-dropped:
		case <-ctx.Done():
		}
	}
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

// hold answers with the replica held, resolving one when none is. Resolution
// runs under the resolving lock, so requests arriving while it runs wait for
// its result. It answers with an error when resolution fails, and the request
// that asked gets it; the next request asks again.
func (u *Upstream) hold(ctx context.Context) (string, error) {
	u.resolving.Lock()
	defer u.resolving.Unlock()

	if address := u.Address(); address != "" {
		return address, nil
	}

	// The Watch on the candidate is open before the Discover stream closes,
	// so no registration falls between the two. The watcher runs under the
	// process context alone: it outlives the request.
	var w *watcher

	address, err := u.discovery.resolve(ctx, u.method, true, func(ctx context.Context, address string) error {
		w = u.watch(address)

		if !opened(ctx, w) {
			w.stop()

			return ctx.Err()
		}

		return nil
	})
	if err != nil {
		return "", err
	}

	u.mu.Lock()
	u.address = address
	u.watcher = w
	u.dropped = make(chan struct{})
	u.mu.Unlock()

	go u.follow(w)

	return address, nil
}

// follow moves the upstream wherever grpcd says to, for as long as w's
// address is the one held. Each move is probed and, if reachable, watched
// before it is taken, so no registration falls between the old watch and the
// new. It returns when the watcher it follows is stopped: by drop, by the
// process ending, or by a move it lost to.
func (u *Upstream) follow(w *watcher) {
	ctx := u.discovery.ctx
	log := u.log

	for {
		next, open := <-w.moves
		if !open {
			return
		}

		if err := u.discovery.probe(ctx, u.method, next); err != nil {
			log.InfoContext(ctx, "Told to move to an unreachable address, staying",
				slog.String("address", next), slog.Any("error", err))

			continue
		}

		nw := u.watch(next)

		if !opened(ctx, nw) {
			nw.stop()

			return
		}

		if !u.swap(w, next, nw) {
			// Dropped or replaced while the new watch was opening; whoever did
			// it owns the address now.
			nw.stop()

			return
		}

		log.InfoContext(ctx, "Moved", slog.String("to", next))

		w.stop()
		w = nw
	}
}

// drop forgets address if it is the one held, and stops its watcher, which
// ends the goroutine following it, and tells a holder to resolve again. An
// address no longer held is left alone: a later request already replaced it.
func (u *Upstream) drop(address string) {
	u.mu.Lock()
	defer u.mu.Unlock()

	if u.address != address {
		return
	}

	u.address = ""
	u.watcher.stop()
	u.watcher = nil
	close(u.dropped)
}

// swap moves from w's address to next, watched by nw, unless w is no longer
// the watcher held, in which case the move is stale and it reports false.
func (u *Upstream) swap(w *watcher, next string, nw *watcher) bool {
	u.mu.Lock()
	defer u.mu.Unlock()

	if u.watcher != w {
		return false
	}

	u.address = next
	u.watcher = nw

	return true
}

// watch starts a watcher on address, under the process context.
func (u *Upstream) watch(address string) *watcher {
	ctx, cancel := context.WithCancel(u.discovery.ctx)

	moves := make(chan string)
	opened := make(chan struct{})
	done := make(chan struct{})

	go u.keepWatching(ctx, address, moves, opened, done)

	return &watcher{moves: moves, opened: opened, done: done, stop: cancel}
}

// keepWatching holds one Watch stream after another until ctx ends.
func (u *Upstream) keepWatching(
	ctx context.Context, address string, moves chan<- string, opened, done chan struct{},
) {
	defer close(done)
	defer close(moves)

	var once sync.Once

	log := u.log.With(slog.String("address", address))

	request := &grpcd.WatchRequest{MethodName: u.method, Address: address}

	for ctx.Err() == nil {
		stream, err := u.discovery.service.Watch(ctx, request)
		if err != nil {
			log.ErrorContext(ctx, "Failed to open watch", slog.Any("error", err))

			continue
		}

		for {
			response, err := stream.Receive()
			if err != nil {
				log.InfoContext(ctx, "Watch ended", slog.Any("error", err))

				break
			}

			// grpcd holds the stream once anything arrives on it. Its first
			// message is empty, saying only that; a move carries an address.
			once.Do(func() { close(opened) })

			next := response.GetAddress()
			if next == "" {
				continue
			}

			select {
			case moves <- next:
			case <-ctx.Done():
				_ = stream.Close()

				return
			}
		}

		_ = stream.Close()
	}
}

// opened waits for w's first Watch to be open, answering false if ctx ends
// first.
func opened(ctx context.Context, w *watcher) bool {
	select {
	case <-w.opened:
		return true
	case <-ctx.Done():
		return false
	}
}
