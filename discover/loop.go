package discover

import (
	"context"
	"log/slog"

	grpcd "github.com/grpcd/protos"
)

// hold answers with the replica held, discovering one when none is. Discovery
// runs under the lock, so requests arriving while it runs wait for its result.
// It answers with an error when discovery fails, and the request that asked
// gets it; the next request asks again.
func (u *Upstream) hold(ctx context.Context) (string, error) {
	u.mu.Lock()
	defer u.mu.Unlock()

	if u.address != "" {
		return u.address, nil
	}

	address, w, err := u.discover(ctx)
	if err != nil {
		return "", err
	}

	u.address = address
	u.watcher = w

	go u.follow(w)

	return address, nil
}

// drop forgets address if it is the one held, and stops its watcher, which
// ends the goroutine following it. An address no longer held is left alone: a
// later request already replaced it.
func (u *Upstream) drop(address string) {
	u.mu.Lock()
	defer u.mu.Unlock()

	if u.address != address {
		return
	}

	u.address = ""
	u.watcher.stop()
	u.watcher = nil
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

// discover works one Discover stream: it takes the first candidate that probes
// reachable, reports each that does not, and answers with the address and the
// watcher holding a Watch on it.
//
// The stream runs under the process context, ended early when the request's
// ends, so a caller that gave up does not leave a discovery running and a
// discovery in progress is not tied to the request that started it. The
// watcher runs under the process context alone: it outlives the request.
//
// A Watch on the taken address is open before the Discover stream is closed,
// so no registration falls between the two. Closing the stream is how grpcd
// is told the candidate worked.
func (u *Upstream) discover(ctx context.Context) (string, *watcher, error) {
	askCtx, cancel := context.WithCancel(u.discovery.ctx)
	defer cancel()

	stop := context.AfterFunc(ctx, cancel)
	defer stop()

	askCtx, span := u.discovery.tracer.Start(askCtx, "discover")
	defer span.End()

	log := u.log

	stream, err := u.discovery.service.Discover(askCtx)
	if err != nil {
		log.ErrorContext(askCtx, "Failed to open discovery", slog.Any("error", err))

		return "", nil, err
	}
	defer stream.Close()

	request := &grpcd.DiscoverRequest{
		Step: &grpcd.DiscoverRequest_MethodName{MethodName: u.method},
	}

	if err = stream.Send(request); err != nil {
		log.ErrorContext(askCtx, "Failed to ask for the method", slog.Any("error", err))

		return "", nil, err
	}

	for {
		response, err := stream.Receive()
		if err != nil {
			log.ErrorContext(askCtx, "Discovery ended without an address", slog.Any("error", err))

			return "", nil, err
		}

		address := response.GetAddress()

		if err = u.discovery.probe(askCtx, address); err != nil {
			log.InfoContext(askCtx, "Candidate unreachable, reporting it dead",
				slog.String("address", address), slog.Any("error", err))

			dead := &grpcd.DiscoverRequest{
				Step: &grpcd.DiscoverRequest_DeadAddress{DeadAddress: address},
			}

			if err = stream.Send(dead); err != nil {
				log.ErrorContext(askCtx, "Failed to report the candidate dead", slog.Any("error", err))

				return "", nil, err
			}

			continue
		}

		w := u.watch(address)

		if !opened(askCtx, w) {
			w.stop()

			return "", nil, askCtx.Err()
		}

		// Delivery of the close is not waited on: the address is held either
		// way, and the deferred Close ends the stream regardless.
		_ = stream.CloseSend()

		log.InfoContext(askCtx, "Upstream discovered", slog.String("address", address))

		return address, w, nil
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

		if err := u.discovery.probe(ctx, next); err != nil {
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
