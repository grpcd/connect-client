package discover

import (
	"context"
	"errors"
	"io"
	"log/slog"

	"go.opentelemetry.io/otel/trace"

	grpcd "github.com/grpcd/protos"
)

// resolve works one Discover stream for method: it takes the first candidate
// that probes reachable, reports each that does not, and answers with the
// address. accept, when given, runs on the candidate before the stream is
// closed and may refuse it with an error, which ends the resolution; a holder
// opens its Watch there, so no registration falls between the two.
//
// wait is whether grpcd holds the stream for a registration when nothing
// serves the method. A holder waits: the dependency is the caller's to have.
// A resolution for one request does not, and gets NotFound at once.
//
// The stream runs under the process context, ended early when the caller's
// ends, so a caller that gave up does not leave a resolution running and a
// resolution in progress is not tied to the request that started it. It runs
// under the caller's trace, so the lookup and grpcd's side of it are part of
// the request that needed it. Closing the stream is how grpcd is told the
// candidate worked.
func (d *Discovery) resolve(
	ctx context.Context, method string, wait bool, accept func(ctx context.Context, address string) error,
) (string, error) {
	askCtx, cancel := context.WithCancel(d.ctx)
	defer cancel()

	stop := context.AfterFunc(ctx, cancel)
	defer stop()

	askCtx = trace.ContextWithSpanContext(askCtx, trace.SpanContextFromContext(ctx))

	askCtx, span := d.tracer.Start(askCtx, "discover")
	defer span.End()

	log := d.log.With(slog.String("method", method))

	stream, err := d.service.Discover(askCtx)
	if err != nil {
		log.ErrorContext(askCtx, "Failed to open discovery", slog.Any("error", err))

		return "", err
	}
	defer stream.Close()

	request := &grpcd.DiscoverRequest{
		Step:   &grpcd.DiscoverRequest_MethodName{MethodName: method},
		NoWait: !wait,
	}

	if err = stream.Send(request); err != nil {
		log.ErrorContext(askCtx, "Failed to ask for the method", slog.Any("error", err))

		return "", err
	}

	for {
		response, err := stream.Receive()
		if err != nil {
			log.ErrorContext(askCtx, "Discovery ended without an address", slog.Any("error", err))

			return "", err
		}

		address := response.GetAddress()

		if err = d.probe(askCtx, method, address); err != nil {
			log.InfoContext(askCtx, "Candidate unreachable, reporting it dead",
				slog.String("address", address), slog.Any("error", err))

			dead := &grpcd.DiscoverRequest{
				Step: &grpcd.DiscoverRequest_DeadAddress{DeadAddress: address},
			}

			if err = stream.Send(dead); err != nil {
				log.ErrorContext(askCtx, "Failed to report the candidate dead", slog.Any("error", err))

				return "", err
			}

			continue
		}

		if accept != nil {
			if err = accept(askCtx, address); err != nil {
				return "", err
			}
		}

		// grpcd takes the close as the verdict and ends the stream once it has
		// read it. That end is waited for: a stream torn down before then
		// reaches grpcd as a cancellation, and the verdict with it. The address
		// is resolved either way.
		if err = stream.CloseSend(); err == nil {
			_, err = stream.Receive()
		}

		if !errors.Is(err, io.EOF) {
			log.WarnContext(askCtx, "Verdict may not have reached grpcd", slog.Any("error", err))
		}

		log.InfoContext(askCtx, "Resolved", slog.String("address", address))

		return address, nil
	}
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
	u.mu.Unlock()

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
