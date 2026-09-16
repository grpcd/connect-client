package discover

import (
	"context"
	"log/slog"
	"sync"

	grpcd "github.com/grpcd/protos"
)

// watcher holds a Watch stream naming one address for as long as its context
// lives, reopening the stream whenever it ends, and delivers each address
// grpcd says to move to.
//
// It runs on its own goroutine so nothing blocks on opening a stream: the
// wait while grpcd is unreachable is the ready transport's, under the
// connection the caller built.
type watcher struct {
	// moves carries each address grpcd sends. Closed once the watcher stops.
	moves <-chan string

	// opened is closed once grpcd holds the first stream, which is what
	// discovery waits for before closing the stream it is replacing.
	opened <-chan struct{}

	// done is closed once the watcher has stopped: its stream is closed and
	// moves is closed too.
	done <-chan struct{}

	// stop ends the watcher.
	stop context.CancelFunc
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
