//revive:disable:package-comments
package discover

import (
	"context"
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
