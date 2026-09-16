package discover

import (
	"context"
	"errors"
	"io"
	"slices"
	"strings"
	"testing"

	"connectrpc.com/connect/v2"

	"github.com/pbrpc/connect-testing/mocks/transport"
)

const (
	replicaA = "10.0.0.1:50051"
	replicaB = "10.0.0.2:50051"
)

// offers scripts one Discover stream offering the given candidates and then
// waiting, the way grpcd does once it has nothing more.
func offers(candidates ...string) []discoverScript {
	return []discoverScript{{candidates: candidates}}
}

func TestRoundTrip(t *testing.T) {
	t.Run("discovers on the first request and holds the address after", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		stub := &grpcdStub{scripts: offers(replicaA)}
		base := newBaseStub()
		u := newUpstream(ctx, stub, probeStub(), base)

		for range 2 {
			got, err := call(t.Context(), t, u)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != replicaA {
				t.Errorf("answered by %q, want %q", got, replicaA)
			}
		}

		if stub.discoveries() != 1 {
			t.Errorf("discoveries = %d, want one for two requests", stub.discoveries())
		}
		if got := u.Address(); got != replicaA {
			t.Errorf("address = %q, want %q", got, replicaA)
		}
		if got := stub.watchedAddresses(); !slices.Equal(got, []string{replicaA}) {
			t.Errorf("watched %v, want [%s]", got, replicaA)
		}
		if got := base.sentTo(); !slices.Equal(got, []string{replicaA, replicaA}) {
			t.Errorf("sent to %v, want the replica both times", got)
		}
	})

	t.Run("reports a dead candidate and takes the next", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		stub := &grpcdStub{scripts: offers(replicaA, replicaB)}
		u := newUpstream(ctx, stub, probeStub(replicaA), newBaseStub())

		got, err := call(t.Context(), t, u)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got != replicaB {
			t.Errorf("answered by %q, want %q", got, replicaB)
		}
		if reported := stub.reported(); !slices.Equal(reported, []string{replicaA}) {
			t.Errorf("reported %v dead, want [%s]", reported, replicaA)
		}
	})

	t.Run("fails the request when discovery ends without an address, and asks again next time", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		stub := &grpcdStub{scripts: []discoverScript{
			{end: errors.New("store unavailable")},
			{candidates: []string{replicaA}},
		}}
		u := newUpstream(ctx, stub, probeStub(), newBaseStub())

		if _, err := call(t.Context(), t, u); err == nil {
			t.Fatal("expected the failed discovery to fail the request")
		}
		if got := u.Address(); got != "" {
			t.Errorf("address = %q, want none held", got)
		}

		got, err := call(t.Context(), t, u)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got != replicaA {
			t.Errorf("answered by %q, want %q", got, replicaA)
		}
		if stub.discoveries() != 2 {
			t.Errorf("discoveries = %d, want 2", stub.discoveries())
		}
	})

	t.Run("fails the request when discovery cannot be opened", func(t *testing.T) {
		stub := &grpcdStub{scripts: offers(replicaA), discoverErr: errors.New("unavailable")}
		u := newUpstream(t.Context(), stub, probeStub(), newBaseStub())

		if _, err := call(t.Context(), t, u); err == nil {
			t.Fatal("expected error")
		}
	})

	t.Run("fails the request when the stream cannot be opened at all", func(t *testing.T) {
		u := newStubbedUpstream(t.Context(), transport.Once(nil, errors.New("unavailable")))

		if _, err := call(t.Context(), t, u); err == nil {
			t.Fatal("expected error")
		}
	})

	t.Run("fails the request when the method cannot be sent", func(t *testing.T) {
		u := newStubbedUpstream(t.Context(), transport.Once(discoverStream(errors.New("broken"), 0), nil))

		if _, err := call(t.Context(), t, u); err == nil {
			t.Fatal("expected error")
		}
	})

	t.Run("fails the request when a dead report cannot be sent", func(t *testing.T) {
		u := newStubbedUpstream(t.Context(), transport.Once(discoverStream(errors.New("broken"), 1, replicaA), nil))
		u.discovery.probe = probeStub(replicaA)

		if _, err := call(t.Context(), t, u); err == nil {
			t.Fatal("expected error")
		}
	})

	t.Run("gives up the candidate when the watch cannot open before the request ends", func(t *testing.T) {
		requestCtx, cancel := context.WithCancel(t.Context())
		defer cancel()

		// Discovery offers a candidate; the Watch on it never opens, and the
		// request ends while it is being waited for.
		tp := transport.New(func(n int, ctx context.Context, _ connect.Spec) (connect.ClientStream, error) {
			if n == 1 {
				return discoverStream(nil, 0, replicaA), nil
			}

			cancel()
			<-ctx.Done()

			return nil, ctx.Err()
		})
		u := newStubbedUpstream(t.Context(), tp)

		if _, err := call(requestCtx, t, u); !errors.Is(err, context.Canceled) {
			t.Fatalf("error = %v, want the cancellation", err)
		}
		if got := u.Address(); got != "" {
			t.Errorf("address = %q, want none held", got)
		}
	})

	t.Run("fails the request when the process ends while discovering", func(t *testing.T) {
		processCtx, end := context.WithCancel(t.Context())

		// The stream offers nothing and waits; ending the process is what
		// ends it.
		u := newUpstream(processCtx, &grpcdStub{scripts: offers()}, probeStub(), newBaseStub())

		results := make(chan error, 1)
		go func() {
			_, err := call(t.Context(), t, u)
			results <- err
		}()

		end()

		if err := <-results; err == nil {
			t.Fatal("expected the ended process to fail the request")
		}
	})

	t.Run("drops the replica when it does not answer and resends to the next", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		stub := &grpcdStub{scripts: []discoverScript{{candidates: []string{replicaA}}, {candidates: []string{replicaB}}}}
		base := newBaseStub(replicaA)
		u := newUpstream(ctx, stub, probeStub(), base)

		got, err := call(t.Context(), t, u)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got != replicaB {
			t.Errorf("answered by %q, want %q", got, replicaB)
		}
		if sent := base.sentTo(); !slices.Equal(sent, []string{replicaA, replicaB}) {
			t.Errorf("sent to %v, want the dead replica then the next", sent)
		}
		if got := u.Address(); got != replicaB {
			t.Errorf("address = %q, want %q", got, replicaB)
		}
		if stub.discoveries() != 2 {
			t.Errorf("discoveries = %d, want 2", stub.discoveries())
		}
	})

	t.Run("drops the replica but does not resend a request that cannot be sent twice", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		stub := &grpcdStub{scripts: offers(replicaA)}
		base := newBaseStub(replicaA)
		u := newUpstream(ctx, stub, probeStub(), base)

		if _, err := u.RoundTrip(newCall(t.Context(), t, nil)); !errors.Is(err, errUnreachable) {
			t.Fatalf("error = %v, want %v", err, errUnreachable)
		}
		if got := u.Address(); got != "" {
			t.Errorf("address = %q, want dropped", got)
		}
		if sent := base.sentTo(); !slices.Equal(sent, []string{replicaA}) {
			t.Errorf("sent to %v, want one attempt", sent)
		}
	})

	t.Run("keeps the original error when the body cannot be rebuilt", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		u := newUpstream(ctx, &grpcdStub{scripts: offers(replicaA)}, probeStub(), newBaseStub(replicaA))

		request := newCall(t.Context(), t, strings.NewReader("request"))
		request.GetBody = func() (io.ReadCloser, error) { return nil, errors.New("gone") }

		if _, err := u.RoundTrip(request); !errors.Is(err, errUnreachable) {
			t.Fatalf("error = %v, want %v", err, errUnreachable)
		}
	})

	t.Run("keeps the original error when nothing else can be discovered", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		stub := &grpcdStub{scripts: []discoverScript{{candidates: []string{replicaA}}, {end: errors.New("nothing")}}}
		u := newUpstream(ctx, stub, probeStub(), newBaseStub(replicaA))

		if _, err := call(t.Context(), t, u); !errors.Is(err, errUnreachable) {
			t.Fatalf("error = %v, want %v", err, errUnreachable)
		}
	})

	t.Run("keeps the replica when the request's own context ended", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		base := newBaseStub()
		u := newUpstream(ctx, &grpcdStub{scripts: offers(replicaA)}, probeStub(), base)

		// Held first, with a live request.
		if _, err := call(t.Context(), t, u); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// The base fails from now on, and the request that meets the failure
		// has already been cancelled by its caller: that is not the replica's
		// fault.
		base.down[replicaA] = true

		requestCtx, cancelRequest := context.WithCancel(t.Context())
		cancelRequest()

		if _, err := u.RoundTrip(newCall(requestCtx, t, strings.NewReader("request"))); err == nil {
			t.Fatal("expected error")
		}
		if got := u.Address(); got != replicaA {
			t.Errorf("address = %q, want %q still held", got, replicaA)
		}
	})
}

// newFollowing builds an upstream holding replicaA with its watch open, on a
// stub the test can feed moves to and observe watches on.
func newFollowing(ctx context.Context, t *testing.T, probe Probe) (*grpcdStub, *Upstream) {
	t.Helper()

	stub := &grpcdStub{
		scripts:     offers(replicaA),
		moves:       make(chan string),
		endWatch:    make(chan struct{}),
		watchOpened: make(chan struct{}, 4),
		watchEnded:  make(chan struct{}, 4),
	}
	u := newUpstream(ctx, stub, probe, newBaseStub())

	if _, err := call(t.Context(), t, u); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	await(t, stub.watchOpened, "the first watch never opened")

	return stub, u
}

func TestFollow(t *testing.T) {
	t.Run("moves when told and watches the new address", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		stub, u := newFollowing(ctx, t, probeStub())

		stub.moves <- replicaB

		// The old watch ends only after the swap, so its end is the move
		// having landed.
		await(t, stub.watchOpened, "the watch on the new address never opened")
		await(t, stub.watchEnded, "the watch on the old address never ended")

		if got := u.Address(); got != replicaB {
			t.Errorf("address = %q, want %q", got, replicaB)
		}

		got, err := call(t.Context(), t, u)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got != replicaB {
			t.Errorf("answered by %q, want %q", got, replicaB)
		}
		if watched := stub.watchedAddresses(); !slices.Equal(watched, []string{replicaA, replicaB}) {
			t.Errorf("watched %v, want both in order", watched)
		}

		// A drop of the address moved away from is stale and changes nothing.
		u.drop(replicaA)

		if got := u.Address(); got != replicaB {
			t.Errorf("address = %q, want %q kept", got, replicaB)
		}
	})

	t.Run("stays when told an address it cannot reach", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		// The probe reports what it was asked about, so the test knows the
		// move was considered and refused.
		probed := make(chan string, 1)
		probe := func(_ context.Context, address string) error {
			if address == replicaB {
				probed <- address

				return errors.New("unreachable")
			}

			return nil
		}

		stub, u := newFollowing(ctx, t, probe)

		stub.moves <- replicaB

		select {
		case <-probed:
		case <-t.Context().Done():
			t.Fatal("the move was never probed")
		}

		if got := u.Address(); got != replicaA {
			t.Errorf("address = %q, want %q kept", got, replicaA)
		}
		if watched := stub.watchedAddresses(); !slices.Equal(watched, []string{replicaA}) {
			t.Errorf("watched %v, want only the address kept", watched)
		}
	})

	t.Run("gives up a move that lost to a drop", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		var u *Upstream

		// The replica is dropped while the move is being probed, so the move
		// arrives for an address no longer held and is discarded.
		probe := func(_ context.Context, address string) error {
			if address == replicaB {
				u.drop(replicaA)
			}

			return nil
		}

		stub, following := newFollowing(ctx, t, probe)
		u = following

		stub.moves <- replicaB

		// The old watch ends on the drop; the new one ends when the move is
		// discarded.
		await(t, stub.watchEnded, "the dropped address's watch never ended")
		await(t, stub.watchOpened, "the watch on the new address never opened")
		await(t, stub.watchEnded, "the discarded move's watch never ended")

		if got := u.Address(); got != "" {
			t.Errorf("address = %q, want none held", got)
		}
	})

	t.Run("reopens the watch when it ends", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		stub, _ := newFollowing(ctx, t, probeStub())

		stub.endWatch <- struct{}{}

		await(t, stub.watchEnded, "the watch never ended")
		await(t, stub.watchOpened, "the watch was never reopened")

		if watched := stub.watchedAddresses(); !slices.Equal(watched, []string{replicaA, replicaA}) {
			t.Errorf("watched %v, want the same address twice", watched)
		}
	})

	t.Run("lets go of a move it was holding when stopped", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		stub := &grpcdStub{
			moves:       make(chan string),
			watchOpened: make(chan struct{}, 1),
			watchEnded:  make(chan struct{}, 1),
			moveSent:    make(chan struct{}, 1),
		}
		u := newUpstream(ctx, stub, probeStub(), newBaseStub())

		// A watcher nobody follows: the move it takes has no reader, so it is
		// held until the watcher is stopped.
		w := u.watch(replicaA)
		await(t, stub.watchOpened, "the watch never opened")

		stub.moves <- replicaB
		await(t, stub.moveSent, "the move was never taken")

		w.stop()

		await(t, stub.watchEnded, "the watch never ended")

		// The held move may still be delivered to a reader that turns up
		// before the stop is seen; either way the channel closes, which is
		// what a follower needs to return.
		for range w.moves {
		}
	})

	t.Run("gives up a move when the process ends before its watch opens", func(t *testing.T) {
		processCtx, end := context.WithCancel(t.Context())
		defer end()

		// The process ends while the move is being probed, so the watch on
		// it is started under an ended context and never opens.
		probe := func(_ context.Context, address string) error {
			if address == replicaB {
				end()
			}

			return nil
		}

		stub, u := newFollowing(processCtx, t, probe)

		stub.moves <- replicaB

		// The held address's watch ends with the process; the move's never
		// reaches grpcd.
		await(t, stub.watchEnded, "the held address's watch never ended")

		if got := u.Address(); got != replicaA {
			t.Errorf("address = %q, want %q unchanged", got, replicaA)
		}
		if watched := stub.watchedAddresses(); !slices.Equal(watched, []string{replicaA}) {
			t.Errorf("watched %v, want only the held address", watched)
		}
	})
}
