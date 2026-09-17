package discover

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
)

// malformed are grpcd URLs whose path names no procedure.
var malformed = []string{BaseURL, BaseURL + "pkg.Service", BaseURL + "pkg.Service/", "grpcd:////Method", URL(method) + "/extra"}

func TestURL(t *testing.T) {
	for _, procedure := range []string{method, "pkg.Service/Method"} {
		if got := URL(procedure); got != "grpcd:///pkg.Service/Method" {
			t.Errorf("URL(%q) = %q, want grpcd:///pkg.Service/Method", procedure, got)
		}
	}
}

func TestUpstream(t *testing.T) {
	t.Run("names the method by its grpcd URL and reuses it", func(t *testing.T) {
		d := newDiscovery(t.Context(), &grpcdStub{}, probeStub(), newBaseStub())

		first, err := d.Upstream(URL(method))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		second, err := d.Upstream(URL(method))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if first != second || first != d.upstream(method) {
			t.Error("expected the one upstream for the method")
		}
		if got := first.Address(); got != "" {
			t.Errorf("address = %q, want none before anything is resolved", got)
		}
	})

	t.Run("refuses a URL that names no procedure", func(t *testing.T) {
		d := newDiscovery(t.Context(), &grpcdStub{}, probeStub(), newBaseStub())

		for _, rawURL := range append(slices.Clone(malformed), "http://"+replicaA+method, "grpcd://%", method) {
			if _, err := d.Upstream(rawURL); !errors.Is(err, ErrNoProcedure) {
				t.Errorf("%q: error = %v, want %v", rawURL, err, ErrNoProcedure)
			}
		}
	})

	t.Run("builds one upstream under concurrent first sight", func(t *testing.T) {
		d := newDiscovery(t.Context(), &grpcdStub{}, probeStub(), newBaseStub())

		upstreams := make([]*Upstream, 8)

		var wg sync.WaitGroup
		for i := range upstreams {
			wg.Go(func() { upstreams[i] = d.upstream(method) })
		}
		wg.Wait()

		for _, u := range upstreams[1:] {
			if u != upstreams[0] {
				t.Fatal("expected every caller to get the same upstream")
			}
		}
	})
}

func TestResolve(t *testing.T) {
	t.Run("holds the replica before any request", func(t *testing.T) {
		stub := &grpcdStub{scripts: offers(replicaA)}
		u := newUpstream(t.Context(), stub, probeStub(), newBaseStub())

		if err := u.Resolve(t.Context()); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got := u.Address(); got != replicaA {
			t.Errorf("address = %q, want %q held", got, replicaA)
		}

		// Held, so a second resolve and the first request ask nothing more.
		if err := u.Resolve(t.Context()); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if _, err := call(t.Context(), t, u); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if stub.discoveries() != 1 {
			t.Errorf("discoveries = %d, want one", stub.discoveries())
		}
	})

	t.Run("reports no address while a resolution is in progress", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		// Nothing serves the method and the holder waits, so the resolution
		// is in progress until the test ends it.
		stub := &grpcdStub{scripts: offers(), discoverOpened: make(chan struct{}, 1)}
		u := newUpstream(ctx, stub, probeStub(), newBaseStub())

		resolved := make(chan error, 1)
		go func() { resolved <- u.Resolve(ctx) }()

		await(t, stub.discoverOpened, "the resolution never started")

		// A reader answers now, without waiting for the resolution.
		if got := u.Address(); got != "" {
			t.Errorf("address = %q, want none while resolving", got)
		}

		cancel()

		select {
		case err := <-resolved:
			if err == nil {
				t.Error("expected the cancelled resolution to be reported")
			}
		case <-t.Context().Done():
			t.Fatal("the resolution never ended")
		}
	})

	t.Run("shares one resolution between requests that arrive during it", func(t *testing.T) {
		stub := &grpcdStub{scripts: offers(replicaA)}

		// The probe holds the first resolution until the test releases it, so
		// the second request arrives while it is in progress.
		probing := make(chan struct{}, 1)
		release := make(chan struct{})
		probe := func(ctx context.Context, _, _ string) error {
			signal(probing)

			select {
			case <-release:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		u := newUpstream(t.Context(), stub, probe, newBaseStub())

		answers := make(chan string, 2)
		for range 2 {
			go func() {
				got, _ := call(t.Context(), t, u)
				answers <- got
			}()
		}

		await(t, probing, "the resolution never started")
		close(release)

		for range 2 {
			select {
			case got := <-answers:
				if got != replicaA {
					t.Errorf("answered by %q, want %q", got, replicaA)
				}
			case <-t.Context().Done():
				t.Fatal("a request never answered")
			}
		}

		if stub.discoveries() != 1 {
			t.Errorf("discoveries = %d, want the one shared by both", stub.discoveries())
		}
	})

	t.Run("reports a resolution that fails", func(t *testing.T) {
		u := newUpstream(t.Context(), &grpcdStub{discoverErr: errors.New("unavailable")}, probeStub(), newBaseStub())

		if err := u.Resolve(t.Context()); err == nil {
			t.Fatal("expected error")
		}
		if got := u.Address(); got != "" {
			t.Errorf("address = %q, want none", got)
		}
	})
}
