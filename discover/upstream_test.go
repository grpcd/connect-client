package discover

import (
	"errors"
	"log/slog"
	"net/http"
	"slices"
	"sync"
	"testing"
)

func TestNew(t *testing.T) {
	t.Run("substitutes a logger and a probe when given none", func(t *testing.T) {
		d := New(t.Context(), nil, newService(&grpcdStub{}), nil, newBaseStub())

		if d.log == nil {
			t.Fatal("expected a logger")
		}
		if d.probe == nil {
			t.Fatal("expected a probe")
		}
	})

	t.Run("substitutes the standard transport when given none", func(t *testing.T) {
		d := New(t.Context(), slog.New(slog.DiscardHandler), newService(&grpcdStub{}), probeStub(), nil)

		base, ok := d.base.(*http.Transport)
		if !ok {
			t.Fatalf("base = %T, want the standard transport", d.base)
		}
		if !base.Protocols.UnencryptedHTTP2() {
			t.Errorf("protocols = %v, want cleartext HTTP/2", base.Protocols)
		}
	})
}

func TestURL(t *testing.T) {
	for _, procedure := range []string{method, "pkg.Service/Method"} {
		if got := URL(procedure); got != "grpcd:///pkg.Service/Method" {
			t.Errorf("URL(%q) = %q, want grpcd:///pkg.Service/Method", procedure, got)
		}
	}
}

// malformed are grpcd URLs whose path names no procedure.
var malformed = []string{BaseURL, BaseURL + "pkg.Service", BaseURL + "pkg.Service/", "grpcd:////Method", URL(method) + "/extra"}

func TestDiscoveryRoundTrip(t *testing.T) {
	t.Run("resolves a grpcd URL and sends the replica a cleartext HTTP request", func(t *testing.T) {
		var seen *http.Request

		base := roundTripperFunc(func(request *http.Request) (*http.Response, error) {
			seen = request

			return newBaseStub().RoundTrip(request)
		})
		d := newDiscovery(t.Context(), &grpcdStub{scripts: offers(replicaA)}, probeStub(), base)

		got, err := call(t.Context(), t, d)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got != replicaA {
			t.Errorf("answered by %q, want %q", got, replicaA)
		}
		if seen.URL.Scheme != "http" || seen.URL.Host != replicaA || seen.URL.Path != method {
			t.Errorf("sent %s, want http://%s%s", seen.URL, replicaA, method)
		}
	})

	t.Run("resolves every request on its own and keeps nothing", func(t *testing.T) {
		stub := &grpcdStub{scripts: offers(replicaA)}
		d := newDiscovery(t.Context(), stub, probeStub(), newBaseStub())

		for range 2 {
			if _, err := call(t.Context(), t, d); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		}

		if stub.discoveries() != 2 {
			t.Errorf("discoveries = %d, want one per request", stub.discoveries())
		}
		if watched := stub.watchedAddresses(); len(watched) != 0 {
			t.Errorf("watched %v, want nothing held", watched)
		}
		if len(d.upstreams) != 0 {
			t.Errorf("upstreams = %v, want none", d.upstreams)
		}
	})

	t.Run("reports a dead candidate and sends to the next", func(t *testing.T) {
		stub := &grpcdStub{scripts: offers(replicaA, replicaB)}
		d := newDiscovery(t.Context(), stub, probeStub(replicaA), newBaseStub())

		got, err := call(t.Context(), t, d)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got != replicaB {
			t.Errorf("answered by %q, want %q", got, replicaB)
		}
		if reported := stub.reported(); !slices.Equal(reported, []string{replicaA}) {
			t.Errorf("reported %v dead, want %v", reported, []string{replicaA})
		}
	})

	t.Run("fails the request when nothing resolves", func(t *testing.T) {
		d := newDiscovery(t.Context(), &grpcdStub{discoverErr: errors.New("unavailable")}, probeStub(), newBaseStub())

		if _, err := call(t.Context(), t, d); err == nil {
			t.Fatal("expected error")
		}
	})

	t.Run("passes any other URL to the base transport as it is", func(t *testing.T) {
		stub := &grpcdStub{}
		d := newDiscovery(t.Context(), stub, probeStub(), newBaseStub())

		request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://"+replicaB+"/healthz", nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		got, err := send(t, d, request)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got != replicaB {
			t.Errorf("answered by %q, want the host as addressed", got)
		}
		if stub.discoveries() != 0 {
			t.Errorf("discoveries = %d, want none for a plain URL", stub.discoveries())
		}
	})

	t.Run("refuses a grpcd URL that names no procedure", func(t *testing.T) {
		stub := &grpcdStub{}
		d := newDiscovery(t.Context(), stub, probeStub(), newBaseStub())

		for _, rawURL := range malformed {
			request, err := http.NewRequestWithContext(t.Context(), http.MethodPost, rawURL, nil)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if _, err := d.RoundTrip(request); !errors.Is(err, ErrNoProcedure) {
				t.Errorf("%q: error = %v, want %v", rawURL, err, ErrNoProcedure)
			}
		}

		if stub.discoveries() != 0 {
			t.Errorf("discoveries = %d, want none for a malformed path", stub.discoveries())
		}
	})
}

func TestHeld(t *testing.T) {
	t.Run("resolves once per method and holds the replica", func(t *testing.T) {
		stub := &grpcdStub{scripts: offers(replicaA)}
		d := newDiscovery(t.Context(), stub, probeStub(), newBaseStub())

		for range 2 {
			got, err := call(t.Context(), t, d.Held())
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != replicaA {
				t.Errorf("answered by %q, want %q", got, replicaA)
			}
		}

		if stub.discoveries() != 1 {
			t.Errorf("discoveries = %d, want the one lookup shared by both calls", stub.discoveries())
		}
		if watched := stub.watchedAddresses(); !slices.Equal(watched, []string{replicaA}) {
			t.Errorf("watched %v, want the held replica", watched)
		}
		if got := d.upstream(method).Address(); got != replicaA {
			t.Errorf("address = %q, want %q held", got, replicaA)
		}
	})

	t.Run("passes any other URL to the base transport as it is", func(t *testing.T) {
		stub := &grpcdStub{}
		d := newDiscovery(t.Context(), stub, probeStub(), newBaseStub())

		request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://"+replicaB+"/healthz", nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		got, err := send(t, d.Held(), request)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got != replicaB {
			t.Errorf("answered by %q, want the host as addressed", got)
		}
		if stub.discoveries() != 0 {
			t.Errorf("discoveries = %d, want none for a plain URL", stub.discoveries())
		}
	})

	t.Run("refuses a grpcd URL that names no procedure", func(t *testing.T) {
		d := newDiscovery(t.Context(), &grpcdStub{}, probeStub(), newBaseStub())

		for _, rawURL := range malformed {
			request, err := http.NewRequestWithContext(t.Context(), http.MethodPost, rawURL, nil)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if _, err := d.Held().RoundTrip(request); !errors.Is(err, ErrNoProcedure) {
				t.Errorf("%q: error = %v, want %v", rawURL, err, ErrNoProcedure)
			}
		}

		if len(d.upstreams) != 0 {
			t.Errorf("upstreams = %v, want none for a malformed path", d.upstreams)
		}
	})
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

// roundTripperFunc adapts a function to http.RoundTripper.
type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}
