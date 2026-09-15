package discover

import (
	"errors"
	"log/slog"
	"net/http"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/pbrpc/connect-service/diagnostics"
	"github.com/pbrpc/connect-service/health"
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

func TestDiscoveryRoundTrip(t *testing.T) {
	t.Run("routes a grpcd URL to the replica held for its procedure", func(t *testing.T) {
		base := newBaseStub()
		d := newDiscovery(t.Context(), &grpcdStub{scripts: offers(replicaA)}, probeStub(), base)

		got, err := call(t.Context(), t, d)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got != replicaA {
			t.Errorf("answered by %q, want %q", got, replicaA)
		}
		if sent := base.sentTo(); !slices.Equal(sent, []string{replicaA}) {
			t.Errorf("sent to %v, want the replica", sent)
		}
	})

	t.Run("sends the replica a cleartext HTTP request", func(t *testing.T) {
		var seen *http.Request

		base := roundTripperFunc(func(request *http.Request) (*http.Response, error) {
			seen = request

			return newBaseStub().RoundTrip(request)
		})
		d := newDiscovery(t.Context(), &grpcdStub{scripts: offers(replicaA)}, probeStub(), base)

		if _, err := call(t.Context(), t, d); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if seen.URL.Scheme != "http" || seen.URL.Host != replicaA || seen.URL.Path != method {
			t.Errorf("sent %s, want http://%s%s", seen.URL, replicaA, method)
		}
	})

	t.Run("holds one upstream per procedure and reuses it", func(t *testing.T) {
		stub := &grpcdStub{scripts: offers(replicaA)}
		d := newDiscovery(t.Context(), stub, probeStub(), newBaseStub())

		for range 2 {
			if _, err := call(t.Context(), t, d); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		}

		if stub.discoveries() != 1 {
			t.Errorf("discoveries = %d, want the one lookup shared by both calls", stub.discoveries())
		}
		if d.Upstream(method) != d.Upstream(method) {
			t.Error("expected the same upstream for the same method")
		}
	})

	t.Run("builds one upstream under concurrent first sight", func(t *testing.T) {
		d := newDiscovery(t.Context(), &grpcdStub{}, probeStub(), newBaseStub())

		upstreams := make([]*Upstream, 8)

		var wg sync.WaitGroup
		for i := range upstreams {
			wg.Go(func() { upstreams[i] = d.Upstream(method) })
		}
		wg.Wait()

		for _, u := range upstreams[1:] {
			if u != upstreams[0] {
				t.Fatal("expected every caller to get the same upstream")
			}
		}
	})

	t.Run("passes any other URL to the base transport as it is", func(t *testing.T) {
		stub := &grpcdStub{}
		base := newBaseStub()
		d := newDiscovery(t.Context(), stub, probeStub(), base)

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

		for _, path := range []string{"", "/", "/pkg.Service", "/pkg.Service/", "//Method", "/pkg.Service/Method/extra"} {
			request, err := http.NewRequestWithContext(t.Context(), http.MethodPost, BaseURL+strings.TrimPrefix(path, "/"), nil)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if _, err := d.RoundTrip(request); !errors.Is(err, ErrNoProcedure) {
				t.Errorf("%q: error = %v, want %v", path, err, ErrNoProcedure)
			}
		}

		if stub.discoveries() != 0 {
			t.Errorf("discoveries = %d, want none for a malformed path", stub.discoveries())
		}
	})
}

func TestAddresses(t *testing.T) {
	d := newDiscovery(t.Context(), &grpcdStub{scripts: offers(replicaA)}, probeStub(), newBaseStub())

	if got := d.Addresses(); len(got) != 0 {
		t.Fatalf("addresses = %v, want none before any procedure is seen", got)
	}

	other := "/pkg.Service/Other"
	d.Upstream(other)

	if _, err := call(t.Context(), t, d); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	got := d.Addresses()

	if got[method] != replicaA {
		t.Errorf("addresses[%s] = %q, want %q", method, got[method], replicaA)
	}
	if held, ok := got[other]; !ok || held != "" {
		t.Errorf("addresses[%s] = %q, %v; want seen and not held", other, held, ok)
	}
}

func TestCheck(t *testing.T) {
	const grpcdAddress = "grpcd:50051"

	t.Run("reports nothing held before any lookup", func(t *testing.T) {
		d := newDiscovery(t.Context(), &grpcdStub{}, probeStub(), newBaseStub())
		d.Upstream(method)

		dependency, err := d.Check(grpcdAddress)(t.Context())
		if !errors.Is(err, ErrNothingHeld) {
			t.Fatalf("error = %v, want %v", err, ErrNothingHeld)
		}
		if dependency.GetAddress() != grpcdAddress {
			t.Errorf("address = %q, want %q", dependency.GetAddress(), grpcdAddress)
		}
		if dependency.GetState() != diagnostics.StateUnreachable || dependency.GetServing() != string(health.StatusUnknown) {
			t.Errorf("state = %q, serving = %q; want unreachable and unknown", dependency.GetState(), dependency.GetServing())
		}
		if got := dependency.GetDetails(); got["procedures"] != "1" || got["held"] != "0" {
			t.Errorf("details = %v, want one procedure, none held", got)
		}
	})

	t.Run("reports grpcd reachable while a replica is held", func(t *testing.T) {
		d := newDiscovery(t.Context(), &grpcdStub{scripts: offers(replicaA)}, probeStub(), newBaseStub())

		if _, err := call(t.Context(), t, d); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		dependency, err := d.Check(grpcdAddress)(t.Context())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if dependency.GetState() != diagnostics.StateReachable || dependency.GetServing() != string(health.StatusServing) {
			t.Errorf("state = %q, serving = %q; want reachable and serving", dependency.GetState(), dependency.GetServing())
		}
		if got := dependency.GetDetails(); got["held"] != "1" {
			t.Errorf("details = %v, want one held", got)
		}
	})
}

func TestUpstream(t *testing.T) {
	t.Run("reports no address before anything is discovered", func(t *testing.T) {
		u := newUpstream(t.Context(), &grpcdStub{}, probeStub(), newBaseStub())

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
