//revive:disable:package-comments
package discover

import (
	"errors"
	"net/http"
	"slices"
	"testing"

	"connectrpc.com/connect/v2"
	"github.com/pbrpc/otel-testing/mocks/tracer"
	"github.com/pbrpc/testing/mocks/roundtripper"
	"go.opentelemetry.io/otel/trace"
)

func TestNew(t *testing.T) {
	t.Run("substitutes a logger and a probe when given none", func(t *testing.T) {
		d := New(t.Context(), nil, newService(&grpcdStub{}), probeStub(), newBaseStub())

		if d.log == nil {
			t.Fatal("expected a logger")
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
		if waits := stub.waits(); !slices.Equal(waits, []bool{true}) {
			t.Errorf("asked to wait: %v, want a holder waiting for its dependency", waits)
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

func TestDiscoveryRoundTrip(t *testing.T) {
	t.Run("resolves a grpcd URL and sends the replica a cleartext HTTP request", func(t *testing.T) {
		var seen *http.Request

		base := roundtripper.Func(func(request *http.Request) (*http.Response, error) {
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

	t.Run("resolves under the request's trace", func(t *testing.T) {
		tt, requestCtx := tracer.New(t)
		defer tt.Shutdown(t)

		d := newDiscovery(t.Context(), &grpcdStub{scripts: offers(replicaA)}, probeStub(), newBaseStub())
		d.tracer = tt.Tracer(component)

		if _, err := call(requestCtx, t, d); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		tt.EndSpan()

		var request, discover *trace.SpanContext

		for _, span := range tt.GetSpans() {
			switch span.Name {
			case "test-span":
				request = &span.SpanContext
			case "discover":
				discover = &span.Parent
			}
		}

		if request == nil || discover == nil {
			t.Fatalf("spans = %v, want the request's and the discovery's", tt.GetSpans())
		}
		if discover.SpanID() != request.SpanID() {
			t.Errorf("discover span's parent = %s, want the request's span %s", discover.SpanID(), request.SpanID())
		}
	})

	t.Run("does not wait for a procedure nothing serves", func(t *testing.T) {
		stub := &grpcdStub{scripts: offers()}
		d := newDiscovery(t.Context(), stub, probeStub(), newBaseStub())

		// The test's own context is never cancelled here: the answer has to
		// come from grpcd declining, not from the caller giving up.
		_, err := call(t.Context(), t, d)
		if connect.CodeOf(err) != connect.CodeNotFound {
			t.Fatalf("error = %v, want grpcd's not found", err)
		}
		if waits := stub.waits(); !slices.Equal(waits, []bool{false}) {
			t.Errorf("asked to wait: %v, want not", waits)
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
