package discover

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"connectrpc.com/connect/v2"
	"connectrpc.com/connect/v2/connecthttp"

	"github.com/grpcd/protos/grpcdconnect"
)

// mountedClient stands in for the HTTP client a probe asks with: it hands
// every request to a mux with the grpcd service mounted on it, so the probe
// runs against the real procedure handlers with nothing listening.
type mountedClient struct {
	mux *http.ServeMux
}

func newMountedClient() *mountedClient {
	rpc := connect.NewServer()
	grpcdconnect.RegisterGRPCDServiceHandler(rpc, &grpcdStub{})

	mux := http.NewServeMux()
	connecthttp.Mount(mux, rpc)

	return &mountedClient{mux: mux}
}

func (c *mountedClient) Do(request *http.Request) (*http.Response, error) {
	// Presented the way the standard transport presents it: over HTTP/2,
	// which a bidi procedure's handler requires before it looks at anything
	// else.
	request.ProtoMajor, request.ProtoMinor = 2, 0

	recorder := httptest.NewRecorder()
	c.mux.ServeHTTP(recorder, request)

	return recorder.Result(), nil
}

// failingClient fails every request with err.
type failingClient struct {
	err error
}

func (c *failingClient) Do(*http.Request) (*http.Response, error) {
	return nil, c.err
}

func TestNewProbe(t *testing.T) {
	t.Run("substitutes the standard HTTP client when given none", func(t *testing.T) {
		if NewProbe(nil) == nil {
			t.Fatal("expected a probe")
		}
	})

	t.Run("passes an address that serves the method", func(t *testing.T) {
		probe := NewProbe(newMountedClient())

		if err := probe(t.Context(), grpcdconnect.GRPCDServiceDiscoverProcedure, "10.0.0.1:50051"); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("fails an address that serves the service but not the method", func(t *testing.T) {
		probe := NewProbe(newMountedClient())

		if err := probe(t.Context(), "/grpcd.GRPCDService/Missing", "10.0.0.1:50051"); err == nil {
			t.Fatal("expected error")
		}
	})

	t.Run("fails an address that does not serve the service", func(t *testing.T) {
		probe := NewProbe(newMountedClient())

		if err := probe(t.Context(), method, "10.0.0.1:50051"); err == nil {
			t.Fatal("expected error")
		}
	})

	t.Run("fails an address that does not answer", func(t *testing.T) {
		want := errors.New("connection refused")

		if err := NewProbe(&failingClient{err: want})(t.Context(), method, "10.0.0.1:50051"); !errors.Is(err, want) {
			t.Fatalf("error = %v, want %v", err, want)
		}
	})

	t.Run("fails a method that makes no URL", func(t *testing.T) {
		if err := NewProbe(newMountedClient())(t.Context(), "/pkg.Service/Method\x00", "10.0.0.1:50051"); err == nil {
			t.Fatal("expected error")
		}
	})
}
