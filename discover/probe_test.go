package discover

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"connectrpc.com/connect/v2"
	"connectrpc.com/connect/v2/connecthttp"
	"github.com/pbrpc/testing/mocks/roundtripper"

	"github.com/grpcd/protos/grpcdconnect"
)

// mounted answers with a transport that hands every request to a mux with
// the grpcd service mounted on it, so a probe runs against the real procedure
// handlers with nothing listening. The request is presented the way the
// standard transport presents it, over HTTP/2, which a bidi procedure's
// handler requires before it looks at anything else.
func mounted() http.RoundTripper {
	rpc := connect.NewServer()
	grpcdconnect.RegisterGRPCDServiceHandler(rpc, &grpcdStub{})

	mux := http.NewServeMux()
	connecthttp.Mount(mux, rpc)

	return roundtripper.Func(func(request *http.Request) (*http.Response, error) {
		request.ProtoMajor, request.ProtoMinor = 2, 0

		recorder := httptest.NewRecorder()
		mux.ServeHTTP(recorder, request)

		return recorder.Result(), nil
	})
}

// mountedProbe answers with the production probe over mounted.
func mountedProbe() Probe {
	return newProbe(&http.Client{Transport: mounted()})
}

func TestNewProbe(t *testing.T) {
	t.Run("passes an address that serves the method", func(t *testing.T) {
		if err := mountedProbe()(t.Context(), grpcdconnect.GRPCDServiceDiscoverProcedure, "10.0.0.1:50051"); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("fails an address that serves the service but not the method", func(t *testing.T) {
		if err := mountedProbe()(t.Context(), "/grpcd.GRPCDService/Missing", "10.0.0.1:50051"); err == nil {
			t.Fatal("expected error")
		}
	})

	t.Run("fails an address that does not serve the service", func(t *testing.T) {
		if err := mountedProbe()(t.Context(), method, "10.0.0.1:50051"); err == nil {
			t.Fatal("expected error")
		}
	})

	t.Run("fails an address that does not answer", func(t *testing.T) {
		want := errors.New("connection refused")
		probe := newProbe(&http.Client{Transport: roundtripper.Fail(want)})

		if err := probe(t.Context(), method, "10.0.0.1:50051"); !errors.Is(err, want) {
			t.Fatalf("error = %v, want %v", err, want)
		}
	})

	t.Run("fails a method that makes no URL", func(t *testing.T) {
		if err := mountedProbe()(t.Context(), "/pkg.Service/Method\x00", "10.0.0.1:50051"); err == nil {
			t.Fatal("expected error")
		}
	})
}
