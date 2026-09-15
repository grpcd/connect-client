package discover

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/pbrpc/connect-service/health"
)

// healthClient stands in for the HTTP client a probe asks with: it hands
// every request to a health server's probe route directly, so the probe runs
// against the real route with nothing listening.
type healthClient struct {
	srv *health.Server
}

func (c *healthClient) Do(request *http.Request) (*http.Response, error) {
	recorder := httptest.NewRecorder()
	c.srv.ServeHTTP(recorder, request)

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

	t.Run("passes an address that answers", func(t *testing.T) {
		if err := NewProbe(&healthClient{srv: health.NewServer()})(t.Context(), "10.0.0.1:50051"); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("passes an address that answers not serving", func(t *testing.T) {
		srv := health.NewServer()
		srv.Shutdown()

		if err := NewProbe(&healthClient{srv: srv})(t.Context(), "10.0.0.1:50051"); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("fails an address that does not answer", func(t *testing.T) {
		want := errors.New("connection refused")

		if err := NewProbe(&failingClient{err: want})(t.Context(), "10.0.0.1:50051"); !errors.Is(err, want) {
			t.Fatalf("error = %v, want %v", err, want)
		}
	})
}
