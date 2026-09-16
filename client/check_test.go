package client

import (
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"

	"github.com/pbrpc/connect-service/diagnostics"
	"github.com/pbrpc/connect-service/health"
)

// healthStub stands in for grpcd's HTTP side. It answers every request with
// code and body, or fails it at the transport with err, and records what it
// was asked.
type healthStub struct {
	code int
	body string
	err  error

	mu       sync.Mutex
	requests []*http.Request
}

func (s *healthStub) RoundTrip(request *http.Request) (*http.Response, error) {
	s.mu.Lock()
	s.requests = append(s.requests, request)
	s.mu.Unlock()

	if s.err != nil {
		return nil, s.err
	}

	return &http.Response{
		StatusCode: s.code,
		Status:     http.StatusText(s.code),
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(strings.NewReader(s.body)),
		Request:    request,
	}, nil
}

// asked answers with the requests the stub was sent, in order.
func (s *healthStub) asked() []*http.Request {
	s.mu.Lock()
	defer s.mu.Unlock()

	return append([]*http.Request(nil), s.requests...)
}

// probing builds a connection whose HTTP client sends over stub, for a check
// that probes and nothing else.
func probing(stub *healthStub) *Connection {
	return newConnection(nil, &http.Client{Transport: stub}, grpcdAddress)
}

func TestCheck(t *testing.T) {
	t.Run("reports what grpcd says of itself", func(t *testing.T) {
		stub := &healthStub{code: http.StatusOK, body: `{"status":"SERVING"}`}

		got, err := Check(probing(stub))(t.Context())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got.GetAddress() != grpcdAddress {
			t.Errorf("address = %q, want %q", got.GetAddress(), grpcdAddress)
		}
		if got.GetState() != diagnostics.StateReachable {
			t.Errorf("state = %q, want reachable", got.GetState())
		}
		if got.GetServing() != string(health.StatusServing) {
			t.Errorf("serving = %q, want SERVING", got.GetServing())
		}
		if got.GetLastChecked() == 0 {
			t.Error("expected a last checked time")
		}

		// The probe is grpcd's plain HTTP health route at the address the
		// connection was built for.
		requests := stub.asked()
		if len(requests) != 1 {
			t.Fatalf("expected one probe, got %d", len(requests))
		}
		if requests[0].Method != http.MethodGet {
			t.Errorf("method = %q, want GET", requests[0].Method)
		}
		if requests[0].URL.Host != grpcdAddress || requests[0].URL.Path != health.HTTPPath {
			t.Errorf("probed %s, want %s%s", requests[0].URL, grpcdAddress, health.HTTPPath)
		}
	})

	t.Run("reports grpcd not serving when it says so", func(t *testing.T) {
		stub := &healthStub{code: http.StatusServiceUnavailable, body: `{"status":"NOT_SERVING"}`}

		got, err := Check(probing(stub))(t.Context())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got.GetState() != diagnostics.StateReachable {
			t.Errorf("state = %q, want reachable", got.GetState())
		}
		if got.GetServing() != string(health.StatusNotServing) {
			t.Errorf("serving = %q, want NOT_SERVING", got.GetServing())
		}
	})

	t.Run("reports grpcd unreachable when the probe is not answered", func(t *testing.T) {
		stub := &healthStub{err: errors.New("connection refused")}

		got, err := Check(probing(stub))(t.Context())
		if err == nil {
			t.Fatal("expected the failure to be reported")
		}
		if got.GetAddress() != grpcdAddress {
			t.Errorf("address = %q, want %q", got.GetAddress(), grpcdAddress)
		}
		if got.GetState() != diagnostics.StateUnreachable {
			t.Errorf("state = %q, want unreachable", got.GetState())
		}
		if got.GetServing() != string(health.StatusUnknown) {
			t.Errorf("serving = %q, want UNKNOWN", got.GetServing())
		}
	})
}
