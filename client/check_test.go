//revive:disable:package-comments
package client

import (
	"errors"
	"net/http"
	"testing"

	"github.com/pbrpc/connect-service/diagnostics"
	"github.com/pbrpc/connect-service/health"
	"github.com/pbrpc/testing/mocks/roundtripper"
)

// answering stands in for grpcd's HTTP side: every probe is answered with
// code and body, and recorded.
func answering(code int, body string) *roundtripper.Recorder {
	return roundtripper.Record(roundtripper.Respond(code, http.Header{"Content-Type": {"application/json"}}, body))
}

// probing builds a connection whose HTTP client sends over rt, for a check
// that probes and nothing else.
func probing(rt http.RoundTripper) *Connection {
	return newConnection(nil, &http.Client{Transport: rt}, grpcdAddress)
}

func TestCheck(t *testing.T) {
	t.Run("reports what grpcd says of itself", func(t *testing.T) {
		stub := answering(http.StatusOK, `{"status":"SERVING"}`)

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
		sent := stub.Sent()
		if len(sent) != 1 {
			t.Fatalf("expected one probe, got %d", len(sent))
		}
		if sent[0].Request.Method != http.MethodGet {
			t.Errorf("method = %q, want GET", sent[0].Request.Method)
		}
		if sent[0].Request.URL.Host != grpcdAddress || sent[0].Request.URL.Path != health.HTTPPath {
			t.Errorf("probed %s, want %s%s", sent[0].Request.URL, grpcdAddress, health.HTTPPath)
		}
	})

	t.Run("reports grpcd not serving when it says so", func(t *testing.T) {
		got, err := Check(probing(answering(http.StatusServiceUnavailable, `{"status":"NOT_SERVING"}`)))(t.Context())
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
		got, err := Check(probing(roundtripper.Fail(errors.New("connection refused"))))(t.Context())
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
