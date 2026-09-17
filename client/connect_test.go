//revive:disable:package-comments
package client

import (
	"errors"
	"testing"

	"github.com/pbrpc/testing/mocks/roundtripper"
)

func TestConnect(t *testing.T) {
	t.Run("connects to the address in the environment", func(t *testing.T) {
		// Building the connection dials nothing; the connection is made on the
		// first call, which this never makes.
		conn := Connect(grpcdAddress, roundtripper.Fail(errors.New("unexpected request")))
		if conn == nil {
			t.Fatal("expected a connection")
		}
		if conn.Address() != grpcdAddress {
			t.Errorf("address = %q, want %q", conn.Address(), grpcdAddress)
		}
		if conn.GRPCDServiceClient == nil || conn.client == nil {
			t.Error("expected the generated client and the one it is built on")
		}
		if conn.httpClient == nil {
			t.Error("expected the HTTP client the probe goes over")
		}
	})
}
