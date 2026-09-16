package client

import "testing"

func TestConnect(t *testing.T) {
	t.Run("answers nil when no address is set", func(t *testing.T) {
		if Connect() != nil {
			t.Fatal("expected no connection without an address")
		}
	})

	t.Run("connects to the address in the environment", func(t *testing.T) {
		t.Setenv(GRPCDAddressKey, grpcdAddress)

		// Building the connection dials nothing; the connection is made on the
		// first call, which this never makes.
		conn := Connect()
		if conn == nil {
			t.Fatal("expected a connection")
		}
		if conn.Address() != grpcdAddress {
			t.Errorf("address = %q, want %q", conn.Address(), grpcdAddress)
		}
		if conn.GRPCDServiceClient == nil || conn.client == nil {
			t.Error("expected the generated client and the one it is built on")
		}
	})
}
