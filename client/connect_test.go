package client

import "testing"

func TestConnect(t *testing.T) {
	// Building the client dials nothing; the connection is made on the first
	// call, which this never makes.
	if Connect(grpcdAddress) == nil {
		t.Fatal("expected a client")
	}
}
