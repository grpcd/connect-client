package client

import (
	"context"
	"errors"
	"testing"

	"github.com/pbrpc/connect-service/diagnostics"
	"github.com/pbrpc/connect-service/health"
)

func TestCheck(t *testing.T) {
	t.Run("reports grpcd unreachable while the stream is not held", func(t *testing.T) {
		client := newClient(&grpcdStub{}, []string{method})

		got, err := client.Check(t.Context())
		if !errors.Is(err, ErrNotRegistered) {
			t.Fatalf("error = %v, want %v", err, ErrNotRegistered)
		}
		if got.Address != grpcdAddress {
			t.Errorf("address = %q, want %q", got.Address, grpcdAddress)
		}
		if got.State != diagnostics.StateUnreachable {
			t.Errorf("state = %q, want %q", got.State, diagnostics.StateUnreachable)
		}
		if want := string(health.StatusUnknown); got.Serving != want {
			t.Errorf("serving = %q, want %q", got.Serving, want)
		}
		if got.LastChecked == 0 {
			t.Error("last checked was not recorded")
		}
		if got.Details == nil {
			t.Error("details map is nil")
		}
	})

	t.Run("reports grpcd serving while the stream is held", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		client := newClient(&grpcdStub{}, []string{method})

		held := make(chan bool, 2)
		client.onHeld = func(h bool) { held <- h }

		done := make(chan struct{})
		go func() {
			client.Register(ctx)
			close(done)
		}()

		select {
		case <-held:
		case <-t.Context().Done():
			t.Fatal("the stream was never held")
		}

		got, err := client.Check(t.Context())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got.State != diagnostics.StateReachable {
			t.Errorf("state = %q, want %q", got.State, diagnostics.StateReachable)
		}
		if want := string(health.StatusServing); got.Serving != want {
			t.Errorf("serving = %q, want %q", got.Serving, want)
		}

		// Ending the registration clears it.
		cancel()
		<-done

		if _, err := client.Check(t.Context()); !errors.Is(err, ErrNotRegistered) {
			t.Fatalf("error = %v, want %v after the stream ended", err, ErrNotRegistered)
		}
	})
}
