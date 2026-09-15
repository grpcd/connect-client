package discover

import (
	"log/slog"
	"net/http"
	"testing"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

func TestNew(t *testing.T) {
	t.Run("substitutes a logger and a probe when given none", func(t *testing.T) {
		d := New(t.Context(), nil, newService(&grpcdStub{}), nil)

		if d.log == nil {
			t.Fatal("expected a logger")
		}
		if d.probe == nil {
			t.Fatal("expected a probe")
		}
	})
}

func TestUpstream(t *testing.T) {
	t.Run("substitutes the standard transport when given none", func(t *testing.T) {
		u := New(t.Context(), slog.New(slog.DiscardHandler), newService(&grpcdStub{}), probeStub()).Upstream(method, nil)

		base, ok := u.base.(*http.Transport)
		if !ok {
			t.Fatalf("base = %T, want the standard transport", u.base)
		}
		if !base.Protocols.UnencryptedHTTP2() {
			t.Errorf("protocols = %v, want cleartext HTTP/2", base.Protocols)
		}
	})

	t.Run("builds its client on itself under tracing", func(t *testing.T) {
		u := newUpstream(t.Context(), &grpcdStub{}, probeStub(), newBaseStub())

		if _, ok := u.HTTPClient().Transport.(*otelhttp.Transport); !ok {
			t.Fatalf("transport = %T, want the tracing transport", u.HTTPClient().Transport)
		}
	})

	t.Run("names the service in its base URL", func(t *testing.T) {
		u := newUpstream(t.Context(), &grpcdStub{}, probeStub(), newBaseStub())

		if got := u.BaseURL(); got != "http://"+service {
			t.Errorf("base URL = %q, want http://%s", got, service)
		}
	})

	t.Run("reports no address before anything is discovered", func(t *testing.T) {
		u := newUpstream(t.Context(), &grpcdStub{}, probeStub(), newBaseStub())

		if got := u.Address(); got != "" {
			t.Errorf("address = %q, want none", got)
		}
	})
}

func TestServiceName(t *testing.T) {
	cases := map[string]string{
		"/pkg.Service/Method": service,
		"pkg.Service/Method":  service,
		"/pkg.Service":        service,
		"":                    "",
	}

	for method, want := range cases {
		t.Run(method, func(t *testing.T) {
			if got := serviceName(method); got != want {
				t.Errorf("serviceName = %q, want %q", got, want)
			}
		})
	}
}
