package discover

import (
	"context"

	"connectrpc.com/connect/v2/connecthttp"

	foundationclient "github.com/pbrpc/connect-foundation/client"
	"github.com/pbrpc/connect-service/health"
)

// Probe reports whether address can be reached. It is the client's own probe,
// from the client's own network position: grpcd never dials anything, and an
// address is reported dead only because a probe of it failed.
type Probe func(ctx context.Context, address string) error

// NewProbe answers with the Probe production uses: the health probe every
// Connect service serves, asked over httpClient. A candidate passes when it
// answers that probe with a status, serving or not; one that cannot be
// reached, or answers with anything else, fails. nil means the foundation's
// standard HTTP client, which is how a test substitutes one that dials
// nothing.
func NewProbe(httpClient connecthttp.HTTPClient) Probe {
	if httpClient == nil {
		httpClient = foundationclient.NewHTTPClient(nil)
	}

	return func(ctx context.Context, address string) error {
		_, err := health.Check(ctx, httpClient, address)

		return err
	}
}
