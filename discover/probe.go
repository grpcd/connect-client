package discover

import (
	"context"
	"fmt"
	"io"
	"net/http"

	"connectrpc.com/connect/v2/connecthttp"
)

// Probe reports whether address serves method. It is the client's own probe,
// from the client's own network position: grpcd never dials anything, and an
// address is reported dead only because a probe of it failed.
type Probe func(ctx context.Context, method, address string) error

// NewProbe answers with the Probe production uses, asked over httpClient: an
// OPTIONS request to the method's path at the address. A Connect server
// answers it before reading anything, 405 with Allow on a procedure it
// mounts and 404 on one it does not, so the request confirms the method
// without running it. A candidate passes on the 405; one that cannot be
// reached, or answers with anything else, fails. nil means the foundation's
// standard HTTP client, which is how a test substitutes one that dials
// nothing.
func NewProbe(httpClient connecthttp.HTTPClient) Probe {
	return func(ctx context.Context, method, address string) error {
		request, err := http.NewRequestWithContext(ctx, http.MethodOptions, "http://"+address+method, nil)
		if err != nil {
			return err
		}

		response, err := httpClient.Do(request)
		if err != nil {
			return err
		}
		defer response.Body.Close()

		_, _ = io.Copy(io.Discard, response.Body)

		if response.StatusCode != http.StatusMethodNotAllowed {
			return fmt.Errorf("%s answered %s with %s", address, method, response.Status)
		}

		return nil
	}
}
