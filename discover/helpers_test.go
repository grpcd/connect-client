package discover

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"testing"

	"connectrpc.com/connect/v2"
	"connectrpc.com/connect/v2/connectinprocess"
	"google.golang.org/protobuf/proto"

	"github.com/pbrpc/connect-testing/mocks/transport"

	grpcd "github.com/grpcd/protos"
	"github.com/grpcd/protos/grpcdconnect"
)

const (
	method  = "/pkg.Service/Method"
	service = "pkg.Service"
)

// discoverScript is one Discover stream's answers: the candidates offered in
// order, and how the stream ends once they are spent. A stream with a nil
// end waits for the client to close it, the way grpcd waits when it has
// nothing more to offer.
type discoverScript struct {
	candidates []string
	end        error
}

// grpcdStub stands in for grpcd. Discover hands out the scripted streams in
// order, repeating the last, and records every dead report. Watch hands out a
// stream fed from moves, which the test writes to. The embedded handler
// supplies Register, which discovery never calls.
type grpcdStub struct {
	grpcdconnect.UnimplementedGRPCDServiceHandler

	scripts []discoverScript

	// discoverErr fails every Discover before any candidate is offered.
	discoverErr error

	// onWatch runs on every Watch with the call's ordinal; an error it answers
	// with fails that Watch. nil accepts every one.
	onWatch func(n int) error

	// moves feeds every open Watch stream: an address sent here is delivered
	// to the client as a move. nil never moves.
	moves chan string

	// endWatch ends the open Watch stream cleanly when a value is sent, so a
	// test can see the client reopen it. nil never ends one.
	endWatch chan struct{}

	// moveSent receives one value per move the client has taken from the
	// stream: the transport hands messages over unbuffered, so Send returning
	// is the client's Receive having returned. Buffered; a test that sets it
	// reads it.
	moveSent chan struct{}

	// watchOpened receives one value per Watch opened, and watchEnded one per
	// Watch returned, so a test can wait for either. A move has landed once
	// the old address's watch has ended, since the swap precedes stopping it.
	// Buffered; a test that sets them reads them.
	watchOpened chan struct{}
	watchEnded  chan struct{}

	// discoverOpened receives one value per Discover opened, so a test can
	// act while a resolution is in progress. Buffered; a test that sets it
	// reads it.
	discoverOpened chan struct{}

	mu        sync.Mutex
	discovers int
	waited    []bool
	dead      []string
	watched   []string
}

func (s *grpcdStub) Discover(ctx context.Context, stream grpcdconnect.GRPCDServiceDiscoverServerStream) error {
	s.mu.Lock()
	s.discovers++
	script := s.scripts[min(s.discovers, len(s.scripts))-1]
	s.mu.Unlock()

	signal(s.discoverOpened)

	if s.discoverErr != nil {
		return s.discoverErr
	}

	// The method, then verdicts on each candidate.
	request, err := stream.Receive()
	if err != nil {
		return err
	}

	s.mu.Lock()
	s.waited = append(s.waited, !request.GetNoWait())
	s.mu.Unlock()

	for _, candidate := range script.candidates {
		if err := stream.Send(&grpcd.DiscoverResponse{Address: candidate}); err != nil {
			return err
		}

		report, err := stream.Receive()
		if errors.Is(err, io.EOF) {
			// The client closed the stream: the candidate worked.
			return nil
		}
		if err != nil {
			return err
		}

		s.mu.Lock()
		s.dead = append(s.dead, report.GetDeadAddress())
		s.mu.Unlock()
	}

	if script.end != nil {
		return script.end
	}

	// Nothing left to offer: a caller that declined to wait is told so, the
	// way grpcd tells it; any other waits for a registration, which here is
	// the client giving up.
	if request.GetNoWait() {
		return connect.NewError(connect.CodeNotFound, "nothing serves the method")
	}

	<-ctx.Done()

	return nil
}

// waits answers with whether each Discover asked to wait, in order.
func (s *grpcdStub) waits() []bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	return append([]bool(nil), s.waited...)
}

func (s *grpcdStub) Watch(
	ctx context.Context, request *grpcd.WatchRequest, stream grpcdconnect.GRPCDServiceWatchServerStream,
) error {
	s.mu.Lock()
	s.watched = append(s.watched, request.GetAddress())
	n := len(s.watched)
	s.mu.Unlock()

	defer signal(s.watchEnded)

	if s.onWatch != nil {
		if err := s.onWatch(n); err != nil {
			return err
		}
	}

	signal(s.watchOpened)

	for {
		select {
		case next := <-s.moves:
			if err := stream.Send(&grpcd.WatchResponse{Address: next}); err != nil {
				return err
			}

			signal(s.moveSent)
		case <-s.endWatch:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// signal sends on ch without waiting; nil or full means nobody is listening.
func signal(ch chan struct{}) {
	if ch == nil {
		return
	}

	select {
	case ch <- struct{}{}:
	default:
	}
}

// reported answers with the addresses reported dead, in order.
func (s *grpcdStub) reported() []string {
	s.mu.Lock()
	defer s.mu.Unlock()

	return append([]string(nil), s.dead...)
}

// watchedAddresses answers with the address named by each Watch, in order.
func (s *grpcdStub) watchedAddresses() []string {
	s.mu.Lock()
	defer s.mu.Unlock()

	return append([]string(nil), s.watched...)
}

// discoveries answers with how many Discover streams were opened.
func (s *grpcdStub) discoveries() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.discovers
}

// newService registers stub on a dispatcher and answers with a generated
// client whose transport calls that dispatcher directly: plain function
// calls, no listener, no network.
func newService(stub *grpcdStub) grpcdconnect.GRPCDServiceClient {
	rpc := connect.NewServer()
	grpcdconnect.RegisterGRPCDServiceHandler(rpc, stub)

	return grpcdconnect.NewGRPCDServiceClient(connect.NewClient(connectinprocess.New(rpc)))
}

// probeStub fails the addresses named in dead and passes every other.
func probeStub(dead ...string) Probe {
	return func(_ context.Context, address string) error {
		for _, d := range dead {
			if d == address {
				return errors.New("unreachable")
			}
		}

		return nil
	}
}

var errUnreachable = errors.New("connection refused")

// baseStub stands in for the transport under the upstream. It answers every
// request with 200 and the host it was sent to as the body, unless the host
// is in down, which fails at the transport. It records the hosts in order.
type baseStub struct {
	down map[string]bool

	mu    sync.Mutex
	hosts []string
}

func newBaseStub(down ...string) *baseStub {
	s := &baseStub{down: map[string]bool{}}
	for _, host := range down {
		s.down[host] = true
	}

	return s
}

func (s *baseStub) RoundTrip(request *http.Request) (*http.Response, error) {
	host := request.URL.Host

	s.mu.Lock()
	s.hosts = append(s.hosts, host)
	s.mu.Unlock()

	if s.down[host] {
		return nil, errUnreachable
	}

	// Whatever body arrived is drained, the way a real transport sends it.
	if request.Body != nil {
		_, _ = io.Copy(io.Discard, request.Body)
	}

	return &http.Response{
		StatusCode: http.StatusOK,
		Body:       io.NopCloser(strings.NewReader(host)),
		Request:    request,
	}, nil
}

// sentTo answers with the hosts requests were sent to, in order.
func (s *baseStub) sentTo() []string {
	s.mu.Lock()
	defer s.mu.Unlock()

	return append([]string(nil), s.hosts...)
}

// newDiscovery builds a Discovery on stub, probing with probe and sending
// over base, under ctx as the process context.
func newDiscovery(ctx context.Context, stub *grpcdStub, probe Probe, base http.RoundTripper) *Discovery {
	return New(ctx, slog.New(slog.DiscardHandler), newService(stub), probe, base)
}

// newUpstream builds the upstream for method on stub, probing with probe and
// sending over base, under ctx as the process context.
func newUpstream(ctx context.Context, stub *grpcdStub, probe Probe, base http.RoundTripper) *Upstream {
	return newDiscovery(ctx, stub, probe, base).upstream(method)
}

// newCall builds a request for method, addressed the way a Connect client
// built against BaseURL addresses it. body nil is a request that cannot be
// sent twice; any other body can be, the way net/http sets GetBody for one it
// can rewind.
func newCall(ctx context.Context, t *testing.T, body io.Reader) *http.Request {
	t.Helper()

	request, err := http.NewRequestWithContext(ctx, http.MethodPost, URL(method), body)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	return request
}

// call sends a resendable request for method through rt and answers with the
// host that answered, read from the body.
func call(ctx context.Context, t *testing.T, rt http.RoundTripper) (string, error) {
	t.Helper()

	return send(t, rt, newCall(ctx, t, strings.NewReader("request")))
}

// send sends request through rt and answers with the host that answered,
// read from the body.
func send(t *testing.T, rt http.RoundTripper, request *http.Request) (string, error) {
	t.Helper()

	response, err := rt.RoundTrip(request)
	if err != nil {
		return "", err
	}
	defer response.Body.Close()

	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	return string(body), nil
}

// discoverStream builds a Discover stream stub offering candidates in order,
// whose sends fail with sendErr after failAfter successes.
func discoverStream(sendErr error, failAfter int, candidates ...string) *transport.Stream {
	messages := make([]proto.Message, 0, len(candidates))
	for _, candidate := range candidates {
		messages = append(messages, &grpcd.DiscoverResponse{Address: candidate})
	}

	return transport.NewStream(sendErr, failAfter, messages...)
}

// newStubbedUpstream builds an upstream whose grpcd client dispatches over
// tp rather than a handler, for the failures a handler cannot produce.
func newStubbedUpstream(ctx context.Context, tp connect.Transport) *Upstream {
	service := grpcdconnect.NewGRPCDServiceClient(connect.NewClient(tp))

	return New(ctx, slog.New(slog.DiscardHandler), service, probeStub(), newBaseStub()).upstream(method)
}

// await blocks until signal fires, failing the test if the test's own
// context ends first.
func await(t *testing.T, signal <-chan struct{}, message string) {
	t.Helper()

	select {
	case <-signal:
	case <-t.Context().Done():
		t.Fatal(message)
	}
}
