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

	mu        sync.Mutex
	discovers int
	dead      []string
	watched   []string
}

func (s *grpcdStub) Discover(ctx context.Context, stream grpcdconnect.GRPCDServiceDiscoverServerStream) error {
	s.mu.Lock()
	s.discovers++
	script := s.scripts[min(s.discovers, len(s.scripts))-1]
	s.mu.Unlock()

	if s.discoverErr != nil {
		return s.discoverErr
	}

	// The method, then verdicts on each candidate.
	if _, err := stream.Receive(); err != nil {
		return err
	}

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

	<-ctx.Done()

	return nil
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

// streamStub stands in for a Discover stream whose sends fail: the first
// after failAfter sends. Receive offers candidates in order and then blocks
// until the stream is closed.
type streamStub struct {
	candidates []string
	failAfter  int
	sendErr    error

	mu       sync.Mutex
	sends    int
	receives int
	closed   chan struct{}
}

func newStreamStub(sendErr error, failAfter int, candidates ...string) *streamStub {
	return &streamStub{candidates: candidates, failAfter: failAfter, sendErr: sendErr, closed: make(chan struct{})}
}

func (s *streamStub) SendHeaders() error { return nil }

func (s *streamStub) Send(any) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.sends++
	if s.sends > s.failAfter {
		return s.sendErr
	}

	return nil
}

func (s *streamStub) CloseSend() error { return nil }

func (s *streamStub) Receive(msg any) error {
	s.mu.Lock()
	n := s.receives
	s.receives++
	s.mu.Unlock()

	if n < len(s.candidates) {
		msg.(*grpcd.DiscoverResponse).Address = s.candidates[n]

		return nil
	}

	<-s.closed

	return io.EOF
}

func (s *streamStub) Close() error {
	select {
	case <-s.closed:
	default:
		close(s.closed)
	}

	return nil
}

// transportStub opens streams through open, which sees each call's ordinal
// and context, so a test scripts the Discover open and the Watch opens after
// it separately.
type transportStub struct {
	open func(n int, ctx context.Context) (connect.ClientStream, error)

	mu    sync.Mutex
	calls int
}

func (t *transportStub) NewClientStream(ctx context.Context, _ connect.Spec) (connect.ClientStream, error) {
	t.mu.Lock()
	t.calls++
	n := t.calls
	t.mu.Unlock()

	return t.open(n, ctx)
}

// once answers every open with the same stream, or the same error.
func once(stream connect.ClientStream, err error) *transportStub {
	return &transportStub{open: func(int, context.Context) (connect.ClientStream, error) {
		return stream, err
	}}
}

// newStubbedUpstream builds an upstream whose grpcd client dispatches over
// transport rather than a handler, for the failures a handler cannot produce.
func newStubbedUpstream(ctx context.Context, transport connect.Transport) *Upstream {
	service := grpcdconnect.NewGRPCDServiceClient(connect.NewClient(transport))

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
