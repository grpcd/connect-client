# `grpcd` Connect Client

Go client library for the [grpcd](https://github.com/grpcd) method discovery
system, for services built on connect-go.

## About

This library provides the Go implementation for interfacing with grpcd, a
system that maps method names to network addresses. Services register the
methods they implement; clients query grpcd to discover where those methods are
available.

For system architecture, protocol definitions, and design documentation, see
[grpcd-protos](https://github.com/grpcd/protos).

## Installation

```bash
go get github.com/grpcd/connect-client
```

## What's Included

- **`client/`** - The connection every call to grpcd goes over, method
  registration, and grpcd's health as a dependency check
- **`discover/`** - Transports that resolve `grpcd:///` URLs to the replica
  discovered for the procedure they name, per request or held

The endpoints a service exposes and the method list it advertises come from
[connect-service](https://github.com/pbrpc/connect-service); this library takes
that list and registers it.

## Usage

`client/example_test.go` holds the whole sequence as a Go `Example`: assembling
the server, connecting to grpcd only when `GRPCD_ADDRESS` is set, reaching an
upstream, reporting both as dependencies, and holding the registration. It has
no `// Output:` comment, so `go test` compiles it and never runs it, which keeps
it type-checked against the real API. Read it there rather than from a copy
here.

### Connecting

`client.Connect` reads `GRPCD_ADDRESS` and builds the one connection every
call to grpcd goes over: the foundation's standard HTTP client, speaking the
gRPC protocol grpcd serves, under the foundation's ready transport, so a grpcd
that cannot be reached is retried on a backoff schedule rather than on every
call. It is the only place the variable is read; with it unset, `Connect`
answers nil and the service decides whether to serve without grpcd. The
`*client.Connection` is the generated `grpcdconnect.GRPCDServiceClient`, so it
goes wherever one is taken, and knows the address it was built for.

### Registering

`client.New` takes the connection and your listener's address: grpcd reads the
IP off the connection and cannot see the port you are serving on, so the port
half comes from there. A test supplies any generated grpcd client in the
connection's place.

`Register` opens the registration stream and holds it. The stream is the
registration — grpcd writes the rows when it opens and removes them when it
ends — so there is no interval to refresh and nothing to deregister on the way
out. A broken stream is reopened, paced by the ready transport.

Given an empty method list there is nothing to register, so `Register` logs
that and returns rather than holding a stream that claims otherwise.

### Reaching an Upstream

A service that depends on grpcd-registered methods addresses them by the
`grpcd:///` scheme: `grpcd:///package.Service/Method` names a method as it was
registered, with no authority, and nothing in it is ever dialed as written.
`discover.URL` builds one from a generated procedure constant.

Resolving the scheme is the `Discover` loop and nothing else: ask grpcd for
the method in the path, probe each candidate from the service's own network
position, report the ones it cannot reach, close the stream on the one it
can, and send there. `discover.New` is built once per process on the
connection `Connect` answered with, and offers that resolution two ways, both
`http.RoundTripper`s:

- The `Discovery` itself resolves every request on its own and keeps nothing,
  so each request lands where grpcd sends it. It asks grpcd not to wait: a
  request naming a method nothing serves fails with grpcd's `NotFound` at
  once. A gateway forwards through it.
- `discovery.Held()` resolves a method on the first request for it, holds the
  replica, and sends every later request for that method there. It waits for
  a method nothing serves yet, since the dependency is the caller's to have.
  A service reaches its dependencies through it:

```go
discovery := discover.New(serveCtx, log, conn, discover.NewProbe(nil), nil)
httpClient := foundationclient.NewHTTPClient(discovery.Held())
upstream := upstreamconnect.NewUpstreamServiceClient(foundationclient.New(httpClient, discover.BaseURL, nil))
```

A held replica that stops answering at the transport is dropped; the next
request resolves again, and one whose body can be sent again is sent to the
new replica without the caller seeing the change. A request to any other URL
goes over the standard transport untouched through either transport, so the
same client reaches a fixed `http://host:port` too. The application holds
plain clients and never sees an address.

`Watch` is what holding adds. A held method has a `Watch` naming the address
it took, opened before the `Discover` that gave it closes, so no registration
falls between the two. When a replica of the service registers later, grpcd
tells a share of the holders to move to it; the method probes the new
address, opens a `Watch` naming it, and sends the requests that follow there.
A new replica takes its share of existing clients that way, and a move that
cannot be reached is a no-op.

`discovery.Upstream(url)` names one held method: its `Address()` is what
diagnostics report, and `Resolve(ctx)` holds it before the first request, for
a process that wants a method's `Discover` and `Watch` open from startup.

### Reporting Dependencies

Both are dependencies the service's diagnostics should report. grpcd goes in
under `client.CheckName` as `client.Check(conn)`, which asks grpcd's own health
procedure over the same connection everything else uses. An upstream goes in
through `diagnostics.NewUpstreamCheck` with the HTTP client above and
`discovery.Upstream(url)`, which probes and reports the replica that method is
on.

## Configuration

- `GRPCD_ADDRESS` - Address of the grpcd service (e.g., `grpcd.example.com:443`).
  When unset, the service neither registers nor discovers, and serves anyway.

### Method Names

Methods are wire format:

```
/package.Service/Method
```

Examples:

- `/auth.AuthService/Login`
- `/api.v1.UserService/GetUser`

`service.Register` in connect-service derives these names from what is
registered on your server, so you do not maintain the list by hand.
