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

- **`client/`** - Client SDK for method registration, and the connection every
  call to grpcd goes over
- **`discover/`** - Transport that keeps a Connect client pointed at a
  discovered upstream

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

`client.Connect` builds the one `*connect.Client` every call to grpcd goes
over: the foundation's standard HTTP client, speaking the gRPC protocol grpcd
serves, under the foundation's ready transport, so a grpcd that cannot be
reached is retried on a backoff schedule rather than on every call. The
generated `grpcdconnect.NewGRPCDServiceClient` is built on it and shared by the
registration and every discovery.

### Registering

`client.New` takes the `grpcdconnect.GRPCDServiceClient` rather than an
address, so the connection is yours to build and a test can supply a fake. It
also takes your listener's address: grpcd reads the IP off the connection and
cannot see the port you are serving on, so the port half comes from there. The
grpcd address is what the diagnostics entry reports.

`Register` opens the registration stream and holds it. The stream is the
registration — grpcd writes the rows when it opens and removes them when it
ends — so there is no interval to refresh and nothing to deregister on the way
out. A broken stream is reopened, paced by the ready transport.

Given an empty method list there is nothing to register, so `Register` logs
that and returns rather than holding a stream that claims otherwise.

### Reaching an Upstream

A service that depends on another grpcd-registered service holds one Connect
client to it for the life of the process. The `discover` package supplies the
transport under that client: it asks grpcd for the method, probes each
candidate from the service's own network position, reports the ones it cannot
reach, and sends every request to the one it can. When the replica stops
answering at the transport, the next request rediscovers, and one whose body
can be sent again is sent to the new replica without the caller seeing the
change. The application holds a plain client and never sees an address.

`discover.New` is built once per process on the same grpcd client the
registration uses. Each upstream is one `Upstream`, named by one of its methods
(a replica registers every method of its service, so one stands for the whole).
Its `HTTPClient()` and `BaseURL()` go to `foundationclient.New` like any other
client and URL. Discovery runs on the first request, and a request that finds
nothing fails; the next one asks again.

The upstream also holds a `Watch` naming the address it took. When a replica of
the upstream registers later, grpcd tells a share of the holders to move to it;
the upstream probes the new address, opens a `Watch` naming it, and sends the
requests that follow there. A new replica takes its share of existing clients
that way, and a move that cannot be reached is a no-op.

### Reporting Dependencies

Both are dependencies the service's diagnostics should report. grpcd goes in
under `client.CheckName` as the registration's own `Check`: while the stream is
held, grpcd accepted this service's registration and the connection is alive,
which is more than a health call could say. The upstream goes in through
`diagnostics.NewUpstreamCheck` with the upstream's `HTTPClient()`, which
probes and reports the replica the client is on.

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
