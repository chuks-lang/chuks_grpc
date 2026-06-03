# chuks_grpc

Wire-compatible **gRPC** for Chuks — a pure-Chuks implementation of the full
gRPC transport stack:

- **HTTP/2** — framing, SETTINGS, flow control, PING, GOAWAY, over cleartext
  (h2c) **or TLS / mTLS**
- **HPACK** — header compression (static + dynamic table, Huffman decode)
- **Protocol Buffers** — varint / zig-zag / fixed32 / fixed64 / length-delimited
- **gRPC framing** — 5-byte length-prefixed messages, `grpc-status` trailers

On top of the transport it implements the gRPC feature set:

- **Unary and streaming** RPCs — server-streaming, client-streaming, and
  bidirectional
- **Deadlines / timeouts**, **retries** with backoff, and client-side
  **load balancing** (pick-first, round-robin)
- **Metadata**, **per-message compression**, and configurable
  **message-size limits**
- **Health checking**, **server reflection** (`list_services`), **stats
  handlers**, and the **rich error model** (`google.rpc.Status` details)

It interoperates with standard gRPC tooling. The examples below are verified
against [`grpcurl`](https://github.com/fullstorydev/grpcurl) calling a Chuks
server over a real gRPC connection (see [`tests/interop`](tests/interop)).

## Install

```
chuks add chuks_grpc
```

## Quick start

### Server

```chuks
import { Buffer } from "std/buffer"
import { GrpcServer, GrpcRequest, GrpcResponse, grpcOk } from "chuks_grpc"
import { ProtoWriter, ProtoReader, ProtoTag, PB_LEN } from "chuks_grpc/src/protobuf.chuks"

// message EchoRequest  { string name  = 1; }
// message EchoResponse { string reply = 1; }

function sayHandler(req: GrpcRequest): GrpcResponse {
    // Decode field 1 (string) from the request message.
    var r: ProtoReader = new ProtoReader(req.data)
    var name: string = ""
    while (r.hasMore()) {
        var tag: ProtoTag = r.readTag()
        if (tag.field == 1 && tag.wireType == PB_LEN) {
            name = r.readString()
        } else {
            r.skip(tag.wireType)
        }
    }

    // Encode the reply message.
    var w: ProtoWriter = new ProtoWriter()
    w.writeString(1, "hello, " + name)
    return grpcOk(w.toBuffer())
}

var srv: GrpcServer = new GrpcServer("127.0.0.1", 50151)
srv.register("/echo.Echo/Say", sayHandler)
println("listening on " + srv.addr())
srv.serve()
```

Call it with `grpcurl`:

```sh
grpcurl -plaintext \
    -import-path examples -proto echo.proto \
    -d '{"name":"world"}' \
    127.0.0.1:50151 echo.Echo/Say
# { "reply": "hello, world" }
```

### Client

```chuks
import { GrpcClient, GrpcReply, STATUS_OK } from "chuks_grpc"
import { ProtoWriter, ProtoReader, ProtoTag, PB_LEN } from "chuks_grpc/src/protobuf.chuks"

var req: ProtoWriter = new ProtoWriter()
req.writeString(1, "world")

var client: GrpcClient = new GrpcClient("127.0.0.1", 50151)
var reply: GrpcReply = client.unary("/echo.Echo/Say", req.toBuffer())

if (reply.status == STATUS_OK) {
    var r: ProtoReader = new ProtoReader(reply.data)
    while (r.hasMore()) {
        var tag: ProtoTag = r.readTag()
        if (tag.field == 1 && tag.wireType == PB_LEN) {
            println(r.readString())   // "hello, world"
        } else {
            r.skip(tag.wireType)
        }
    }
}
client.close()
```

## How it fits together

A gRPC call is just an HTTP/2 request to a **method path** (`/package.Service/Method`)
whose request and response bodies are length-prefixed **Protocol Buffers**
messages. chuks_grpc gives you the transport and the call semantics; **you encode
and decode the message bytes yourself** with `ProtoWriter` / `ProtoReader` (there
is no `.proto` codegen).

- **Server**: construct a `GrpcServer`, `register*` one handler per method, then
  `serve()`.
- **Client**: construct a `GrpcClient` (or a load-balanced `GrpcChannel`), then
  call `unary` / `serverStream` / `clientStream` / `bidiStream`.
- **Status**: every reply carries a `status` (`STATUS_OK` == success), a
  `message`, and optional structured `details`.

### Shared message helpers

`encStr` / `decStr` are **your own helper functions** (not part of the package) —
they encode and decode the protobuf message `{ string value = 1; }` using
`ProtoWriter` / `ProtoReader`. Define them once; **every snippet below reuses
them** (and assumes `Buffer` is imported), so they aren't repeated each time:

```chuks
import { Buffer } from "std/buffer"
import { ProtoWriter, ProtoReader, ProtoTag, PB_LEN } from "chuks_grpc/src/protobuf.chuks"

// Encode a single string field (field number 1).
function encStr(s: string): Buffer {
    var w: ProtoWriter = new ProtoWriter()
    w.writeString(1, s)
    return w.toBuffer()
}

// Decode field 1 (string) from a message; returns "" if absent.
function decStr(data: Buffer): string {
    var r: ProtoReader = new ProtoReader(data)
    var out: string = ""
    while (r.hasMore()) {
        var tag: ProtoTag = r.readTag()
        if (tag.field == 1 && tag.wireType == PB_LEN) {
            out = r.readString()
        } else {
            r.skip(tag.wireType)
        }
    }
    return out
}
```

`ProtoWriter` / `ProtoReader` also handle `int32`/`int64`/`bool`/`bytes`/nested
messages/`repeated` fields — see [`src/protobuf.chuks`](src/protobuf.chuks) for
the full wire-type API.

> **Running a server.** `srv.serve()` blocks forever accepting connections. In a
> real program that is your main loop. In tests (where client and server share a
> process) spawn it as a task and use `serveOnce()` to handle exactly one
> connection:
>
> ```chuks
> async function runServer(srv: GrpcServer): Task<void> { srv.serveOnce() }
> var t: Task<void> = spawn runServer(srv)
> // ... drive the client ...
> await t
> ```

## Unary RPC

One request, one response — covered in [Quick start](#quick-start) above. The
client offers timeout and metadata variants:

```chuks
var reply: GrpcReply = client.unary("/echo.Echo/Say", encStr("world"))
var reply2: GrpcReply = client.unaryWithin("/echo.Echo/Say", encStr("world"), 2000) // 2s deadline
```

## Server streaming

One request fans out into a stream of responses. The handler receives the request
plus a `GrpcServerStream` and calls `send()` for each message:

```chuks
import { GrpcServer, GrpcRequest, GrpcServerStream } from "chuks_grpc"

// Server: "/echo.Echo/Count" emits name0, name1, name2.
function countHandler(req: GrpcRequest, stream: GrpcServerStream): void {
    var name: string = decStr(req.data)
    var i: int = 0
    while (i < 3) {
        stream.send(encStr(name + string(i)))
        i = i + 1
    }
    // Returning ends the stream with STATUS_OK. To fail, call
    // stream.finish(STATUS_INTERNAL, "reason") instead.
}

var srv: GrpcServer = new GrpcServer("127.0.0.1", 50151)
srv.registerServerStream("/echo.Echo/Count", countHandler)
srv.serve()
```

```chuks
import { GrpcClient, GrpcClientCall, GrpcReply, STATUS_OK } from "chuks_grpc"

// Client: drain messages until recv() returns null, then read the final status.
var client: GrpcClient = new GrpcClient("127.0.0.1", 50151)
var call: GrpcClientCall = client.serverStream("/echo.Echo/Count", encStr("x"))
var msg: Buffer? = call.recv()
while (msg != null) {
    println(decStr(msg))          // x0, x1, x2
    msg = call.recv()
}
var reply: GrpcReply = call.result()
println("status=" + string(reply.status))   // 0 (STATUS_OK)
client.close()
```

## Client streaming

The client sends a stream of requests and gets back one response. The handler
drains the inbound stream, then returns a single `GrpcResponse`:

```chuks
import { GrpcServer, GrpcServerStream, GrpcResponse, grpcOk } from "chuks_grpc"

// Server: "/echo.Echo/Collect" counts the requests it received.
function collectHandler(stream: GrpcServerStream): GrpcResponse {
    var count: int = 0
    var m: Buffer? = stream.recv()
    while (m != null) {
        count = count + 1
        m = stream.recv()
    }
    return grpcOk(encStr("count=" + string(count)))
}

var srv: GrpcServer = new GrpcServer("127.0.0.1", 50151)
srv.registerClientStream("/echo.Echo/Collect", collectHandler)
srv.serve()
```

```chuks
// Client: send N messages, closeSend(), then read the single reply.
var call: GrpcClientCall = client.clientStream("/echo.Echo/Collect")
call.send(encStr("a"))
call.send(encStr("b"))
call.send(encStr("c"))
call.closeSend()
var m: Buffer? = call.recv()
if (m != null) {
    println(decStr(m))            // count=3
}
var reply: GrpcReply = call.result()
client.close()
```

## Bidirectional streaming

Both sides stream independently over one call. The handler interleaves `recv()`
and `send()`:

```chuks
// Server: "/echo.Echo/Chat" echoes each request back as "echo:<value>".
function chatHandler(stream: GrpcServerStream): void {
    var m: Buffer? = stream.recv()
    while (m != null) {
        stream.send(encStr("echo:" + decStr(m)))
        m = stream.recv()
    }
}

srv.registerBidiStream("/echo.Echo/Chat", chatHandler)
```

```chuks
// Client: send all requests, closeSend(), then drain the responses.
var call: GrpcClientCall = client.bidiStream("/echo.Echo/Chat")
call.send(encStr("1"))
call.send(encStr("2"))
call.send(encStr("3"))
call.closeSend()
var m: Buffer? = call.recv()
while (m != null) {
    println(decStr(m))            // echo:1, echo:2, echo:3
    m = call.recv()
}
var reply: GrpcReply = call.result()
client.close()
```

## Metadata (custom headers)

Attach key/value headers to a call and read them on the server. Use `Metadata`
on the client and `metadataFromHeaders(req.metadata)` on the server:

```chuks
import { Metadata, metadataFromHeaders } from "chuks_grpc"

// Server: read an "authorization" header off the request.
function whoHandler(req: GrpcRequest): GrpcResponse {
    var md: Metadata = metadataFromHeaders(req.metadata)
    var token: string = md.get("authorization")   // "" if absent
    if (token == "") {
        return grpcError(STATUS_UNAUTHENTICATED, "missing token")
    }
    return grpcOk(encStr("hello " + token))
}
```

```chuks
// Client: send metadata with the call.
var md: Metadata = new Metadata()
md.set("authorization", "bearer abc123")
var reply: GrpcReply = client.unaryMeta("/echo.Echo/Who", encStr(""), md)

// Response headers/trailers come back on the reply.
var hdrs: Metadata = metadataFromHeaders(reply.headers)
var trls: Metadata = metadataFromHeaders(reply.trailers)
```

Inside a streaming handler, use `stream.requestMetadata()`,
`stream.setHeader(k, v)` and `stream.setTrailer(k, v)`. Streaming clients pass
metadata with `serverStreamMeta` / `clientStreamMeta` / `bidiStreamMeta`.

## Deadlines and timeouts

The client sends a deadline; the server sees it and can stop early. Use the
`*Within` call variants to set a per-call timeout in milliseconds:

```chuks
import { STATUS_DEADLINE_EXCEEDED } from "chuks_grpc"

// Client: fail the call if it takes longer than 500ms.
var reply: GrpcReply = client.unaryWithin("/echo.Echo/Slow", encStr("x"), 500)
if (reply.status == STATUS_DEADLINE_EXCEEDED) {
    println("timed out")
}
```

```chuks
// Server: inspect the incoming deadline.
function slowHandler(req: GrpcRequest): GrpcResponse {
    if (req.deadlineMs > 0) {
        println("client deadline at epoch-ms " + string(req.deadlineMs))
    }
    return grpcOk(encStr("done"))
}
```

In streaming handlers, check `stream.deadlineMs()`, `stream.deadlineExceeded()`,
and `stream.cancelled()` to bail out of long loops.

## Errors and the rich error model

Return `grpcError(status, message)` for a simple failure, or
`grpcErrorWithDetails(...)` to attach structured `google.rpc.Status` details that
travel in the `grpc-status-details-bin` trailer:

```chuks
import {
    grpcError, grpcErrorWithDetails, mkErrorDetail, ErrorDetail,
    STATUS_INVALID_ARGUMENT
} from "chuks_grpc"

function failHandler(req: GrpcRequest): GrpcResponse {
    // A google.rpc.ErrorInfo-style detail (any protobuf message works).
    var info: ProtoWriter = new ProtoWriter()
    info.writeString(1, "OUT_OF_RANGE")
    var details: []ErrorDetail = [
        mkErrorDetail("type.googleapis.com/google.rpc.ErrorInfo", info.toBuffer())
    ]
    return grpcErrorWithDetails(STATUS_INVALID_ARGUMENT, "bad input", details)
}
```

```chuks
// Client: read status, message, and decoded details off the reply.
var reply: GrpcReply = client.unary("/echo.Echo/Fail", encStr("x"))
if (reply.status != STATUS_OK) {
    println("error " + string(reply.status) + ": " + reply.message)
    var ds: []ErrorDetail = reply.details
    var i: int = 0
    while (i < ds.length) {
        println("  detail: " + ds[i].typeUrl)
        i = i + 1
    }
}
```

All status codes are exported as `STATUS_*` (`STATUS_OK`, `STATUS_NOT_FOUND`,
`STATUS_UNAVAILABLE`, `STATUS_UNAUTHENTICATED`, `STATUS_RESOURCE_EXHAUSTED`, …).

## Automatic retries

Enable client-side retries for transient failures. The default policy retries
`UNAVAILABLE` up to 3 times with exponential backoff; each attempt is a fresh
RPC:

```chuks
import { defaultRetryPolicy, RetryPolicy, STATUS_UNAVAILABLE } from "chuks_grpc"

client.setRetryPolicy(defaultRetryPolicy())

// Or a custom policy:
var codes: []int = [STATUS_UNAVAILABLE]
var policy: RetryPolicy = {
    maxAttempts: 5,
    initialBackoffMs: 50,
    maxBackoffMs: 1000,
    backoffMultiplier: 2,
    retryableStatuses: codes
}
client.setRetryPolicy(policy)
// client.disableRetries() turns it back off.
```

Retries apply to unary calls (streaming calls are not auto-retried).

## Compression

Opt into gzip. The server compresses responses for clients that advertise gzip;
the client compresses requests when you set its codec:

```chuks
import { ENCODING_GZIP } from "chuks_grpc"

srv.enableCompression()              // server: gzip responses when negotiated
client.setCompression(ENCODING_GZIP) // client: gzip outbound requests
```

## Message-size limits

Cap inbound message sizes to protect against unbounded memory use. Oversized
messages are rejected with `STATUS_RESOURCE_EXHAUSTED` (default cap 4 MiB; pass
`0` to disable):

```chuks
srv.setMaxRecvMessageBytes(1048576)     // server: reject requests > 1 MiB
client.setMaxRecvMessageBytes(1048576)  // client: reject responses > 1 MiB
```

## TLS

Serve over encrypted HTTP/2 by passing TLS options to the server, and have the
client trust the server's CA. (`tlsSelfSigned` from `std/net` is handy for local
testing; in production load real cert/key/CA bytes.)

```chuks
import { tlsSelfSigned } from "std/net"

var certs: any = tlsSelfSigned(["localhost", "127.0.0.1"])

// Server: present a certificate.
var srvOpts: any = { "cert": certs["cert"], "key": certs["key"] }
var srv: GrpcServer = new GrpcServer("127.0.0.1", 50151, srvOpts)
srv.register("/echo.Echo/Say", sayHandler)
srv.serve()
```

```chuks
// Client: trust the server's CA (or use { "skipVerify": true } for dev only).
var client: GrpcClient = new GrpcClient("127.0.0.1", 50151, { "ca": certs["ca"] })
var reply: GrpcReply = client.unary("/echo.Echo/Say", encStr("tls"))
```

## Mutual TLS (mTLS)

Require the client to present its own certificate, and read its Common Name on
the server:

```chuks
// Server: require + verify client certs.
var srvOpts: any = {
    "cert": certs["cert"], "key": certs["key"],
    "clientCA": certs["ca"], "requireClientCert": true
}
var srv: GrpcServer = new GrpcServer("127.0.0.1", 50151, srvOpts)

// The verified peer CN is on the request (and stream.peerCommonName()).
function peerHandler(req: GrpcRequest): GrpcResponse {
    return grpcOk(encStr("cn=" + req.peerCN))
}
srv.register("/echo.Echo/Peer", peerHandler)
```

```chuks
// Client: present a certificate alongside trusting the server CA.
var cliOpts: any = { "ca": certs["ca"], "cert": certs["cert"], "key": certs["key"] }
var client: GrpcClient = new GrpcClient("127.0.0.1", 50151, cliOpts)
```

## Load balancing

`GrpcChannel` dials several endpoints and spreads calls across the reachable ones
(`LB_PICK_FIRST` for failover, `LB_ROUND_ROBIN` to rotate). The target is a
comma-separated `host:port` list; pass TLS options as the third argument or
`null` for h2c:

```chuks
import { GrpcChannel, LB_ROUND_ROBIN } from "chuks_grpc"

var ch: GrpcChannel = new GrpcChannel("127.0.0.1:50151,127.0.0.1:50152", LB_ROUND_ROBIN, null)
println("ready subchannels: " + string(ch.readyCount()))

var reply: GrpcReply = ch.unary("/echo.Echo/Say", encStr("balanced"))
var call: GrpcClientCall = ch.serverStream("/echo.Echo/Count", encStr("x"))
ch.close()
```

## Health checking

Expose the standard `grpc.health.v1.Health` service and flip serving status as
your dependencies come and go:

```chuks
import {
    HealthService, registerHealthService, HealthClient,
    SERVING_SERVING, SERVING_NOT_SERVING
} from "chuks_grpc"

// Server: advertise health.
var health: HealthService = new HealthService()
health.setServing("echo.Echo")          // mark a service healthy
// health.setNotServing("echo.Echo")    // ... or unhealthy
registerHealthService(srv, health)
```

```chuks
// Client: query it.
var hc: HealthClient = new HealthClient(client)
if (hc.isServing("echo.Echo")) {
    println("backend is serving")
}
var status: int = hc.check("echo.Echo")  // SERVING_SERVING / SERVING_NOT_SERVING / ...
```

## Server reflection

Let tools like `grpcurl` discover your services. Register a `ReflectionService`
seeded from the server's registered methods (`list_services` is supported;
descriptor lookups are not):

```chuks
import { ReflectionService, registerReflectionService, ReflectionClient } from "chuks_grpc"

// Server.
var refl: ReflectionService = new ReflectionService()
refl.addServerServices(srv)             // derive service names from registered methods
// refl.addService("calc.Math")         // ... or add names explicitly
registerReflectionService(srv, refl)
```

```chuks
// Client (or `grpcurl -plaintext host:port list`).
var rc: ReflectionClient = new ReflectionClient(client)
var services: []string = rc.listServices()
```

## Observability: stats handlers

Register a callback that fires once per finished RPC with timing and status, on
either the server or the client:

```chuks
import { RpcStats } from "chuks_grpc"

srv.useStatsHandler(function(s: RpcStats): void {
    println(s.method + " -> status " + string(s.status) + " in " + string(s.durationMs) + "ms")
})

client.useStatsHandler(function(s: RpcStats): void {
    println("client " + s.method + " took " + string(s.durationMs) + "ms")
})
```

## Interceptors

Wrap every handler with cross-cutting logic (auth, logging, metrics).
Interceptors run outermost-first; call `next(req)` to continue, or return a
response to short-circuit:

```chuks
// Server unary interceptor: reject calls without an auth header.
srv.useUnaryInterceptor(function(req: GrpcRequest, next: any): GrpcResponse {
    var md: Metadata = metadataFromHeaders(req.metadata)
    if (md.get("authorization") == "") {
        return grpcError(STATUS_UNAUTHENTICATED, "no token")
    }
    return next(req)
})
```

Use `useStreamInterceptor` for streaming methods, and the client's
`useUnaryInterceptor` to wrap outbound calls.

## Keepalive

Drop half-open connections by pinging idle peers. Configure on the server (per
accepted connection) and/or the client:

```chuks
srv.enableKeepalive(30000, 10000)     // ping every 30s, drop if no ack in 10s
client.enableKeepalive(30000, 10000)
```

## API

| Member                                  | Description                                                                                                                              |
| --------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------- |
| `new GrpcServer(host, port)`            | Bind a listener immediately. Pass port `0` for an OS-assigned port.                                                                      |
| `register(method, handler)`             | Register a unary handler for a method path, e.g. `"/pkg.Service/Method"`. Handler signature: `function(req: GrpcRequest): GrpcResponse`. |
| `registerServerStream(method, handler)` | Register a server-streaming handler: `function(req: GrpcRequest, stream: GrpcServerStream): void`.                                       |
| `registerClientStream(method, handler)` | Register a client-streaming handler: `function(stream: GrpcServerStream): GrpcResponse`.                                                 |
| `registerBidiStream(method, handler)`   | Register a bidirectional-streaming handler: `function(stream: GrpcServerStream): void`.                                                  |
| `useStatsHandler(fn)`                   | Observe every completed RPC: `function(s: RpcStats): void`.                                                                              |
| `setMaxRecvMessageBytes(n)`             | Reject inbound messages larger than `n` bytes with `RESOURCE_EXHAUSTED` (default 4 MiB).                                                 |
| `enableKeepalive(...)`                  | Send HTTP/2 PINGs to keep idle connections alive.                                                                                        |
| `addr()`                                | The listening address (`"host:port"`).                                                                                                   |
| `serve()`                               | Accept connections forever, handling each to completion.                                                                                 |
| `serveOnce()`                           | Accept and fully handle exactly one connection, then return (handy for tests).                                                           |
| `close()`                               | Stop listening.                                                                                                                          |

Server reflection and health checking are added with
`registerReflectionService(server, new ReflectionService())` and
`registerHealthService(server, new HealthService())`.

### `GrpcClient`

| Member                          | Description                                                    |
| ------------------------------- | -------------------------------------------------------------- |
| `new GrpcClient(host, port)`    | Connect and exchange the HTTP/2 preface.                       |
| `unary(method, message)`        | Send one request message, await one `GrpcReply`.               |
| `serverStream(method, message)` | Open a server-streaming call.                                  |
| `clientStream(method)`          | Open a client-streaming call.                                  |
| `bidiStream(method)`            | Open a bidirectional-streaming call.                           |
| `setMaxRecvMessageBytes(n)`     | Reject inbound messages larger than `n` bytes (default 4 MiB). |
| `close()`                       | Close the connection.                                          |

For TLS, deadlines, retries, and load balancing across multiple endpoints, see
`GrpcChannel` / `Endpoint` and the `RetryPolicy` / `LB_*` exports.

### Data types

```chuks
dataType GrpcRequest  { method: string; data: Buffer; metadata: []HpackHeader; }
dataType GrpcResponse { data: Buffer; status: int; message: string; }
dataType GrpcReply    { data: Buffer; status: int; message: string; }
```

`grpcOk(data)` and `grpcError(status, message)` build responses, and
`grpcErrorWithDetails(status, message, details)` attaches structured
`google.rpc.Status` details (serialized into the `grpc-status-details-bin`
trailer). All `STATUS_*` constants (`STATUS_OK`, `STATUS_NOT_FOUND`,
`STATUS_UNIMPLEMENTED`, …) are exported.

### Lower-level modules

The transport layers are usable on their own:

- `chuks_grpc/src/protobuf.chuks` — `ProtoWriter`, `ProtoReader`, wire-type constants
- `chuks_grpc/src/framing.chuks` — gRPC length-prefixed message framing
- `chuks_grpc/src/hpack.chuks` — `HpackEncoder`, `HpackDecoder`
- `chuks_grpc/src/http2.chuks` — HTTP/2 frame encode/decode

## Scope & limitations

- **No `.proto` codegen.** You serialize and deserialize messages directly with
  `ProtoWriter` / `ProtoReader`.
- **Server reflection** answers `list_services` only; descriptor-based requests
  (`file_containing_symbol`, `file_by_filename`) return `UNIMPLEMENTED`. Drive
  `grpcurl` with `-proto` for message encoding and reflection only for `list`.
- **Single HEADERS frame per header block** (no `CONTINUATION` reassembly).
- `grpc-message` values are sent as plain ASCII (not percent-encoded).

## Examples

See [`examples/echo_server.chuks`](examples/echo_server.chuks) and
[`examples/echo.proto`](examples/echo.proto) for a runnable server you can drive
with `grpcurl`.

## Tests

```
chuks run tests/grpc.test.chuks       # end-to-end h2c round-trip
chuks run tests/protobuf.test.chuks
chuks run tests/framing.test.chuks
chuks run tests/hpack.test.chuks
chuks run tests/http2.test.chuks
```

`tests/interop` validates real gRPC wire interop against `grpcurl` (reflection
list, unary, server-streaming, and rich-error propagation) in both VM and AOT
modes:

```
bash tests/interop/run.sh vm
bash tests/interop/run.sh aot
```
