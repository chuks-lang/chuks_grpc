# chuks_grpc

Wire-compatible **gRPC** for Chuks — a pure-Chuks implementation of the full
gRPC transport stack:

- **HTTP/2** (cleartext / h2c) — framing, SETTINGS, flow control, PING, GOAWAY
- **HPACK** — header compression (static + dynamic table, Huffman decode)
- **Protocol Buffers** — varint / zig-zag / fixed32 / fixed64 / length-delimited
- **gRPC framing** — 5-byte length-prefixed messages, `grpc-status` trailers

It interoperates with standard gRPC tooling. The examples below are verified
against [`grpcurl`](https://github.com/fullstorydev/grpcurl) calling a Chuks
server over a real plaintext gRPC connection.

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

## API

### `GrpcServer`

| Member                       | Description                                                                                                                              |
| ---------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------- |
| `new GrpcServer(host, port)` | Bind a listener immediately. Pass port `0` for an OS-assigned port.                                                                      |
| `register(method, handler)`  | Register a unary handler for a method path, e.g. `"/pkg.Service/Method"`. Handler signature: `function(req: GrpcRequest): GrpcResponse`. |
| `addr()`                     | The listening address (`"host:port"`).                                                                                                   |
| `serve()`                    | Accept connections forever, handling each to completion.                                                                                 |
| `serveOnce()`                | Accept and fully handle exactly one connection, then return (handy for tests).                                                           |
| `close()`                    | Stop listening.                                                                                                                          |

### `GrpcClient`

| Member                       | Description                                      |
| ---------------------------- | ------------------------------------------------ |
| `new GrpcClient(host, port)` | Connect and exchange the HTTP/2 preface.         |
| `unary(method, message)`     | Send one request message, await one `GrpcReply`. |
| `close()`                    | Close the connection.                            |

### Data types

```chuks
dataType GrpcRequest  { method: string; data: Buffer; metadata: []HpackHeader; }
dataType GrpcResponse { data: Buffer; status: int; message: string; }
dataType GrpcReply    { data: Buffer; status: int; message: string; }
```

`grpcOk(data)` and `grpcError(status, message)` build responses. All
`STATUS_*` constants (`STATUS_OK`, `STATUS_NOT_FOUND`, `STATUS_UNIMPLEMENTED`,
…) are exported.

### Lower-level modules

The transport layers are usable on their own:

- `chuks_grpc/src/protobuf.chuks` — `ProtoWriter`, `ProtoReader`, wire-type constants
- `chuks_grpc/src/framing.chuks` — gRPC length-prefixed message framing
- `chuks_grpc/src/hpack.chuks` — `HpackEncoder`, `HpackDecoder`
- `chuks_grpc/src/http2.chuks` — HTTP/2 frame encode/decode

## Scope & limitations (v0.1)

- **Unary RPCs only.** Client/server/bidirectional streaming is not yet implemented.
- **Cleartext (h2c) only.** The server listens over plaintext HTTP/2; there is
  no built-in TLS listener. Use it behind a TLS-terminating proxy for transport
  security, and connect peers with their "insecure"/"plaintext" option.
- **Single HEADERS frame per header block** (no `CONTINUATION` reassembly).
- **No `.proto` codegen.** You serialize and deserialize messages directly with
  `ProtoWriter` / `ProtoReader`.
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
