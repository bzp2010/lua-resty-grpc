# lua-resty-grpc

A gRPC client implementation on OpenResty, featuring all request modes.

## Table of Contents

- [Synopsis](#synopsis)
- [Limitations](#limitations)
- [Installation](#installation)
- [Methods](#methods)
  - [new](#new)
  - [close](#close)
  - [unary](#unary)
  - [server\_streaming](#server_streaming)
  - [new\_streaming\_request](#new_streaming_request)
  - [client\_streaming](#client_streaming)
  - [streaming](#streaming)

## Synopsis

```nginx
http {
    lua_package_path "/path/to/lua-resty-grpc/lib/?.lua;;";

    server {
        listen 8080;

        location /unary {
            content_by_lua_block {
                local grpc = require("resty.grpc")
                local cli, err = grpc.new({
                  host   = "grpcb.in",
                  port   = 9000,
                  ssl    = false,
                  protos = { "/path/to/helloworld.proto" },
                })
                if not cli then
                  ngx.say("new client error: ", err)
                  return
                end

                local res, err = cli:unary("hello.HelloService", "SayHello", { greeting = "world" })
                if not res then
                  ngx.say("unary error: ", err)
                  return
                end
                ngx.say("unary result: ", require("cjson").encode(res))
                cli:close()
            }
        }
    }
}
```

## Limitations

HTTP/2 over TLS, which gRPC over TLS relies on, requires the Application Layer Protocol Negotiation (ALPN) `h2` for handshaking. This is not supported by OpenResty cosocket.

This requires a bundle of custom patches.

https://github.com/openresty/lua-nginx-module/pull/2460

https://github.com/openresty/lua-resty-core/pull/513

The author has submitted this patch upstream but is unsure whether it will be accepted.
If so, you MUST use the OpenResty version with this modification, otherwise, the `ssl` option will not take effect.
If these patches cannot be merged upstream, you will need to modify your OpenResty build accordingly.

## Installation

This library can be installed via Luarocks. Simply use the following command:

```bash
luarocks install lua-resty-grpc
```

Please note that this approach will not apply the cosocket ALPN patches for OpenResty on your behalf. You will still need to wait for them merged upstream or apply the patches yourself.

## Methods

### new

**syntax:** `cli, err = grpc.new(opts)`

Create a client.

The `opts` parameter is a Lua table with named options:

- `host` (string, required): server hostname or IP.
- `port` (number, required): server port.
- `ssl` (boolean, optional): enable TLS, default `false`.
- `ssl_server_name` (string, optional): SNI; defaults to `host`.
- `ssl_verify` (boolean, optional): verify server certificate.
- `protos` (string[], required): pre-generated proto descriptor files (e.g., `.desc`).

### close

**syntax:** `cli:close()`

Close the client and free resources.

### unary

**syntax:** `res, err = cli:unary(service, method, data)`

Send a unary request where `service` is like `hello.HelloService`, `method` is `SayHello`.

The `data` is a Lua table. It will be encoded according to the protobuf definition.

It returns response data as a Lua table. If the request fails, it returns `nil` and an error message.

### server_streaming

**syntax:** `err = cli:server_streaming(service, method, data, on_message)`

Send a server streaming request where `service` is like `hello.HelloService`, `method` is `LotsOfReplies`.

The `data` is a Lua table. It will be encoded according to the protobuf definition.

The `on_message` accepts a function with prototype `function(res)` as a callback. Whenever a server message is received, it will be invoked.

When the request is normal, this function will block here. Each message sent by the server is returned via the `on_message` callback. When all server messages have been received and the stream is closed, this function will return.

When the request is abnormal, this function will immediately return an error message.

### new_streaming_request

**syntax:** `req, err = cli:new_streaming_request(service, method)`

Create a streaming request, which is typically used for client streaming and bidirectional streaming.

It returns a request handle containing the following methods:

- `req:send(data)` – Send one message. The `data` is a Lua table. It will be encoded according to the protobuf definition.
- `req:done()` – Finish the request.

When req:done() is called, the client will close the HTTP/2 stream for the request.

### client_streaming

**syntax:** `res, err = cli:client_streaming(service, method, req)`

Send a client streaming request where `service` is like `hello.HelloService`, `method` is `LotsOfGreetings`.

The `req` is a streaming request handle created by `cli:new_streaming_request`.

It returns response data as a Lua table. If the request fails, it returns `nil` and an error message.

Please note that you need to understand the nature of client streaming requests.
The server will only send a response after your request stream has been closed. So, if you do not call `req.done()`, the server will never send a response because the HTTP/2 stream for the request remains open.

Therefore, you can typically construct the entire request first and then send it using `cli:client_streaming()` after calling `req:done()`.

```lua
local req = c:new_streaming_request("hello.HelloService", "LotsOfGreetings")

req:send({ greeting = "msg1" })
req:send({ greeting = "msg2" })
req:send({ greeting = "msg3" })
req:done()

local res = c:client_streaming("hello.HelloService", "LotsOfGreetings", req)
```

### streaming

**syntax:** `err = cli:streaming(service, method, req, on_message)`

Send a bidirectional streaming request.

It combines flavors of server streaming and client streaming. You need to construct streaming requests and receive response data through callback.

The `service` is like `hello.HelloService`, `method` is `BidiHello`.

The `req` is a streaming request handle created by `cli:new_streaming_request`.

The `on_message` accepts a function with prototype `function(res)` as a callback. Whenever a server message is received, it will be invoked.

When the request is normal, this function will block here. Each message sent by the server is returned via the `on_message` callback. When all server messages have been received and the stream is closed, this function will return.

When the request is abnormal, this function will immediately return an error message.

The gRPC bidirectional streams represent two completely independent streams. Each request and each response are uncorrelated and decoupled.
This means that when we sometimes expect to send and receive responses in parallel, you can adopt the following pattern.

```lua
local req = c:new_streaming_request("hello.HelloService", "BidiHello")

ngx.thread.spawn(function()
  c:streaming("hello.HelloService", "BidiHello", req, function(res)
    print("received: ", res.reply)
  end)
end)

req:send({ greeting = "msg-1" })
req:send({ greeting = "msg-2" })
req:send({ greeting = "msg-3" })
req:done()
```

When both the client stream and server stream are closed, this function will return.
