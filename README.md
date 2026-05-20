# s3-rdma

GPU-Direct RDMA provider for S3 object storage — multi-language SDK (Rust, Python, Java).

## What is GPU-Direct RDMA?

RDMA (Remote Direct Memory Access) lets a remote machine read from or write to your GPU or host
memory **without involving the CPU or kernel network stack** — data travels directly over an RDMA
fabric (InfiniBand, RoCE) between the NIC and GPU memory.

```
Traditional PUT (GPU → S3):
  GPU memory → cudaMemcpy → host buffer → kernel TCP/IP → NIC → network → server

GPU-Direct RDMA PUT:
  GPU memory ──────────────────────────────────────────── NIC → network → server
                       (CPU not in the data path)
```

This project exposes that capability through a uniform `RdmaProvider` interface implemented in
Rust and wrapped for Python and Java.  No NVIDIA hardware is required for development — a
`MockRdmaProvider` exercises the full API path over POSIX shared memory (`/dev/shm`).

---

## Repository layout

```
s3-rdma/
├── core/                    # RdmaProvider traits, MockRdmaProvider, RdmaPlugin, BufferPool
├── providers/
│   ├── client/              # Client cdylib plugin → libs3rdmaclient.so
│   └── server/              # Server cdylib plugin → libs3rdmaserver.so
└── bindings/
    ├── java/                # JNI Rust crate + Maven project (io.s3rdma)
    └── python/              # PyO3 / maturin extension (s3_rdma)
```

`~/work/cuobj/` is a separate standalone crate that wraps the low-level cuObject SDK C++ API.
It is an optional dependency of `providers/client` and `providers/server` (behind the `cuobj`
feature).

`bindings/python/` is a separate maturin project — it is **not** a Cargo workspace member.

---

## Provider model

The provider model is built around three focused traits:

```
RdmaProvider: RdmaClientProvider + RdmaServerProvider
  ├── RdmaClientProvider  — bind buffers, produce RdmaClientChannel
  │   └── RdmaClientChannel — prepare_put / prepare_get / complete_put / complete_get
  └── RdmaServerProvider  — open channels for key+token pairs
      └── RdmaServerChannel — handle_put / handle_get (closure-based, zero-copy)
```

Consumers use the narrowest trait that fits their role:

| Scenario | Use |
|---|---|
| S3 client node (bind memory, generate tokens) | `Arc<dyn RdmaClientProvider>` |
| S3 server / gateway node (handle PUT/GET) | `Arc<dyn RdmaServerProvider>` |
| Both roles in the same node / bindings | `Arc<dyn RdmaProvider>` |

### Client side — channel API

```
  Client                                       Server
  ──────                                       ──────
  ch = provider.bind(size, key)
  put = ch.prepare_put(offset, size)
    │  PUT /bucket/key  +  x-amz-rdma-token: <put.token()>
    │                           ↓             ch = provider.open_channel(key, put.token())
    │                           ↓             n = ch.handle_put(offset, size, write_fn)
    │  ← x-amz-rdma-reply: 200, x-amz-rdma-bytes: N
  ch.complete_put(put, 200, N)?
  // drop(ch) deregisters the buffer
```

`RdmaClientChannel` methods:

| Method | Description |
|--------|-------------|
| `prepare_put(offset, size)` | Generate a PUT token; returns `RdmaPutHandle` |
| `prepare_get(offset, size)` | Generate a GET token; returns `RdmaGetHandle` |
| `complete_put(handle, rdma_status, transferred)` | Confirm upload |
| `complete_get(handle, rdma_status, transferred)` | Confirm download and copy-back |
| `put(offset, size, write, send)` | All-in-one: fill buffer, prepare, send, complete |
| `get(offset, size, send, read)` | All-in-one: prepare, send, complete, read data |
| `ptr() -> *const u8` | Base pointer of the registered buffer |
| `size() -> usize` | Buffer capacity |

### Server side — channel API

```
  ch = provider.open_channel(key, token)
  n  = ch.handle_put(offset, size, |data: &[u8]| { store(data) })
  n  = ch.handle_get(offset, size, |buf: &mut [u8]| { load(buf) })
```

`RdmaServerChannel` methods:

| Method | Description |
|--------|-------------|
| `handle_put(offset, size, write)` | RDMA-read from client buffer; pass bytes to closure |
| `handle_get(offset, size, read)` | Fill buffer via closure; RDMA-write to client |

The write/read closures are `FnOnce` — they receive or fill the RDMA-registered memory
directly, with no intermediate copy.

---

## HTTP protocol

### Headers

| Header | Direction | Description |
|--------|-----------|-------------|
| `x-amz-rdma-token` | Client → Server | RDMA buffer descriptor(s), `\|`-separated |
| `x-amz-rdma-reply` | Server → Client | Status code (200/206 = ok, 501 = fallback) |
| `x-amz-rdma-bytes` | Server → Client | `written/total` byte count |
| `x-amz-rdma-stream` | Client → Server | `"true"` to enable streaming token mode |

### Header mode (default)

All descriptors are encoded into a single `x-amz-rdma-token` request header, pipe-separated
for multi-chunk transfers.

```
Client PUT:
  x-amz-rdma-token: <token1>|<token2>|…
  Content-Length: 0

Server response:
  x-amz-rdma-reply: 200
  x-amz-rdma-bytes: <written>/<total>
```

### Stream mode (`x-amz-rdma-stream: true`)

Token lines are delivered as a newline-delimited request body.  The server sends per-token ACK
lines in the response body.  Requires HTTP/2 so the server can begin RDMA_READs while later
tokens are still arriving.

### Mock token format

The `MockRdmaProvider` generates human-readable ASCII tokens useful for debugging:

```
RDMA_{PUT|GET}_{shm_name}_{size}_{offset}[_key_{s3_key}]

Examples:
  RDMA_PUT_rdma-buf-0_8192_0
  RDMA_GET_rdma-buf-0_8192_0_key_my-bucket/my-object.bin
```

`shm_name` must not contain underscores (used as field separator).  Hyphens are fine.

---

## Providers

### MockRdmaProvider

Built into `core`.  Uses `/dev/shm` files as the transfer medium so client and server can run
in separate processes on the same host (or Docker containers sharing `ipc: host`).  Generates
deterministic ASCII tokens.  Supports both client and server roles.  No hardware or CUDA SDK
required.

### RdmaPlugin (runtime-loaded)

Two separate plugins loaded at runtime via `libloading`:

- **`libs3rdmaclient.so`** (`providers/client/`) — exports `s3_rdma_get_client_vtable(name)`
- **`libs3rdmaserver.so`** (`providers/server/`) — exports `s3_rdma_get_server_vtable(name)`

Both accept `"mock"` or `"cuobj"` as the backend name.  Plugin search order:
1. `LD_LIBRARY_PATH` / RPATH / ldconfig cache
2. Directory alongside the running executable
3. `/lib64/`, `/usr/lib64/`

The main binary has **no compile-time dependency** on any RDMA SDK.

### Feature flags

| Feature | Effect |
|---------|--------|
| `rdma-client` | Enable `RdmaPluginClient` / `RdmaPlugin` client role (`libloading`) |
| `rdma-server` | Enable `RdmaPluginServer` / `RdmaPlugin` server role (`libloading`) |
| `buffer-pool` | Enable `BufferPool` for pre-registered RDMA buffers |
| `engine` | Enable `RdmaEngine` fixed-size worker pool |
| `pyo3` | Python bindings via PyO3 (used by `bindings/python/`) |
| (none) | Only `MockRdmaProvider` available |

### Provider builder

```rust
// Full provider (client+server roles by default)
let ep: Arc<dyn RdmaProvider> = RdmaProviderConfig::builder()
    .provider("auto")
    .debug(true)
    .build()?;

// Client-only
let ep: Arc<dyn RdmaClientProvider> = RdmaProviderConfig::builder()
    .provider("mock")
    .build_client()?;

// Server-only
let ep: Arc<dyn RdmaServerProvider> = RdmaProviderConfig::builder()
    .provider("cuobj")
    .listen_addr("192.168.1.1")
    .listen_port(4791)
    .build_server()?;
```

`"auto"` tries `"cuobj"` first, then falls back to `"mock"`.

---

## Usage

### Rust

```toml
[dependencies]
s3-rdma = { path = "core" }
```

```rust
use s3_rdma::{MockRdmaProvider, RdmaClientProvider, RdmaServerProvider};

let provider = MockRdmaProvider::new(true, "/dev/shm", 0);
let key = b"my-bucket/my-object.bin";

// Client: bind a 4 MiB buffer and prepare a PUT token
let ch = provider.bind(4 * 1024 * 1024, key)?;
let put = ch.prepare_put(0, ch.size())?;
let token_bytes = put.token().to_vec();

// ... attach token_bytes as x-amz-rdma-token on the PUT request ...

// Server: open a channel with the received token and handle the PUT
let srv_ch = provider.open_channel(key, &token_bytes)?;
let received: Vec<u8> = Vec::new();
let n = srv_ch.handle_put(0, 0, Box::new(|data: &[u8]| {
    // store data (write to disk, append to buffer, etc.)
    Ok(data.len())
}))?;

// Client: complete the transfer
ch.complete_put(put, 200, n)?;
```

Run the full example:

```sh
cargo run --example mock_provider --manifest-path core/Cargo.toml
```

### Python

Build the extension once (requires a virtualenv with maturin):

```sh
python3 -m venv ~/.venv && source ~/.venv/bin/activate
pip install maturin
cd bindings/python && maturin develop
```

```python
import s3_rdma

# Mock provider — both roles (typical for testing)
provider = s3_rdma.MockRdmaProvider(debug=True)
data  = bytearray(b"\x42" * 64 * 1024)
s3key = b"my-bucket/my-object.bin"

# Client: bind + prepare PUT token (register_memory is a no-op; bind() does it internally)
token = provider.prepare_put_token(data, 0, len(data), s3key)

# Server: handle PUT
received = provider.handle_put(s3key, token, size=len(data))
assert received == bytes(data)

# Client: confirm transfer
provider.process_rdma_reply(200, token, len(data), lambda code, reply: None)

# Auto-select provider (tries rdma plugin first, falls back to mock)
ep = s3_rdma.RdmaProvider(provider="auto", role="both")
print(f"Using: {ep!r}, protocol: {ep.protocol()}")
```

Note: `register_memory()` and `deregister_memory()` are accepted for backward compatibility but
are no-ops — memory binding happens inside `prepare_put_token` / `prepare_get_token`.

Run the full example:

```sh
cd bindings/python
python3 examples/example.py
```

### Java

Build the JNI library and Java classes:

```sh
cargo build --manifest-path bindings/java/Cargo.toml
mvn compile -f bindings/java/pom.xml
```

```java
import io.s3rdma.*;
import java.util.Arrays;

try (RdmaProvider ep = RdmaProvider.create("mock", "both", RdmaProviderConfig.defaults())) {
    byte[] key  = "my-bucket/my-object.bin".getBytes();
    byte[] data = new byte[64 * 1024];
    Arrays.fill(data, (byte) 0x42);

    try (RdmaBuffer buf = new RdmaBuffer(data.length)) {
        buf.put(data, 0, data.length);

        // registerMemory / deregisterMemory are no-ops; binding happens inside prepare*Token
        byte[] token = ep.preparePutToken(key, buf.buffer(), 0, data.length);

        // Server handles PUT
        byte[] received = ep.handlePut(key, token, 0, data.length);

        // Client confirms transfer
        ep.processRdmaReply(200, token, data.length,
            (code, reply) -> System.out.println("done, code=" + code));
    }
}
```

Note: `registerMemory()` / `deregisterMemory()` are no-ops in the channel-based implementation.

Run the full example:

```sh
java -Djava.library.path=target/debug \
     -cp bindings/java/target/classes \
     io.s3rdma.examples.Example
```

---

## Error handling

`RdmaError` variants:

| Variant | Meaning | Fallback eligible? |
|---------|---------|-------------------|
| `MemoryNotSuitable` | Buffer not usable for RDMA | ✅ |
| `TokenGenerationFailed` | Could not build descriptor | ✅ |
| `RdmaPluginLoadFailed` | Plugin `.so` not found or init failed | ✅ |
| `ProviderUnavailable` | Named provider unknown or init failed | ✅ |
| `BufferTooSmall` | Server slot smaller than requested transfer | ✅ |
| `TelemetryError` | Non-critical telemetry failure | ✅ |
| `MemoryRegistrationFailed` | RDMA registration error | ❌ |
| `MemoryDeregistrationFailed` | RDMA deregistration error | ❌ |
| `ReplyTokenProcessingFailed` | Server reply invalid | ❌ |
| `TransferFailed` | RDMA data movement error | ❌ |
| `Io` | Underlying I/O error | ❌ |
| `OperationNotSupported` | Method not available for this role | ❌ |

`RdmaError::is_fallback_eligible()` returns `true` for the first six — safe to downgrade to
plain HTTP.

---

## Building

```sh
# Core library + all providers + Java JNI
cargo build --workspace

# Client plugin, mock backend only (no CUDA SDK)
cargo build -p s3-rdma-client --release
# → target/release/libs3rdmaclient.so

# Client plugin with cuObject hardware backend (requires CUDA SDK + cuobj crate)
CUOBJ_ROOT_DIR=/usr/local/cuda-13.2 \
cargo build -p s3-rdma-client --release --features cuobj

# Server plugin
cargo build -p s3-rdma-server --release
# → target/release/libs3rdmaserver.so

# Server plugin with cuObject backend
CUOBJ_ROOT_DIR=/usr/local/cuda-13.2 \
cargo build -p s3-rdma-server --release --features cuobj

# Python extension (from bindings/python/)
source ~/.venv/bin/activate
cd bindings/python && maturin develop

# Java classes
mvn compile -f bindings/java/pom.xml
```

Place `libs3rdmaclient.so` and `libs3rdmaserver.so` alongside the gateway binary or on
`LD_LIBRARY_PATH` before starting the client or server process.

Minimum Rust edition: **2021**.  `unsafe` in `core` is limited to raw-pointer forwarding
across plugin boundaries and POSIX SHM operations in `MockRdmaProvider`.
