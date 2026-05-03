# s3-rdma

GPU-Direct RDMA provider for S3 object storage — multi-language SDK.

## What is GPU-Direct RDMA?

RDMA (Remote Direct Memory Access) lets a remote machine read from or write to your GPU or host memory **without involving the CPU or kernel network stack** — data travels directly over an RDMA fabric (InfiniBand, RoCE) between the NIC and GPU memory.

```
Traditional PUT (GPU → S3):
  GPU memory → cudaMemcpy → host buffer → kernel TCP/IP → NIC → network → server

GPU-Direct RDMA PUT:
  GPU memory ──────────────────────────────────────────── NIC → network → server
                       (CPU not in the data path)
```

This project exposes that capability through a uniform `RdmaProvider` interface implemented in Rust and wrapped for Python and Java. No NVIDIA hardware is required for development — a `MockRdmaProvider` exercises the full API path without touching any hardware.

---

## Repository layout

```
s3-rdma/
├── core/                    # RdmaProvider trait, MockRdmaProvider, CuObjProvider, BufferPool
├── providers/
│   ├── cuobj-client/        # Client cdylib plugin → libs3rdmacuobjclient.so
│   └── cuobj-server/        # Server cdylib plugin → libs3rdmacuobjserver.so
└── bindings/
    ├── java/                # JNI Rust crate + Maven project (io.s3rdma)
    └── python/              # PyO3 / maturin extension (s3_rdma)
```

`~/work/cuobj/` is a separate standalone crate that wraps the low-level cuObject SDK C++ API. It is a dependency of `providers/cuobj-client` and `providers/cuobj-server`.

---

## Provider model

The provider model is built around three focused traits:

```
RdmaProvider: RdmaClientProvider + RdmaServerProvider
  ├── RdmaClientProvider  — register memory, generate tokens, process replies
  └── RdmaServerProvider  — handle PUT/GET transfers from clients
```

Consumers use the narrowest trait that fits their role:

| Scenario | Trait |
|---|---|
| S3 client node (register memory, generate tokens) | `Arc<dyn RdmaClientProvider>` |
| S3 server / gateway node (handle PUT/GET) | `Arc<dyn RdmaServerProvider>` |
| Both roles in the same node / mock testing | `Arc<dyn RdmaProvider>` |

### Client side (`RdmaClientProvider`)

The client-side abstraction used by S3 clients and relay nodes:

```
client                        S3 server
  │                               │
  │  register_memory(buf)         │
  │  prepare_put_token(key, buf) ─┼─► x-amz-rdma-token header on PUT request
  │                               │      server sets up RDMA READ
  │◄─ x-amz-rdma-reply header ───┤
  │  process_rdma_reply(status)   │
  │      RDMA transfer completes  │
```

| Step | API | Description |
|------|-----|-------------|
| 1 | `is_memory_suitable(ptr, size)` | Check whether buffer is RDMA-eligible |
| 2 | `register_memory(ptr, size)` | Pin the buffer with the RDMA subsystem |
| 3 | `prepare_put_token(key, buf, size, offset)` | Generate upload descriptor (PUT) |
| 4 | `prepare_get_token(key, buf, size, offset)` | Generate download descriptor (GET) |
| 5 | `process_rdma_reply(status, token, transferred, cb)` | Confirm transfer; fires completion callback |
| 6 | `deregister_memory(ptr)` | Unpin the buffer |

Header names are provider-configurable (`x-amz-rdma-token`, `x-amz-rdma-reply`).

### Server side (`RdmaServerProvider`)

The server-side abstraction used by the S3 gateway:

| Method | Description |
|--------|-------------|
| `handle_put(key, token, offset, size)` | RDMA_READ from client buffer; returns transferred bytes |
| `handle_get(key, data, token, offset)` | Copy `data` into pool slot and RDMA_WRITE to client buffer |

The server provider owns a `BufferPool` of pinned DMA slots. Each `handle_*` call acquires a slot, performs the RDMA transfer, and releases the slot back to the pool.

---

## HTTP/2 streaming token delivery protocol

The S3 gateway and relay support two RDMA token delivery modes between a relay node and the
upstream gateway.  The mode is negotiated via an HTTP header on the request.

### Header mode (default)

All RDMA buffer descriptors are encoded into a single `x-amz-rdma-token` request header,
pipe-separated for multi-chunk transfers.  The server reads data from each buffer and returns
a single `x-amz-rdma-reply` header on the response:

```
Client PUT:
  x-amz-rdma-token: <token1>|<token2>|…
  Content-Length: 0
  (no body)

Server response:
  x-amz-rdma-reply: 200     ← 200 = success, 501 = fallback to plain HTTP
  x-amz-rdma-bytes: <written>/<total>
```

### Stream mode (`x-amz-rdma-stream: true`)

Token lines are delivered as a newline-delimited request body.  The server sends per-token
ACK lines back in the response body.  Requires HTTP/2 (no head-of-line blocking; server can
begin RDMA_READs while later tokens are still arriving).

**PUT direction (client → server):**

```
Client PUT:
  x-amz-rdma-stream: true
  Content-Length: <token-body-size>

  <token1>\n
  <token2>\n
  …

Server response (200 OK):
  x-amz-rdma-reply: 200
  (standard S3 PutObject/UploadPart response body)
```

**GET direction (server → client):**

```
Client GET:
  x-amz-rdma-stream: true
  Content-Length: <token-body-size>

  <token1>\n
  <token2>\n
  …

Server response (200 OK):
  x-amz-rdma-reply: 200

  <written1>/<total>\n    ← one ACK line per token
  <written2>/<total>\n
  …
```

Where `<written>` is the byte count transferred into that buffer and `<total>` is the full
object size.  If `<written> < <total>` the client buffer was smaller than the object
(partial transfer, `x-amz-rdma-reply: 206`).

### Mock token format

The `MockRdmaProvider` generates tokens in this ASCII format — useful for integration tests
and debugging:

```
RDMA_{PUT|GET}_{shm_filename}_{size}_{offset}[_key_{s3_key}]
```

Rules:
- `shm_filename` — filename only, **no `/dev/shm/` prefix** (the mock prepends it)
- `shm_filename` must **not contain underscores** (used as field separator with `splitn(7, '_')`)
- Typical safe names: `rdma-buf-0`, `my-object` (hyphens are fine)
- The `_key_{s3_key}` suffix is optional — used by the server-side mock to verify the key

Examples:
```
RDMA_PUT_rdma-buf-0_8192_0
RDMA_GET_rdma-buf-0_8192_0_key_my-bucket/my-object.bin
```

---

## Providers

### MockRdmaProvider

Built into `core`. Accepts any non-null buffer, generates deterministic ASCII tokens, and invokes the completion callback synchronously. No hardware or SDK required. Uses `/dev/shm` files as the transfer medium so client and server can run in separate processes on the same host (or in Docker containers sharing `ipc: host`). Supports both client and server roles from a single instance.

### CuObj plugins

Two separate plugins are loaded at runtime:

- **`libs3rdmacuobjclient.so`** (`providers/cuobj-client/`) — exports the client vtable:

| C symbol | Direction | Description |
|----------|-----------|-------------|
| `s3_rdma_provider_get_vtable()` | Client | `RdmaProviderVtable` — used by relay/client nodes |

- **`libs3rdmacuobjserver.so`** (`providers/cuobj-server/`) — exports the server vtable:

| C symbol | Direction | Description |
|----------|-----------|-------------|
| `s3_rdma_server_provider_get_vtable()` | Server | `RdmaServerVtable` — used by the gateway |

Plugins are loaded at runtime via `libloading` — no compile-time SDK dependency. `CuObjClientProvider` and `CuObjServerProvider` in `core` search for the plugin in this order:

1. Standard dynamic-linker paths (`LD_LIBRARY_PATH`, RPATH, ldconfig cache)
2. Directory alongside the running executable
3. `/lib64/`, `/usr/lib64/`


---

## Usage

### Rust

Add to `Cargo.toml`:

```toml
[dependencies]
s3-rdma = { path = "core" }
```

```rust
use s3_rdma::{MockRdmaProvider, RdmaClientProvider};

let ep = MockRdmaProvider::with_debug(false);
let mut buf = vec![0u8; 4 * 1024 * 1024];
let key = b"my-bucket/my-object.bin";

ep.register_memory(buf.as_mut_ptr(), buf.len())?;

// S3 PUT: attach the returned token as the x-amz-rdma-token request header
let token = ep.prepare_put_token(key, buf.as_ptr(), buf.len(), 0)?;

// After the server responds with x-amz-rdma-reply, confirm the transfer:
ep.process_rdma_reply(200, &token, buf.len(), Box::new(|result| {
    assert_eq!(result.error_code, 0);
}))?;

ep.deregister_memory(buf.as_mut_ptr())?;
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
cd bindings/python && python3 -m maturin develop
```

```python
import s3_rdma

provider = s3_rdma.MockRdmaProvider(debug=False)
# bytearray (or bytes/memoryview/numpy) — any buffer-protocol object is accepted
data = bytearray(b"\x42" * 4 * 1024 * 1024)
key  = b"my-bucket/my-object.bin"

provider.register_memory(data)
token = provider.prepare_put_token(data, 0, len(data), key)

def on_complete(error_code: int, reply: bytes) -> None:
    assert error_code == 0

provider.process_rdma_reply(200, token, len(data), on_complete)
provider.deregister_memory(data)
```

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
import io.s3rdma.MockRdmaProvider;
import io.s3rdma.RdmaBuffer;
import java.nio.ByteBuffer;

try (MockRdmaProvider provider = new MockRdmaProvider()) {
    byte[] key = "my-bucket/my-object.bin".getBytes();

    // RdmaBuffer wraps ByteBuffer.allocateDirect() — required for RDMA operations
    try (RdmaBuffer rdmaBuf = new RdmaBuffer(4 * 1024 * 1024)) {
        byte[] content = new byte[rdmaBuf.capacity()];
        rdmaBuf.put(content, 0, content.length);

        provider.registerMemory(rdmaBuf.buffer());

        byte[] token = provider.preparePutToken(key, rdmaBuf.buffer(), 0, rdmaBuf.capacity());

        provider.processRdmaReply(200, token, rdmaBuf.capacity(),
            (errorCode, reply) -> System.out.println("done, code=" + errorCode));

        provider.deregisterMemory(rdmaBuf.buffer());
    }
}
```

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
| `MemoryNotSuitable` | Buffer not usable for RDMA | ✅ fall back to plain HTTP |
| `TokenGenerationFailed` | Could not build descriptor | ✅ fall back to plain HTTP |
| `CuObjConnectionFailed` | SDK unavailable at runtime | ✅ fall back to plain HTTP |
| `ProviderUnavailable` | Named provider unknown or init failed | ✅ fall back to plain HTTP |
| `BufferTooSmall` | Server slot smaller than requested transfer | ✅ fall back to plain HTTP |
| `MemoryRegistrationFailed` | RDMA registration error | ❌ propagate |
| `MemoryDeregistrationFailed` | RDMA deregistration error | ❌ propagate |
| `ReplyTokenProcessingFailed` | Server reply invalid | ❌ propagate |
| `TransferFailed` | RDMA data movement error | ❌ propagate |
| `Io` | Underlying I/O error | ❌ propagate |
| `OperationNotSupported` | Method not available for provider's role | ❌ propagate |

`RdmaError::is_fallback_eligible()` returns `true` for the first five variants — safe to silently downgrade to non-RDMA transfers.

---

## Building

```sh
# Core library + all providers + Java JNI
cargo build --workspace

# cuObject client plugin (requires CUDA SDK)
export CUOBJ_ROOT_DIR=/usr/local/cuda-13.2
cargo build -p s3-rdma-cuobj-client --release
# → target/release/libs3rdmacuobjclient.so

# cuObject server plugin (requires CUDA SDK)
cargo build -p s3-rdma-cuobj-server --release
# → target/release/libs3rdmacuobjserver.so

# Python extension (from bindings/python/)
source ~/.venv/bin/activate
cd bindings/python && python3 -m maturin develop

# Java classes
mvn compile -f bindings/java/pom.xml
```

Place `libs3rdmacuobjclient.so` and `libs3rdmacuobjserver.so` alongside the gateway binary or on `LD_LIBRARY_PATH` before starting either the client or server process.

Minimum Rust edition: **2021**.  `unsafe` in `core` is limited to raw pointer forwarding to provider implementations and POSIX SHM operations in `MockRdmaProvider`.

