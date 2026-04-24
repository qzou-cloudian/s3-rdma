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
├── core/                    # RdmaProvider / RdmaServerProvider traits, Mock providers, loaders
├── providers/
│   ├── cuobj/               # Combined cdylib plugin → libs3rdmacuobj.so  ← primary
│   ├── cuobj-client/        # Legacy client-only plugin  (kept for compatibility)
│   └── cuobj-server/        # Legacy server-only plugin  (kept for compatibility)
└── bindings/
    ├── java/                # JNI Rust crate + Maven project (io.s3rdma)
    └── python/              # PyO3 / maturin extension (s3_rdma)
```

`~/work/cuobj/` is a separate standalone crate that wraps the low-level cuObject SDK C++ API. It is a dependency of `providers/cuobj`.

---

## Provider model

### Client side (`RdmaProvider`)

The client-side abstraction used by S3 clients and relay nodes:

```
client                        S3 server
  │                               │
  │  register_memory(buf)         │
  │  prepare_put_token(key, buf) ─┼─► x-amz-rdma-token header on PUT request
  │                               │      server sets up RDMA READ
  │◄─ x-amz-rdma-reply header ───┤
  │  process_reply_token(reply)   │
  │      RDMA transfer completes  │
```

| Step | API | Description |
|------|-----|-------------|
| 1 | `is_memory_suitable(ptr, size)` | Check whether buffer is RDMA-eligible |
| 2 | `register_memory(ptr, size)` | Pin the buffer with the RDMA subsystem |
| 3 | `prepare_put_token(key, buf, size, offset)` | Generate upload descriptor (PUT) |
| 4 | `prepare_get_token(key, buf, size, offset)` | Generate download descriptor (GET) |
| 5 | `process_reply_token(reply, callback)` | Confirm transfer; fires completion callback |
| 6 | `deregister_memory(ptr)` | Unpin the buffer |

HTTP header names are provider-configurable (`x-amz-rdma-token`, `x-amz-rdma-reply`).

### Server side (`RdmaServerProvider`)

The server-side abstraction used by the S3 gateway:

| Method | Description |
|--------|-------------|
| `handle_put(key, token, offset, size)` | RDMA_READ from client buffer; returns transferred bytes |
| `handle_get(key, data, token, offset)` | Copy `data` into pool slot and RDMA_WRITE to client buffer |

The server provider owns a `BufferPool` of pinned DMA slots. Each `handle_*` call acquires a slot, performs the RDMA transfer, and releases the slot back to the pool.

---

## Providers

### MockRdmaProvider / MockRdmaServerProvider

Built into `core`. Accepts any non-null buffer, generates deterministic ASCII tokens, and invokes the completion callback synchronously. No hardware or SDK required. Uses `/dev/shm` files as the transfer medium so client and server can run in separate processes on the same host (or in Docker containers sharing `ipc: host`).

### CuObjProvider (combined plugin)

Loaded at runtime from `providers/cuobj/` — builds a single shared library `libs3rdmacuobj.so` that exports **both** the client and server vtables:

| C symbol | Direction | Description |
|----------|-----------|-------------|
| `s3_rdma_provider_get_vtable()` | Client | `RdmaProviderVtable` — used by relay/client nodes |
| `s3_rdma_server_provider_get_vtable()` | Server | `RdmaServerVtable` — used by the gateway |

The plugin is loaded at runtime via `libloading` — no compile-time SDK dependency. `CuObjClientProvider` and `CuObjServerProvider` in `core` search for the plugin in this order:

1. Standard dynamic-linker paths (`LD_LIBRARY_PATH`, RPATH, ldconfig cache)
2. Directory alongside the running executable
3. `/lib64/`, `/usr/lib64/`

The legacy split plugins (`libs3_rdma_cuobj_client.so` / `libs3_rdma_cuobj_server.so` from `providers/cuobj-client` and `providers/cuobj-server`) are searched as fallbacks if the combined plugin is not found.

---

## Usage

### Rust

Add to `Cargo.toml`:

```toml
[dependencies]
s3-rdma = { path = "core" }
```

```rust
use s3_rdma::{MockRdmaProvider, RdmaProvider};

let provider = MockRdmaProvider::new(false);
let mut buf = vec![0u8; 4 * 1024 * 1024];
let key = b"my-bucket/my-object.bin";

provider.register_memory(buf.as_mut_ptr(), buf.len())?;

// S3 PUT: attach the returned token as the x-amz-rdma-token request header
let token = provider.prepare_put_token(key, buf.as_ptr(), buf.len(), 0)?;

// After the server responds with x-amz-rdma-reply, confirm the transfer:
provider.process_reply_token(&reply, Box::new(|result| {
    assert_eq!(result.error_code, 0);
}))?;

provider.deregister_memory(buf.as_mut_ptr())?;
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
data = bytes(b"\x42" * 4 * 1024 * 1024)
key  = b"my-bucket/my-object.bin"

provider.register_memory(data)
token = provider.prepare_put_token(data, 0, len(data), key)

def on_complete(error_code: int, reply: bytes) -> None:
    assert error_code == 0

provider.process_reply_token(b"server-reply", on_complete)
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

try (MockRdmaProvider provider = new MockRdmaProvider(false)) {
    byte[] buf  = new byte[4 * 1024 * 1024];
    byte[] key  = "my-bucket/my-object.bin".getBytes();

    provider.registerMemory(buf);

    byte[] token = provider.preparePutToken(key, buf, 0, buf.length);

    provider.processReplyToken(serverReply,
        (errorCode, reply) -> assert errorCode == 0);

    provider.deregisterMemory(buf);
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
| `MemoryRegistrationFailed` | RDMA registration error | ❌ propagate |
| `MemoryDeregistrationFailed` | RDMA deregistration error | ❌ propagate |
| `ReplyTokenProcessingFailed` | Server reply invalid | ❌ propagate |
| `Io` | Underlying I/O error | ❌ propagate |

`RdmaError::is_fallback_eligible()` returns `true` for the first three — safe to silently downgrade to non-RDMA transfers.

---

## Building

```sh
# Core library + all providers + Java JNI
cargo build --workspace

# Combined cuObject plugin only (requires CUDA SDK)
export CUOBJ_ROOT_DIR=/usr/local/cuda-13.2
cargo build -p s3-rdma-cuobj --release
# → target/release/libs3rdmacuobj.so

# Python extension (from bindings/python/)
source ~/.venv/bin/activate
cd bindings/python && python3 -m maturin develop

# Java classes
mvn compile -f bindings/java/pom.xml
```

Place `libs3rdmacuobj.so` alongside the gateway binary or on `LD_LIBRARY_PATH` before starting either the client or server process.

Minimum Rust edition: **2021**.  No `unsafe` in `core` beyond raw pointer forwarding to the provider implementation.

