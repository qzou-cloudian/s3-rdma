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
├── core/                    # RdmaProvider trait, MockRdmaProvider, CuObjProvider
├── providers/
│   └── cuobj/               # cdylib plugin — wraps NVIDIA cuObject SDK
└── bindings/
    ├── java/                # JNI Rust crate + Maven project (io.s3rdma)
    └── python/              # PyO3 / maturin extension (s3_rdma)
```

`~/work/cuobj/` is a separate standalone crate that wraps the low-level cuObject SDK C++ API. It is a dependency of `providers/cuobj`.

---

## Provider model

The core abstraction is the `RdmaProvider` trait:

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

---

## Providers

### MockRdmaProvider

Built into `core`. Accepts any non-null buffer, generates deterministic ASCII tokens, and invokes the completion callback synchronously. No hardware or SDK required.

### CuObjProvider

Runtime-loaded from `providers/cuobj/` (`libs3_rdma_cuobj.so`). Delegates to the NVIDIA cuObject SDK for real GPU-Direct RDMA. Enabled by the `cuobj` Cargo feature on the `core` crate; the plugin `.so` is loaded at runtime via `libloading` — no compile-time SDK dependency.

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
# Workspace (core + providers + Java JNI)
cargo build --workspace

# Python extension (from bindings/python/)
source ~/.venv/bin/activate
cd bindings/python && python3 -m maturin develop

# Java classes
mvn compile -f bindings/java/pom.xml
```

Minimum Rust edition: **2021**.  No `unsafe` in `core` beyond raw pointer forwarding to the provider implementation.
