# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build & test commands

```sh
# Build root crate only (default)
cargo build

# Build everything (root + providers + Java JNI)
cargo build --workspace

# Build with specific feature flags
cargo build --features rdma-client,rdma-server

# Client plugin (mock backend only, no CUDA SDK needed)
cargo build -p s3-rdma-client
# Client plugin with cuObj hardware backend
cargo build -p s3-rdma-client --features cuobj

# Server plugin
cargo build -p s3-rdma-server
# Server plugin with cuObj hardware backend
cargo build -p s3-rdma-server --features cuobj

# Run all tests
cargo test --workspace
# Run tests for a single crate
cargo test -p s3-rdma
# Run a specific test
cargo test -p s3-rdma -- token::tests::parse_mock_put

# Run the Rust example
cargo run --example pingpong

# Python: build extension (separate maturin project, NOT a workspace member)
cd bindings/python && python3 -m maturin develop
# Python: run tests (requires venv with pytest)
cd bindings/python && /home/qingshan/.venv/bin/python -m pytest tests/ -v
# Python: run example
cd bindings/python && python3 examples/example.py

# Java: build JNI library + Java classes
cargo build -p s3-rdma-jni
mvn compile -f bindings/java/pom.xml
```

## Architecture

GPU-Direct RDMA provider for S3 object storage.  No NVIDIA hardware needed for development —
the mock provider exercises the full API path over POSIX shared memory.

### Workspace layout

```
s3-rdma/                          # workspace root + s3-rdma crate (Cargo.toml)
├── src/                          # s3-rdma crate — traits, mock provider, plugin loader
├── examples/
├── providers/client/             # cdylib → libs3rdmaclient.so  (not in default-members)
├── providers/server/             # cdylib → libs3rdmaserver.so  (not in default-members)
└── bindings/java/                # cdylib → libs3rdmajni.so + Maven project  (not in default-members)
```

`bindings/python/` is a **separate maturin project** (not a workspace member).  Its Rust source
is `src/python.rs`, compiled with `--features pyo3`.  Build it with
`cd bindings/python && maturin develop`, not `cargo build`.

### Trait hierarchy (`src/provider.rs`)

```
RdmaProvider: RdmaClientProvider + RdmaServerProvider
  │
  ├── RdmaClientProvider       — bind(size, key) → Box<dyn RdmaClientChannel>
  │   └── RdmaClientChannel    — prepare_put / prepare_get / complete_put / complete_get / put / get
  │
  └── RdmaServerProvider       — open_channel(key, token) → Box<dyn RdmaServerChannel>
      └── RdmaServerChannel    — handle_put / handle_get (closure-based)
```

Use the **narrowest** trait:
- `Arc<dyn RdmaClientProvider>` — client nodes (bind memory, generate tokens)
- `Arc<dyn RdmaServerProvider>` — gateway/server nodes (handle PUT/GET)
- `Arc<dyn RdmaProvider>` — both roles (bindings, testing, mock)

All trait methods have default implementations returning `OperationNotSupported`.

### Channel-based API (the current API)

**Client side:**
```rust
let ch = provider.bind(size, key)?;          // allocate + register buffer
let put = ch.prepare_put(offset, size)?;     // token at put.token()
// ... send PUT request with x-amz-rdma-token header ...
ch.complete_put(put, rdma_status, n)?;       // confirm; drops deregisters buffer
```

Convenience combinators on `RdmaClientChannel`:
- `ch.put(offset, size, write_fn, send_fn)` — fill buffer, prepare, send, complete in one call
- `ch.get(offset, size, send_fn, read_fn)` — prepare, send, complete, read data

**Server side:**
```rust
let srv = provider.open_channel(key, token)?;
let n = srv.handle_put(offset, size, Box::new(|data: &[u8]| {
    // store data; return Ok(bytes_written)
    Ok(data.len())
}))?;
```

### Concrete providers

| Provider | Location | Description |
|----------|----------|-------------|
| `MockRdmaProvider` | `src/mock.rs` | `/dev/shm` SHM files; both client+server roles; no hardware |
| `RdmaPlugin` | `src/rdma_client.rs` + `src/rdma_server.rs` | Runtime `.so` loader via `libloading` |

### Plugin / vtable system

The `rdma-client` and `rdma-server` features enable runtime loading of `.so` plugins:

- **`providers/client/`** → `libs3rdmaclient.so` exports `s3_rdma_get_client_vtable(name)`
- **`providers/server/`** → `libs3rdmaserver.so` exports `s3_rdma_get_server_vtable(name)`

Each plugin takes a backend name (`"mock"` or `"cuobj"`).  Plugin search order:
`LD_LIBRARY_PATH` → executable dir → `/lib64/` → `/usr/lib64/`.
The main binary has **no compile-time dependency** on any RDMA SDK.

### Feature flags (`Cargo.toml`)

| Feature | Deps | Effect |
|---------|------|--------|
| `rdma-client` | `libloading` | `RdmaPluginClient`, `rdma_available()`, unified `RdmaPlugin` |
| `rdma-server` | `libloading` | `RdmaPluginServer`, unified `RdmaPlugin` |
| `pyo3` | `pyo3` | Python bindings (`src/python.rs`) |
| (none) | — | Only `MockRdmaProvider` available |

> **Note:** `BufferPool`, `OwnedBufferGuard`, and `RdmaWorkerPool` have moved to the
> `hsg-rdma` crate (hsg project) which owns pool management, observability, and async APIs.

### Provider builder (`src/provider.rs`)

| Builder call | Returns | Notes |
|--------------|---------|-------|
| `RdmaProviderConfig::builder().provider(name).build()` | `Arc<dyn RdmaProvider>` | name: `"mock"`, `"cuobj"`, `"auto"` |
| `RdmaProviderConfig::builder().provider(name).build_client()` | `Arc<dyn RdmaClientProvider>` | client-only role |
| `RdmaProviderConfig::builder().provider(name).build_server()` | `Arc<dyn RdmaServerProvider>` | server-only role |

`"auto"` tries `"cuobj"` first, falls back to `"mock"`.

### Key supporting types

- **`RdmaToken`** (`src/token.rs`): Wraps raw token bytes with parsed metadata (provider,
  size, offset).  Handles mock (`RDMA_{PUT|GET}_name_size_offset_key_k`) and cuObj
  (`vaddr:size:rkey:…`) formats.  `raw()` returns only the base descriptor for the cuObj SDK.
- **`RdmaError`** (`src/error.rs`): Error enum.  `is_fallback_eligible()` returns `true`
  for errors where downgrading to plain HTTP is safe (plugin unavailable, buffer too small, etc.).
- **`ByteCursor`** (`src/ffi.rs`): C-ABI `{ptr, len}` struct for crossing plugin boundaries.
- **`RdmaServerConfig`** (`src/provider.rs`): Unified config for server providers.
  Fields: `shm_path` (mock), `listen_addr`/`listen_port`/`buf_size` (cuObj).

### HTTP protocol constants (`src/provider.rs`)

```
RDMA_TOKEN_HEADER  = "x-amz-rdma-token"
RDMA_REPLY_HEADER  = "x-amz-rdma-reply"
RDMA_BYTES_HEADER  = "x-amz-rdma-bytes"
RDMA_STREAM_HEADER = "x-amz-rdma-stream"
RDMA_TOKEN_SEPARATOR = '|'
```

### Language bindings

**Python (`src/python.rs`, built via `bindings/python/`):**
- `PyRdmaProvider` holds `Arc<dyn RdmaProvider>` + `Mutex<HashMap<Vec<u8>, PendingTransfer>>`
- `prepare_put_token` / `prepare_get_token` call `bind()` and store the channel in the pending map
- `process_rdma_reply` looks up the pending transfer, calls `complete_put/get`, invokes callback
- `register_memory` / `deregister_memory` are no-ops (kept for backward compatibility)
- `handle_put` / `handle_get` call `open_channel` + closure-based transfer

**Java (`bindings/java/src/lib.rs`):**
- `ProviderState` holds `Arc<dyn RdmaProvider>` + `Mutex<HashMap<Vec<u8>, PendingTransfer>>`
- Same pattern as Python: `nativePreparePutToken` → bind + store; `nativeProcessRdmaReply` → complete
- `nativeRegisterMemory` / `nativeDeregisterMemory` are no-ops
- `nativeHandlePut` / `nativeHandleGet` call `open_channel` + collect/supply data via closures
- Supertrait ambiguity: when calling methods on `Arc<dyn RdmaProvider>` that exist on both
  `RdmaClientProvider` and `RdmaServerProvider`, use UFCS:
  `RdmaServerProvider::buf_size(&*state.provider)`

### Known constraints

**jemalloc is incompatible with cuobj** (`providers/client/src/rdma.rs`):
`tikv-jemallocator` intercepts `malloc`/`free` globally.  cuobj's C++ internals use
glibc `new`/`delete`; with jemalloc intercepting `free`, deallocation corrupts the
allocator after ~3 operations.  Build any binary that loads `libs3rdmaclient.so` with
`--no-default-features` (or ensure jemalloc is not a default feature).

### Mock token format

```
RDMA_{PUT|GET}_{shm_name}_{size}_{offset}[_key_{s3_key}]
```

- `shm_name` — filename only, no `/dev/shm/` prefix; **must not contain underscores**
- Parsed with `splitn(7, '_')`; hyphens in names are fine
