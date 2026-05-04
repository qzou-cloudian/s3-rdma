# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build & test commands

```sh
# Build everything (core + providers + Java JNI)
cargo build --workspace

# Build with specific feature flags
cargo build -p s3-rdma --features rdma-client,rdma-server
cargo build -p s3-rdma --features buffer-pool,engine

# Client plugin (mock backend)
cargo build -p s3-rdma-client --features mock
# Client plugin with cuObj hardware backend
cargo build -p s3-rdma-client --features cuobj

# Server plugin (mock backend)
cargo build -p s3-rdma-server --features mock
# Server plugin with cuObj hardware backend
cargo build -p s3-rdma-server --features cuobj

# Run all tests
cargo test --workspace
# Run tests for a single crate
cargo test -p s3-rdma
# Run a specific test
cargo test -p s3-rdma -- token::tests::parse_mock_put

# Run the Rust example
cargo run --example mock_provider --manifest-path core/Cargo.toml

# Python: build extension (requires venv with maturin)
cd bindings/python && python3 -m maturin develop
# Python: run example
cd bindings/python && python3 examples/example.py

# Java: build JNI library + classes
cargo build --manifest-path bindings/java/Cargo.toml
mvn compile -f bindings/java/pom.xml
```

## Architecture

This is a GPU-Direct RDMA provider for S3 object storage with multi-language SDK (Rust, Python, Java). No NVIDIA hardware is needed for development — the mock provider exercises the full API path.

### Workspace layout (`Cargo.toml`)

```
s3-rdma/                          # workspace root
├── core/                         # s3-rdma crate — traits, mock provider, plugin loader
├── providers/client/             # cdylib → libs3rdmaclient.so
├── providers/server/             # cdylib → libs3rdmaserver.so
└── bindings/java/                # cdylib → libs3rdmajni.so + Maven project
```

`bindings/python/` is a separate maturin project (not a workspace member) that depends on `core` with the `pyo3` feature.

### Trait hierarchy (core/src/provider.rs)

```
RdmaProvider: RdmaClientProvider + RdmaServerProvider
  ├── RdmaClientProvider  — register memory, generate PUT/GET tokens, process replies
  └── RdmaServerProvider  — handle_put, handle_get, and zero-copy (_zc) variants
```

Consumer code should use the narrowest trait: `Arc<dyn RdmaClientProvider>` for client nodes, `Arc<dyn RdmaServerProvider>` for server/gateway nodes, `Arc<dyn RdmaProvider>` only when both roles are needed. All trait methods have default implementations returning `OperationNotSupported` — implementors only override what they support.

### Concrete providers

| Provider | Location | Description |
|----------|----------|-------------|
| `MockRdmaProvider` | `core/src/mock.rs` | POSIX shared-memory (`/dev/shm`), both client+server roles |
| `RdmaPlugin` | `core/src/rdma_client.rs` + `core/src/rdma_server.rs` | Runtime `.so` loader, supports client/server/both |
| `RdmaEngine` (feature `engine`) | `core/src/engine.rs` | Fixed-size worker pool wrapping any `RdmaProvider`; dispatches server ops to N threads |

### Plugin / vtable system

The `rdma-client` and `rdma-server` features enable runtime loading of `.so` plugins via `libloading`. Providers are built as `cdylib` crates that export C-ABI vtables:

- **`providers/client/`** → `libs3rdmaclient.so` exports `s3_rdma_get_client_vtable(name)` → returns `*const RdmaProviderVtable`
- **`providers/server/`** → `libs3rdmaserver.so` exports `s3_rdma_get_server_vtable(name)` → returns `*const RdmaServerVtable`

Each plugin accepts a backend name string (`"mock"` or `"rdma"`) and routes to the corresponding implementation. The `"mock"` backend in `providers/client/src/mock.rs` delegates to `MockRdmaProvider`; the `"rdma"` backend in `providers/client/src/rdma.rs` uses the `cuobj` crate for real NVIDIA hardware.

Plugin search order: `LD_LIBRARY_PATH` → executable dir → `/lib64/` → `/usr/lib64/`. This means the main binary has **no compile-time dependency** on any RDMA SDK.

### Feature flags (core/Cargo.toml)

| Feature | Deps | Effect |
|---------|------|--------|
| `rdma-client` | `libloading` | `RdmaPluginClient`, `rdma_available()`, unified `RdmaPlugin` |
| `rdma-server` | `libloading` | `RdmaPluginServer`, unified `RdmaPlugin` |
| `buffer-pool` | `crossbeam-queue`, `tokio` | `BufferPool` / `OwnedBufferGuard` |
| `engine` | (none) | `RdmaEngine` worker pool |
| `pyo3` | `pyo3` | Python bindings (`core/src/python.rs`) |

### Provider factory functions (core/src/lib.rs)

`create_provider(name, debug, role, cfg)` returns `Arc<dyn RdmaProvider>`. Name can be `"mock"`, `"rdma"`, or `"auto"` (tries rdma first, falls back to mock). There are also `create_client_provider` and `create_server_provider` convenience wrappers.

### Key supporting types

- **`RdmaToken`** (`core/src/token.rs`): Wraps raw token bytes with parsed metadata (provider, size, offset). Handles mock (`RDMA_PUT_shm_size_offset_key_k`) and cuObj (`vaddr:size:rkey:...`) formats, plus an extended cuObj format with trailing `:decimal_size:decimal_offset`. The `raw()` method always returns only the base descriptor suitable for the cuObj C SDK.
- **`RdmaError`** (`core/src/error.rs`): Error enum with `is_fallback_eligible()` — returns true for errors where falling back to plain HTTP is safe (memory not suitable, token gen failed, plugin unavailable, buffer too small, provider unavailable).
- **`ByteCursor`** (`core/src/ffi.rs`): C-ABI-compatible `{ptr, len}` struct for crossing plugin boundaries.
- **`BufferPool`** (feature `buffer-pool`, `core/src/pool.rs`): Thread-safe pool of 4 KiB-aligned pre-registered buffers with `try_acquire_owned`, `acquire_owned` (async), and `acquire_owned_blocking`.

### HTTP protocol constants (core/src/provider.rs)

`RDMA_TOKEN_HEADER` (`x-amz-rdma-token`), `RDMA_REPLY_HEADER` (`x-amz-rdma-reply`), `RDMA_BYTES_HEADER` (`x-amz-rdma-bytes`), `RDMA_STREAM_HEADER` (`x-amz-rdma-stream`), `RDMA_TOKEN_SEPARATOR` (`|`). Providers can override header names via the trait methods.
