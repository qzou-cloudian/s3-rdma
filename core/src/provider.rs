//! RDMA provider types, traits, and configuration.
//!
//! This module contains the central [`RdmaProvider`] trait family and all
//! associated types used throughout the s3-rdma crate and its consumers.
//!
//! ## Trait hierarchy
//!
//! ```text
//! RdmaProvider: RdmaClientProvider + RdmaServerProvider
//!   │
//!   ├── RdmaClientProvider  — register memory, generate tokens, process replies
//!   └── RdmaServerProvider  — handle PUT/GET transfers from clients
//! ```
//!
//! Code that only handles client-side operations accepts
//! `Arc<dyn RdmaClientProvider>`.  Code that only handles server-side
//! operations accepts `Arc<dyn RdmaServerProvider>`.  The combined
//! `Arc<dyn RdmaProvider>` is used when both roles are needed (e.g. same-node
//! mock testing, Python/Java/C-ABI bindings).

/// Convert the `x-amz-rdma-reply` numeric status to a callback `error_code`.
///
/// - `0`         → `-1` (absent header — treated as error)
/// - `200..300`  → `0`  (RDMA success)
/// - other       → status cast to `i32` (e.g. `501` for RDMA fallback)
pub(crate) fn rdma_status_to_error_code(rdma_status: u16) -> i32 {
    if rdma_status != 0 && (200..300).contains(&rdma_status) {
        0
    } else if rdma_status == 0 {
        -1
    } else {
        rdma_status as i32
    }
}

/// Result delivered when an RDMA operation completes.
#[derive(Debug)]
pub struct RdmaCompletionResult {
    /// 0 on success; non-zero error code on failure.
    pub error_code: i32,
    /// Reply token echoed back from the server (may be empty).
    /// Used by JNI and C-ABI bindings; otherwise typically empty.
    pub reply_token: Vec<u8>,
}

/// Callback invoked when an async RDMA operation finishes.
pub type RdmaCompletionCallback = Box<dyn FnOnce(RdmaCompletionResult) + Send + 'static>;

// ── Default header names ─────────────────────────────────────────────────────

pub const RDMA_TOKEN_HEADER: &str = "x-amz-rdma-token";
pub const RDMA_REPLY_HEADER: &str = "x-amz-rdma-reply";
pub const RDMA_BYTES_HEADER: &str = "x-amz-rdma-bytes";

/// Default RDMA buffer slot size: 128 MiB.
///
/// Used as the `buf_size` default in [`RdmaServerConfig`] and as the
/// allocation cap in binding-layer `handle_put` helpers.
pub const DEFAULT_RDMA_BUF_SIZE: usize = 128 * 1024 * 1024;

/// Required alignment for RDMA-registered buffers: 4 KiB.
///
/// All buffers passed to [`RdmaClientProvider::register_memory`] must be
/// aligned to this boundary.  Registered buffers are **pinned** (locked in
/// physical memory — not swappable or migratable) for the duration of
/// registration so the RDMA NIC can DMA to/from them without page faults.
pub const RDMA_REQUIRED_ALIGNMENT: usize = 4096;

/// Header that signals HTTP/2 streaming token mode.
///
/// When present with value `"true"`, the PUT request body contains
/// newline-delimited RDMA token strings (one per line) instead of object data.
/// Each token line uses the extended format `<base_token>:<size>:<offset>` so
/// the server knows the exact transfer size and object offset for every chunk.
pub const RDMA_STREAM_HEADER: &str = "x-amz-rdma-stream";

/// Separator used when multiple RDMA tokens (or sizes) are packed into a single
/// HTTP header value.
///
/// cuObj tokens are hex-encoded (`0x…`) and never contain `|`, making it a safe
/// delimiter.  Single-token transfers are unaffected (no `|` present).
///
/// # Multi-token protocol
///
/// **PUT** — the client sends pipe-separated tokens: `tok0|tok1|tok2`.
/// **GET** — the client pre-allocates N receive buffers and sends their tokens
/// the same way.
pub const RDMA_TOKEN_SEPARATOR: char = '|';

// ── TelemLevel ─────────────────────────────────────────────────────────────────

/// Telemetry verbosity level.
///
/// Levels are ordered: `Off` < `Error` < `Info` < `Debug`.  Setting a level
/// enables that level and all less-verbose levels (e.g. `Info` includes
/// `Error`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[repr(u32)]
pub enum TelemLevel {
    /// No telemetry output.
    Off = 0,
    /// Error messages only.
    Error = 1,
    /// Error + info messages.
    Info = 2,
    /// Error + info + debug messages (most verbose).
    Debug = 3,
}

// ── TelemetryConfig ────────────────────────────────────────────────────────────

/// Telemetry configuration for an RDMA provider.
///
/// Controls log output destination (OpenTelemetry or plain stream) and
/// verbosity level.
#[derive(Debug, Clone)]
pub struct TelemetryConfig {
    /// `true` = use OpenTelemetry backend; `false` = write to an output stream.
    pub use_otel: bool,
    /// Optional log file path (ostream mode only; `None` = stdout).
    pub log_file: Option<String>,
    /// Verbosity level.
    pub level: TelemLevel,
}

impl Default for TelemetryConfig {
    fn default() -> Self {
        Self {
            use_otel: false,
            log_file: None,
            level: TelemLevel::Error,
        }
    }
}

// ── RdmaServerConfig ─────────────────────────────────────────────────────────

/// Unified configuration for any RDMA **server** provider.
///
/// Fields that are irrelevant for a given provider are silently ignored:
///
/// | Field           | mock | cuobj |
/// |-----------------|------|-------|
/// | `shm_path`      | ✓    | –     |
/// | `listen_addr`   | –    | ✓     |
/// | `listen_port`   | –    | ✓     |
/// | `buf_size`      | –    | ✓     |
#[derive(Debug, Clone)]
pub struct RdmaServerConfig {
    /// Directory used for POSIX shared-memory files (mock provider).
    pub shm_path: String,
    /// RDMA listen address for the cuObj provider (e.g. `"192.168.1.1"`).
    pub listen_addr: String,
    /// RDMA listen port for the cuObj provider.
    pub listen_port: u16,
    /// Size in bytes of each buffer slot (cuObj provider).
    pub buf_size: usize,
}

impl Default for RdmaServerConfig {
    fn default() -> Self {
        Self {
            shm_path:       "/dev/shm".to_owned(),
            listen_addr:    String::new(),
            listen_port:    0,
            buf_size:       DEFAULT_RDMA_BUF_SIZE,
        }
    }
}

// ── RdmaRole ─────────────────────────────────────────────────────────────────

/// Requested role(s) for a new RDMA provider.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RdmaRole {
    /// Client only — register memory, generate tokens.
    Client,
    /// Server only — handle_put / handle_get.
    Server,
    /// Both client and server roles — typical for a gateway node.
    Both,
}

// ── RdmaClientChannel trait ────────────────────────────────────────────────────

/// An open client-side RDMA session bound to a registered buffer and S3 key.
///
/// Created by [`RdmaClientProvider::bind`].  The channel owns the buffer
/// registration and key, so token generation and reply processing take only
/// offset and size — no pointer or key parameters.
///
/// Dropping the channel deregisters the buffer.
pub trait RdmaClientChannel: Send + Sync {
    /// Generate an RDMA descriptor token for a PUT (upload).
    fn prepare_put_token(&self, offset: usize, size: usize) -> Result<Vec<u8>, crate::error::RdmaError>;

    /// Generate an RDMA descriptor token for a GET (download).
    fn prepare_get_token(&self, offset: usize, size: usize) -> Result<Vec<u8>, crate::error::RdmaError>;

    /// Process the server reply and clean up.
    fn process_rdma_reply(
        &self,
        rdma_status: u16,
        request_token: &[u8],
        transferred: usize,
        callback: RdmaCompletionCallback,
    ) -> Result<(), crate::error::RdmaError>;

    /// The S3 object key this channel is bound to.
    fn key(&self) -> &[u8];

    /// Base pointer of the registered buffer.
    fn ptr(&self) -> *const u8;

    /// Size of the registered buffer in bytes.
    fn size(&self) -> usize;

    /// Maximum single-transfer size for this buffer.
    fn get_max_transfer_size(&self) -> usize;
}

// ── RdmaClientProvider trait ──────────────────────────────────────────────────

/// Client-side RDMA operations: memory registration and token generation.
///
/// Implemented by any provider that acts as an S3 client — the node that
/// initiates transfers by registering buffers and attaching tokens to HTTP
/// requests.
///
/// All methods have default implementations that return
/// [`RdmaError::OperationNotSupported`](crate::error::RdmaError::OperationNotSupported)
/// so implementors only override what they actually support.
pub trait RdmaClientProvider: Send + Sync {
    /// Returns `true` if the memory at `ptr..ptr+size` is eligible for RDMA.
    ///
    /// Implementors should check that `ptr` is non-null, aligned to
    /// [`RDMA_REQUIRED_ALIGNMENT`], and backed by pinnable memory (not
    /// garbage-collected or JIT-compiled pages).
    fn is_memory_suitable(&self, _ptr: *const u8, _size: usize) -> bool {
        false
    }

    /// Bind a buffer to an S3 key, returning an [`RdmaClientChannel`].
    ///
    /// This is the **primary** client-side method.  The returned channel
    /// registers the buffer, owns the key, and provides token generation
    /// and reply processing.  Dropping the channel deregisters the buffer.
    ///
    /// The existing [`register_memory`](Self::register_memory) /
    /// [`deregister_memory`](Self::deregister_memory) /
    /// [`prepare_put_token`](Self::prepare_put_token) /
    /// [`prepare_get_token`](Self::prepare_get_token) methods are
    /// retained for compatibility but new code should prefer `bind`.
    fn bind(
        &self,
        _ptr: *mut u8,
        _size: usize,
        _key: &[u8],
    ) -> Result<Box<dyn RdmaClientChannel>, crate::error::RdmaError> {
        Err(crate::error::RdmaError::OperationNotSupported(
            "bind: not supported by this provider".into(),
        ))
    }

    /// Register a memory region so the RDMA subsystem can access it.
    ///
    /// The registered buffer is **pinned** — the OS must not swap, migrate, or
    /// move the physical pages for the duration of registration.  The provider
    /// locks the pages (e.g. via `mlock` or IOMMU pinning) so the RDMA NIC can
    /// perform DMA to/from them without page faults.
    ///
    /// **Alignment:** `ptr` must be aligned to [`RDMA_REQUIRED_ALIGNMENT`]
    /// (4 KiB).  Implementors should return [`RdmaError::MemoryNotSuitable`]
    /// for unaligned pointers.
    ///
    /// # Safety
    /// `ptr` must remain valid until [`deregister_memory`](Self::deregister_memory).
    fn register_memory(&self, _ptr: *mut u8, _size: usize) -> Result<(), crate::error::RdmaError> {
        Err(crate::error::RdmaError::OperationNotSupported(
            "register_memory: not supported by this provider".into(),
        ))
    }

    /// Deregister a previously registered memory region.
    fn deregister_memory(&self, _ptr: *mut u8) -> Result<(), crate::error::RdmaError> {
        Err(crate::error::RdmaError::OperationNotSupported(
            "deregister_memory: not supported by this provider".into(),
        ))
    }

    /// Maximum single-transfer size for the given buffer.
    /// Default: `usize::MAX` (no provider-imposed limit).
    fn get_max_transfer_size(&self, _ptr: *const u8) -> usize {
        usize::MAX
    }

    /// Generate an RDMA descriptor token for a PUT (upload) sub-request.
    ///
    /// Returns bytes used as the value of the `x-amz-rdma-token` request header.
    fn prepare_put_token(
        &self,
        _s3_key: &[u8],
        _buffer: *const u8,
        _size: usize,
        _offset: usize,
    ) -> Result<Vec<u8>, crate::error::RdmaError> {
        Err(crate::error::RdmaError::OperationNotSupported(
            "prepare_put_token: not supported by this provider".into(),
        ))
    }

    /// Generate an RDMA descriptor token for a GET (download) sub-request.
    fn prepare_get_token(
        &self,
        _s3_key: &[u8],
        _buffer: *mut u8,
        _size: usize,
        _offset: usize,
    ) -> Result<Vec<u8>, crate::error::RdmaError> {
        Err(crate::error::RdmaError::OperationNotSupported(
            "prepare_get_token: not supported by this provider".into(),
        ))
    }

    /// Process the `x-amz-rdma-reply` numeric status returned by the S3 server.
    ///
    /// The default implementation fires `callback` immediately based on whether
    /// `rdma_status` is a 2xx success or not.  Provider overrides (e.g. mock)
    /// use this to copy SHM data into the buffer before calling back.
    fn process_rdma_reply(
        &self,
        rdma_status: u16,
        _request_token: &[u8],
        _transferred: usize,
        callback: RdmaCompletionCallback,
    ) -> Result<(), crate::error::RdmaError> {
        callback(RdmaCompletionResult {
            error_code: rdma_status_to_error_code(rdma_status),
            reply_token: vec![],
        });
        Ok(())
    }

    /// Human-readable name of this provider implementation (e.g. `"MockRdmaProvider"`).
    ///
    /// Default: `"unknown"`.  Providers should override this for logging and debugging.
    fn name(&self) -> &str { "unknown" }

    /// Transport protocol identifier (e.g. `"mock"`, `"cuobj"`).
    ///
    /// Default: `"unknown"`.
    fn protocol(&self) -> &str { "unknown" }

    // ── Telemetry ────────────────────────────────────────────────────────────
    //
    // Lifecycle: call `supports_telemetry()` → `setup_telemetry()` →
    // optionally `set_telem_level()` → `shutdown_telemetry()` before drop.
    // For the cuobj backend these methods forward to the C++ SDK's static
    // `cuObjClient::setupTelemetry` / `cuObjServer::setupTelemetry`;
    // the mock backend stores the config in-process.

    /// Returns `true` if this provider supports telemetry configuration.
    fn supports_telemetry(&self) -> bool { false }

    /// Configure the telemetry output destination and verbosity level.
    ///
    /// May be called multiple times to change the destination; the previous
    /// configuration is replaced.
    fn setup_telemetry(
        &self,
        _config: &TelemetryConfig,
    ) -> Result<(), crate::error::RdmaError> {
        Err(crate::error::RdmaError::OperationNotSupported(
            "telemetry not supported by this provider".into(),
        ))
    }

    /// Shut down telemetry and reset to the default output stream.
    fn shutdown_telemetry(&self) -> Result<(), crate::error::RdmaError> {
        Err(crate::error::RdmaError::OperationNotSupported(
            "telemetry not supported by this provider".into(),
        ))
    }

    /// Change the verbosity level without reconfiguring the output destination.
    fn set_telem_level(&self, _level: TelemLevel) -> Result<(), crate::error::RdmaError> {
        Err(crate::error::RdmaError::OperationNotSupported(
            "telemetry not supported by this provider".into(),
        ))
    }
}

// ── RdmaServerChannel trait ─────────────────────────────────────────────────────────

/// An open RDMA transfer session bound to a specific S3 object key and
/// RDMA descriptor token.
///
/// Created by [`RdmaServerProvider::open_channel`].  The channel owns the key
/// and token, so [`handle_put`](Self::handle_put) / [`handle_get`](Self::handle_get)
/// take only the buffer and offset — no key or token parameters.
///
/// Channels enable:
/// - **Chunked streaming** — open once, call `handle_put`/`handle_get` multiple
///   times with advancing offsets to transfer a large object in chunks.
/// - **Pre-parsed tokens** — the provider validates and parses the token at
///   `open_channel` time, avoiding repeated parsing on every call.
/// - **Per-transfer resources** — the channel can hold a cuObj channel ID,
///   pre-allocated buffer, or other resources for the lifetime of the transfer.
pub trait RdmaServerChannel: Send + Sync {
    /// Perform an RDMA PUT: read from the client buffer into `buf`.
    ///
    /// `offset` is the byte offset into the client's registered buffer.
    fn handle_put(&self, buf: &mut [u8], offset: usize, size: usize) -> Result<usize, crate::error::RdmaError>;

    /// Perform an RDMA GET: write `data` into the client's registered buffer.
    ///
    /// `offset` is the byte offset into the client's registered buffer.
    fn handle_get(&self, data: &[u8], offset: usize) -> Result<usize, crate::error::RdmaError>;

    /// Zero-copy RDMA PUT.
    ///
    /// # Safety
    /// `write_fn_data` must remain valid for the duration of the call.
    unsafe fn handle_put_zc(
        &self,
        _offset: usize,
        _size: usize,
        _write_fn: crate::ffi::RdmaIoWriteFn,
        _write_fn_data: *mut std::ffi::c_void,
    ) -> Result<usize, crate::error::RdmaError> {
        Err(crate::error::RdmaError::OperationNotSupported(
            "handle_put_zc: not supported by this provider".into(),
        ))
    }

    /// Zero-copy RDMA GET.
    ///
    /// # Safety
    /// `read_fn_data` must remain valid for the duration of the call.
    unsafe fn handle_get_zc(
        &self,
        _offset: usize,
        _read_fn: crate::ffi::RdmaIoReadFn,
        _read_fn_data: *mut std::ffi::c_void,
    ) -> Result<usize, crate::error::RdmaError> {
        Err(crate::error::RdmaError::OperationNotSupported(
            "handle_get_zc: not supported by this provider".into(),
        ))
    }

    /// The S3 object key this channel is bound to.
    fn key(&self) -> &[u8];

    /// The RDMA descriptor token this channel is bound to.
    fn token(&self) -> &[u8];

    /// Capacity of the server-side buffer for this channel.
    fn buf_size(&self) -> usize;
}

// ── RdmaServerProvider trait ──────────────────────────────────────────────────

/// Server-side RDMA operations: create channels for PUT/GET transfers.
///
/// Implemented by any provider that acts as an S3 server or gateway.  The only
/// server method is [`open_channel`](Self::open_channel) — it returns an
/// [`RdmaServerChannel`] bound to the given key+token pair.  All transfer operations
/// live on [`RdmaServerChannel`].
pub trait RdmaServerProvider: Send + Sync {
    /// Size in bytes of the server's per-slot transfer buffer.
    fn buf_size(&self) -> usize { 0 }

    /// Open an [`RdmaServerChannel`] for the given key and token.
    ///
    /// The returned channel owns the key and token.  All transfer operations
    /// ([`handle_put`](RdmaServerChannel::handle_put),
    /// [`handle_get`](RdmaServerChannel::handle_get), and their zero-copy variants)
    /// are called on the channel — the provider only creates channels.
    fn open_channel(
        &self,
        _key: &[u8],
        _token: &[u8],
    ) -> Result<Box<dyn RdmaServerChannel>, crate::error::RdmaError> {
        Err(crate::error::RdmaError::OperationNotSupported(
            "open_channel: not supported by this provider".into(),
        ))
    }

    // ── Telemetry (see same methods on [`RdmaClientProvider`]) ───────────────

    fn supports_telemetry(&self) -> bool { false }

    fn setup_telemetry(
        &self,
        _config: &TelemetryConfig,
    ) -> Result<(), crate::error::RdmaError> {
        Err(crate::error::RdmaError::OperationNotSupported(
            "telemetry not supported by this provider".into(),
        ))
    }

    fn shutdown_telemetry(&self) -> Result<(), crate::error::RdmaError> {
        Err(crate::error::RdmaError::OperationNotSupported(
            "telemetry not supported by this provider".into(),
        ))
    }

    fn set_telem_level(&self, _level: TelemLevel) -> Result<(), crate::error::RdmaError> {
        Err(crate::error::RdmaError::OperationNotSupported(
            "telemetry not supported by this provider".into(),
        ))
    }
}

// ── RdmaProvider trait ────────────────────────────────────────────────────────

/// Combined RDMA provider: supports both client and server roles.
///
/// This is the trait implemented by concrete providers such as
/// [`MockRdmaProvider`](crate::mock::MockRdmaProvider) and the
/// [`RdmaPlugin`] (which wraps the cuObj plugin at runtime).  It extends both
/// [`RdmaClientProvider`] and [`RdmaServerProvider`] and adds role query methods.
///
/// ## Choosing the right trait
///
/// | Scenario | Trait to use |
/// |---|---|
/// | S3 client node (register memory, generate tokens) | `Arc<dyn RdmaClientProvider>` |
/// | S3 server/gateway node (handle PUT/GET) | `Arc<dyn RdmaServerProvider>` |
/// | Both roles in the same node / testing | `Arc<dyn RdmaProvider>` |
///
/// ## Implementations
///
/// - [`MockRdmaProvider`](crate::mock::MockRdmaProvider) – `/dev/shm`-backed,
///   same-host only; supports both roles.
/// - [`RdmaPlugin`] – loads `libs3rdmaclient.so` / `libs3rdmaserver.so` at
///   runtime for real NVIDIA cuObject RDMA hardware.
pub trait RdmaProvider: RdmaClientProvider + RdmaServerProvider {
    /// Returns `true` if this provider supports client-side operations.
    fn supports_client(&self) -> bool;

    /// Returns `true` if this provider supports server-side operations.
    fn supports_server(&self) -> bool;
}

// ── TelemState ────────────────────────────────────────────────────────────────

/// Shared telemetry state for provider implementors.
///
/// Wraps an `Option<TelemetryConfig>` behind a mutex and provides the four
/// telemetry methods.  Implementors of [`RdmaClientProvider`] and
/// [`RdmaServerProvider`] can delegate their telemetry methods to a single
/// `TelemState` instance to avoid duplicating the four method bodies for each
/// trait.
///
/// Both traits define identically-named telemetry methods.  When calling
/// telemetry methods through `Arc<dyn RdmaProvider>`, disambiguate with UFCS:
/// `RdmaClientProvider::supports_telemetry(&*provider)`.
pub struct TelemState {
    cfg: std::sync::Mutex<Option<TelemetryConfig>>,
}

impl TelemState {
    pub const fn new() -> Self {
        Self { cfg: std::sync::Mutex::new(None) }
    }

    pub fn supports_telemetry(&self) -> bool { true }

    pub fn setup_telemetry(&self, config: &TelemetryConfig) -> Result<(), crate::RdmaError> {
        *self.cfg.lock().unwrap() = Some(config.clone());
        Ok(())
    }

    pub fn shutdown_telemetry(&self) -> Result<(), crate::RdmaError> {
        *self.cfg.lock().unwrap() = None;
        Ok(())
    }

    pub fn set_telem_level(&self, level: TelemLevel) -> Result<(), crate::RdmaError> {
        if let Some(ref mut cfg) = *self.cfg.lock().unwrap() {
            cfg.level = level;
        }
        Ok(())
    }
}

impl Default for TelemState {
    fn default() -> Self { Self::new() }
}

impl std::fmt::Debug for TelemState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TelemState").finish()
    }
}

// ── RdmaProviderConfig ────────────────────────────────────────────────────────

/// Unified configuration for constructing an [`RdmaProvider`].
///
/// The `server` fields are only used when `role` includes the server role.
/// Telemetry is configured post-construction via
/// [`RdmaClientProvider::setup_telemetry`].
#[derive(Debug, Clone)]
pub struct RdmaProviderConfig {
    /// Transport protocol: `"mock"`, `"cuobj"`, or `"auto"`.
    pub provider: String,
    /// Role(s) this provider should support.
    pub role: RdmaRole,
    /// Emit debug messages to stderr.
    pub debug: bool,
    /// Server-side configuration (used when `role` includes `Server`).
    pub server: RdmaServerConfig,
}

impl Default for RdmaProviderConfig {
    fn default() -> Self {
        Self {
            provider: "mock".to_owned(),
            role: RdmaRole::Both,
            debug: false,
            server: RdmaServerConfig::default(),
        }
    }
}
