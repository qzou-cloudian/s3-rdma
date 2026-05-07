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
            shm_path: "/dev/shm".to_owned(),
            listen_addr: String::new(),
            listen_port: 0,
            buf_size: DEFAULT_RDMA_BUF_SIZE,
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

// ── RdmaPutHandle / RdmaGetHandle ─────────────────────────────────────────────

/// Handle for an in-flight PUT (upload) RDMA operation.
///
/// Created by [`RdmaClientChannel::prepare_put`].  The handle bundles the
/// opaque RDMA descriptor token with the operation's offset and size metadata.
/// Consumed by [`RdmaClientChannel::complete_put`].
///
/// The token bytes are exposed via [`token()`](Self::token) for serialization
/// into the `x-amz-rdma-token` HTTP header.
#[derive(Debug)]
pub struct RdmaPutHandle {
    pub(crate) token: Vec<u8>,
    pub(crate) offset: usize,
    pub(crate) size: usize,
}

/// Handle for an in-flight GET (download) RDMA operation.
///
/// Created by [`RdmaClientChannel::prepare_get`].  The handle bundles the
/// opaque RDMA descriptor token with the operation's offset and size metadata.
/// Consumed by [`RdmaClientChannel::complete_get`].
#[derive(Debug)]
pub struct RdmaGetHandle {
    pub(crate) token: Vec<u8>,
    pub(crate) offset: usize,
    pub(crate) size: usize,
}

impl RdmaPutHandle {
    /// Create a new handle.  Used by provider implementations that construct
    /// handles in external crates (e.g. the `s3-rdma-client` plugin crate).
    pub fn new(token: Vec<u8>, offset: usize, size: usize) -> Self {
        Self {
            token,
            offset,
            size,
        }
    }

    /// Opaque token bytes for the `x-amz-rdma-token` HTTP header.
    pub fn token(&self) -> &[u8] {
        &self.token
    }

    /// Byte offset within the registered buffer for this transfer.
    pub fn offset(&self) -> usize {
        self.offset
    }

    /// Number of bytes this transfer covers.
    pub fn size(&self) -> usize {
        self.size
    }
}

impl RdmaGetHandle {
    /// Create a new handle.  Used by provider implementations that construct
    /// handles in external crates (e.g. the `s3-rdma-client` plugin crate).
    pub fn new(token: Vec<u8>, offset: usize, size: usize) -> Self {
        Self {
            token,
            offset,
            size,
        }
    }

    /// Opaque token bytes for the `x-amz-rdma-token` HTTP header.
    pub fn token(&self) -> &[u8] {
        &self.token
    }

    /// Byte offset within the registered buffer for this transfer.
    pub fn offset(&self) -> usize {
        self.offset
    }

    /// Number of bytes this transfer covers.
    pub fn size(&self) -> usize {
        self.size
    }
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
    /// Prepare a PUT (upload) operation, returning a typed handle.
    ///
    /// The token points directly to the registered buffer for zero-copy
    /// RDMA-read by the server — no intermediate buffer copy is needed.
    fn prepare_put(
        &self,
        offset: usize,
        size: usize,
    ) -> Result<RdmaPutHandle, crate::error::RdmaError>;

    /// Prepare a GET (download) operation, returning a typed handle.
    ///
    /// The token points directly to the registered buffer for zero-copy
    /// RDMA-write by the server.
    fn prepare_get(
        &self,
        offset: usize,
        size: usize,
    ) -> Result<RdmaGetHandle, crate::error::RdmaError>;

    /// Complete a PUT operation.  Consumes the handle.
    ///
    /// `rdma_status` is the `x-amz-rdma-reply` HTTP status code
    /// (200/204/206 = success, 501 = fallback, 0 = absent).
    /// `transferred` is the value of `x-amz-rdma-bytes` (0 if absent).
    fn complete_put(
        &self,
        handle: RdmaPutHandle,
        rdma_status: u16,
        transferred: usize,
    ) -> Result<(), crate::error::RdmaError>;

    /// Complete a GET operation.  Consumes the handle.
    fn complete_get(
        &self,
        handle: RdmaGetHandle,
        rdma_status: u16,
        transferred: usize,
    ) -> Result<(), crate::error::RdmaError>;

    /// Execute a full PUT: call `write` to fill the buffer directly,
    /// prepare the token for the written range, call `send` with the token,
    /// then complete.  Zero-copy — `write` fills the registered buffer
    /// (e.g. via `pread`), then the NIC RDMA-reads from it.
    ///
    /// `write` receives `&mut buf[offset..offset+size]` and must return the
    /// number of bytes actually written.  `send` receives the token bytes
    /// and must return `(rdma_status, transferred_bytes)`.
    fn put(
        &self,
        offset: usize,
        size: usize,
        write: Box<dyn FnOnce(&mut [u8]) -> usize + Send + '_>,
        send: Box<dyn FnOnce(&[u8]) -> Result<(u16, usize), crate::error::RdmaError> + Send + '_>,
    ) -> Result<usize, crate::error::RdmaError> {
        let buf = unsafe { std::slice::from_raw_parts_mut(self.ptr() as *mut u8, self.size()) };
        let cap = buf.len().saturating_sub(offset);
        let n = write(&mut buf[offset..offset.saturating_add(size.min(cap))]);
        let handle = self.prepare_put(offset, n)?;
        let (status, transferred) = send(handle.token())?;
        self.complete_put(handle, status, transferred)?;
        Ok(transferred)
    }

    /// Execute a full GET: prepare token, `send` the token to the server,
    /// complete, then pass the downloaded data to `read`.  The buffer is
    /// accessed only within `get` — no dangling reference can outlive the call.
    ///
    /// `send` receives the token bytes and must return `(rdma_status, transferred)`.
    /// `read` receives the valid slice of the buffer containing the downloaded
    /// data.  Capture the data in the closure (e.g. extend a Vec, copy to a
    /// file descriptor).  Returns the number of bytes transferred.
    fn get(
        &self,
        offset: usize,
        size: usize,
        send: Box<dyn FnOnce(&[u8]) -> Result<(u16, usize), crate::error::RdmaError> + Send + '_>,
        read: Box<dyn FnOnce(&[u8]) + Send + '_>,
    ) -> Result<usize, crate::error::RdmaError> {
        let handle = self.prepare_get(offset, size)?;
        let (status, transferred) = send(handle.token())?;
        self.complete_get(handle, status, transferred)?;
        let buf = unsafe { std::slice::from_raw_parts(self.ptr() as *const u8, self.size()) };
        read(&buf[..transferred.min(buf.len())]);
        Ok(transferred)
    }

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

/// Client-side RDMA operations: allocate, register, and bind buffers.
///
/// Implemented by any provider that acts as an S3 client.  The provider
/// allocates and manages buffers internally — callers access the buffer
/// through the returned [`RdmaClientChannel`].
///
/// All methods have default implementations that return
/// [`RdmaError::OperationNotSupported`](crate::error::RdmaError::OperationNotSupported)
/// so implementors only override what they actually support.
pub trait RdmaClientProvider: Send + Sync {
    /// Allocate, register, and bind a buffer to an S3 key.
    ///
    /// This is the **primary** client-side method.  The provider allocates
    /// a buffer of `size` bytes, registers it, and returns an
    /// [`RdmaClientChannel`].  The buffer is accessible via
    /// [`ch.ptr()`](RdmaClientChannel::ptr) /
    /// [`ch.size()`](RdmaClientChannel::size).
    /// Dropping the channel deregisters and frees the buffer.
    fn bind(
        &self,
        _size: usize,
        _key: &[u8],
    ) -> Result<Box<dyn RdmaClientChannel>, crate::error::RdmaError> {
        Err(crate::error::RdmaError::OperationNotSupported(
            "bind: not supported by this provider".into(),
        ))
    }

    /// Bind a pre-registered buffer from the provider's internal pool.
    ///
    /// Like [`bind`](Self::bind), but uses a buffer that was previously
    /// allocated and registered (e.g. at pool construction time).  Dropping
    /// the channel does **not** free the buffer — it returns to the pool.
    fn bind_registered(
        &self,
        _key: &[u8],
    ) -> Result<Box<dyn RdmaClientChannel>, crate::error::RdmaError> {
        Err(crate::error::RdmaError::OperationNotSupported(
            "bind_registered: not supported by this provider".into(),
        ))
    }

    /// Default buffer size in bytes.  Matches [`RdmaServerProvider::buf_size`].
    fn buf_size(&self) -> usize {
        0
    }

    /// Human-readable provider name.
    fn name(&self) -> &str {
        "unknown"
    }

    /// Transport protocol identifier (e.g. `"mock"`, `"cuobj"`).
    fn protocol(&self) -> &str {
        "unknown"
    }

    // ── Telemetry ────────────────────────────────────────────────────────────

    /// Returns `true` if this provider supports telemetry configuration.
    fn supports_telemetry(&self) -> bool {
        false
    }

    /// Configure the telemetry output destination and verbosity level.
    fn setup_telemetry(&self, _config: &TelemetryConfig) -> Result<(), crate::error::RdmaError> {
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
    /// Zero-copy RDMA PUT: RDMA-read from the client buffer, then pass the
    /// received data to `write` for storage.  The closure receives a `&[u8]`
    /// view of the transferred bytes.
    ///
    /// `offset` is the byte offset into the client's registered buffer.
    /// `size` is the number of bytes to transfer.
    fn handle_put(
        &self,
        offset: usize,
        size:   usize,
        write:  Box<dyn FnOnce(&[u8]) -> Result<usize, crate::error::RdmaError> + Send + '_>,
    ) -> Result<usize, crate::error::RdmaError>;

    /// Zero-copy RDMA GET: call `read` to fill the buffer from storage, then
    /// RDMA-write it to the client.  The closure receives a `&mut [u8]` view
    /// of the RDMA-registered buffer to fill and returns the number of bytes
    /// written into it, or an error.
    ///
    /// `offset` is the byte offset into the client's registered buffer.
    /// `size` is the number of bytes to transfer (0 = use token capacity).
    fn handle_get(
        &self,
        offset: usize,
        size:   usize,
        read:   Box<dyn FnOnce(&mut [u8]) -> Result<usize, crate::error::RdmaError> + Send + '_>,
    ) -> Result<usize, crate::error::RdmaError>;

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
    fn buf_size(&self) -> usize {
        0
    }

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

    fn supports_telemetry(&self) -> bool {
        false
    }

    fn setup_telemetry(&self, _config: &TelemetryConfig) -> Result<(), crate::error::RdmaError> {
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
        Self {
            cfg: std::sync::Mutex::new(None),
        }
    }

    pub fn supports_telemetry(&self) -> bool {
        true
    }

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
    fn default() -> Self {
        Self::new()
    }
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
