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
//!   ├── RdmaClientProvider: RdmaProviderBase  — bind buffers, generate tokens
//!   └── RdmaServerProvider: RdmaProviderBase  — handle PUT/GET transfers
//! ```
//!
//! [`RdmaProviderBase`] provides shared methods (name, protocol, telemetry)
//! available on any provider regardless of role.
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
/// Each token line uses the extended format `<base_token>:<hex_offset>:<hex_size>` so
/// the server knows the exact transfer size and object offset for every chunk.
///
/// Not used inside this crate — exported for HTTP-layer consumers.
pub const RDMA_STREAM_HEADER: &str = "x-amz-rdma-stream";

/// Separator used when multiple RDMA tokens (or sizes) are packed into a single
/// HTTP header value.  Not used inside this crate — exported for HTTP-layer consumers.
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

impl Default for RdmaRole {
    fn default() -> Self {
        Self::Both
    }
}

// ── RdmaTransferHandle ─────────────────────────────────────────────────────────

/// Handle for an in-flight RDMA transfer (PUT or GET).
///
/// Created by [`RdmaClientChannel::prepare_put`] or
/// [`RdmaClientChannel::prepare_get`].  The handle bundles the opaque RDMA
/// descriptor token with the operation's offset and size metadata.
/// Consumed by [`RdmaClientChannel::complete_put`] or
/// [`RdmaClientChannel::complete_get`].
///
/// The token bytes are exposed via [`token()`](Self::token) for serialization
/// into the `x-amz-rdma-token` HTTP header.
#[derive(Debug)]
pub struct RdmaTransferHandle {
    pub(crate) token: Vec<u8>,
    pub(crate) offset: usize,
    pub(crate) size: usize,
}

impl RdmaTransferHandle {
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

// ── RdmaProviderBase trait ──────────────────────────────────────────────────────

/// Methods shared by every RDMA provider regardless of role.
///
/// [`RdmaClientProvider`] and [`RdmaServerProvider`] both extend this trait,
/// so `name()`, `protocol()`, and the four telemetry methods are available
/// on any provider reference without UFCS disambiguation.
///
/// All methods have default implementations returning sentinel values
/// (`"unknown"`, `false`, `OperationNotSupported`).
pub trait RdmaProviderBase: Send + Sync {
    /// Human-readable provider name.
    fn name(&self) -> &str {
        "unknown"
    }

    /// Transport protocol identifier (e.g. `"mock"`, `"cuobj"`).
    fn protocol(&self) -> &str {
        "unknown"
    }

    /// Returns `true` if this provider supports telemetry configuration.
    fn supports_telemetry(&self) -> bool {
        false
    }

    /// Configure the telemetry output destination and verbosity level.
    fn setup_telemetry(&self, _config: &TelemetryConfig) -> Result<(), crate::error::RdmaError> {
        Err(crate::error::RdmaError::OperationNotSupported {
            reason: "telemetry not supported by this provider".into(),
        })
    }

    /// Shut down telemetry and reset to the default output stream.
    fn shutdown_telemetry(&self) -> Result<(), crate::error::RdmaError> {
        Err(crate::error::RdmaError::OperationNotSupported {
            reason: "telemetry not supported by this provider".into(),
        })
    }

    /// Change the verbosity level without reconfiguring the output destination.
    fn set_telem_level(&self, _level: TelemLevel) -> Result<(), crate::error::RdmaError> {
        Err(crate::error::RdmaError::OperationNotSupported {
            reason: "telemetry not supported by this provider".into(),
        })
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
    ) -> Result<RdmaTransferHandle, crate::error::RdmaError>;

    /// Prepare a GET (download) operation, returning a typed handle.
    ///
    /// The token points directly to the registered buffer for zero-copy
    /// RDMA-write by the server.
    fn prepare_get(
        &self,
        offset: usize,
        size: usize,
    ) -> Result<RdmaTransferHandle, crate::error::RdmaError>;

    /// Complete a PUT operation.  Consumes the handle.
    ///
    /// `rdma_status` is the `x-amz-rdma-reply` HTTP status code
    /// (200/204/206 = success, 501 = fallback, 0 = absent).
    /// `transferred` is the value of `x-amz-rdma-bytes` (0 if absent).
    fn complete_put(
        &self,
        handle: RdmaTransferHandle,
        rdma_status: u16,
        transferred: usize,
    ) -> Result<(), crate::error::RdmaError>;

    /// Complete a GET operation.  Consumes the handle.
    fn complete_get(
        &self,
        handle: RdmaTransferHandle,
        rdma_status: u16,
        transferred: usize,
    ) -> Result<(), crate::error::RdmaError>;

    /// The S3 object key this channel is bound to.
    fn key(&self) -> &[u8];

    /// Base pointer of the registered buffer.
    fn ptr(&self) -> *mut u8;

    /// Size of the registered buffer in bytes.
    fn size(&self) -> usize;

    /// Maximum single-transfer size for this buffer.
    fn get_max_transfer_size(&self) -> usize;
}

/// Extension trait for [`RdmaClientChannel`] with ergonomic zero-copy combinators.
///
/// Provides `put` and `get` methods that accept `impl FnOnce` closures directly
/// — no `Box::new(...)` wrapping needed.
///
/// This trait is automatically implemented for all types that implement
/// [`RdmaClientChannel`].
///
/// # Example
///
/// ```ignore
/// use s3_rdma::provider::RdmaClientChannelExt;
///
/// let n = ch.put(0, size,
///     |buf: &mut [u8]| { buf[..4].copy_from_slice(b"ping"); 4 },
///     |token: &[u8]| { /* send token to server */ Ok((200, 4)) },
/// )?;
/// ```
pub trait RdmaClientChannelExt: RdmaClientChannel {
    /// Execute a full PUT: call `write` to fill the buffer, prepare a token,
    /// call `send` with the token, then complete.
    ///
    /// `write` receives `&mut buf[offset..offset+size]` and must return the
    /// number of bytes actually written.
    /// `send` receives the token bytes and must return `(rdma_status, transferred)`.
    fn put<W, S>(
        &self,
        offset: usize,
        size: usize,
        write: W,
        send: S,
    ) -> Result<usize, crate::error::RdmaError>
    where
        W: FnOnce(&mut [u8]) -> usize + Send,
        S: FnOnce(&[u8]) -> Result<(u16, usize), crate::error::RdmaError> + Send;

    /// Execute a full PUT from an existing byte slice: copy `data` into the
    /// RDMA buffer at `offset`, prepare a token, call `send` with the token,
    /// then complete.  Encapsulates the common `unsafe { copy_nonoverlapping }`
    /// pattern used by the relay and S3 backends.
    ///
    /// `send` receives the token bytes and must return `(rdma_status, transferred)`.
    fn write_from_slice<S>(
        &self,
        offset: usize,
        data: &[u8],
        send: S,
    ) -> Result<usize, crate::error::RdmaError>
    where
        S: FnOnce(&[u8]) -> Result<(u16, usize), crate::error::RdmaError> + Send;

    /// Execute a full GET: prepare a token, call `send` with the token, complete,
    /// then pass the downloaded data to `read`.
    ///
    /// `send` receives the token bytes and must return `(rdma_status, transferred)`.
    /// `read` receives the valid slice of the buffer containing the downloaded data.
    fn get<S, R>(
        &self,
        offset: usize,
        size: usize,
        send: S,
        read: R,
    ) -> Result<usize, crate::error::RdmaError>
    where
        S: FnOnce(&[u8]) -> Result<(u16, usize), crate::error::RdmaError> + Send,
        R: FnOnce(&[u8]) + Send;
}

impl<T: RdmaClientChannel + ?Sized> RdmaClientChannelExt for T {
    fn write_from_slice<S>(
        &self,
        offset: usize,
        data: &[u8],
        send: S,
    ) -> Result<usize, crate::error::RdmaError>
    where
        S: FnOnce(&[u8]) -> Result<(u16, usize), crate::error::RdmaError> + Send,
    {
        let cap = self.size().saturating_sub(offset);
        let n = data.len().min(cap);
        unsafe {
            std::ptr::copy_nonoverlapping(data.as_ptr(), self.ptr().add(offset), n);
        }
        let handle = self.prepare_put(offset, n)?;
        let (status, transferred) = send(handle.token())?;
        self.complete_put(handle, status, transferred)?;
        Ok(transferred)
    }

    fn put<W, S>(
        &self,
        offset: usize,
        size: usize,
        write: W,
        send: S,
    ) -> Result<usize, crate::error::RdmaError>
    where
        W: FnOnce(&mut [u8]) -> usize + Send,
        S: FnOnce(&[u8]) -> Result<(u16, usize), crate::error::RdmaError> + Send,
    {
        let buf = unsafe { std::slice::from_raw_parts_mut(self.ptr(), self.size()) };
        let cap = buf.len().saturating_sub(offset);
        let n = write(&mut buf[offset..offset.saturating_add(size.min(cap))]);
        let handle = self.prepare_put(offset, n)?;
        let (status, transferred) = send(handle.token())?;
        self.complete_put(handle, status, transferred)?;
        Ok(transferred)
    }

    fn get<S, R>(
        &self,
        offset: usize,
        size: usize,
        send: S,
        read: R,
    ) -> Result<usize, crate::error::RdmaError>
    where
        S: FnOnce(&[u8]) -> Result<(u16, usize), crate::error::RdmaError> + Send,
        R: FnOnce(&[u8]) + Send,
    {
        let handle = self.prepare_get(offset, size)?;
        let (status, transferred) = send(handle.token())?;
        self.complete_get(handle, status, transferred)?;
        let buf = unsafe { std::slice::from_raw_parts(self.ptr(), self.size()) };
        read(&buf[..transferred.min(buf.len())]);
        Ok(transferred)
    }
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
pub trait RdmaClientProvider: RdmaProviderBase {
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
        Err(crate::error::RdmaError::OperationNotSupported {
            reason: "bind: not supported by this provider".into(),
        })
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
        size: usize,
        write: Box<dyn FnOnce(&[u8]) -> Result<usize, crate::error::RdmaError> + Send + '_>,
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
        size: usize,
        read: Box<dyn FnOnce(&mut [u8]) -> Result<usize, crate::error::RdmaError> + Send + '_>,
    ) -> Result<usize, crate::error::RdmaError>;

    /// The S3 object key this channel is bound to.
    fn key(&self) -> &[u8];

    /// The RDMA descriptor token this channel is bound to.
    fn token(&self) -> &[u8];

    /// Capacity of the server-side buffer for this channel.
    fn buf_size(&self) -> usize;
}

/// Extension trait for [`RdmaServerChannel`] with ergonomic combinators.
///
/// Provides `put` and `get` methods that accept `impl FnOnce` closures directly
/// — no `Box::new(...)` wrapping needed.  Delegates to the required
/// [`RdmaServerChannel::handle_put`] / [`RdmaServerChannel::handle_get`] methods.
pub trait RdmaServerChannelExt: RdmaServerChannel {
    /// Handle an RDMA PUT: read data from the client buffer and pass it to `write`.
    fn put<W>(
        &self,
        offset: usize,
        size: usize,
        write: W,
    ) -> Result<usize, crate::error::RdmaError>
    where
        W: FnOnce(&[u8]) -> Result<usize, crate::error::RdmaError> + Send;

    /// Handle an RDMA GET: call `read` to fill the buffer, then write it to the client.
    fn get<R>(&self, offset: usize, size: usize, read: R) -> Result<usize, crate::error::RdmaError>
    where
        R: FnOnce(&mut [u8]) -> Result<usize, crate::error::RdmaError> + Send;
}

impl<T: RdmaServerChannel + ?Sized> RdmaServerChannelExt for T {
    fn put<W>(&self, offset: usize, size: usize, write: W) -> Result<usize, crate::error::RdmaError>
    where
        W: FnOnce(&[u8]) -> Result<usize, crate::error::RdmaError> + Send,
    {
        self.handle_put(offset, size, Box::new(write))
    }

    fn get<R>(&self, offset: usize, size: usize, read: R) -> Result<usize, crate::error::RdmaError>
    where
        R: FnOnce(&mut [u8]) -> Result<usize, crate::error::RdmaError> + Send,
    {
        self.handle_get(offset, size, Box::new(read))
    }
}

// ── RdmaServerProvider trait ──────────────────────────────────────────────────

/// Server-side RDMA operations: create channels for PUT/GET transfers.
///
/// Implemented by any provider that acts as an S3 server or gateway.  The only
/// server method is [`open_channel`](Self::open_channel) — it returns an
/// [`RdmaServerChannel`] bound to the given key+token pair.  All transfer operations
/// live on [`RdmaServerChannel`].
pub trait RdmaServerProvider: RdmaProviderBase {
    /// Size in bytes of the server's per-slot transfer buffer.
    ///
    /// Returns `0` by default (trait default for providers that don't configure
    /// a fixed buffer size).  Callers should treat `0` as "use
    /// [`DEFAULT_RDMA_BUF_SIZE`]" — it is not an error, just an unset sentinel.
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
        Err(crate::error::RdmaError::OperationNotSupported {
            reason: "open_channel: not supported by this provider".into(),
        })
    }
}

// ── RdmaProvider trait ────────────────────────────────────────────────────────

/// Combined RDMA provider: supports both client and server roles.
///
/// Extends both [`RdmaClientProvider`] and [`RdmaServerProvider`] (which both
/// extend [`RdmaProviderBase`]), adding role query methods.
///
/// ## Choosing the right trait
///
/// | Scenario | Trait to use |
/// |---|---|
/// | S3 client node (bind buffers, generate tokens) | `Arc<dyn RdmaClientProvider>` |
/// | S3 server/gateway node (handle PUT/GET) | `Arc<dyn RdmaServerProvider>` |
/// | Both roles in the same node / testing | `Arc<dyn RdmaProvider>` |
/// | Any role — just need name/protocol/telemetry | `Arc<dyn RdmaProviderBase>` |
///
/// ## Implementations
///
/// - [`MockRdmaProvider`](crate::mock::MockRdmaProvider) – `/dev/shm`-backed,
///   same-host only; supports both roles.
/// - [`RdmaPlugin`] – loads `libs3rdmaclient.so` / `libs3rdmaserver.so` at
///   runtime for real NVIDIA cuObject RDMA hardware.
pub trait RdmaProvider: RdmaClientProvider + RdmaServerProvider {
    /// The role(s) this provider was initialised with.
    fn role(&self) -> RdmaRole;

    /// Returns `true` if this provider supports client-side operations.
    fn supports_client(&self) -> bool {
        matches!(self.role(), RdmaRole::Client | RdmaRole::Both)
    }

    /// Returns `true` if this provider supports server-side operations.
    fn supports_server(&self) -> bool {
        matches!(self.role(), RdmaRole::Server | RdmaRole::Both)
    }
}

// ── TelemState ────────────────────────────────────────────────────────────────

/// Shared telemetry state for provider implementors.
///
/// Wraps an `Option<TelemetryConfig>` behind a mutex and provides the four
/// telemetry methods.  Implementors can delegate their
/// [`RdmaProviderBase`] telemetry methods to a single `TelemState` instance.
///
/// All telemetry methods are defined on [`RdmaProviderBase`], so calling them
/// on `Arc<dyn RdmaProvider>` is unambiguous — no UFCS disambiguation needed.
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

impl RdmaProviderConfig {
    /// Create a new [`RdmaProviderConfigBuilder`] with defaults:
    /// `provider = "auto"`, `debug = false`, server fields from
    /// [`RdmaServerConfig::default()`].
    pub fn builder() -> RdmaProviderConfigBuilder {
        RdmaProviderConfigBuilder::new()
    }
}

// ── RdmaProviderConfigBuilder ─────────────────────────────────────────────────

/// Fluent builder for constructing RDMA providers.
///
/// Created via [`RdmaProviderConfig::builder()`].  Configure with the fluent
/// setter methods, then call one of the terminal methods to produce a provider:
/// - [`build_client`](Self::build_client) — client-only (`Arc<dyn RdmaClientProvider>`)
/// - [`build_server`](Self::build_server) — server-only (`Arc<dyn RdmaServerProvider>`)
/// - [`build`](Self::build) — both roles (`Arc<dyn RdmaProvider>`)
///
/// # Example
///
/// ```ignore
/// let provider = RdmaProviderConfig::builder()
///     .provider("cuobj")
///     .listen_addr("192.168.1.1")
///     .listen_port(4791)
///     .build_server()?;
/// ```
pub struct RdmaProviderConfigBuilder {
    provider: String,
    debug: bool,
    role: RdmaRole,
    server: RdmaServerConfig,
}

impl RdmaProviderConfigBuilder {
    fn new() -> Self {
        Self {
            provider: "auto".to_owned(),
            debug: false,
            role: RdmaRole::Both,
            server: RdmaServerConfig::default(),
        }
    }

    /// Set the provider name: `"mock"`, `"cuobj"`, or `"auto"` (default).
    pub fn provider(mut self, name: impl Into<String>) -> Self {
        self.provider = name.into();
        self
    }

    /// Enable or disable debug logging.
    pub fn debug(mut self, enabled: bool) -> Self {
        self.debug = enabled;
        self
    }

    /// Override the role for the terminal [`build`](Self::build) call.
    ///
    /// Defaults to [`RdmaRole::Both`].  Use this when the role is determined
    /// at runtime; for compile-time-known roles prefer the typed terminal
    /// methods [`build_client`](Self::build_client) /
    /// [`build_server`](Self::build_server) / [`build`](Self::build).
    pub fn role(mut self, role: RdmaRole) -> Self {
        self.role = role;
        self
    }

    /// Shared-memory directory for the mock provider (default: `"/dev/shm"`).
    pub fn shm_path(mut self, path: impl Into<String>) -> Self {
        self.server.shm_path = path.into();
        self
    }

    /// RDMA listen address for the cuObj provider (e.g. `"192.168.1.1"`).
    pub fn listen_addr(mut self, addr: impl Into<String>) -> Self {
        self.server.listen_addr = addr.into();
        self
    }

    /// RDMA listen port for the cuObj provider.
    pub fn listen_port(mut self, port: u16) -> Self {
        self.server.listen_port = port;
        self
    }

    /// Transfer buffer size in bytes for the cuObj provider
    /// (default: [`DEFAULT_RDMA_BUF_SIZE`]).
    pub fn buf_size(mut self, size: usize) -> Self {
        self.server.buf_size = size;
        self
    }

    fn build_for_role(
        provider: &str,
        debug: bool,
        role: RdmaRole,
        server: &RdmaServerConfig,
    ) -> Result<std::sync::Arc<dyn RdmaProvider>, crate::error::RdmaError> {
        let ep: std::sync::Arc<dyn RdmaProvider> = match provider {
            "mock" => {
                if debug {
                    tracing::info!("[rdma] Using MockRdmaProvider (role={role:?})");
                }
                std::sync::Arc::new(crate::MockRdmaProvider::new(debug, &server.shm_path, 0))
            }
            "cuobj" => {
                let result: Result<std::sync::Arc<dyn RdmaProvider>, crate::error::RdmaError>;
                #[cfg(any(feature = "rdma-client", feature = "rdma-server"))]
                {
                    let ep = match role {
                        RdmaRole::Client => crate::RdmaPlugin::new_client(),
                        RdmaRole::Server => crate::RdmaPlugin::new_server(server),
                        RdmaRole::Both => crate::RdmaPlugin::new_both(server),
                    };
                    result = ep
                        .map(|e| {
                            if debug {
                                tracing::info!("[rdma] Using RdmaPlugin (role={role:?})");
                            }
                            std::sync::Arc::new(e) as std::sync::Arc<dyn RdmaProvider>
                        })
                        .map_err(|e| crate::error::RdmaError::ProviderUnavailable {
                            provider: provider.to_owned(),
                            reason: e.to_string(),
                        });
                }
                #[cfg(not(any(feature = "rdma-client", feature = "rdma-server")))]
                {
                    let _ = (debug, role, server);
                    result = Err(crate::error::RdmaError::ProviderUnavailable {
                        provider: provider.to_owned(),
                        reason: "compiled without `rdma-client` or `rdma-server` features".into(),
                    });
                }
                result?
            }
            "auto" => {
                return Self::build_for_role("cuobj", debug, role, server)
                    .or_else(|_| Self::build_for_role("mock", debug, role, server));
            }
            other => {
                return Err(crate::error::RdmaError::ProviderUnavailable {
                    provider: other.to_owned(),
                    reason: "unknown provider (valid: mock, cuobj, auto)".to_owned(),
                })
            }
        };
        Ok(ep)
    }

    /// Build a **client-only** provider (`Arc<dyn RdmaClientProvider>`).
    ///
    /// Equivalent to `.role(RdmaRole::Client).build()` but returns the
    /// narrower `Arc<dyn RdmaClientProvider>` type.
    pub fn build_client(
        self,
    ) -> Result<std::sync::Arc<dyn RdmaClientProvider>, crate::error::RdmaError> {
        Self::build_for_role(&self.provider, self.debug, RdmaRole::Client, &self.server)
            .map(|p| p as std::sync::Arc<dyn RdmaClientProvider>)
    }

    /// Build a **server-only** provider (`Arc<dyn RdmaServerProvider>`).
    ///
    /// Equivalent to `.role(RdmaRole::Server).build()` but returns the
    /// narrower `Arc<dyn RdmaServerProvider>` type.
    pub fn build_server(
        self,
    ) -> Result<std::sync::Arc<dyn RdmaServerProvider>, crate::error::RdmaError> {
        Self::build_for_role(&self.provider, self.debug, RdmaRole::Server, &self.server)
            .map(|p| p as std::sync::Arc<dyn RdmaServerProvider>)
    }

    /// Build a provider with the role set via [`.role()`](Self::role) (default: both).
    ///
    /// Returns `Arc<dyn RdmaProvider>`.  For compile-time-known single roles
    /// prefer [`build_client`](Self::build_client) or
    /// [`build_server`](Self::build_server) for narrower types.
    pub fn build(self) -> Result<std::sync::Arc<dyn RdmaProvider>, crate::error::RdmaError> {
        Self::build_for_role(&self.provider, self.debug, self.role, &self.server)
    }
}

// ── PendingTransfer / InflightTransfers ─────────────────────────────────────────

/// An in-flight client-side RDMA transfer, tracked between
/// `prepare_put`/`prepare_get` and `complete_put`/`complete_get`.
///
/// Owns the client channel (keeping the buffer alive) and the transfer handle.
/// Used by [`InflightTransfers`] to manage the per-provider token→transfer map.
pub struct PendingTransfer {
    pub channel: Box<dyn RdmaClientChannel>,
    pub handle: RdmaTransferHandle,
}

impl PendingTransfer {
    /// Create a new pending transfer.
    pub fn new(channel: Box<dyn RdmaClientChannel>, handle: RdmaTransferHandle) -> Self {
        Self { channel, handle }
    }

    /// Complete a PUT operation, consuming this transfer.
    pub fn complete_put(
        self,
        rdma_status: u16,
        transferred: usize,
    ) -> Result<(), crate::error::RdmaError> {
        self.channel
            .complete_put(self.handle, rdma_status, transferred)
    }

    /// Complete a GET operation, consuming this transfer.
    pub fn complete_get(
        self,
        rdma_status: u16,
        transferred: usize,
    ) -> Result<(), crate::error::RdmaError> {
        self.channel
            .complete_get(self.handle, rdma_status, transferred)
    }

    /// Borrow the client channel (e.g. to access `ptr()` for data copy-back).
    pub fn channel(&self) -> &dyn RdmaClientChannel {
        &*self.channel
    }
}

/// Thread-safe registry of in-flight RDMA transfers, keyed by token bytes.
///
/// Binding layers construct one per provider.  Transfers are inserted when a
/// `prepare_put_token`/`prepare_get_token` call generates a token and removed
/// when `process_rdma_reply` completes the transfer.
pub struct InflightTransfers {
    inner: std::sync::Mutex<std::collections::HashMap<Vec<u8>, PendingTransfer>>,
}

impl InflightTransfers {
    /// Create an empty registry.
    pub fn new() -> Self {
        Self {
            inner: std::sync::Mutex::new(std::collections::HashMap::new()),
        }
    }

    /// Insert a pending transfer keyed by `token`.
    pub fn insert(&self, token: Vec<u8>, transfer: PendingTransfer) {
        self.inner.lock().unwrap().insert(token, transfer);
    }

    /// Remove and return the pending transfer for `token`, or `None`.
    pub fn remove(&self, token: &[u8]) -> Option<PendingTransfer> {
        self.inner.lock().unwrap().remove(token)
    }

    /// Number of pending transfers.
    pub fn len(&self) -> usize {
        self.inner.lock().unwrap().len()
    }

    /// Returns `true` if there are no pending transfers.
    pub fn is_empty(&self) -> bool {
        self.inner.lock().unwrap().is_empty()
    }
}

impl Default for InflightTransfers {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for InflightTransfers {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InflightTransfers")
            .field("len", &self.len())
            .finish()
    }
}
