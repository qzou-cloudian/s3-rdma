//! RDMA provider types, traits, and configuration.
//!
//! This module contains the central [`RdmaProvider`] trait and all associated
//! types used throughout the s3-rdma crate and its consumers.
//!
/// Convert the `x-amz-rdma-reply` numeric status to a callback `error_code`.
///
/// - `0`         → `-1` (absent header — treated as error, avoids `0` == success confusion)
/// - `200..300`  → `0`  (RDMA success)
/// - other       → status cast to `i32` (e.g. `501` for RDMA fallback)
pub fn rdma_status_to_error_code(rdma_status: u16) -> i32 {
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
/// Header that signals HTTP/2 streaming token mode.
///
/// When present with value `"true"`, the PUT request body contains
/// newline-delimited RDMA token strings (one per line) instead of object data.
/// Each token line uses the extended format `<base_token>:<size>:<offset>` so
/// the server knows the exact transfer size and object offset for every chunk.
/// The actual object bytes are fetched from the client's RDMA buffers via
/// concurrent RDMA_READ operations.  Requires HTTP/2 (so the body can be
/// streamed while RDMA_READs proceed in parallel).
pub const RDMA_STREAM_HEADER: &str = "x-amz-rdma-stream";

/// Separator used when multiple RDMA tokens (or sizes) are packed into a single
/// HTTP header value.
///
/// cuObj tokens are hex-encoded (`0x…`) and never contain `|`, making it a safe
/// delimiter.  Single-token transfers are unaffected (no `|` present).
///
/// # Multi-token protocol
///
/// **PUT** — the client registers N non-contiguous (or sub-range) memory regions
/// and sends their tokens as a pipe-separated list:
/// ```text
/// x-amz-rdma-token:  tok0|tok1|tok2
/// ```
/// Each token encodes its own byte count.  The server reads each token's buffer
/// sequentially, feeding bytes directly into the storage backend as one object.
///
/// **GET** — the client pre-allocates N receive buffers and sends their tokens
/// the same way.  The server writes backend data into the buffers in order,
/// advancing to the next buffer when the current one is full.
pub const RDMA_TOKEN_SEPARATOR: char = '|';

// ── RdmaServerConfig ─────────────────────────────────────────────────────────

/// Unified configuration for any RDMA **server** provider.
///
/// Always defined (no feature gate) so callers can build the config without
/// conditional compilation.  Fields that are irrelevant for a given provider
/// are silently ignored:
///
/// | Field         | mock | cuobj |
/// |---------------|------|-------|
/// | `shm_path`    | ✓    | –     |
/// | `listen_addr` | –    | ✓     |
/// | `listen_port` | –    | ✓     |
/// | `pool_count`  | –    | ✓     |
/// | `buf_size`    | –    | ✓     |
#[derive(Debug, Clone)]
pub struct RdmaServerConfig {
    /// Directory used for POSIX shared-memory files (mock provider).
    /// Defaults to `"/dev/shm"`.
    pub shm_path: String,
    /// RDMA listen address for the cuObj provider (e.g. `"192.168.1.1"`).
    pub listen_addr: String,
    /// RDMA listen port for the cuObj provider.
    pub listen_port: u16,
    /// Number of pre-allocated buffer slots (cuObj provider).
    pub pool_count: usize,
    /// Size in bytes of each buffer slot (cuObj provider).
    pub buf_size: usize,
}

impl Default for RdmaServerConfig {
    fn default() -> Self {
        Self {
            shm_path:    "/dev/shm".to_owned(),
            listen_addr: String::new(),
            listen_port: 0,
            pool_count:  4,
            buf_size:    128 * 1024 * 1024,
        }
    }
}


// ── RdmaRole ─────────────────────────────────────────────────────────────────

/// Requested role(s) for a new RDMA provider.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RdmaRole {
    /// Client only — can initiate RDMA transfers (register memory, generate tokens).
    Client,
    /// Server only — can accept RDMA transfers (handle_put / handle_get).
    Server,
    /// Both client and server roles — typical for a gateway node.
    Both,
}

// ── RdmaProvider trait ────────────────────────────────────────────────────────

/// Unified RDMA abstraction covering both client-side token generation and
/// server-side data movement.
///
/// ## Role model
///
/// | Role   | Methods used                                                 |
/// |--------|--------------------------------------------------------------|
/// | Client | `register_memory`, `prepare_{put,get}_token`, `process_rdma_reply`, `deregister_memory` |
/// | Server | `handle_put`, `handle_get`, `buf_size`                       |
/// | Both   | All of the above (typical for a gateway node)                |
///
/// A provider reports which roles it supports via [`supports_client`] and
/// [`supports_server`].  Calling a server method on a client-only provider
/// (or vice-versa) returns `Err(RdmaError::OperationNotSupported)`.
///
/// ## Implementations
///
/// - [`MockRdmaProvider`](crate::mock::MockRdmaProvider) – `/dev/shm`-backed,
///   same-host only; supports both client and server roles.
/// - [`CuObjProvider`](crate::cuobj_client::CuObjProvider) – real NVIDIA cuObject;
///   each role requires the corresponding plugin to be loadable at runtime.
///
/// [`supports_client`]: RdmaProvider::supports_client
/// [`supports_server`]: RdmaProvider::supports_server
pub trait RdmaProvider: Send + Sync {
    /// Human-readable provider name (e.g. `"MockRdmaProvider"`).
    fn name(&self) -> &str;

    /// Transport protocol string: `"mock"` or `"cuobj"`.
    fn protocol(&self) -> &str;

    /// Returns `true` if this provider supports client-side operations
    /// (`register_memory`, `prepare_*_token`, `process_rdma_reply`).
    fn supports_client(&self) -> bool;

    /// Returns `true` if this provider supports server-side operations
    /// (`handle_put`, `handle_get`).
    fn supports_server(&self) -> bool;

    // ── Client-side methods ───────────────────────────────────────────────────

    /// Returns `true` if the memory at `ptr..ptr+size` is eligible for RDMA.
    fn is_memory_suitable(&self, _ptr: *const u8, _size: usize) -> bool {
        false
    }

    /// Register a memory region so the RDMA subsystem can access it.
    ///
    /// # Safety
    /// `ptr` must remain valid until [`deregister_memory`](Self::deregister_memory).
    fn register_memory(&self, _ptr: *mut u8, _size: usize) -> Result<(), crate::error::RdmaError> {
        Err(crate::error::RdmaError::OperationNotSupported(
            "register_memory: provider is server-only".into(),
        ))
    }

    /// Deregister a previously registered memory region.
    fn deregister_memory(&self, _ptr: *mut u8) -> Result<(), crate::error::RdmaError> {
        Err(crate::error::RdmaError::OperationNotSupported(
            "deregister_memory: provider is server-only".into(),
        ))
    }

    /// Maximum single-transfer size for the given buffer.
    /// Default: `usize::MAX` (no provider-imposed limit).
    fn get_max_transfer_size(&self, _ptr: *const u8) -> usize {
        usize::MAX
    }

    /// Generate an RDMA descriptor token for a PUT (upload) sub-request.
    ///
    /// The returned bytes are used as the value of the `x-amz-rdma-token`
    /// request header.
    fn prepare_put_token(
        &self,
        _s3_key: &[u8],
        _buffer: *const u8,
        _size: usize,
        _offset: usize,
    ) -> Result<Vec<u8>, crate::error::RdmaError> {
        Err(crate::error::RdmaError::OperationNotSupported(
            "prepare_put_token: provider is server-only".into(),
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
            "prepare_get_token: provider is server-only".into(),
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

    /// Header name for the outbound RDMA token (default: `x-amz-rdma-token`).
    fn rdma_token_header_name(&self) -> &[u8] {
        RDMA_TOKEN_HEADER.as_bytes()
    }

    /// Header name for the server RDMA reply status (default: `x-amz-rdma-reply`).
    fn rdma_reply_header_name(&self) -> &[u8] {
        RDMA_REPLY_HEADER.as_bytes()
    }

    /// Header name for the RDMA byte-count (default: `x-amz-rdma-bytes`).
    fn rdma_bytes_header_name(&self) -> &[u8] {
        RDMA_BYTES_HEADER.as_bytes()
    }

    // ── Server-side methods ───────────────────────────────────────────────────

    /// Size in bytes of the server's per-slot transfer buffer.
    fn buf_size(&self) -> usize {
        0
    }

    /// Handle an RDMA PUT from a client: read `size` bytes from the client's
    /// registered buffer identified by `token` (semantics: RDMA_READ).
    ///
    /// Returns the number of bytes written into `buf`.
    fn handle_put(
        &self,
        _s3_key: &[u8],
        _token: &[u8],
        _buf: &mut [u8],
        _offset: usize,
        _size: usize,
    ) -> Result<usize, crate::error::RdmaError> {
        Err(crate::error::RdmaError::OperationNotSupported(
            "handle_put: provider is client-only".into(),
        ))
    }

    /// Handle an RDMA GET from a client: write `data` into the client's
    /// registered buffer identified by `token` (semantics: RDMA_WRITE).
    ///
    /// Returns the number of bytes actually written.
    fn handle_get(
        &self,
        _s3_key: &[u8],
        _data: &[u8],
        _token: &[u8],
        _offset: usize,
    ) -> Result<usize, crate::error::RdmaError> {
        Err(crate::error::RdmaError::OperationNotSupported(
            "handle_get: provider is client-only".into(),
        ))
    }
}

// ── RdmaProviderConfig ────────────────────────────────────────────────────────

/// Unified configuration for constructing an [`RdmaProvider`].
///
/// The `server_cfg` fields are only used when `role` includes the server role.
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
