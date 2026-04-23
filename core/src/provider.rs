use crate::error::RdmaError;

// ── Completion callback ──────────────────────────────────────────────────────

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

/// Separator used when multiple RDMA tokens (or sizes) are packed into a single
/// HTTP header value.
///
/// cuObj tokens are hex-encoded (`0x…`) and never contain `|`, making it a safe
/// delimiter.  Single-token transfers are unaffected (no `|` present).
///
/// # Multi-token protocol
///
/// **PUT** — the client registers N non-contiguous (or sub-range) memory regions
/// and sends their tokens and byte counts as pipe-separated lists:
/// ```text
/// x-amz-rdma-token:  tok0|tok1|tok2
/// x-amz-rdma-sizes:  1073741824|1073741824|536870912
/// ```
/// The server reads each token's buffer sequentially (with its own offset loop),
/// feeding bytes directly into the storage backend as one contiguous object.
///
/// **GET** — the client pre-allocates N receive buffers, sends their tokens and
/// capacities the same way.  The server writes backend data into the buffers in
/// order, advancing to the next buffer when the current one is full.
pub const RDMA_TOKEN_SEPARATOR: char = '|';

/// Header name for per-token buffer sizes in a multi-token RDMA transfer
/// (`x-amz-rdma-sizes`).
///
/// Value: `|`-separated decimal byte counts, one per entry in the matching
/// `x-amz-rdma-token` header.  Absent for single-token transfers where the
/// size can be inferred from the mock-token format or `x-amz-rdma-bytes`.
pub const RDMA_SIZES_HEADER: &str = "x-amz-rdma-sizes";

// ── RdmaProvider trait ───────────────────────────────────────────────────────

/// Rust interface for an RDMA provider, modelled on the cuObject plugin vtable.
///
/// Implementations:
/// - [`MockRdmaProvider`](crate::mock::MockRdmaProvider) – no-hardware mock.
/// - [`CuObjProvider`](crate::cuobj::CuObjProvider) – real NVIDIA cuObject
///   bindings (requires the `cuobj` Cargo feature).
pub trait RdmaProvider: Send + Sync {
    /// Human-readable provider name.
    fn name(&self) -> &str;

    /// Numeric provider version.
    fn version(&self) -> u32;

    /// Returns `true` if the memory region at `ptr` with `size` bytes is
    /// eligible for RDMA (e.g. CUDA device/pinned host memory).
    fn is_memory_suitable(&self, ptr: *const u8, size: usize) -> bool;

    /// Register a memory region so the RDMA subsystem can access it.
    ///
    /// # Safety
    /// `ptr` must remain valid until [`deregister_memory`](Self::deregister_memory).
    fn register_memory(&self, ptr: *mut u8, size: usize) -> Result<(), RdmaError>;

    /// Deregister a previously registered memory region.
    fn deregister_memory(&self, ptr: *mut u8) -> Result<(), RdmaError>;

    /// Maximum single-transfer size for the given buffer.
    /// Default: `usize::MAX` (no limit).  Override when the provider imposes a cap.
    fn get_max_transfer_size(&self, _ptr: *const u8) -> usize {
        usize::MAX
    }

    /// Generate an RDMA descriptor token for a PUT (upload) sub-request.
    ///
    /// The returned bytes are used as the value of the `x-amz-rdma-token`
    /// request header.
    fn prepare_put_token(
        &self,
        s3_key: &[u8],
        buffer: *const u8,
        size: usize,
        offset: usize,
    ) -> Result<Vec<u8>, RdmaError>;

    /// Generate an RDMA descriptor token for a GET (download) sub-request.
    fn prepare_get_token(
        &self,
        s3_key: &[u8],
        buffer: *mut u8,
        size: usize,
        offset: usize,
    ) -> Result<Vec<u8>, RdmaError>;

    /// Process the `x-amz-rdma-reply` status code returned by the S3 server.
    ///
    /// `rdma_status` is the numeric value of the `x-amz-rdma-reply` response
    /// header (200, 204, 206 = success; 501 = RDMA fallback/failure).
    /// `request_token` is the raw value of the original `x-amz-rdma-token`
    /// request header (used by the mock to locate the shm region).
    /// `transferred` is the number of bytes transferred (from `x-amz-rdma-bytes`).
    ///
    /// The default implementation fires `callback` immediately: error_code=0
    /// for 2xx (real RDMA already placed data in the buffer), non-zero for
    /// other status codes.  Mock providers override this to copy shm data.
    fn process_rdma_reply(
        &self,
        rdma_status: u16,
        _request_token: &[u8],
        _transferred: usize,
        callback: RdmaCompletionCallback,
    ) -> Result<(), RdmaError> {
        // rdma_status == 0 means the reply header was absent — treat as an error.
        let error_code = if rdma_status != 0 && (200..300).contains(&rdma_status) { 0 } else { rdma_status as i32 };
        callback(RdmaCompletionResult { error_code, reply_token: vec![] });
        Ok(())
    }

    /// Process the `x-amz-rdma-reply` token returned by the S3 server.
    ///
    /// **Deprecated**: use [`process_rdma_reply`](Self::process_rdma_reply)
    /// instead. The reply value is now a numeric status code (200/204/206/501),
    /// not an opaque echo token.
    #[deprecated(note = "use process_rdma_reply with numeric rdma_status instead")]
    fn process_reply_token(
        &self,
        reply_token: &[u8],
        callback: RdmaCompletionCallback,
    ) -> Result<(), RdmaError> {
        // Legacy default: treat as success.
        let _ = reply_token;
        callback(RdmaCompletionResult { error_code: 0, reply_token: vec![] });
        Ok(())
    }

    // ── HTTP header names ────────────────────────────────────────────────────

    /// Header name for the outbound RDMA token (default: `x-amz-rdma-token`).
    fn rdma_token_header_name(&self) -> &[u8] {
        RDMA_TOKEN_HEADER.as_bytes()
    }

    /// Header name for the server RDMA reply token (default: `x-amz-rdma-reply`).
    fn rdma_reply_header_name(&self) -> &[u8] {
        RDMA_REPLY_HEADER.as_bytes()
    }

    /// Header name for the RDMA byte-count (default: `x-amz-rdma-bytes`).
    fn rdma_bytes_header_name(&self) -> &[u8] {
        RDMA_BYTES_HEADER.as_bytes()
    }

    /// Header name for per-token buffer sizes in a multi-token transfer
    /// (default: `x-amz-rdma-sizes`).
    fn rdma_sizes_header_name(&self) -> &[u8] {
        RDMA_SIZES_HEADER.as_bytes()
    }
}

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

// ── RdmaServerProvider trait ─────────────────────────────────────────────────

/// Server-side RDMA provider, symmetric counterpart to [`RdmaProvider`].
///
/// An S3 gateway implements this trait to move data between its own buffers
/// and a remote client's registered memory region — either by reading actual
/// RDMA fabric operations (cuObj) or by reading/writing `/dev/shm` files
/// (mock).
///
/// Implementations:
/// - [`MockRdmaServerProvider`](crate::mock::MockRdmaServerProvider) —
///   `/dev/shm`-backed, no hardware required.
/// - For real cuObj server support, wrap [`cuobj::CuObjServer`] via the
///   `hsg-io` buffer pool; that path requires the `rdma` feature in `hsg-relay`.
pub trait RdmaServerProvider: Send + Sync {
    /// Human-readable provider name.
    fn name(&self) -> &str;

    /// Size in bytes of the server's per-slot transfer buffer.
    ///
    /// This is the maximum number of bytes that can be transferred in a
    /// single `handle_put` call.  When the object is larger than this value
    /// the gateway drives a loop: it calls `handle_put` repeatedly with
    /// `offset` advancing by `buf_size()` each time, streaming each chunk
    /// directly to the storage backend.
    ///
    /// Should match the chunked-backend chunk size for best efficiency.
    fn buf_size(&self) -> usize;

    /// Handle an RDMA PUT from a client: read `size` bytes from the client's
    /// registered buffer identified by `token`, starting at byte `offset`
    /// within that buffer (semantics: RDMA_READ).
    ///
    /// `s3_key` is the object path; `token` is the raw value of the
    /// `x-amz-rdma-token` request header; `offset` is the start position
    /// within the client's buffer; `size` is the number of bytes to read
    /// (0 = read the full buffer as described by the token, only valid when
    /// `offset == 0`).
    ///
    /// Returns the received `data` for this chunk.  The HTTP handler drives
    /// the loop and feeds each chunk to the storage backend directly.
    fn handle_put(
        &self,
        s3_key: &[u8],
        token: &[u8],
        offset: usize,
        size: usize,
    ) -> Result<Vec<u8>, crate::RdmaError>;

    /// Handle an RDMA GET from a client: write `data` into the client's
    /// registered buffer identified by `token`, starting at byte `offset`
    /// within that buffer (semantics: RDMA_WRITE).
    ///
    /// `offset=0` writes from the start of the client's buffer (original
    /// single-shot behaviour).  Streaming callers advance `offset` by the
    /// `written` count returned from the previous call so each chunk lands at
    /// the correct position.
    ///
    /// Writes at most `client_capacity - offset` bytes (clips to the
    /// remaining space in the client buffer so it is never overfilled).
    ///
    /// Returns `written` — the bytes actually transferred for this chunk
    /// (≤ `data.len()`).  The HTTP handler drives the loop and accumulates
    /// `written` to produce the final `x-amz-rdma-bytes` header.
    fn handle_get(
        &self,
        s3_key: &[u8],
        data: &[u8],
        token: &[u8],
        offset: usize,
    ) -> Result<usize, crate::RdmaError>;
}
