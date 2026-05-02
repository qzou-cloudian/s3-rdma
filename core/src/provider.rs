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

