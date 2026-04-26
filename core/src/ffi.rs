//! C-ABI types for the hsc RDMA provider vtable.
//!
//! These types define the wire format shared between `hsc-rdma` (the loader)
//! and any RDMA provider plugin compiled as a `cdylib`.  The layout must
//! match exactly – types cross a `dlopen`/`dlsym` boundary.

use std::ffi::c_void;
use std::os::raw::c_char;

// ─────────────────────────────────────────────────────────────────────────────
// ByteCursor
// ─────────────────────────────────────────────────────────────────────────────

/// Non-owning view into a byte buffer (mirrors `aws_byte_cursor`).
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct ByteCursor {
    pub len: usize,
    pub ptr: *const u8,
}

impl ByteCursor {
    pub const fn null() -> Self {
        Self { ptr: std::ptr::null(), len: 0 }
    }

    pub fn from_slice(s: &[u8]) -> Self {
        Self { ptr: s.as_ptr(), len: s.len() }
    }

    /// # Safety
    /// `ptr` must be valid for `len` bytes.
    pub unsafe fn as_slice(&self) -> &[u8] {
        if self.ptr.is_null() || self.len == 0 {
            &[]
        } else {
            unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
        }
    }
}

impl Default for ByteCursor {
    fn default() -> Self { Self::null() }
}

// ─────────────────────────────────────────────────────────────────────────────
// Completion callback
// ─────────────────────────────────────────────────────────────────────────────

/// C completion callback invoked when an async RDMA operation finishes.
///
/// - `user_data`   – opaque pointer supplied by the caller.
/// - `error_code`  – 0 on success; non-zero on failure.
/// - `reply_token` – server reply token (may be null / empty).
pub type RdmaCompletionFn = unsafe extern "C" fn(
    user_data:   *mut c_void,
    error_code:  i32,
    reply_token: *const ByteCursor,
);

// ─────────────────────────────────────────────────────────────────────────────
// RdmaProviderVtable
// ─────────────────────────────────────────────────────────────────────────────

/// C vtable exported by every RDMA provider plugin.
///
/// A plugin `cdylib` must export:
/// ```c
/// const RdmaProviderVtable *s3_rdma_provider_get_vtable(void);
/// ```
///
/// Function pointers may be `None`/null if the operation is not supported.
#[repr(C)]
pub struct RdmaProviderVtable {
    /// Human-readable provider name (null-terminated C string).
    pub provider_name:    *const c_char,
    /// Version integer for compatibility checks.
    pub provider_version: u32,

    /// Initialise the provider.
    pub init:    Option<unsafe extern "C" fn(*mut c_void, *mut *mut c_void) -> i32>,
    /// Destroy the provider.
    pub cleanup: Option<unsafe extern "C" fn(*mut c_void)>,

    /// Returns `true` if `ptr[0..size]` is eligible for RDMA.
    pub is_memory_suitable:    Option<unsafe extern "C" fn(*mut c_void, *const c_void, usize) -> bool>,
    /// Register a memory region for RDMA access.
    pub register_memory:       Option<unsafe extern "C" fn(*mut c_void, *mut c_void, usize) -> i32>,
    /// Deregister a previously registered memory region.
    pub deregister_memory:     Option<unsafe extern "C" fn(*mut c_void, *mut c_void) -> i32>,
    /// Maximum single-transfer size (`usize::MAX` = no limit).
    pub get_max_transfer_size: Option<unsafe extern "C" fn(*mut c_void, *const c_void) -> usize>,

    /// Generate an RDMA token for a PUT (upload) operation.
    pub prepare_put_token: Option<unsafe extern "C" fn(
        *mut c_void, *const ByteCursor,
        *const c_void, usize, usize,
        *mut ByteCursor,
    ) -> i32>,

    /// Generate an RDMA token for a GET (download) operation.
    pub prepare_get_token: Option<unsafe extern "C" fn(
        *mut c_void, *const ByteCursor,
        *mut c_void, usize, usize,
        *mut ByteCursor,
    ) -> i32>,

    /// Process the RDMA reply token returned by the S3 server.
    pub process_reply_token: Option<unsafe extern "C" fn(
        *mut c_void, *const ByteCursor,
        *mut c_void, Option<RdmaCompletionFn>,
    ) -> i32>,

    /// Name of the outbound RDMA token header (e.g. `x-amz-rdma-token`).
    pub rdma_token_header_name: Option<unsafe extern "C" fn(*mut c_void) -> ByteCursor>,
    /// Name of the server reply header (e.g. `x-amz-rdma-reply`).
    pub rdma_reply_header_name: Option<unsafe extern "C" fn(*mut c_void) -> ByteCursor>,
    /// Name of the byte-count header (e.g. `x-amz-rdma-bytes`).
    pub rdma_bytes_header_name: Option<unsafe extern "C" fn(*mut c_void) -> ByteCursor>,
}

// SAFETY: the vtable contains only raw function pointers which are always safe
// to send and share across threads once the library is loaded.
unsafe impl Send for RdmaProviderVtable {}
unsafe impl Sync for RdmaProviderVtable {}

/// Function signature of the plugin entry point.
///
/// Every RDMA provider `.so` must export:
/// ```c
/// const RdmaProviderVtable *s3_rdma_provider_get_vtable(void);
/// ```
pub type GetVtableFn = unsafe extern "C" fn() -> *const RdmaProviderVtable;

// ─────────────────────────────────────────────────────────────────────────────
// RdmaServerVtable
// ─────────────────────────────────────────────────────────────────────────────

/// Configuration passed to `RdmaServerVtable::init`.
///
/// All fields must be valid for the duration of the `init` call; the plugin
/// is responsible for copying any strings it needs to retain.
#[repr(C)]
pub struct RdmaServerInitParams {
    /// Null-terminated listen address string (e.g. `"192.168.1.1"`).
    pub listen_addr: *const c_char,
    /// Listen port for the RDMA server.
    pub listen_port: u16,
    /// Number of pre-allocated buffer slots in the pool.
    pub pool_count:  usize,
    /// Size in bytes of each buffer slot.
    pub buf_size:    usize,
}

// SAFETY: only used during init() on the calling thread.
unsafe impl Send for RdmaServerInitParams {}
unsafe impl Sync for RdmaServerInitParams {}

/// C vtable exported by every RDMA **server** provider plugin.
///
/// A plugin `cdylib` must export:
/// ```c
/// const RdmaServerVtable *s3_rdma_server_provider_get_vtable(void);
/// ```
///
/// Function pointers may be `None`/null if the operation is not supported.
#[repr(C)]
pub struct RdmaServerVtable {
    /// Human-readable provider name (null-terminated C string).
    pub provider_name:    *const c_char,
    /// Version integer for compatibility checks.
    pub provider_version: u32,

    /// Initialise the server provider with the given config.
    /// On success sets `*ctx` and returns 0; non-zero on failure.
    pub init:    Option<unsafe extern "C" fn(*const RdmaServerInitParams, *mut *mut c_void) -> i32>,
    /// Destroy the server provider context.
    pub cleanup: Option<unsafe extern "C" fn(*mut c_void)>,

    /// Handle an RDMA PUT: the server RDMA_READs `size` bytes from the
    /// client buffer identified by `token`, starting at `offset` bytes into
    /// the client's buffer.
    ///
    /// `offset=0, size=0` means "read the full buffer as described by the token".
    ///
    /// On success copies the received bytes into the caller-provided buffer `buf`
    /// and writes the byte count into `*out_transferred`.
    ///
    /// The plugin guarantees `*out_transferred <= plugin_pool_slot_size`.  The
    /// caller must ensure `buf` is at least `plugin_pool_slot_size` bytes (i.e.
    /// both pools are configured to the same `rdma_buffer_size`).
    /// Returns 0 on success, non-zero on failure.
    pub handle_put: Option<unsafe extern "C" fn(
        ctx:             *mut c_void,
        s3_key:          *const ByteCursor,
        token:           *const ByteCursor,
        buf:             *mut u8,
        offset:          usize,
        size:            usize,
        out_transferred: *mut usize,
    ) -> i32>,

    /// Handle an RDMA GET: the server RDMA_WRITEs `data` into the client
    /// buffer identified by `token`, starting at byte `offset` within that
    /// buffer.
    ///
    /// `offset=0` writes from the start of the client buffer (original
    /// single-shot behaviour).  When streaming, callers advance `offset` by
    /// the `written` count returned from the previous call.
    ///
    /// On success writes the number of bytes transferred into `*out_written`.
    /// Returns 0 on success, non-zero on failure.
    pub handle_get: Option<unsafe extern "C" fn(
        ctx:         *mut c_void,
        s3_key:      *const ByteCursor,
        data:        *const ByteCursor,
        token:       *const ByteCursor,
        offset:      usize,
        out_written: *mut usize,
    ) -> i32>,
}

// SAFETY: vtable contains only function pointers and a static string pointer,
// all safe to share across threads.
unsafe impl Send for RdmaServerVtable {}
unsafe impl Sync for RdmaServerVtable {}

/// Function signature of the server plugin entry point.
///
/// Every RDMA server provider `.so` must export:
/// ```c
/// const RdmaServerVtable *s3_rdma_server_provider_get_vtable(void);
/// ```
pub type GetServerVtableFn = unsafe extern "C" fn() -> *const RdmaServerVtable;
