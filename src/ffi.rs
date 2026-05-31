//! C-ABI types for the s3-rdma RDMA provider vtable.
//!
//! These types define the wire format shared between `s3-rdma` (the loader)
//! and any RDMA provider plugin compiled as a `cdylib`.  The layout must
//! match exactly — types cross a `dlopen`/`dlsym` boundary.

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
    /// Returns a zero-length cursor with a null pointer.
    ///
    /// Used as a sentinel value where a `ByteCursor` is required but no data
    /// is available (e.g. absent optional fields in C structs).
    pub const fn null() -> Self {
        Self {
            ptr: std::ptr::null(),
            len: 0,
        }
    }

    pub fn from_slice(s: &[u8]) -> Self {
        Self {
            ptr: s.as_ptr(),
            len: s.len(),
        }
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
    fn default() -> Self {
        Self::null()
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Completion callback
// ─────────────────────────────────────────────────────────────────────────────

/// C completion callback invoked when an async RDMA operation finishes.
///
/// - `user_data`   – opaque pointer supplied by the caller.
/// - `error_code`  – 0 on success; non-zero on failure.
/// - `reply_token` – server reply token (may be null / empty).
pub type RdmaCompletionFn =
    unsafe extern "C" fn(user_data: *mut c_void, error_code: i32, reply_token: *const ByteCursor);

// ─────────────────────────────────────────────────────────────────────────────
// C-ABI telemetry types
// ─────────────────────────────────────────────────────────────────────────────

/// C-ABI telemetry configuration (mirrors [`TelemetryConfig`](crate::provider::TelemetryConfig)).
#[repr(C)]
pub struct CTelemetryConfig {
    /// `1` = use OpenTelemetry backend; `0` = use ostream.
    pub use_otel: i32,
    /// Null-terminated log file path (may be null).
    pub log_file: *const c_char,
    /// [`TelemLevel`](crate::provider::TelemLevel) as its `#[repr(u32)]` value.
    pub level: u32,
}

// ─────────────────────────────────────────────────────────────────────────────
// RdmaProviderVtable
// ─────────────────────────────────────────────────────────────────────────────

/// Configuration passed to `RdmaProviderVtable::init` for client providers.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct RdmaClientInitParams {
    /// Size in bytes of each client-side registered buffer slot.
    pub client_buf_size: usize,
    /// Maximum number of client-side registered buffer slots.
    pub client_pool_slots: usize,
    /// Time to wait for a free client pool slot before failing bind.
    pub client_pool_wait_ms: u64,
}

// ── bind_channel return codes ──────────────────────────────────────────────
//
// `bind_channel` returns an `i32`.  `0` is success; negative values encode why
// the bind failed so the loader can reconstruct the matching [`RdmaError`]
// variant (and decide fallback eligibility) without an out-param.  Shared by
// the plugin (which returns them) and the loader (which maps them back).

/// `bind_channel` succeeded; `*out_channel` is valid.
pub const RDMA_BIND_OK: i32 = 0;
/// Bind failed for an unspecified reason.
pub const RDMA_BIND_ERR_GENERIC: i32 = -1;
/// Requested transfer is larger than a single pool slot.
pub const RDMA_BIND_ERR_TOO_LARGE: i32 = -2;
/// All pool slots are leased and the wait timed out.
pub const RDMA_BIND_ERR_EXHAUSTED: i32 = -3;
/// A slot buffer could not be allocated/registered (genuine hardware fault).
pub const RDMA_BIND_ERR_REGISTER: i32 = -4;
/// The backend is not ready (plugin/client not initialised or not connected).
pub const RDMA_BIND_ERR_NOT_READY: i32 = -5;

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
    pub provider_name: *const c_char,
    /// Version integer for compatibility checks.
    pub provider_version: u32,

    /// Initialise the provider.
    ///
    /// Client providers receive a pointer to [`RdmaClientInitParams`] cast to
    /// `*mut c_void`.  Older providers may ignore this pointer.
    pub init: Option<unsafe extern "C" fn(*mut c_void, *mut *mut c_void) -> i32>,
    /// Destroy the provider.
    pub cleanup: Option<unsafe extern "C" fn(*mut c_void)>,

    /// Allocate, register, and bind a buffer to an S3 key.
    ///
    /// The plugin allocates `size` bytes of RDMA-suitable memory, registers
    /// it, and returns a double-boxed channel.  The returned `*mut c_void`
    /// is a `Box<Box<dyn RdmaClientChannel>>` — the caller must unbox it
    /// with `Box::from_raw` twice.
    ///
    /// Returns null on error.
    pub bind_channel: Option<
        unsafe extern "C" fn(
            *mut c_void,       // ctx
            usize,             // size
            *const ByteCursor, // key
            *mut *mut c_void,  // out_channel (Box<Box<dyn RdmaClientChannel>>)
        ) -> i32,
    >,

    // ── Telemetry ────────────────────────────────────────────────────────────
    /// Returns `1` if this provider supports telemetry.
    pub supports_telemetry: Option<unsafe extern "C" fn(*mut c_void) -> i32>,

    /// Set up telemetry output for this provider.
    pub setup_telemetry: Option<unsafe extern "C" fn(*mut c_void, *const CTelemetryConfig) -> i32>,

    /// Shut down telemetry (reset to default output stream).
    pub shutdown_telemetry: Option<unsafe extern "C" fn(*mut c_void) -> i32>,

    /// Set telemetry log-path flags.
    pub set_telem_level: Option<unsafe extern "C" fn(*mut c_void, level: u32) -> i32>,
}

// SAFETY: the vtable contains only raw function pointers which are always safe
// to send and share across threads once the library is loaded.
unsafe impl Send for RdmaProviderVtable {}
unsafe impl Sync for RdmaProviderVtable {}

// SAFETY: only used during init() on the calling thread; `Sync` is not needed.
unsafe impl Send for RdmaClientInitParams {}

/// Function signature of the plugin entry point for the client provider.
///
/// Every RDMA client provider `.so` must export:
/// ```c
/// const RdmaProviderVtable *s3_rdma_get_client_vtable(const char *provider_name);
/// ```
///
/// Returns a pointer to the vtable for the requested provider, or `NULL` if
/// the provider name is not recognised.
pub type GetNamedClientVtableFn =
    unsafe extern "C" fn(name: *const c_char) -> *const RdmaProviderVtable;

// ─────────────────────────────────────────────────────────────────────────────
// RdmaServerVtable
// ─────────────────────────────────────────────────────────────────────────────

/// Synchronous I/O read callback for zero-copy RDMA GET.
///
/// Called by the plugin's I/O worker to fill `buf[0..capacity]` directly from
/// the caller's data source (e.g., `pread(fd, buf, capacity, offset)`).
/// Returns the number of bytes read (≥ 0) or a negative error code.
///
/// Must be thread-safe: the plugin may invoke it from any worker thread.
pub type RdmaIoReadFn =
    unsafe extern "C" fn(user_data: *mut c_void, buf: *mut c_void, capacity: usize) -> isize;

/// Synchronous I/O write callback for zero-copy RDMA PUT.
///
/// Called by the plugin's I/O worker to flush `buf[0..size]` directly to the
/// caller's data sink (e.g., `pwrite(fd, buf, size, offset)`).
/// Returns the number of bytes written (≥ 0) or a negative error code.
///
/// Must be thread-safe: the plugin may invoke it from any worker thread.
pub type RdmaIoWriteFn =
    unsafe extern "C" fn(user_data: *mut c_void, buf: *const c_void, size: usize) -> isize;

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
    /// Size in bytes of each buffer slot.
    pub buf_size: usize,
}

/// Callback for the HTTP round-trip in [`RdmaClientChannel::put`]/[`get`].
///
/// Receives the serialised token bytes and opaque `user_data`.  Must write
/// the server's `(rdma_status, transferred_bytes)` to `out_status` /
/// `out_transferred`.  Called synchronously between prepare and complete.
pub type RdmaSendFn = unsafe extern "C" fn(
    token: *const u8,
    token_len: usize,
    user_data: *mut std::ffi::c_void,
    out_status: *mut u16,
    out_transferred: *mut usize,
);

// SAFETY: only used during init() on the calling thread; `Sync` is not needed.
unsafe impl Send for RdmaServerInitParams {}

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
    pub provider_name: *const c_char,
    /// Version integer for compatibility checks.
    pub provider_version: u32,

    /// Initialise the server provider with the given config.
    /// On success sets `*ctx` and returns 0; non-zero on failure.
    pub init: Option<unsafe extern "C" fn(*const RdmaServerInitParams, *mut *mut c_void) -> i32>,
    /// Destroy the server provider context.
    pub cleanup: Option<unsafe extern "C" fn(*mut c_void)>,

    /// Handle an RDMA PUT: the server RDMA_READs `size` bytes from the
    /// client buffer identified by `token`, starting at `offset` bytes into
    /// Zero-copy RDMA PUT: RDMA-reads from the client's buffer.
    ///
    /// On success copies the received bytes into the caller-provided buffer `buf`
    /// and writes the byte count into `*out_transferred`.
    ///
    /// `offset=0, size=0` means "read the full buffer as described by the token".
    /// The plugin RDMA-reads from the client buffer, then calls
    /// `write_fn(write_fn_data, rdma_buf_ptr, transferred)` to flush the data
    /// directly from its internal RDMA-registered buffer to the storage layer.
    /// No intermediate copy into a caller-provided slice.
    ///
    /// `write_fn` must be thread-safe.  The plugin may call it from a worker
    /// thread.  Returns 0 on success, non-zero on failure.
    pub handle_put: Option<
        unsafe extern "C" fn(
            ctx: *mut c_void,
            s3_key: *const ByteCursor,
            token: *const ByteCursor,
            offset: usize,
            size: usize,
            write_fn: RdmaIoWriteFn,
            write_fn_data: *mut c_void,
            out_transferred: *mut usize,
        ) -> i32,
    >,

    /// Handle an RDMA GET.
    ///
    /// The plugin calls `read_fn(read_fn_data, rdma_buf_ptr, capacity)` to fill
    /// its RDMA-registered buffer directly from the storage layer, then
    /// RDMA_WRITEs that buffer to the client.  No intermediate copy.
    ///
    /// `read_fn` must be thread-safe.  Returns 0 on success, non-zero on failure.
    pub handle_get: Option<
        unsafe extern "C" fn(
            ctx: *mut c_void,
            s3_key: *const ByteCursor,
            token: *const ByteCursor,
            offset: usize,
            read_fn: RdmaIoReadFn,
            read_fn_data: *mut c_void,
            out_written: *mut usize,
        ) -> i32,
    >,

    // ── Telemetry ────────────────────────────────────────────────────────────
    /// Returns `1` if this provider supports telemetry.
    pub supports_telemetry: Option<unsafe extern "C" fn(*mut c_void) -> i32>,

    /// Set up telemetry output for this server provider.
    /// `cfg` points to a [`CTelemetryConfig`]; valid for the duration of the call.
    /// Returns 0 on success, non-zero on failure.
    pub setup_telemetry: Option<unsafe extern "C" fn(*mut c_void, *const CTelemetryConfig) -> i32>,

    /// Shut down telemetry (reset to default output stream).
    /// Returns 0 on success, non-zero on failure.
    pub shutdown_telemetry: Option<unsafe extern "C" fn(*mut c_void) -> i32>,

    /// Set telemetry log-path flags.
    /// Returns 0 on success, non-zero on failure.
    pub set_telem_level: Option<unsafe extern "C" fn(*mut c_void, level: u32) -> i32>,

    /// Open an RdmaServerChannel for key+token.
    /// On success, *out_channel receives a heap-allocated Box<dyn RdmaServerChannel>.
    /// Returns 0 on success, non-zero on failure.
    pub open_channel: Option<
        unsafe extern "C" fn(
            ctx: *mut c_void,
            key: *const ByteCursor,
            token: *const ByteCursor,
            out_channel: *mut *mut c_void,
        ) -> i32,
    >,
}

// SAFETY: vtable contains only function pointers and a static string pointer,
// all safe to share across threads.
unsafe impl Send for RdmaServerVtable {}
unsafe impl Sync for RdmaServerVtable {}

/// Function signature of the server plugin entry point.
///
/// Every RDMA server provider `.so` must export:
/// ```c
/// const RdmaServerVtable *s3_rdma_get_server_vtable(const char *provider_name);
/// ```
///
/// Returns a pointer to the vtable for the requested provider, or `NULL` if
/// the provider name is not recognised.
pub type GetNamedServerVtableFn =
    unsafe extern "C" fn(name: *const c_char) -> *const RdmaServerVtable;

// ── Shared library loader ─────────────────────────────────────────────────

/// Search for `lib_name` in standard locations and return the loaded library.
///
/// Requires `rdma-client` or `rdma-server` feature for `libloading`.
/// Search order: LD_LIBRARY_PATH → executable directory → `/lib64/` → `/usr/lib64/` → `/usr/local/lib/`.
#[cfg(any(feature = "rdma-client", feature = "rdma-server"))]
pub fn find_library(lib_name: &str) -> Option<libloading::Library> {
    fn try_load(path: impl AsRef<std::ffi::OsStr>) -> Option<libloading::Library> {
        unsafe { libloading::Library::new(path) }.ok()
    }
    if let Some(lib) = try_load(lib_name) {
        return Some(lib);
    }
    if let Ok(exe) = std::env::current_exe() {
        if let Some(dir) = exe.parent() {
            if let Some(lib) = try_load(dir.join(lib_name)) {
                return Some(lib);
            }
        }
    }
    for dir in &["/lib64", "/usr/lib64", "/usr/local/lib"] {
        if let Some(lib) = try_load(format!("{dir}/{lib_name}")) {
            return Some(lib);
        }
    }
    None
}
