//! cuObject RDMA client backend — NVIDIA SDK hardware implementation.
//!
//! Uses a process-global registered buffer and creates the matching cuObj token
//! lazily in `prepare_put` / `prepare_get`.  Token generation is intentionally
//! operation-specific; a PUT should not create an unused GET token during bind.
//!
//! # Why jemalloc must be disabled
//!
//! `tikv-jemallocator` intercepts `malloc`/`free` globally.  cuobj's C++ code
//! uses glibc `new`/`delete` internally; with jemalloc intercepting `free`,
//! cuobj's deallocation corrupts the allocator after ~3 operations.
//! Build the relay binary with `--no-default-features --features rdma`.
//!
//! # Buffer
//!
//! One global mmap buffer is registered once and never deregistered
//! (`cuMemObjPutDescriptor` also triggers the same corruption pattern).
//! Sequential relay GET requests copy the buffer before the next request.

use std::ffi::c_void;
use std::os::raw::c_char;
use std::sync::OnceLock;

use tracing::{error, info, warn};

use cuobj::CuObjClient;
use cuobj_sys::{self, CUOBJ_GET, CUOBJ_PUT, CU_OBJ_SUCCESS};
use s3_rdma::error::RdmaError;
use s3_rdma::ffi::{ByteCursor, CTelemetryConfig, RdmaProviderVtable};
use s3_rdma::provider::{RdmaClientChannel, RdmaTransferHandle};

static PROVIDER_NAME: &[u8] = b"s3-rdma-cuobj\0";

/// Page size for mmap allocations (4 KiB).
const PAGE_SIZE: usize = 4096;

/// Minimum RDMA buffer size: 64 MiB.  The global buffer is allocated once
/// at first bind_channel — using this floor ensures that even when the first
/// transfer is small, subsequent transfers up to 64 MiB can use RDMA without
/// falling back to plain HTTP.
const MIN_BUF_SIZE: usize = 64 * 1024 * 1024;

fn mmap_alloc(size: usize) -> *mut u8 {
    let pages = size.div_ceil(PAGE_SIZE) * PAGE_SIZE;
    let ptr = unsafe {
        libc::mmap(
            std::ptr::null_mut(),
            pages,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_ANONYMOUS | libc::MAP_PRIVATE,
            -1,
            0,
        )
    };
    if ptr == libc::MAP_FAILED {
        std::ptr::null_mut()
    } else {
        ptr as *mut u8
    }
}

// ── Global state (created once, lives for the process lifetime) ────────────────

struct GlobalState {
    /// Raw cuObjClient handle (standard, no ops callbacks).
    client_raw: usize,
    /// mmap'd, ibv-registered buffer.
    ptr: usize,
    /// Registered size in bytes.
    size: usize,
}

unsafe impl Send for GlobalState {}
unsafe impl Sync for GlobalState {}

static GLOBAL: OnceLock<GlobalState> = OnceLock::new();

fn global_init(requested_size: usize) -> Option<&'static GlobalState> {
    let requested_size = requested_size.max(MIN_BUF_SIZE);
    let state = GLOBAL.get_or_init(|| {
        // 1. Create a standard cuObjClient (no ops callbacks needed).
        let client_raw = unsafe { cuobj_sys::cuobj_client_create() };
        if client_raw.is_null() {
            error!("[s3-rdma] global init: cuobj_client_create returned null");
            return GlobalState { client_raw: 0, ptr: 0, size: 0 };
        }
        if unsafe { cuobj_sys::cuobj_client_is_connected(client_raw) } == 0 {
            error!("[s3-rdma] global init: cuObjClient not connected to RDMA fabric");
            return GlobalState { client_raw: 0, ptr: 0, size: 0 };
        }

        // 2. Allocate and register the buffer (once, never freed/deregistered).
        let ptr = mmap_alloc(requested_size);
        if ptr.is_null() {
            error!("[s3-rdma] global init: mmap failed size={requested_size}");
            return GlobalState { client_raw: 0, ptr: 0, size: 0 };
        }
        let rc = unsafe {
            cuobj_sys::cuobj_client_mem_get_descriptor(client_raw, ptr as *mut c_void, requested_size)
        };
        if rc != CU_OBJ_SUCCESS {
            error!("[s3-rdma] global init: cuMemObjGetDescriptor failed rc={rc}");
            return GlobalState { client_raw: 0, ptr: 0, size: 0 };
        }
        info!("[s3-rdma] global init: buffer registered ptr={ptr:?} size={requested_size}");

        GlobalState {
            client_raw: client_raw as usize,
            ptr:        ptr as usize,
            size:       requested_size,
        }
    });
    if state.client_raw == 0 {
        None
    } else {
        Some(state)
    }
}

// ── CuObjClientChannel ────────────────────────────────────────────────────────

/// One RDMA channel — a view into the global registered buffer.
struct CuObjClientChannel {
    key: Vec<u8>,
    client_raw: usize,
    ptr: *mut u8,
    size: usize,
}

unsafe impl Send for CuObjClientChannel {}
unsafe impl Sync for CuObjClientChannel {}

impl Drop for CuObjClientChannel {
    fn drop(&mut self) {
        // Global buffer and DCT are never freed — process-lifetime resources.
    }
}

impl RdmaClientChannel for CuObjClientChannel {
    fn key(&self) -> &[u8] {
        &self.key
    }
    fn ptr(&self) -> *mut u8 {
        self.ptr
    }
    fn size(&self) -> usize {
        self.size
    }

    fn get_max_transfer_size(&self) -> usize {
        usize::MAX
    }

    /// Return a PUT DCT token — the server will RDMA_READ from
    /// our buffer using the token; data is already in place from the caller's
    /// copy via `ptr()` after `bind()`.
    ///
    /// Appends `:offset:size` (hex-encoded) to the base cuObj token so the gateway
    /// can determine the exact transfer size without relying on Content-Length.
    /// The gateway's `RdmaToken::parse` strips this extension before passing
    /// the base token to the cuObj C SDK.
    fn prepare_put(&self, offset: usize, size: usize) -> Result<RdmaTransferHandle, RdmaError> {
        let token_put = self.token(CUOBJ_PUT, offset, size)?;
        info!("[s3-rdma] prepare_put: token_len={} offset={offset} size={size}", token_put.len());
        let base = String::from_utf8_lossy(&token_put);
        let token = format!("{base}:{offset:x}:{size:x}").into_bytes();
        Ok(RdmaTransferHandle::new(token, offset, size))
    }

    /// Return a GET DCT token.  The upstream will
    /// RDMA_WRITE into our buffer using the token; we read the buffer after `complete_get`.
    ///
    /// Appends `:offset:size` (hex-encoded) so the server knows how many bytes to RDMA_WRITE.
    fn prepare_get(&self, offset: usize, size: usize) -> Result<RdmaTransferHandle, RdmaError> {
        let token_get = self.token(CUOBJ_GET, offset, size)?;
        info!("[s3-rdma] prepare_get: token_len={} offset={offset} size={size}", token_get.len());
        let base = String::from_utf8_lossy(&token_get);
        let token = format!("{base}:{offset:x}:{size:x}").into_bytes();
        Ok(RdmaTransferHandle::new(token, offset, size))
    }

    /// The server has finished RDMA_READ from our buffer. Confirm the reply status.
    fn complete_put(
        &self,
        _handle: RdmaTransferHandle,
        rdma_status: u16,
        _transferred: usize,
    ) -> Result<(), RdmaError> {
        info!("[s3-rdma] complete_put: status={rdma_status}");
        if (200..300).contains(&(rdma_status as u32)) {
            Ok(())
        } else {
            Err(RdmaError::TransferFailed {
                reason: format!("RDMA PUT reply status={rdma_status}"),
            })
        }
    }

    /// The upstream has already written data into our buffer via RDMA_WRITE
    /// by the time the HTTP 200 arrives.  Nothing to signal.
    fn complete_get(
        &self,
        _handle: RdmaTransferHandle,
        rdma_status: u16,
        _transferred: usize,
    ) -> Result<(), RdmaError> {
        info!("[s3-rdma] complete_get: status={rdma_status}");
        if (200..300).contains(&(rdma_status as u32)) {
            Ok(())
        } else {
            Err(RdmaError::TransferFailed {
                reason: format!("RDMA GET reply status={rdma_status}"),
            })
        }
    }
}

impl CuObjClientChannel {
    fn token(&self, op: i32, _offset: usize, _size: usize) -> Result<Vec<u8>, RdmaError> {
        let mut desc: *mut c_char = std::ptr::null_mut();
        let rc = unsafe {
            cuobj_sys::cuobj_client_mem_get_rdma_token(
                self.client_raw as *mut c_void,
                self.ptr as *mut c_void,
                self.size,
                0,
                op,
                &mut desc,
            )
        };
        if rc != CU_OBJ_SUCCESS || desc.is_null() {
            return Err(RdmaError::TokenGenerationFailed {
                key: String::from_utf8_lossy(&self.key).into_owned(),
                reason: format!("cuMemObjGetRDMAToken op={op} failed rc={rc}"),
            });
        }
        Ok(unsafe { std::ffi::CStr::from_ptr(desc).to_bytes().to_vec() })
    }
}

// ── C vtable handlers ──────────────────────────────────────────────────────────

unsafe extern "C" fn c_init(_: *mut c_void, _: *mut *mut c_void) -> i32 {
    // Buffer size will be determined at first bind_channel call; just probe connectivity.
    match CuObjClient::new() {
        Ok(c) if c.is_connected() => 0,
        Ok(_) => {
            error!("[s3-rdma] c_init: cuObjClient not connected");
            -1
        }
        Err(e) => {
            error!("[s3-rdma] c_init: {e:?}");
            -1
        }
    }
}

unsafe extern "C" fn c_cleanup(_: *mut c_void) {}

unsafe extern "C" fn c_bind_channel(
    _ctx: *mut c_void,
    size: usize,
    key: *const ByteCursor,
    out: *mut *mut c_void,
) -> i32 {
    if key.is_null() || out.is_null() {
        return -1;
    }
    let key_slice = unsafe { (*key).as_slice() };

    info!("[s3-rdma] bind_channel: size={size}");

    let state = match global_init(size) {
        Some(s) => s,
        None => {
            error!("[s3-rdma] bind_channel: global state unavailable");
            return -1;
        }
    };

    // The global buffer is fixed-size (allocated on first bind). If the
    // requested size exceeds it, return an error so the caller falls back
    // to plain HTTP rather than panicking on a too-small slice.
    if size > state.size {
        warn!(
            "[s3-rdma] bind_channel: size={size} > buffer_size={} — falling back",
            state.size
        );
        return -1;
    }

    let channel: Box<dyn RdmaClientChannel> = Box::new(CuObjClientChannel {
        key: key_slice.to_vec(),
        client_raw: state.client_raw,
        ptr: state.ptr as *mut u8,
        size: state.size,
    });
    let boxed: Box<Box<dyn RdmaClientChannel>> = Box::new(channel);
    unsafe {
        *out = Box::into_raw(boxed) as *mut c_void;
    }
    0
}

// ── Telemetry ──────────────────────────────────────────────────────────────────
// Map our 0-3 level to the cuobj SDK flag bits (CUOBJ_LOG_PATH_*).
// Kept in sync with providers/server/src/rdma.rs.
fn level_to_cuobj_flags(level: u32) -> u32 {
    match level {
        0 => 0,
        1 => 0x0004,
        2 => 0x0005,
        _ => 0x0007,
    }
}

unsafe extern "C" fn c_setup_telemetry(ctx: *mut c_void, cfg: *const CTelemetryConfig) -> i32 {
    let rc = crate::telem::setup(ctx, cfg);
    if rc != 0 || cfg.is_null() {
        return rc;
    }
    let c = unsafe { &*cfg };
    let log_file = if c.log_file.is_null() {
        None
    } else {
        unsafe { std::ffi::CStr::from_ptr(c.log_file) }
            .to_str()
            .ok()
            .map(str::to_owned)
    };
    CuObjClient::setup_telemetry(c.use_otel != 0, log_file.as_deref());
    CuObjClient::set_telem_flags(level_to_cuobj_flags(c.level));
    0
}

unsafe extern "C" fn c_shutdown_telemetry(ctx: *mut c_void) -> i32 {
    crate::telem::shutdown(ctx);
    CuObjClient::shutdown_telemetry();
    0
}

unsafe extern "C" fn c_set_telem_level(ctx: *mut c_void, level: u32) -> i32 {
    crate::telem::set_level(ctx, level);
    CuObjClient::set_telem_flags(level_to_cuobj_flags(level));
    0
}

static VTABLE: RdmaProviderVtable = RdmaProviderVtable {
    provider_name: PROVIDER_NAME.as_ptr() as *const c_char,
    provider_version: 1,
    init: Some(c_init),
    cleanup: Some(c_cleanup),
    bind_channel: Some(c_bind_channel),
    supports_telemetry: Some(crate::telem::supports),
    setup_telemetry: Some(c_setup_telemetry),
    shutdown_telemetry: Some(c_shutdown_telemetry),
    set_telem_level: Some(c_set_telem_level),
};

pub fn vtable() -> *const RdmaProviderVtable {
    &VTABLE
}
