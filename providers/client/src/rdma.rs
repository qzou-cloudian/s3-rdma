//! cuObject RDMA client backend — NVIDIA SDK hardware implementation.
//!
//! Uses the **persistent-token** strategy: `cuMemObjGetRDMAToken` is called
//! **once** at startup to create a DCT QP and obtain its descriptor string.
//! The descriptor is cached globally and returned from every `prepare_get`
//! call without creating a new DCT QP.
//!
//! # Why persistent instead of per-request cuObjGet
//!
//! `cuObjGet` creates and tears down a DCT QP per call (~15 ms overhead).
//! With a persistent token the DCT QP lives for the process lifetime and
//! each request incurs only the RDMA_WRITE time (< 2 ms for 1 MiB).
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

use tracing::{error, info};

use cuobj::CuObjClient;
use cuobj_sys::{self, CU_OBJ_SUCCESS, CUOBJ_GET};
use s3_rdma::error::RdmaError;
use s3_rdma::ffi::{ByteCursor, CTelemetryConfig, RdmaProviderVtable};
use s3_rdma::provider::{RdmaClientChannel, RdmaPutHandle, RdmaGetHandle};

static PROVIDER_NAME: &[u8] = b"s3-rdma-cuobj\0";

/// Page size for mmap allocations (4 KiB).
const PAGE_SIZE: usize = 4096;

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
    if ptr == libc::MAP_FAILED { std::ptr::null_mut() } else { ptr as *mut u8 }
}

// ── Global state (created once, lives for the process lifetime) ────────────────

struct GlobalState {
    /// Raw cuObjClient handle (standard, no ops callbacks).
    client_raw: usize,
    /// mmap'd, ibv-registered buffer.
    ptr:        usize,
    /// Registered size in bytes.
    size:       usize,
    /// Persistent DCT descriptor string (returned from every prepare_get).
    token:      Vec<u8>,
}

unsafe impl Send for GlobalState {}
unsafe impl Sync for GlobalState {}

static GLOBAL: OnceLock<GlobalState> = OnceLock::new();

fn global_init(requested_size: usize) -> Option<&'static GlobalState> {
    let state = GLOBAL.get_or_init(|| {
        // 1. Create a standard cuObjClient (no ops callbacks needed).
        let client_raw = unsafe { cuobj_sys::cuobj_client_create() };
        if client_raw.is_null() {
            error!("[s3-rdma] global init: cuobj_client_create returned null");
            return GlobalState { client_raw: 0, ptr: 0, size: 0, token: Vec::new() };
        }
        if unsafe { cuobj_sys::cuobj_client_is_connected(client_raw) } == 0 {
            error!("[s3-rdma] global init: cuObjClient not connected to RDMA fabric");
            return GlobalState { client_raw: 0, ptr: 0, size: 0, token: Vec::new() };
        }

        // 2. Allocate and register the buffer (once, never freed/deregistered).
        let ptr = mmap_alloc(requested_size);
        if ptr.is_null() {
            error!("[s3-rdma] global init: mmap failed size={requested_size}");
            return GlobalState { client_raw: 0, ptr: 0, size: 0, token: Vec::new() };
        }
        let rc = unsafe {
            cuobj_sys::cuobj_client_mem_get_descriptor(client_raw, ptr as *mut c_void, requested_size)
        };
        if rc != CU_OBJ_SUCCESS {
            error!("[s3-rdma] global init: cuMemObjGetDescriptor failed rc={rc}");
            return GlobalState { client_raw: 0, ptr: 0, size: 0, token: Vec::new() };
        }

        // 3. Get the persistent DCT descriptor (creates one DCT QP, lives forever).
        let mut desc_str: *mut c_char = std::ptr::null_mut();
        let rc = unsafe {
            cuobj_sys::cuobj_client_mem_get_rdma_token(
                client_raw,
                ptr as *mut c_void,
                requested_size,
                0,            // buf_offset
                CUOBJ_GET,
                &mut desc_str,
            )
        };
        if rc != CU_OBJ_SUCCESS || desc_str.is_null() {
            error!("[s3-rdma] global init: cuMemObjGetRDMAToken failed rc={rc}");
            return GlobalState { client_raw: 0, ptr: 0, size: 0, token: Vec::new() };
        }

        let token = unsafe {
            std::ffi::CStr::from_ptr(desc_str).to_bytes().to_vec()
        };
        // Intentionally do NOT call cuMemObjPutRDMAToken — the DCT QP stays
        // alive and the token is reused for every request.
        info!("[s3-rdma] global init: DCT ready, token_len={} ptr={ptr:?} size={requested_size}", token.len());

        GlobalState {
            client_raw: client_raw as usize,
            ptr:        ptr as usize,
            size:       requested_size,
            token,
        }
    });
    if state.client_raw == 0 { None } else { Some(state) }
}

// ── CuObjClientChannel ────────────────────────────────────────────────────────

/// One RDMA channel — a view into the global buffer + persistent DCT token.
struct CuObjClientChannel {
    key:  Vec<u8>,
    ptr:  *mut u8,
    size: usize,
    token: Vec<u8>,
}

unsafe impl Send for CuObjClientChannel {}
unsafe impl Sync for CuObjClientChannel {}

impl Drop for CuObjClientChannel {
    fn drop(&mut self) {
        // Global buffer and DCT are never freed — process-lifetime resources.
    }
}

impl RdmaClientChannel for CuObjClientChannel {
    fn key(&self)  -> &[u8]     { &self.key }
    fn ptr(&self)  -> *const u8 { self.ptr }
    fn size(&self) -> usize      { self.size }

    fn get_max_transfer_size(&self) -> usize { usize::MAX }

    fn prepare_put(&self, _offset: usize, _size: usize) -> Result<RdmaPutHandle, RdmaError> {
        Err(RdmaError::TokenGenerationFailed {
            key: String::from_utf8_lossy(&self.key).into_owned(),
            reason: "PUT not implemented for cuobj provider".into(),
        })
    }

    /// Return the cached persistent DCT token immediately — no thread spawn,
    /// no cuObjGet, no DCT QP creation.  The upstream will RDMA_WRITE into
    /// our buffer using the token; we read the buffer after `complete_get`.
    fn prepare_get(&self, offset: usize, size: usize) -> Result<RdmaGetHandle, RdmaError> {
        info!("[s3-rdma] prepare_get: token_len={} offset={offset} size={size}", self.token.len());
        Ok(RdmaGetHandle::new(self.token.clone(), offset, size))
    }

    fn complete_put(&self, _: RdmaPutHandle, _: u16, _: usize) -> Result<(), RdmaError> {
        Err(RdmaError::TransferFailed("PUT not implemented for cuobj provider".into()))
    }

    /// The upstream has already written data into our buffer via RDMA_WRITE
    /// by the time the HTTP 200 arrives.  Nothing to signal.
    fn complete_get(&self, _handle: RdmaGetHandle, rdma_status: u16, _transferred: usize) -> Result<(), RdmaError> {
        info!("[s3-rdma] complete_get: status={rdma_status}");
        if (200..300).contains(&(rdma_status as u32)) {
            Ok(())
        } else {
            Err(RdmaError::TransferFailed(format!("RDMA GET reply status={rdma_status}")))
        }
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
    _ctx: *mut c_void, size: usize, key: *const ByteCursor, out: *mut *mut c_void,
) -> i32 {
    if key.is_null() || out.is_null() { return -1; }
    let key_slice = unsafe { (*key).as_slice() };

    info!("[s3-rdma] bind_channel: size={size}");

    let state = match global_init(size) {
        Some(s) => s,
        None => {
            error!("[s3-rdma] bind_channel: global state unavailable");
            return -1;
        }
    };

    let channel: Box<dyn RdmaClientChannel> = Box::new(CuObjClientChannel {
        key:   key_slice.to_vec(),
        ptr:   state.ptr as *mut u8,
        size:  state.size,
        token: state.token.clone(),
    });
    let boxed: Box<Box<dyn RdmaClientChannel>> = Box::new(channel);
    unsafe { *out = Box::into_raw(boxed) as *mut c_void; }
    0
}

// ── Telemetry ──────────────────────────────────────────────────────────────────

unsafe extern "C" fn c_supports_telemetry(_: *mut c_void) -> i32 { 1 }

fn level_to_cuobj_flags(level: u32) -> u32 {
    match level { 0 => 0, 1 => 0x0004, 2 => 0x0005, _ => 0x0007 }
}

unsafe extern "C" fn c_setup_telemetry(_: *mut c_void, cfg: *const CTelemetryConfig) -> i32 {
    if cfg.is_null() { return -1; }
    let c = unsafe { &*cfg };
    let log_file = if c.log_file.is_null() {
        None
    } else {
        Some(unsafe { std::ffi::CStr::from_ptr(c.log_file) }.to_string_lossy())
    };
    CuObjClient::setup_telemetry(c.use_otel != 0, log_file.as_deref());
    CuObjClient::set_telem_flags(level_to_cuobj_flags(c.level));
    0
}

unsafe extern "C" fn c_shutdown_telemetry(_: *mut c_void) -> i32 {
    CuObjClient::shutdown_telemetry();
    0
}

unsafe extern "C" fn c_set_telem_level(_: *mut c_void, level: u32) -> i32 {
    CuObjClient::set_telem_flags(level_to_cuobj_flags(level));
    0
}

static VTABLE: RdmaProviderVtable = RdmaProviderVtable {
    provider_name:       PROVIDER_NAME.as_ptr() as *const c_char,
    provider_version:    1,
    init:                Some(c_init),
    cleanup:             Some(c_cleanup),
    bind_channel:        Some(c_bind_channel),
    supports_telemetry:  Some(c_supports_telemetry),
    setup_telemetry:     Some(c_setup_telemetry),
    shutdown_telemetry:  Some(c_shutdown_telemetry),
    set_telem_level:     Some(c_set_telem_level),
};

pub fn vtable() -> *const RdmaProviderVtable { &VTABLE }
