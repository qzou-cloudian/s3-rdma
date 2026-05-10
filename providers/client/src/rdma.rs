//! cuObject RDMA client backend — NVIDIA SDK hardware implementation.
//!
//! Uses `cuObjClient::cuObjGet` (DCI mode, RDMA_READ) via the `ops.get`
//! callback interface. This avoids the heap-corruption bug in the explicit
//! `cuMemObjGetRDMAToken` / `cuMemObjPutRDMAToken` API that causes crashes
//! after 3-4 sequential operations.
//!
//! # Flow
//!
//! 1. `c_bind_channel`: allocate mmap buffer, create CuObjClient with
//!    `ops.get = ops_get_callback`, register buffer.
//! 2. `prepare_get`: spawn a thread that calls `cuobj_client_get` (blocking).
//!    cuobj internally manages the DCT token and calls `ops_get_callback` with
//!    the RDMA descriptor string. The callback sends the descriptor to the relay
//!    backend and waits for an HTTP status code.
//! 3. Relay backend sends the descriptor to the upstream via HTTP and waits for
//!    the 200 response (indicating RDMA_WRITE complete).
//! 4. `complete_get`: signals the callback with the HTTP status; cuobj returns;
//!    the thread exits.
//!
//! # Buffer registration
//!
//! `cuMemObjPutDescriptor` (deregister) corrupts the jemalloc heap after
//! ~3 calls, causing crashes in unrelated allocations.  To avoid this, the
//! buffer is registered once in `c_bind_channel` and **never deregistered**.
//! The ibv MR becomes stale when the channel is dropped, but the mmap region
//! is freed and the leak is bounded by the process lifetime.

use std::ffi::{CStr, c_void};
use std::os::raw::c_char;
use std::sync::{Mutex, OnceLock};
use std::sync::mpsc::{self, SyncSender, Receiver};
use std::thread::JoinHandle;

use tracing::{error, info};

use cuobj::CuObjClient;
use cuobj_sys::{CuFileRdmaInfo, cuobj_client_create_with_get_cb,
                cuobj_client_get, cuobj_client_get_ctx_from_handle};
use s3_rdma::error::RdmaError;
use s3_rdma::ffi::{ByteCursor, CTelemetryConfig, RdmaProviderVtable};
use s3_rdma::provider::{RdmaClientChannel, RdmaPutHandle, RdmaGetHandle};

/// Global CuObjClient handle — created once, never destroyed.
///
/// Creating and destroying CuObjClient repeatedly corrupts cuobj's internal
/// state (leaves global/static state that prevents a new client from
/// connecting).  We create one client at init time, register the ops.get
/// callback, and reuse it for the lifetime of the process.
///
/// Safety: `*mut c_void` is not `Sync`, but we only read this after
/// initialization (in `bind_channel`), and cuobj itself is designed to be
/// called from multiple threads.
static GLOBAL_CLIENT: OnceLock<usize> = OnceLock::new();

/// Global RDMA buffer — mmap'd and registered with ibv once, never deregistered.
///
/// `cuMemObjPutDescriptor` (deregister) corrupts the jemalloc heap after
/// repeated calls.  Additionally, after `munmap` without deregistering, the
/// next `mmap` may reuse the same virtual address and cuobj refuses to
/// re-register it ("already registered").  The fix: allocate one large buffer
/// at startup, register it once, and reuse it for all transfers.
///
/// Sequential relay GET requests each copy the buffer data before the next
/// request starts, so buffer sharing is safe.
struct GlobalBuffer {
    ptr:  usize,   // mmap base address
    size: usize,   // registered size in bytes
}
unsafe impl Send for GlobalBuffer {}
unsafe impl Sync for GlobalBuffer {}
static GLOBAL_BUFFER: OnceLock<GlobalBuffer> = OnceLock::new();

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

fn mmap_free(ptr: *mut u8, size: usize) {
    let pages = size.div_ceil(PAGE_SIZE) * PAGE_SIZE;
    unsafe { libc::munmap(ptr as *mut libc::c_void, pages); }
}

// ── Global CuObjClient and buffer init ────────────────────────────────────────

/// Initialise the global CuObjClient (once) and return its raw handle.
///
/// Returns `None` if cuobj is unavailable or the client is not connected.
fn global_client() -> Option<*mut c_void> {
    let raw = *GLOBAL_CLIENT.get_or_init(|| {
        let client_raw = unsafe { cuobj_client_create_with_get_cb(ops_get_callback) };
        if client_raw.is_null() {
            error!("[s3-rdma] global client: cuobj_client_create_with_get_cb returned null");
            return 0usize;
        }
        if unsafe { cuobj_sys::cuobj_client_is_connected(client_raw) } == 0 {
            error!("[s3-rdma] global client: cuObjClient not connected to RDMA fabric");
            // intentionally leak — calling destroy corrupts state
            return 0usize;
        }
        info!("[s3-rdma] global cuObjClient created and connected");
        client_raw as usize
    });
    if raw == 0 { None } else { Some(raw as *mut c_void) }
}

/// Initialise the global RDMA buffer (once) and return (ptr, size).
///
/// The buffer is mmap'd and registered with the global CuObjClient.
/// It is never freed or deregistered — both operations corrupt cuobj's state.
fn global_buffer(requested_size: usize) -> Option<(*mut u8, usize)> {
    let buf = GLOBAL_BUFFER.get_or_init(|| {
        let client_raw = match global_client() {
            Some(r) => r,
            None => return GlobalBuffer { ptr: 0, size: 0 },
        };
        let ptr = mmap_alloc(requested_size);
        if ptr.is_null() {
            error!("[s3-rdma] global buffer: mmap failed size={requested_size}");
            return GlobalBuffer { ptr: 0, size: 0 };
        }
        let rc = unsafe {
            cuobj_sys::cuobj_client_mem_get_descriptor(client_raw, ptr as *mut c_void, requested_size)
        };
        if rc != cuobj_sys::CU_OBJ_SUCCESS {
            error!("[s3-rdma] global buffer: cuMemObjGetDescriptor failed rc={rc}");
            // intentionally don't munmap — leave it as a permanent allocation
            return GlobalBuffer { ptr: 0, size: 0 };
        }
        info!("[s3-rdma] global buffer: registered ptr={ptr:?} size={requested_size}");
        GlobalBuffer { ptr: ptr as usize, size: requested_size }
    });
    if buf.ptr == 0 || buf.size < requested_size {
        if buf.ptr != 0 {
            error!("[s3-rdma] global buffer: size {} < requested {}", buf.size, requested_size);
        }
        None
    } else {
        Some((buf.ptr as *mut u8, buf.size))
    }
}

// ── ops.get callback ───────────────────────────────────────────────────────────

/// State shared between `prepare_get` (relay backend thread) and the cuobj
/// `ops.get` callback (cuobj thread).
struct GetCtx {
    /// Sends the RDMA descriptor string to `prepare_get`.
    desc_tx: SyncSender<String>,
    /// Receives the HTTP completion status from `complete_get`.
    done_rx: Receiver<u16>,
}

/// An in-progress cuObjGet call (spawned thread + completion sender).
struct PendingGet {
    /// Signal the ops.get callback with the HTTP status returned by upstream.
    done_tx: SyncSender<u16>,
    /// Thread running `cuobj_client_get`; must be joined in `complete_get`.
    thread:  JoinHandle<isize>,
}

/// cuobj ops.get callback: called by cuObjGet with the RDMA descriptor string.
///
/// Forwards the descriptor to the relay backend via a rendezvous channel,
/// then waits for the relay to signal RDMA completion (via the HTTP response
/// status from the upstream).
unsafe extern "C" fn ops_get_callback(
    handle:    *const c_void,
    _ptr:      *mut c_char,
    size:      usize,
    _offset:   i64,
    rdma_info: *const CuFileRdmaInfo,
) -> isize {
    // Retrieve the GetCtx pointer passed as `ctx` to cuobj_client_get.
    let ctx_ptr = unsafe { cuobj_client_get_ctx_from_handle(handle) } as *const GetCtx;
    if ctx_ptr.is_null() || rdma_info.is_null() {
        error!("[s3-rdma] ops_get_callback: null handle or rdma_info");
        return -1;
    }
    let ctx = unsafe { &*ctx_ptr };

    let desc_str = unsafe {
        let info = &*rdma_info;
        if info.desc_str.is_null() {
            error!("[s3-rdma] ops_get_callback: null desc_str");
            return -1;
        }
        CStr::from_ptr(info.desc_str).to_string_lossy().into_owned()
    };

    info!("[s3-rdma] ops_get_callback: descriptor len={}", desc_str.len());

    // Hand the descriptor to the relay backend.
    if ctx.desc_tx.send(desc_str).is_err() {
        error!("[s3-rdma] ops_get_callback: desc channel closed");
        return -1;
    }

    // Wait for the relay backend to report the HTTP status.
    match ctx.done_rx.recv() {
        Ok(status) if (200..300).contains(&(status as u32)) => {
            info!("[s3-rdma] ops_get_callback: RDMA complete, status={status}");
            size as isize
        }
        Ok(status) => {
            error!("[s3-rdma] ops_get_callback: upstream status={status}");
            -1
        }
        Err(_) => {
            error!("[s3-rdma] ops_get_callback: done channel closed");
            -1
        }
    }
}

// ── CuObjClientChannel ────────────────────────────────────────────────────────

/// One RDMA channel.  Owns:
///   - a raw cuObjClient handle with ops.get = ops_get_callback
///   - an mmap-allocated, RDMA-registered buffer
///   - state for any in-progress cuObjGet call
struct CuObjClientChannel {
    key:        Vec<u8>,
    /// Raw cuObjClient handle (created via cuobj_client_create_with_get_cb).
    client_raw: *mut c_void,
    ptr:        *mut u8,
    size:       usize,
    pending:    Mutex<Option<PendingGet>>,
}

unsafe impl Send for CuObjClientChannel {}
unsafe impl Sync for CuObjClientChannel {}

impl Drop for CuObjClientChannel {
    fn drop(&mut self) {
        // Both the client and the buffer are global — never freed here.
    }
}

fn rdma_status_ok(status: u16) -> bool {
    (200..300).contains(&status)
}

impl RdmaClientChannel for CuObjClientChannel {
    fn key(&self)  -> &[u8]     { &self.key }
    fn ptr(&self)  -> *const u8 { self.ptr }
    fn size(&self) -> usize      { self.size }

    fn get_max_transfer_size(&self) -> usize {
        usize::MAX
    }

    fn prepare_put(&self, _offset: usize, _size: usize) -> Result<RdmaPutHandle, RdmaError> {
        Err(RdmaError::TokenGenerationFailed {
            key: String::from_utf8_lossy(&self.key).into_owned(),
            reason: "PUT not implemented for cuObjGet provider".into(),
        })
    }

    fn prepare_get(&self, offset: usize, size: usize) -> Result<RdmaGetHandle, RdmaError> {
        info!("[s3-rdma] prepare_get: ptr={:?} size={} offset={}", self.ptr, size, offset);

        // Rendezvous channels: relay ↔ ops_get_callback thread.
        let (desc_tx, desc_rx) = mpsc::sync_channel::<String>(0);
        let (done_tx, done_rx) = mpsc::sync_channel::<u16>(0);

        // Cast raw pointers to usize so the closure is Send.
        // SAFETY: the CuObjClientChannel outlives the thread (joined in complete_get).
        let client_raw = self.client_raw as usize;
        let buf_ptr    = self.ptr as usize;

        let thread = std::thread::spawn(move || {
            // GetCtx lives on this thread's stack for the duration of cuobj_client_get.
            let ctx = GetCtx { desc_tx, done_rx };
            let ret = unsafe {
                cuobj_client_get(
                    client_raw as *mut c_void,
                    &ctx as *const GetCtx as *mut c_void,
                    buf_ptr as *mut c_void,
                    size,
                    offset as i64,
                    0,
                )
            };
            // ctx dropped here: closes desc_tx and done_rx.
            ret
        });

        // Wait for the callback to deliver the RDMA descriptor.
        let descriptor = desc_rx.recv().map_err(|_| RdmaError::TokenGenerationFailed {
            key: String::from_utf8_lossy(&self.key).into_owned(),
            reason: "cuObjGet ops.get callback did not fire".into(),
        })?;

        info!("[s3-rdma] prepare_get: got descriptor len={}", descriptor.len());

        *self.pending.lock().unwrap() = Some(PendingGet { done_tx, thread });
        Ok(RdmaGetHandle::new(descriptor.into_bytes(), offset, size))
    }

    fn complete_put(&self, _handle: RdmaPutHandle, _status: u16, _: usize) -> Result<(), RdmaError> {
        Err(RdmaError::TransferFailed("PUT not implemented for cuObjGet provider".into()))
    }

    fn complete_get(&self, _handle: RdmaGetHandle, rdma_status: u16, _: usize) -> Result<(), RdmaError> {
        info!("[s3-rdma] complete_get: status={}", rdma_status);

        let pending = self.pending.lock().unwrap().take()
            .ok_or_else(|| RdmaError::TransferFailed("no pending GET".into()))?;

        // Signal the ops.get callback with the upstream HTTP status.
        let _ = pending.done_tx.send(rdma_status);

        // Wait for cuobj_client_get to return.
        let ret = pending.thread.join()
            .map_err(|_| RdmaError::TransferFailed("cuObjGet thread panicked".into()))?;

        if ret < 0 {
            return Err(RdmaError::TransferFailed(format!("cuObjGet returned {ret}")));
        }

        if rdma_status_ok(rdma_status) { Ok(()) }
        else { Err(RdmaError::TransferFailed(format!("RDMA GET reply status={rdma_status}"))) }
    }
}

// ── C vtable handlers ──────────────────────────────────────────────────────────

unsafe extern "C" fn c_init(_: *mut c_void, _: *mut *mut c_void) -> i32 {
    if global_client().is_some() { 0 } else { -1 }
}

unsafe extern "C" fn c_cleanup(_: *mut c_void) {}

unsafe extern "C" fn c_bind_channel(
    _ctx: *mut c_void, size: usize, key: *const ByteCursor, out: *mut *mut c_void,
) -> i32 {
    if key.is_null() || out.is_null() { return -1; }
    let key_slice = unsafe { (*key).as_slice() };

    info!("[s3-rdma] bind_channel: size={} key={:?}", size, String::from_utf8_lossy(key_slice));

    // Retrieve the global CuObjClient and the pre-registered buffer.
    // Both are created/registered exactly once and never destroyed.
    let client_raw = match global_client() {
        Some(r) => r,
        None => {
            error!("[s3-rdma] bind_channel: global cuObjClient unavailable");
            return -1;
        }
    };
    let (ptr, buf_size) = match global_buffer(size) {
        Some(r) => r,
        None => {
            error!("[s3-rdma] bind_channel: global buffer unavailable for size={size}");
            return -1;
        }
    };

    let channel: Box<dyn RdmaClientChannel> = Box::new(CuObjClientChannel {
        key: key_slice.to_vec(),
        client_raw,
        ptr,
        size: buf_size,
        pending: Mutex::new(None),
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
