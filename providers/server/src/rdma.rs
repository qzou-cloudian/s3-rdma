//! cuObject RDMA server backend — NVIDIA SDK hardware implementation.

use std::ffi::{c_void, CStr};
use std::os::raw::c_char;
use std::sync::{Arc, OnceLock};

use tracing::{error, info};

use cuobj::server::CuObjServer;
use s3_rdma::ffi::{ByteCursor, CTelemetryConfig, RdmaServerInitParams, RdmaServerVtable};
use s3_rdma::provider::DEFAULT_RDMA_BUF_SIZE;
use s3_rdma::provider::RdmaServerProvider;

use crate::worker_pool::CuObjWorkerPool;

static PROVIDER_NAME: &[u8] = b"s3-rdma-server\0";

struct ServerProvider {
    pool: CuObjWorkerPool,
}

unsafe impl Send for ServerProvider {}
unsafe impl Sync for ServerProvider {}

static PROVIDER: OnceLock<Option<Box<ServerProvider>>> = OnceLock::new();

fn get_provider() -> Option<&'static ServerProvider> {
    PROVIDER.get()?.as_deref()
}

unsafe extern "C" fn c_init(params: *const RdmaServerInitParams, _ctx: *mut *mut c_void) -> i32 {
    if params.is_null() {
        error!("[s3-rdma-server] c_init: null params");
        return -1;
    }
    let p = unsafe { &*params };

    let addr = if p.listen_addr.is_null() {
        error!("[s3-rdma-server] c_init: null listen_addr");
        return -1;
    } else {
        match unsafe { CStr::from_ptr(p.listen_addr) }.to_str() {
            Ok(s)  => s.to_owned(),
            Err(e) => {
                error!("[s3-rdma-server] c_init: invalid listen_addr: {e}");
                return -1;
            }
        }
    };

    let srv = match CuObjServer::new(&addr, p.listen_port) {
        Ok(s)  => {
            if !s.is_connected() {
                error!(
                    "[s3-rdma-server] CuObjServer created but not connected to RDMA fabric \
                     at {addr}:{}", p.listen_port
                );
                return -1;
            }
            Arc::new(s)
        }
        Err(e) => {
            error!("[s3-rdma-server] CuObjServer::new({addr}:{}) failed: {e:?}", p.listen_port);
            return -1;
        }
    };

    let buf_size = if p.buf_size > 0 { p.buf_size } else { DEFAULT_RDMA_BUF_SIZE };
    let pool = CuObjWorkerPool::new(srv, 4, buf_size, 5); // 4 workers, 5 buffers each

    info!("[s3-rdma-server] listening {addr}:{port}, buf={mb}MiB",
          port = p.listen_port, mb = buf_size / (1024 * 1024));

    if PROVIDER.set(Some(Box::new(ServerProvider { pool }))).is_err() {
        error!("[s3-rdma-server] c_init: called more than once");
        return -1;
    }
    0
}

unsafe extern "C" fn c_cleanup(_ctx: *mut c_void) {
    let _ = PROVIDER.set(None); // drops pool → joins workers → frees channels
}

// ── Vtable handlers — all dispatch through the worker pool ────────────────────

unsafe extern "C" fn c_handle_put(
    _ctx: *mut c_void, s3_key: *const ByteCursor, token: *const ByteCursor,
    buf: *mut u8, offset: usize, size: usize, out_transferred: *mut usize,
) -> i32 {
    if s3_key.is_null() || token.is_null() || buf.is_null() || out_transferred.is_null() { return -1; }
    let Some(p) = get_provider() else { return -1 };
    let key_bytes   = unsafe { (*s3_key).as_slice() };
    let token_bytes = unsafe { (*token).as_slice() };
    let buf_slice   = unsafe { std::slice::from_raw_parts_mut(buf, p.pool.buf_size()) };
    match p.pool.open_channel(key_bytes, token_bytes)
        .and_then(|ch| ch.handle_put(buf_slice, offset, size))
    {
        Ok(n) => { unsafe { *out_transferred = n; } 0 }
        Err(e) => { error!("[s3-rdma-server] handle_put: {e}"); -1 }
    }
}

unsafe extern "C" fn c_handle_get(
    _ctx: *mut c_void, s3_key: *const ByteCursor, data: *const ByteCursor,
    token: *const ByteCursor, offset: usize, out_written: *mut usize,
) -> i32 {
    if s3_key.is_null() || data.is_null() || token.is_null() || out_written.is_null() { return -1; }
    let Some(p) = get_provider() else { return -1 };
    let key_bytes   = unsafe { (*s3_key).as_slice() };
    let data_bytes  = unsafe { (*data).as_slice() };
    let token_bytes = unsafe { (*token).as_slice() };
    match p.pool.open_channel(key_bytes, token_bytes)
        .and_then(|ch| ch.handle_get(data_bytes, offset))
    {
        Ok(n) => { unsafe { *out_written = n; } 0 }
        Err(e) => { error!("[s3-rdma-server] handle_get: {e}"); -1 }
    }
}

unsafe extern "C" fn c_handle_put_zc(
    _ctx: *mut c_void, s3_key: *const ByteCursor, token: *const ByteCursor,
    offset: usize, size: usize, write_fn: s3_rdma::ffi::RdmaIoWriteFn,
    write_fn_data: *mut c_void, out_transferred: *mut usize,
) -> i32 {
    if s3_key.is_null() || token.is_null() || out_transferred.is_null() { return -1; }
    let Some(p) = get_provider() else { return -1 };
    let key_bytes   = unsafe { (*s3_key).as_slice() };
    let token_bytes = unsafe { (*token).as_slice() };
    match p.pool.open_channel(key_bytes, token_bytes)
        .and_then(|ch| unsafe { ch.handle_put_zc(offset, size, write_fn, write_fn_data) })
    {
        Ok(n) => { unsafe { *out_transferred = n; } 0 }
        Err(e) => { error!("[s3-rdma-server] handle_put_zc: {e}"); -1 }
    }
}

unsafe extern "C" fn c_handle_get_zc(
    _ctx: *mut c_void, s3_key: *const ByteCursor, token: *const ByteCursor,
    offset: usize, read_fn: s3_rdma::ffi::RdmaIoReadFn, read_fn_data: *mut c_void,
    out_written: *mut usize,
) -> i32 {
    if s3_key.is_null() || token.is_null() || out_written.is_null() { return -1; }
    let Some(p) = get_provider() else { return -1 };
    let key_bytes   = unsafe { (*s3_key).as_slice() };
    let token_bytes = unsafe { (*token).as_slice() };
    match p.pool.open_channel(key_bytes, token_bytes)
        .and_then(|ch| unsafe { ch.handle_get_zc(offset, read_fn, read_fn_data) })
    {
        Ok(n) => { unsafe { *out_written = n; } 0 }
        Err(e) => { error!("[s3-rdma-server] handle_get_zc: {e}"); -1 }
    }
}

// ── Telemetry ────────────────────────────────────────────────────────────────

fn level_to_cuobj_flags(level: u32) -> u32 {
    match level { 0 => 0, 1 => 0x0004, 2 => 0x0005, _ => 0x0007 }
}

unsafe extern "C" fn c_supports_telemetry(_: *mut c_void) -> i32 { 1 }

unsafe extern "C" fn c_setup_telemetry(_: *mut c_void, cfg: *const CTelemetryConfig) -> i32 {
    if cfg.is_null() { return -1; }
    let c = unsafe { &*cfg };
    let log_file = if c.log_file.is_null() { None }
        else { Some(unsafe { std::ffi::CStr::from_ptr(c.log_file) }.to_string_lossy()) };
    CuObjServer::setup_telemetry(c.use_otel != 0, log_file.as_deref());
    CuObjServer::set_telem_flags(level_to_cuobj_flags(c.level));
    0
}

unsafe extern "C" fn c_shutdown_telemetry(_: *mut c_void) -> i32 { CuObjServer::shutdown_telemetry(); 0 }

unsafe extern "C" fn c_set_telem_level(_: *mut c_void, level: u32) -> i32 {
    CuObjServer::set_telem_flags(level_to_cuobj_flags(level)); 0
}

// ── Vtable ───────────────────────────────────────────────────────────────────

static VTABLE: RdmaServerVtable = RdmaServerVtable {
    provider_name:       PROVIDER_NAME.as_ptr() as *const c_char,
    provider_version:    5,
    init:                Some(c_init),
    cleanup:             Some(c_cleanup),
    handle_put:          Some(c_handle_put),
    handle_get:          Some(c_handle_get),
    handle_put_zc:       Some(c_handle_put_zc),
    handle_get_zc:       Some(c_handle_get_zc),
    supports_telemetry:  Some(c_supports_telemetry),
    setup_telemetry:     Some(c_setup_telemetry),
    shutdown_telemetry:  Some(c_shutdown_telemetry),
    set_telem_level:     Some(c_set_telem_level),
};

pub fn vtable() -> *const RdmaServerVtable { &VTABLE }
