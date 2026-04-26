//! cuObject RDMA combined provider plugin — client + server in one shared library.
//!
//! Builds as a `cdylib` (`libs3rdmacuobj.so`) and exports two C ABI entry points:
//!
//! ```c
//! // Client vtable (called by S3 clients / relay):
//! const RdmaProviderVtable *s3_rdma_provider_get_vtable(void);
//!
//! // Server vtable (called by S3 gateway):
//! const RdmaServerVtable *s3_rdma_server_provider_get_vtable(void);
//! ```
//!
//! Consolidating both providers into a single `.so` simplifies deployment:
//! only one file needs to be installed alongside the binary instead of two.

use std::collections::HashMap;
use std::ffi::{c_void, CStr};
use std::os::raw::c_char;
use std::sync::{Arc, Mutex, OnceLock};

use cuobj::client::RegisteredBuffer;
use cuobj::{BufferPool, CuObjClient, CuObjClientError, RdmaToken};
use s3_rdma::ffi::{ByteCursor, RdmaCompletionFn, RdmaProviderVtable, RdmaServerInitParams, RdmaServerVtable};
use s3_rdma::provider::{RDMA_BYTES_HEADER, RDMA_REPLY_HEADER, RDMA_TOKEN_HEADER};

// ─────────────────────────────────────────────────────────────────────────────
// ── Client provider
// ─────────────────────────────────────────────────────────────────────────────

/// Keeps an RDMA-registered buffer and its outstanding descriptor token alive
/// for the duration of one RDMA transfer.
struct PendingOp {
    _buf:   RegisteredBuffer<'static>,
    _token: RdmaToken,
}

// SAFETY: both fields wrap opaque handles into the thread-safe cuObj SDK.
unsafe impl Send for PendingOp {}

struct CuObjClientProviderInner {
    client:  CuObjClient,
    pending: Mutex<HashMap<Vec<u8>, PendingOp>>,
}

unsafe impl Send for CuObjClientProviderInner {}
unsafe impl Sync for CuObjClientProviderInner {}

fn get_client() -> Option<&'static CuObjClientProviderInner> {
    static INSTANCE: OnceLock<Option<Box<CuObjClientProviderInner>>> = OnceLock::new();
    INSTANCE.get_or_init(|| {
        match CuObjClient::new() {
            Ok(client) => Some(Box::new(CuObjClientProviderInner {
                client,
                pending: Mutex::new(HashMap::new()),
            })),
            Err(e) => {
                eprintln!("[s3rdmacuobj] CuObjClient::new failed: {e:?}");
                None
            }
        }
    }).as_deref()
}

fn do_prepare_put(
    p:      &'static CuObjClientProviderInner,
    ptr:    *const u8,
    size:   usize,
    offset: usize,
) -> Result<Vec<u8>, CuObjClientError> {
    let mptr = ptr as *mut u8;
    let buf: RegisteredBuffer<'static> = match p.client.register_raii(mptr, size) {
        Ok(b) => b,
        Err(CuObjClientError::AlreadyRegistered { .. }) => {
            {
                let mut pending = p.pending.lock().unwrap();
                pending.retain(|_, op| op._buf.ptr() != mptr);
            }
            p.client.register_raii(mptr, size)?
        }
        Err(e) => return Err(e),
    };
    let token = buf.put_token(offset, size)?;
    let key = token.as_bytes().to_vec();
    p.pending.lock().unwrap().insert(key.clone(), PendingOp { _buf: buf, _token: token });
    Ok(key)
}

fn do_prepare_get(
    p:      &'static CuObjClientProviderInner,
    ptr:    *mut u8,
    size:   usize,
    offset: usize,
) -> Result<Vec<u8>, CuObjClientError> {
    let buf: RegisteredBuffer<'static> = p.client.register_raii(ptr, size)?;
    let token = buf.get_token(offset, size)?;
    let key = token.as_bytes().to_vec();
    p.pending.lock().unwrap().insert(key.clone(), PendingOp { _buf: buf, _token: token });
    Ok(key)
}

static CLIENT_NAME: &[u8] = b"s3rdmacuobj-client\0";

unsafe extern "C" fn cl_init(_: *mut c_void, _: *mut *mut c_void) -> i32 {
    if get_client().is_some() { 0 } else { -1 }
}

unsafe extern "C" fn cl_cleanup(_: *mut c_void) {}

unsafe extern "C" fn cl_is_memory_suitable(
    _: *mut c_void, ptr: *const c_void, _size: usize,
) -> bool {
    CuObjClient::is_rdma_suitable(ptr as *const u8)
}

unsafe extern "C" fn cl_register_memory(_: *mut c_void, _: *mut c_void, _: usize) -> i32 { 0 }
unsafe extern "C" fn cl_deregister_memory(_: *mut c_void, _: *mut c_void) -> i32 { 0 }

unsafe extern "C" fn cl_get_max_transfer_size(
    _: *mut c_void, ptr: *const c_void,
) -> usize {
    let Some(p) = get_client() else { return usize::MAX; };
    let v = p.client.max_transfer_size(ptr as *const u8);
    if v == 0 { usize::MAX } else { v }
}

unsafe extern "C" fn cl_prepare_put_token(
    _: *mut c_void,
    s3_key: *const ByteCursor,
    buffer: *const c_void,
    size:   usize,
    offset: usize,
    out:    *mut ByteCursor,
) -> i32 {
    if s3_key.is_null() || out.is_null() { return -1; }
    let Some(p) = get_client() else { return -1; };
    match do_prepare_put(p, buffer as *const u8, size, offset) {
        Ok(token) => {
            let len = token.len();
            let ptr = Box::into_raw(token.into_boxed_slice()) as *const u8;
            unsafe { *out = ByteCursor { ptr, len }; }
            0
        }
        Err(e) => {
            let key = if s3_key.is_null() { b"?" as &[u8] } else { unsafe { (*s3_key).as_slice() } };
            eprintln!("[s3rdmacuobj] prepare_put_token({}): {e:?}", String::from_utf8_lossy(key));
            -1
        }
    }
}

unsafe extern "C" fn cl_prepare_get_token(
    _: *mut c_void,
    s3_key: *const ByteCursor,
    buffer: *mut c_void,
    size:   usize,
    offset: usize,
    out:    *mut ByteCursor,
) -> i32 {
    if s3_key.is_null() || out.is_null() { return -1; }
    let Some(p) = get_client() else { return -1; };
    match do_prepare_get(p, buffer as *mut u8, size, offset) {
        Ok(token) => {
            let len = token.len();
            let ptr = Box::into_raw(token.into_boxed_slice()) as *const u8;
            unsafe { *out = ByteCursor { ptr, len }; }
            0
        }
        Err(e) => {
            let key = if s3_key.is_null() { b"?" as &[u8] } else { unsafe { (*s3_key).as_slice() } };
            eprintln!("[s3rdmacuobj] prepare_get_token({}): {e:?}", String::from_utf8_lossy(key));
            -1
        }
    }
}

unsafe extern "C" fn cl_process_reply_token(
    _: *mut c_void,
    reply:     *const ByteCursor,
    user_data: *mut c_void,
    callback:  Option<RdmaCompletionFn>,
) -> i32 {
    if reply.is_null() { return -1; }
    let reply_bytes = unsafe { (*reply).as_slice() };
    if let Some(p) = get_client() {
        p.pending.lock().unwrap().remove(reply_bytes);
    }
    if let Some(f) = callback {
        let cursor = ByteCursor::from_slice(reply_bytes);
        unsafe { f(user_data, 0, &cursor); }
    }
    0
}

unsafe extern "C" fn cl_rdma_token_header_name(_: *mut c_void) -> ByteCursor {
    ByteCursor::from_slice(RDMA_TOKEN_HEADER.as_bytes())
}

unsafe extern "C" fn cl_rdma_reply_header_name(_: *mut c_void) -> ByteCursor {
    ByteCursor::from_slice(RDMA_REPLY_HEADER.as_bytes())
}

unsafe extern "C" fn cl_rdma_bytes_header_name(_: *mut c_void) -> ByteCursor {
    ByteCursor::from_slice(RDMA_BYTES_HEADER.as_bytes())
}

static CLIENT_VTABLE: RdmaProviderVtable = RdmaProviderVtable {
    provider_name:    CLIENT_NAME.as_ptr() as *const c_char,
    provider_version: 1,
    init:    Some(cl_init),
    cleanup: Some(cl_cleanup),
    is_memory_suitable:     Some(cl_is_memory_suitable),
    register_memory:        Some(cl_register_memory),
    deregister_memory:      Some(cl_deregister_memory),
    get_max_transfer_size:  Some(cl_get_max_transfer_size),
    prepare_put_token:      Some(cl_prepare_put_token),
    prepare_get_token:      Some(cl_prepare_get_token),
    process_reply_token:    Some(cl_process_reply_token),
    rdma_token_header_name: Some(cl_rdma_token_header_name),
    rdma_reply_header_name: Some(cl_rdma_reply_header_name),
    rdma_bytes_header_name: Some(cl_rdma_bytes_header_name),
};

/// Client ABI entry point.
#[no_mangle]
pub extern "C" fn s3_rdma_provider_get_vtable() -> *const RdmaProviderVtable {
    &CLIENT_VTABLE
}

// ─────────────────────────────────────────────────────────────────────────────
// ── Server provider
// ─────────────────────────────────────────────────────────────────────────────

struct ServerProviderInner {
    pool: Arc<BufferPool>,
}

unsafe impl Send for ServerProviderInner {}
unsafe impl Sync for ServerProviderInner {}

static SERVER_PROVIDER: OnceLock<Option<Box<ServerProviderInner>>> = OnceLock::new();

fn get_server() -> Option<&'static ServerProviderInner> {
    SERVER_PROVIDER.get()?.as_deref()
}

unsafe extern "C" fn sv_init(params: *const RdmaServerInitParams, _ctx: *mut *mut c_void) -> i32 {
    if params.is_null() {
        eprintln!("[s3rdmacuobj] sv_init: null params");
        return -1;
    }
    let p = unsafe { &*params };

    let addr = if p.listen_addr.is_null() {
        eprintln!("[s3rdmacuobj] sv_init: null listen_addr");
        return -1;
    } else {
        match unsafe { CStr::from_ptr(p.listen_addr) }.to_str() {
            Ok(s) => s.to_owned(),
            Err(e) => {
                eprintln!("[s3rdmacuobj] sv_init: invalid listen_addr: {e}");
                return -1;
            }
        }
    };

    let srv = match cuobj::server::CuObjServer::new(&addr, p.listen_port) {
        Ok(s) => Arc::new(s),
        Err(e) => {
            eprintln!("[s3rdmacuobj] CuObjServer::new({addr}:{}) failed: {e:?}", p.listen_port);
            return -1;
        }
    };

    let count  = p.pool_count.max(1);
    let buf_sz = if p.buf_size > 0 { p.buf_size } else { 128 * 1024 * 1024 };

    let pool = match BufferPool::builder()
        .count(count)
        .size(buf_sz)
        .with_server(srv)
        .build()
    {
        Ok(pool) => Arc::new(pool),
        Err(e) => {
            eprintln!("[s3rdmacuobj] BufferPool::build failed: {e}");
            return -1;
        }
    };

    eprintln!("[s3rdmacuobj] server listening on {addr}:{}, pool={count}x{}MiB",
              p.listen_port, buf_sz / (1024 * 1024));

    if SERVER_PROVIDER.set(Some(Box::new(ServerProviderInner { pool }))).is_err() {
        eprintln!("[s3rdmacuobj] sv_init: called more than once — ignoring");
        return -1;
    }
    0
}

unsafe extern "C" fn sv_cleanup(_ctx: *mut c_void) {}

unsafe extern "C" fn sv_handle_put(
    _ctx:            *mut c_void,
    s3_key:          *const ByteCursor,
    token:           *const ByteCursor,
    buf:             *mut u8,
    buf_cap:         usize,
    offset:          usize,
    size:            usize,
    out_transferred: *mut usize,
) -> i32 {
    if s3_key.is_null() || token.is_null() || buf.is_null() || out_transferred.is_null() { return -1; }
    let Some(p) = get_server() else { return -1; };

    let key_bytes   = unsafe { (*s3_key).as_slice() };
    let token_bytes = unsafe { (*token).as_slice() };
    let key_str   = match std::str::from_utf8(key_bytes)   { Ok(s) => s, Err(_) => return -1 };
    let token_str = match std::str::from_utf8(token_bytes) { Ok(s) => s, Err(_) => return -1 };

    let guard = p.pool.acquire();
    let n = if size > 0 {
        if size > guard.len() {
            eprintln!(
                "[s3rdmacuobj] handle_put({key_str}): \
                 server buffer too small (cap={}, requested={size})",
                guard.len()
            );
            return -1;
        }
        size
    } else {
        guard.len()
    };

    match guard.server_handle_put(key_str, offset, n, token_str) {
        Ok(transferred) => {
            let copy_n = transferred.min(buf_cap);
            unsafe {
                std::ptr::copy_nonoverlapping(guard.as_slice().as_ptr(), buf, copy_n);
                *out_transferred = copy_n;
            }
            0
        }
        Err(e) => {
            eprintln!("[s3rdmacuobj] handle_put({key_str}): {e}");
            -1
        }
    }
}

unsafe extern "C" fn sv_handle_get(
    _ctx:        *mut c_void,
    s3_key:      *const ByteCursor,
    data:        *const ByteCursor,
    token:       *const ByteCursor,
    offset:      usize,
    out_written: *mut usize,
) -> i32 {
    if s3_key.is_null() || data.is_null() || token.is_null() || out_written.is_null() {
        return -1;
    }
    let Some(p) = get_server() else { return -1; };

    let key_bytes   = unsafe { (*s3_key).as_slice() };
    let data_bytes  = unsafe { (*data).as_slice() };
    let token_bytes = unsafe { (*token).as_slice() };
    let key_str   = match std::str::from_utf8(key_bytes)   { Ok(s) => s, Err(_) => return -1 };
    let token_str = match std::str::from_utf8(token_bytes) { Ok(s) => s, Err(_) => return -1 };

    let mut guard = p.pool.acquire();
    let n = data_bytes.len().min(guard.len());
    guard.as_mut_slice()[..n].copy_from_slice(&data_bytes[..n]);

    match guard.server_handle_get(key_str, offset, n, token_str) {
        Ok(written) => {
            unsafe { *out_written = written; }
            0
        }
        Err(e) => {
            eprintln!("[s3rdmacuobj] handle_get({key_str}): {e}");
            -1
        }
    }
}

static SERVER_NAME: &[u8] = b"s3rdmacuobj-server\0";

static SERVER_VTABLE: RdmaServerVtable = RdmaServerVtable {
    provider_name:    SERVER_NAME.as_ptr() as *const c_char,
    provider_version: 1,
    init:             Some(sv_init),
    cleanup:          Some(sv_cleanup),
    handle_put:       Some(sv_handle_put),
    handle_get:       Some(sv_handle_get),
};

/// Server ABI entry point.
#[no_mangle]
pub extern "C" fn s3_rdma_server_provider_get_vtable() -> *const RdmaServerVtable {
    &SERVER_VTABLE
}
