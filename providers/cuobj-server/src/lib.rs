//! cuObject RDMA server provider plugin.
//!
//! Builds as a `cdylib` (`libs3rdmacuobjserver.so`) and exports:
//! ```c
//! const RdmaServerVtable *s3_rdma_server_provider_get_vtable(void);
//! ```
//!
//! At init time the plugin creates a `CuObjServer` on the given listen
//! address/port and allocates an `cuobj::BufferPool` backed by
//! `PersistentBuffer` slots registered with that server.
//!
//! `handle_put` → server performs RDMA_READ from client's buffer.
//! `handle_get` → server performs RDMA_WRITE into client's buffer.

use std::ffi::{c_void, CStr};
use std::sync::{Arc, OnceLock};

use cuobj::BufferPool;
use s3_rdma::ffi::{ByteCursor, RdmaServerInitParams, RdmaServerVtable};

// ─────────────────────────────────────────────────────────────────────────────
// Singleton provider
// ─────────────────────────────────────────────────────────────────────────────

struct ServerProvider {
    pool: Arc<BufferPool>,
}

unsafe impl Send for ServerProvider {}
unsafe impl Sync for ServerProvider {}

static PROVIDER: OnceLock<Option<Box<ServerProvider>>> = OnceLock::new();

fn get_provider() -> Option<&'static ServerProvider> {
    PROVIDER.get()?.as_deref()
}

// ─────────────────────────────────────────────────────────────────────────────
// C-ABI shims
// ─────────────────────────────────────────────────────────────────────────────

/// Initialise the singleton `CuObjServer` and allocate the `BufferPool`.
/// Returns 0 on success, -1 on error or if called more than once.
unsafe extern "C" fn c_init(params: *const RdmaServerInitParams, _ctx: *mut *mut c_void) -> i32 {
    if params.is_null() {
        eprintln!("[s3-rdma-cuobj-server] c_init: null params");
        return -1;
    }
    let p = unsafe { &*params };

    let addr = if p.listen_addr.is_null() {
        eprintln!("[s3-rdma-cuobj-server] c_init: null listen_addr");
        return -1;
    } else {
        match unsafe { CStr::from_ptr(p.listen_addr) }.to_str() {
            Ok(s) => s.to_owned(),
            Err(e) => {
                eprintln!("[s3-rdma-cuobj-server] c_init: invalid listen_addr: {e}");
                return -1;
            }
        }
    };

    let srv = match cuobj::server::CuObjServer::new(&addr, p.listen_port) {
        Ok(s) => Arc::new(s),
        Err(e) => {
            eprintln!("[s3-rdma-cuobj-server] CuObjServer::new({addr}:{}) failed: {e:?}", p.listen_port);
            return -1;
        }
    };

    let count   = p.pool_count.max(1);
    let buf_sz  = if p.buf_size > 0 { p.buf_size } else { 128 * 1024 * 1024 };

    let pool = match BufferPool::builder()
        .count(count)
        .size(buf_sz)
        .with_server(srv)
        .build()
    {
        Ok(pool) => Arc::new(pool),
        Err(e) => {
            eprintln!("[s3-rdma-cuobj-server] BufferPool::build failed: {e}");
            return -1;
        }
    };

    eprintln!("[s3-rdma-cuobj-server] listening on {addr}:{}, pool={count}x{}MiB",
              p.listen_port, buf_sz / (1024 * 1024));

    if PROVIDER.set(Some(Box::new(ServerProvider { pool }))).is_err() {
        eprintln!("[s3-rdma-cuobj-server] c_init: called more than once — ignoring");
        return -1;
    }
    0
}

unsafe extern "C" fn c_cleanup(_ctx: *mut c_void) {
    // PROVIDER is a OnceLock; we cannot clear it, but the process is
    // shutting down so the pool will be dropped via normal destructors.
}

/// RDMA PUT: perform a cuObject `RDMA_READ` from the client's registered buffer
/// into a pool slot, then copy the transferred bytes into the caller-provided buffer.
///
/// `buf[..buf_cap]` is supplied by the caller (hsg's pre-allocated pool slot).
/// `*out_transferred` is set to the number of bytes copied on success.
/// Returns 0 on success, -1 on error.
unsafe extern "C" fn c_handle_put(
    _ctx:            *mut c_void,
    s3_key:          *const ByteCursor,
    token:           *const ByteCursor,
    buf:             *mut u8,
    offset:          usize,
    size:            usize,
    out_transferred: *mut usize,
) -> i32 {
    if s3_key.is_null() || token.is_null() || buf.is_null() || out_transferred.is_null() { return -1; }
    let Some(p) = get_provider() else { return -1; };

    let key_bytes   = unsafe { (*s3_key).as_slice() };
    let token_bytes = unsafe { (*token).as_slice() };
    let key_str   = match std::str::from_utf8(key_bytes)   { Ok(s) => s, Err(_) => return -1 };
    let token_str = match std::str::from_utf8(token_bytes) { Ok(s) => s, Err(_) => return -1 };

    let guard = p.pool.acquire();
    let n = if size > 0 {
        if size > guard.len() {
            eprintln!(
                "[s3-rdma-cuobj-server] handle_put({key_str}): \
                 server buffer too small (cap={}, requested={size}) — caller should retry via HTTP",
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
            // SAFETY: caller guarantees buf has capacity >= guard.len() >= transferred
            // (both pools are configured to the same rdma_buffer_size).
            unsafe {
                std::ptr::copy_nonoverlapping(guard.as_slice().as_ptr(), buf, transferred);
                *out_transferred = transferred;
            }
            0
        }
        Err(e) => {
            eprintln!("[s3-rdma-cuobj-server] handle_put({key_str}): {e}");
            -1
        }
    }
}

/// RDMA GET: perform a cuObject `RDMA_WRITE` of `data` into the client's
/// registered buffer via the pool slot identified by `token`.
///
/// `*out_written` is set to the number of bytes actually written.
/// Returns 0 on success, -1 on error.
unsafe extern "C" fn c_handle_get(
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
    let Some(p) = get_provider() else { return -1; };

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
            eprintln!("[s3-rdma-cuobj-server] handle_get({key_str}): {e}");
            -1
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Static vtable + entry point
// ─────────────────────────────────────────────────────────────────────────────

static PROVIDER_NAME: &[u8] = b"s3-rdma-cuobj-server\0";

static VTABLE: RdmaServerVtable = RdmaServerVtable {
    provider_name:    PROVIDER_NAME.as_ptr() as *const std::os::raw::c_char,
    provider_version: 1,
    init:             Some(c_init),
    cleanup:          Some(c_cleanup),
    handle_put:       Some(c_handle_put),
    handle_get:       Some(c_handle_get),
};

/// C ABI entry point: returns the statically allocated RDMA server vtable.
#[no_mangle]
pub extern "C" fn s3_rdma_server_provider_get_vtable() -> *const RdmaServerVtable {
    &VTABLE
}
