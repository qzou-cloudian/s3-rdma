//! cuObject RDMA server backend — NVIDIA SDK hardware implementation.
//!
//! Implements `handle_put` and `handle_get` (memcpy path).
//! Zero-copy `handle_put_zc` / `handle_get_zc` are compiled into `hsg-rdma`
//! (the gateway binary) and are not exported here.

use std::ffi::{c_void, CStr};
use std::os::raw::c_char;
use std::sync::{Arc, OnceLock};

use cuobj::server::CuObjServer;
use s3_rdma::ffi::{ByteCursor, RdmaServerInitParams, RdmaServerVtable};

static PROVIDER_NAME: &[u8] = b"s3-rdma-plugin-server\0";

struct ServerProvider {
    server:   Arc<CuObjServer>,
    buf_size: usize,
}

unsafe impl Send for ServerProvider {}
unsafe impl Sync for ServerProvider {}

static PROVIDER: OnceLock<Option<Box<ServerProvider>>> = OnceLock::new();

fn get_provider() -> Option<&'static ServerProvider> {
    PROVIDER.get()?.as_deref()
}

unsafe extern "C" fn c_init(params: *const RdmaServerInitParams, _ctx: *mut *mut c_void) -> i32 {
    if params.is_null() {
        eprintln!("[s3-rdma-plugin-server] c_init: null params");
        return -1;
    }
    let p = unsafe { &*params };

    let addr = if p.listen_addr.is_null() {
        eprintln!("[s3-rdma-plugin-server] c_init: null listen_addr");
        return -1;
    } else {
        match unsafe { CStr::from_ptr(p.listen_addr) }.to_str() {
            Ok(s)  => s.to_owned(),
            Err(e) => {
                eprintln!("[s3-rdma-plugin-server] c_init: invalid listen_addr: {e}");
                return -1;
            }
        }
    };

    let srv = match CuObjServer::new(&addr, p.listen_port) {
        Ok(s)  => Arc::new(s),
        Err(e) => {
            eprintln!("[s3-rdma-plugin-server] CuObjServer::new({addr}:{}) failed: {e:?}", p.listen_port);
            return -1;
        }
    };

    let buf_size = if p.buf_size > 0 { p.buf_size } else { 128 * 1024 * 1024 };
    eprintln!(
        "[s3-rdma-plugin-server] listening {addr}:{port}, buf={mb}MiB",
        port = p.listen_port,
        mb   = buf_size / (1024 * 1024),
    );

    if PROVIDER.set(Some(Box::new(ServerProvider { server: srv, buf_size }))).is_err() {
        eprintln!("[s3-rdma-plugin-server] c_init: called more than once");
        return -1;
    }
    0
}

unsafe extern "C" fn c_cleanup(_ctx: *mut c_void) {
    // INTENTIONAL NO-OP: the CuObjServer singleton lives for the process
    // lifetime.  cuobj_srv_destroy is called implicitly when the process exits.
}

unsafe extern "C" fn c_handle_put(
    _ctx:            *mut c_void,
    s3_key:          *const ByteCursor,
    token:           *const ByteCursor,
    buf:             *mut u8,
    offset:          usize,
    size:            usize,
    out_transferred: *mut usize,
) -> i32 {
    if s3_key.is_null() || token.is_null() || buf.is_null() || out_transferred.is_null() {
        return -1;
    }
    let Some(p) = get_provider() else { return -1 };

    let key_bytes   = unsafe { (*s3_key).as_slice() };
    let token_bytes = unsafe { (*token).as_slice() };
    let key_str   = match std::str::from_utf8(key_bytes)   { Ok(s) => s, Err(_) => return -1 };
    let token_str = match std::str::from_utf8(token_bytes) { Ok(s) => s, Err(_) => return -1 };

    let channel = match p.server.alloc_channel() {
        Ok(ch) => ch,
        Err(e) => {
            eprintln!("[s3-rdma-plugin-server] handle_put({key_str}): alloc_channel: {e}");
            return -1;
        }
    };
    let rdma_buf = match p.server.alloc_buffer(p.buf_size) {
        Ok(b)  => b,
        Err(e) => {
            eprintln!("[s3-rdma-plugin-server] handle_put({key_str}): alloc buffer: {e}");
            return -1;
        }
    };

    let buf_cap = if size > 0 { size } else { rdma_buf.len() };
    match p.server.handle_put(key_str, &rdma_buf, offset as u64, buf_cap, token_str, &channel) {
        Ok(n) => {
            let copy_len = n.min(buf_cap);
            unsafe {
                std::ptr::copy_nonoverlapping(rdma_buf.as_ptr(), buf, copy_len);
                *out_transferred = copy_len;
            }
            0
        }
        Err(e) => {
            eprintln!("[s3-rdma-plugin-server] handle_put({key_str}): {e}");
            -1
        }
    }
}

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
    let Some(p) = get_provider() else { return -1 };

    let key_bytes   = unsafe { (*s3_key).as_slice() };
    let data_bytes  = unsafe { (*data).as_slice() };
    let token_bytes = unsafe { (*token).as_slice() };
    let key_str   = match std::str::from_utf8(key_bytes)   { Ok(s) => s, Err(_) => return -1 };
    let token_str = match std::str::from_utf8(token_bytes) { Ok(s) => s, Err(_) => return -1 };

    let channel = match p.server.alloc_channel() {
        Ok(ch) => ch,
        Err(e) => {
            eprintln!("[s3-rdma-plugin-server] handle_get({key_str}): alloc_channel: {e}");
            return -1;
        }
    };
    let rdma_buf = match p.server.alloc_buffer(p.buf_size) {
        Ok(b)  => b,
        Err(e) => {
            eprintln!("[s3-rdma-plugin-server] handle_get({key_str}): alloc buffer: {e}");
            return -1;
        }
    };

    let n = data_bytes.len().min(rdma_buf.len());
    if data_bytes.len() > rdma_buf.len() {
        eprintln!(
            "[s3-rdma-plugin-server] handle_get({key_str}): data ({}) > rdma_buf ({}), truncating",
            data_bytes.len(), rdma_buf.len()
        );
    }
    unsafe {
        std::ptr::copy_nonoverlapping(data_bytes.as_ptr(), rdma_buf.as_ptr() as *mut u8, n);
    }

    match p.server.handle_get(key_str, &rdma_buf, offset as u64, n, token_str, &channel) {
        Ok(written) => {
            unsafe { *out_written = written; }
            0
        }
        Err(e) => {
            eprintln!("[s3-rdma-plugin-server] handle_get({key_str}): {e}");
            -1
        }
    }
}

static VTABLE: RdmaServerVtable = RdmaServerVtable {
    provider_name:    PROVIDER_NAME.as_ptr() as *const c_char,
    provider_version: 3,
    init:             Some(c_init),
    cleanup:          Some(c_cleanup),
    handle_put:       Some(c_handle_put),
    handle_get:       Some(c_handle_get),
    handle_put_zc:    None, // zero-copy path lives in hsg-rdma (gateway binary)
    handle_get_zc:    None, // zero-copy path lives in hsg-rdma (gateway binary)
};

pub fn vtable() -> *const RdmaServerVtable {
    &VTABLE
}
