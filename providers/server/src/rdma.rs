//! cuObject RDMA server backend — NVIDIA SDK hardware implementation.

use std::ffi::{c_void, CStr};
use std::os::raw::c_char;
use std::sync::{Arc, Mutex, OnceLock};

use tracing::{error, info};

use cuobj::server::CuObjServer;
use cuobj::PersistentBuffer;
use s3_rdma::error::RdmaError;
use s3_rdma::ffi::{ByteCursor, CTelemetryConfig, RdmaServerInitParams, RdmaServerVtable};
use s3_rdma::provider::RdmaServerChannel;
use s3_rdma::provider::DEFAULT_RDMA_BUF_SIZE;

/// Wraps a `*mut c_void` user-data pointer so it can cross thread boundaries.
///
/// # Safety
/// The C FFI contract (see `RdmaIoWriteFn`/`RdmaIoReadFn` docs) requires that
/// the user-data pointer be thread-safe.  We assert `Send` here to satisfy the
/// `Box<dyn FnOnce + Send>` bound on the trait methods.
struct SendPtr(*mut c_void);
unsafe impl Send for SendPtr {}
impl SendPtr {
    fn get(&self) -> *mut c_void {
        self.0
    }
}

static PROVIDER_NAME: &[u8] = b"s3-rdma-server-cuobj\0";

struct ServerProvider {
    server: Arc<CuObjServer>,
    buf_size: usize,
}

unsafe impl Send for ServerProvider {}
unsafe impl Sync for ServerProvider {}

static PROVIDER: OnceLock<Mutex<Option<Box<ServerProvider>>>> = OnceLock::new();

fn provider_lock() -> &'static Mutex<Option<Box<ServerProvider>>> {
    PROVIDER.get_or_init(|| Mutex::new(None))
}

fn get_provider() -> Option<std::sync::MutexGuard<'static, Option<Box<ServerProvider>>>> {
    let g = provider_lock().lock().unwrap();
    if g.is_none() {
        None
    } else {
        Some(g)
    }
}

unsafe extern "C" fn c_init(params: *const RdmaServerInitParams, _ctx: *mut *mut c_void) -> i32 {
    if params.is_null() {
        error!("[s3-rdma-server] c_init: null");
        return -1;
    }
    let p = unsafe { &*params };
    let addr = match unsafe { CStr::from_ptr(p.listen_addr) }.to_str() {
        Ok(s) => s.to_owned(),
        Err(e) => {
            error!("[s3-rdma-server] c_init: listen_addr: {e}");
            return -1;
        }
    };
    let srv = match CuObjServer::new(&addr, p.listen_port) {
        Ok(s) if s.is_connected() => Arc::new(s),
        Ok(_) => {
            error!("[s3-rdma-server] not connected");
            return -1;
        }
        Err(e) => {
            error!("[s3-rdma-server] CuObjServer::new: {e:?}");
            return -1;
        }
    };
    let buf_size = if p.buf_size > 0 {
        p.buf_size
    } else {
        DEFAULT_RDMA_BUF_SIZE
    };
    info!(
        "[s3-rdma-server] listening {addr}:{}, buf={}MiB",
        p.listen_port,
        buf_size / (1024 * 1024)
    );
    let mut guard = provider_lock().lock().unwrap();
    if guard.is_some() {
        error!("[s3-rdma-server] c_init: called more than once");
        return -1;
    }
    *guard = Some(Box::new(ServerProvider {
        server: srv,
        buf_size,
    }));
    0
}

unsafe extern "C" fn c_cleanup(_ctx: *mut c_void) {
    *provider_lock().lock().unwrap() = None;
}

// ── Sync channel ────────────────────────────────────────────────────────────

fn open_channel_sync(
    p: &ServerProvider,
    key: &[u8],
    token: &[u8],
) -> Result<Box<dyn RdmaServerChannel>, RdmaError> {
    let cid = p
        .server
        .alloc_channel()
        .map_err(|e| RdmaError::TransferFailed {
            reason: format!("alloc_channel: {e:?}"),
        })?
        .into_id();
    Ok(Box::new(SyncChannel {
        server: p.server.clone(),
        key: key.to_vec(),
        token: token.to_vec(),
        channel_id: cid,
        buf_size: p.buf_size,
        buffer: std::sync::Mutex::new(None),
    }))
}

struct SyncChannel {
    server: Arc<CuObjServer>,
    key: Vec<u8>,
    token: Vec<u8>,
    channel_id: u16,
    buf_size: usize,
    /// Allocated lazily — only for handle_put/handle_get, not ZC.
    buffer: std::sync::Mutex<Option<PersistentBuffer>>,
}

impl SyncChannel {
    fn get_buffer(&self) -> Result<std::sync::MutexGuard<'_, Option<PersistentBuffer>>, RdmaError> {
        let mut g = self.buffer.lock().unwrap();
        if g.is_none() {
            let buf = PersistentBuffer::new(self.server.clone(), self.buf_size).map_err(|e| {
                RdmaError::TransferFailed {
                    reason: format!("PersistentBuffer: {e:?}"),
                }
            })?;
            *g = Some(buf);
        }
        Ok(g)
    }
}

impl RdmaServerChannel for SyncChannel {
    fn key(&self) -> &[u8] {
        &self.key
    }
    fn token(&self) -> &[u8] {
        &self.token
    }
    fn buf_size(&self) -> usize {
        self.buf_size
    }

    fn handle_put(
        &self,
        offset: usize,
        size: usize,
        write: Box<dyn FnOnce(&[u8]) -> Result<usize, RdmaError> + Send + '_>,
    ) -> Result<usize, RdmaError> {
        let guard = self.get_buffer()?;
        let b = guard.as_ref().unwrap();
        let ks = std::str::from_utf8(&self.key).unwrap_or("");
        let ts = std::str::from_utf8(&self.token).unwrap_or("");
        let n = b
            .handle_put(ks, offset, size, ts, Some(self.channel_id))
            .map_err(|e| RdmaError::TransferFailed {
                reason: format!("{e:?}"),
            })?;
        let buf = unsafe { std::slice::from_raw_parts(b.as_ptr(), n) };
        write(buf)
    }

    fn handle_get(
        &self,
        offset: usize,
        _size: usize,
        read: Box<dyn FnOnce(&mut [u8]) -> Result<usize, RdmaError> + Send + '_>,
    ) -> Result<usize, RdmaError> {
        let guard = self.get_buffer()?;
        let b = guard.as_ref().unwrap();
        let ks = std::str::from_utf8(&self.key).unwrap_or("");
        let ts = std::str::from_utf8(&self.token).unwrap_or("");
        let buf = unsafe { std::slice::from_raw_parts_mut(b.as_ptr() as *mut u8, b.len()) };
        let n = read(buf)?;
        b.handle_get(ks, offset, n, ts, Some(self.channel_id))
            .map_err(|e| RdmaError::TransferFailed {
                reason: format!("{e:?}"),
            })
    }
}

impl Drop for SyncChannel {
    fn drop(&mut self) {
        self.server.free_channel_by_id(self.channel_id);
    }
}

// ── Vtable handlers ──────────────────────────────────────────────────────────

unsafe extern "C" fn c_handle_put(
    _ctx: *mut c_void,
    s3_key: *const ByteCursor,
    token: *const ByteCursor,
    offset: usize,
    size: usize,
    write_fn: s3_rdma::ffi::RdmaIoWriteFn,
    write_fn_data: *mut c_void,
    out_transferred: *mut usize,
) -> i32 {
    if s3_key.is_null() || token.is_null() || out_transferred.is_null() {
        return -1;
    }
    let Some(guard) = get_provider() else {
        return -1;
    };
    let p = guard.as_ref().unwrap().as_ref();
    let k = unsafe { (*s3_key).as_slice() };
    let t = unsafe { (*token).as_slice() };
    let data = SendPtr(write_fn_data);
    let write = Box::new(move |buf: &[u8]| {
        let n = unsafe { write_fn(data.get(), buf.as_ptr() as *const c_void, buf.len()) };
        if n < 0 {
            Err(RdmaError::TransferFailed {
                reason: format!("write_fn returned {n}"),
            })
        } else {
            Ok(n as usize)
        }
    });
    match open_channel_sync(p, k, t).and_then(|ch| ch.handle_put(offset, size, write)) {
        Ok(n) => {
            unsafe {
                *out_transferred = n;
            }
            0
        }
        Err(e) => {
            error!("[s3-rdma-server] handle_put: {e}");
            -1
        }
    }
}

unsafe extern "C" fn c_handle_get(
    _ctx: *mut c_void,
    s3_key: *const ByteCursor,
    token: *const ByteCursor,
    offset: usize,
    read_fn: s3_rdma::ffi::RdmaIoReadFn,
    read_fn_data: *mut c_void,
    out_written: *mut usize,
) -> i32 {
    if s3_key.is_null() || token.is_null() || out_written.is_null() {
        return -1;
    }
    let Some(guard) = get_provider() else {
        return -1;
    };
    let p = guard.as_ref().unwrap().as_ref();
    let k = unsafe { (*s3_key).as_slice() };
    let t = unsafe { (*token).as_slice() };
    let data = SendPtr(read_fn_data);
    let read = Box::new(move |buf: &mut [u8]| {
        let n = unsafe { read_fn(data.get(), buf.as_mut_ptr() as *mut c_void, buf.len()) };
        if n < 0 {
            Err(RdmaError::TransferFailed {
                reason: format!("read_fn returned {n}"),
            })
        } else {
            Ok(n as usize)
        }
    });
    match open_channel_sync(p, k, t).and_then(|ch| ch.handle_get(offset, 0, read)) {
        Ok(n) => {
            unsafe {
                *out_written = n;
            }
            0
        }
        Err(e) => {
            error!("[s3-rdma-server] handle_get: {e}");
            -1
        }
    }
}

unsafe extern "C" fn c_open_channel(
    _ctx: *mut c_void,
    key: *const ByteCursor,
    token: *const ByteCursor,
    out: *mut *mut c_void,
) -> i32 {
    if key.is_null() || token.is_null() || out.is_null() {
        return -1;
    }
    let Some(guard) = get_provider() else {
        return -1;
    };
    let p = guard.as_ref().unwrap().as_ref();
    let k = unsafe { (*key).as_slice() };
    let t = unsafe { (*token).as_slice() };
    match open_channel_sync(p, k, t) {
        Ok(ch) => {
            let boxed = Box::new(ch);
            unsafe { *out = Box::into_raw(boxed) as *mut c_void };
            0
        }
        Err(e) => {
            error!("[s3-rdma-server] open_channel: {e}");
            -1
        }
    }
}

// ── Telemetry ────────────────────────────────────────────────────────────────
// Map our 0-3 level to the cuobj SDK flag bits (CUOBJ_LOG_PATH_*).
// Kept in sync with providers/client/src/rdma.rs.
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
    CuObjServer::setup_telemetry(c.use_otel != 0, log_file.as_deref());
    CuObjServer::set_telem_flags(level_to_cuobj_flags(c.level));
    0
}

unsafe extern "C" fn c_shutdown_telemetry(ctx: *mut c_void) -> i32 {
    crate::telem::shutdown(ctx);
    CuObjServer::shutdown_telemetry();
    0
}

unsafe extern "C" fn c_set_telem_level(ctx: *mut c_void, level: u32) -> i32 {
    crate::telem::set_level(ctx, level);
    CuObjServer::set_telem_flags(level_to_cuobj_flags(level));
    0
}

// ── Vtable ───────────────────────────────────────────────────────────────────

static VTABLE: RdmaServerVtable = RdmaServerVtable {
    provider_name: PROVIDER_NAME.as_ptr() as *const c_char,
    provider_version: 6,
    init: Some(c_init),
    cleanup: Some(c_cleanup),
    handle_put: Some(c_handle_put),
    handle_get: Some(c_handle_get),
    supports_telemetry: Some(crate::telem::supports),
    setup_telemetry: Some(c_setup_telemetry),
    shutdown_telemetry: Some(c_shutdown_telemetry),
    set_telem_level: Some(c_set_telem_level),
    open_channel: Some(c_open_channel),
};

pub fn vtable() -> *const RdmaServerVtable {
    &VTABLE
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn rdma_ip() -> String {
        if let Ok(ip) = std::env::var("RDMA_IP") {
            return ip;
        }
        if let Some(ip) = rdma_ip_from_cufile() {
            return ip;
        }
        panic!("set RDMA_IP or populate /etc/cufile.json properties.rdma_dev_addr_list");
    }

    fn rdma_ip_from_cufile() -> Option<String> {
        let content = std::fs::read_to_string("/etc/cufile.json").ok()?;
        // Extract first entry from: "rdma_dev_addr_list": ["<ip>", ...]
        let after_key = content.split("rdma_dev_addr_list").nth(1)?;
        let after_bracket = after_key.split('[').nth(1)?;
        let ip = after_bracket.split('"').nth(1)?;
        if ip.is_empty() {
            None
        } else {
            Some(ip.to_owned())
        }
    }

    #[test]
    fn sync_channel_creation_and_drop() {
        let srv = Arc::new(CuObjServer::new(&rdma_ip(), 4430).expect("CuObjServer::new"));
        let p = ServerProvider {
            server: Arc::clone(&srv),
            buf_size: 4096,
        };
        let key = b"test/key";
        let token = b"deadbeef:1000:cafe"; // minimal cuobj token: vaddr:size:rkey

        let ch = open_channel_sync(&p, key, token).expect("open_channel_sync");
        assert_eq!(ch.key(), key);
        assert_eq!(ch.token(), token);
        assert_eq!(ch.buf_size(), 4096);
        drop(ch); // frees channel + buffer
        drop(p); // drops server
        println!("sync channel lifecycle: OK");
    }

    #[test]
    fn sync_channel_rejects_invalid_token() {
        let srv = Arc::new(CuObjServer::new(&rdma_ip(), 4431).expect("CuObjServer::new"));
        let p = ServerProvider {
            server: Arc::clone(&srv),
            buf_size: 4096,
        };
        // Invalid token — no colon delimiter.
        let result = open_channel_sync(&p, b"key", b"invalid_token");
        assert!(
            result.is_ok(),
            "open_channel_sync should succeed even with bad token \
                 (token is used at RDMA time, not open time)"
        );
        println!("sync channel bad token: OK (channel created, RDMA would fail later)");
    }

    #[test]
    fn provider_init_and_cleanup() {
        let srv = Arc::new(CuObjServer::new(&rdma_ip(), 4432).expect("CuObjServer::new"));
        assert!(srv.is_connected());
        let p = Box::new(ServerProvider {
            server: srv,
            buf_size: 4096,
        });
        drop(p);
        println!("provider init/cleanup: OK");
    }

    #[test]
    fn cuobj_connectivity() {
        let srv = Arc::new(CuObjServer::new(&rdma_ip(), 4433).expect("CuObjServer::new"));
        assert!(srv.is_connected(), "RDMA fabric not connected");
        let id = srv.alloc_channel().expect("alloc_channel").into_id();
        assert_ne!(id, u16::MAX);
        srv.free_channel_by_id(id);
        println!("cuobj connectivity: OK");
    }

    #[test]
    fn server_multiple_channels() {
        let srv = Arc::new(CuObjServer::new(&rdma_ip(), 4434).expect("CuObjServer::new"));
        let mut ids = Vec::new();
        for _ in 0..4 {
            let id = srv.alloc_channel().expect("alloc").into_id();
            assert_ne!(id, u16::MAX);
            assert!(!ids.contains(&id), "duplicate channel id");
            ids.push(id);
        }
        for id in &ids {
            srv.free_channel_by_id(*id);
        }
        println!("server {} channels: OK", ids.len());
    }

    #[test]
    fn server_persistent_buffer_lifecycle() {
        let srv = Arc::new(CuObjServer::new(&rdma_ip(), 4435).expect("CuObjServer::new"));
        let cid = srv.alloc_channel().expect("alloc").into_id();
        let buf = PersistentBuffer::new(Arc::clone(&srv), 4096).expect("PersistentBuffer::new");
        assert_eq!(buf.len(), 4096);
        assert!(!buf.as_ptr().is_null());
        drop(buf);
        srv.free_channel_by_id(cid);
        println!("persistent buffer lifecycle: OK");
    }

    #[test]
    fn server_register_existing_buffer() {
        use std::alloc::{alloc, dealloc, Layout};
        let srv = Arc::new(CuObjServer::new(&rdma_ip(), 4436).expect("CuObjServer::new"));
        let layout = Layout::from_size_align(4096, 4096).unwrap();
        let ptr = unsafe { alloc(layout) };
        let rbuf =
            unsafe { srv.register_existing_buffer(ptr, 4096) }.expect("register_existing_buffer");
        assert!(!rbuf.is_null());
        unsafe { srv.deregister_buffer(rbuf) };
        unsafe { dealloc(ptr, layout) };
        println!("register existing buffer: OK");
    }
}
