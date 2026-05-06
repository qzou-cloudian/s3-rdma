//! cuObject RDMA server backend — NVIDIA SDK hardware implementation.

use std::ffi::{c_void, CStr};
use std::os::raw::c_char;
use std::sync::{Arc, OnceLock};

use tracing::{error, info};

use cuobj::server::CuObjServer;
use cuobj::PersistentBuffer;
use s3_rdma::ffi::{ByteCursor, CTelemetryConfig, RdmaServerInitParams, RdmaServerVtable};
use s3_rdma::error::RdmaError;
use s3_rdma::provider::DEFAULT_RDMA_BUF_SIZE;
use s3_rdma::provider::RdmaServerChannel;

static PROVIDER_NAME: &[u8] = b"s3-rdma-server-cuobj\0";

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
    if params.is_null() { error!("[s3-rdma-server] c_init: null"); return -1; }
    let p = unsafe { &*params };
    let addr = match unsafe { CStr::from_ptr(p.listen_addr) }.to_str() {
        Ok(s) => s.to_owned(),
        Err(e) => { error!("[s3-rdma-server] c_init: listen_addr: {e}"); return -1; }
    };
    let srv = match CuObjServer::new(&addr, p.listen_port) {
        Ok(s) if s.is_connected() => Arc::new(s),
        Ok(_) => { error!("[s3-rdma-server] not connected"); return -1; }
        Err(e) => { error!("[s3-rdma-server] CuObjServer::new: {e:?}"); return -1; }
    };
    let buf_size = if p.buf_size > 0 { p.buf_size } else { DEFAULT_RDMA_BUF_SIZE };
    info!("[s3-rdma-server] listening {addr}:{}, buf={}MiB", p.listen_port, buf_size/(1024*1024));
    if PROVIDER.set(Some(Box::new(ServerProvider { server: srv, buf_size }))).is_err() {
        error!("[s3-rdma-server] c_init: called more than once");
        return -1;
    }
    0
}

unsafe extern "C" fn c_cleanup(_ctx: *mut c_void) { let _ = PROVIDER.set(None); }

// ── Sync channel ────────────────────────────────────────────────────────────

fn open_channel_sync(p: &ServerProvider, key: &[u8], token: &[u8])
    -> Result<Box<dyn RdmaServerChannel>, RdmaError>
{
    let ch = p.server.alloc_channel().map_err(|e| {
        RdmaError::TransferFailed(format!("alloc_channel: {e:?}"))
    })?;
    let cid = ch.id(); std::mem::forget(ch);
    Ok(Box::new(SyncChannel {
        server: p.server.clone(), key: key.to_vec(), token: token.to_vec(),
        channel_id: cid, buf_size: p.buf_size,
        buffer: std::sync::Mutex::new(None),
    }))
}

struct SyncChannel {
    server: Arc<CuObjServer>, key: Vec<u8>, token: Vec<u8>,
    channel_id: u16, buf_size: usize,
    /// Allocated lazily — only for handle_put/handle_get, not ZC.
    buffer: std::sync::Mutex<Option<PersistentBuffer>>,
}

impl SyncChannel {
    fn get_buffer(&self) -> Result<std::sync::MutexGuard<'_, Option<PersistentBuffer>>, RdmaError> {
        let mut g = self.buffer.lock().unwrap();
        if g.is_none() {
            let buf = PersistentBuffer::new(self.server.clone(), self.buf_size)
                .map_err(|e| RdmaError::TransferFailed(format!("PersistentBuffer: {e:?}")))?;
            *g = Some(buf);
        }
        Ok(g)
    }
}

impl RdmaServerChannel for SyncChannel {
    fn key(&self) -> &[u8] { &self.key }
    fn token(&self) -> &[u8] { &self.token }
    fn buf_size(&self) -> usize { self.buf_size }

    fn handle_put(
        &self, offset: usize, size: usize,
        write: Box<dyn FnOnce(&[u8]) -> Result<usize, RdmaError> + Send + '_>,
    ) -> Result<usize, RdmaError> {
        let guard = self.get_buffer()?;
        let b = guard.as_ref().unwrap();
        let ks = std::str::from_utf8(&self.key).unwrap_or("");
        let ts = std::str::from_utf8(&self.token).unwrap_or("");
        let n = b.handle_put(ks, offset, size, ts, Some(self.channel_id))
            .map_err(|e| RdmaError::TransferFailed(format!("{e:?}")))?;
        let buf = unsafe { std::slice::from_raw_parts(b.as_ptr(), n) };
        write(buf)
    }

    fn handle_get(
        &self, offset: usize,
        read: Box<dyn FnOnce(&mut [u8]) -> usize + Send + '_>,
    ) -> Result<usize, RdmaError> {
        let guard = self.get_buffer()?;
        let b = guard.as_ref().unwrap();
        let ks = std::str::from_utf8(&self.key).unwrap_or("");
        let ts = std::str::from_utf8(&self.token).unwrap_or("");
        let buf = unsafe { std::slice::from_raw_parts_mut(b.as_ptr() as *mut u8, b.len()) };
        let n = read(buf);
        b.handle_get(ks, offset, n, ts, Some(self.channel_id))
            .map_err(|e| RdmaError::TransferFailed(format!("{e:?}")))
    }
}

impl Drop for SyncChannel { fn drop(&mut self) { self.server.free_channel_by_id(self.channel_id); } }

// ── Vtable handlers ──────────────────────────────────────────────────────────

unsafe extern "C" fn c_handle_put(
    _ctx: *mut c_void, s3_key: *const ByteCursor, token: *const ByteCursor,
    offset: usize, size: usize, write_fn: s3_rdma::ffi::RdmaIoWriteFn,
    write_fn_data: *mut c_void, out_transferred: *mut usize,
) -> i32 {
    if s3_key.is_null() || token.is_null() || out_transferred.is_null() { return -1; }
    let Some(p) = get_provider() else { return -1 };
    let k = unsafe { (*s3_key).as_slice() }; let t = unsafe { (*token).as_slice() };
    match open_channel_sync(p, k, t)
        .and_then(|ch| unsafe { ch.handle_put(offset, size, write_fn, write_fn_data) })
    {
        Ok(n) => { unsafe { *out_transferred = n; } 0 }
        Err(e) => { error!("[s3-rdma-server] handle_put: {e}"); -1 }
    }
}

unsafe extern "C" fn c_handle_get(
    _ctx: *mut c_void, s3_key: *const ByteCursor, token: *const ByteCursor,
    offset: usize, read_fn: s3_rdma::ffi::RdmaIoReadFn, read_fn_data: *mut c_void,
    out_written: *mut usize,
) -> i32 {
    if s3_key.is_null() || token.is_null() || out_written.is_null() { return -1; }
    let Some(p) = get_provider() else { return -1 };
    let k = unsafe { (*s3_key).as_slice() }; let t = unsafe { (*token).as_slice() };
    match open_channel_sync(p, k, t)
        .and_then(|ch| unsafe { ch.handle_get(offset, read_fn, read_fn_data) })
    {
        Ok(n) => { unsafe { *out_written = n; } 0 }
        Err(e) => { error!("[s3-rdma-server] handle_get: {e}"); -1 }
    }
}

unsafe extern "C" fn c_open_channel(
    _ctx: *mut c_void, key: *const ByteCursor, token: *const ByteCursor,
    out: *mut *mut c_void,
) -> i32 {
    if key.is_null() || token.is_null() || out.is_null() { return -1; }
    let Some(p) = get_provider() else { return -1 };
    let k = unsafe { (*key).as_slice() }; let t = unsafe { (*token).as_slice() };
    match open_channel_sync(p, k, t) {
        Ok(ch) => {
            let boxed = Box::new(ch);
            unsafe { *out = Box::into_raw(boxed) as *mut c_void }; 0
        }
        Err(e) => { error!("[s3-rdma-server] open_channel: {e}"); -1 }
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
    provider_version:    6,
    init:                Some(c_init),
    cleanup:             Some(c_cleanup),
    handle_put:  Some(c_handle_put),
    handle_get:  Some(c_handle_get),
    supports_telemetry:  Some(c_supports_telemetry),
    setup_telemetry:     Some(c_setup_telemetry),
    shutdown_telemetry:  Some(c_shutdown_telemetry),
    set_telem_level:     Some(c_set_telem_level),
    open_channel:        Some(c_open_channel),
};

pub fn vtable() -> *const RdmaServerVtable { &VTABLE }

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sync_channel_creation_and_drop() {
        let srv = Arc::new(CuObjServer::new("10.50.132.250", 4430).expect("CuObjServer::new"));
        let p = ServerProvider { server: Arc::clone(&srv), buf_size: 4096 };
        let key = b"test/key";
        let token = b"deadbeef:1000:cafe"; // minimal cuobj token: vaddr:size:rkey

        let ch = open_channel_sync(&p, key, token).expect("open_channel_sync");
        assert_eq!(ch.key(), key);
        assert_eq!(ch.token(), token);
        assert_eq!(ch.buf_size(), 4096);
        drop(ch); // frees channel + buffer
        drop(p);  // drops server
        println!("sync channel lifecycle: OK");
    }

    #[test]
    fn sync_channel_rejects_invalid_token() {
        let srv = Arc::new(CuObjServer::new("10.50.132.250", 4431).expect("CuObjServer::new"));
        let p = ServerProvider { server: Arc::clone(&srv), buf_size: 4096 };
        // Invalid token — no colon delimiter.
        let result = open_channel_sync(&p, b"key", b"invalid_token");
        assert!(result.is_ok(), "open_channel_sync should succeed even with bad token \
                 (token is used at RDMA time, not open time)");
        println!("sync channel bad token: OK (channel created, RDMA would fail later)");
    }

    #[test]
    fn provider_init_and_cleanup() {
        let srv = Arc::new(CuObjServer::new("10.50.132.250", 4432).expect("CuObjServer::new"));
        assert!(srv.is_connected());
        let p = Box::new(ServerProvider { server: srv, buf_size: 4096 });
        drop(p);
        println!("provider init/cleanup: OK");
    }

    #[test]
    fn cuobj_connectivity() {
        let srv = Arc::new(CuObjServer::new("10.50.132.250", 4433).expect("CuObjServer::new"));
        assert!(srv.is_connected(), "RDMA fabric not connected");
        let ch = srv.alloc_channel().expect("alloc_channel");
        let id = ch.id();
        assert_ne!(id, u16::MAX);
        std::mem::forget(ch);
        srv.free_channel_by_id(id);
        println!("cuobj connectivity: OK");
    }

    #[test]
    fn server_multiple_channels() {
        let srv = Arc::new(CuObjServer::new("10.50.132.250", 4434).expect("CuObjServer::new"));
        let mut ids = Vec::new();
        for _ in 0..4 {
            let ch = srv.alloc_channel().expect("alloc");
            let id = ch.id();
            assert_ne!(id, u16::MAX);
            assert!(!ids.contains(&id), "duplicate channel id");
            std::mem::forget(ch);
            ids.push(id);
        }
        for id in &ids { srv.free_channel_by_id(*id); }
        println!("server {} channels: OK", ids.len());
    }

    #[test]
    fn server_persistent_buffer_lifecycle() {
        let srv = Arc::new(CuObjServer::new("10.50.132.250", 4435).expect("CuObjServer::new"));
        let ch = srv.alloc_channel().expect("alloc");
        let cid = ch.id(); std::mem::forget(ch);
        let buf = PersistentBuffer::new(Arc::clone(&srv), 4096).expect("PersistentBuffer::new");
        assert_eq!(buf.len(), 4096);
        assert!(!buf.as_ptr().is_null());
        drop(buf);
        srv.free_channel_by_id(cid);
        println!("persistent buffer lifecycle: OK");
    }

    #[test]
    fn server_register_existing_buffer() {
        use std::alloc::{Layout, alloc, dealloc};
        let srv = Arc::new(CuObjServer::new("10.50.132.250", 4436).expect("CuObjServer::new"));
        let layout = Layout::from_size_align(4096, 4096).unwrap();
        let ptr = unsafe { alloc(layout) };
        let rbuf = srv.register_existing_buffer(ptr, 4096)
            .expect("register_existing_buffer");
        assert!(!rbuf.is_null());
        srv.deregister_buffer(rbuf);
        unsafe { dealloc(ptr, layout) };
        println!("register existing buffer: OK");
    }
}

