//! cuObject RDMA client backend — NVIDIA SDK hardware implementation.

use std::collections::HashMap;
use std::ffi::c_void;
use std::os::raw::c_char;
use std::sync::{Mutex, OnceLock};

use tracing::error;

use cuobj::client::RegisteredBuffer;
use cuobj::{CuObjClient, CuObjClientError, CuObjRdmaToken};
use s3_rdma::error::RdmaError;
use s3_rdma::ffi::{ByteCursor, CTelemetryConfig, RdmaProviderVtable};
use s3_rdma::provider::{RdmaClientChannel, RdmaPutHandle, RdmaGetHandle, RDMA_REQUIRED_ALIGNMENT};

static PROVIDER_NAME: &[u8] = b"s3-rdma-cuobj\0";

struct PendingOp {
    _buf:   RegisteredBuffer<'static>,
    _token: CuObjRdmaToken,
}

unsafe impl Send for PendingOp {}

struct CuObjSdkClient {
    client:  CuObjClient,
    pending: Mutex<HashMap<Vec<u8>, PendingOp>>,
}

unsafe impl Send for CuObjSdkClient {}
unsafe impl Sync for CuObjSdkClient {}

// ── CuObjClientChannel ──────────────────────────────────────────────────────

struct CuObjClientChannel {
    key:      Vec<u8>,
    ptr:      *mut u8,
    size:     usize,
    provider: &'static CuObjSdkClient,
}

unsafe impl Send for CuObjClientChannel {}
unsafe impl Sync for CuObjClientChannel {}

fn rdma_status_ok(status: u16) -> bool {
    (200..300).contains(&status)
}

impl RdmaClientChannel for CuObjClientChannel {
    fn key(&self) -> &[u8] { &self.key }
    fn ptr(&self) -> *const u8 { self.ptr }
    fn size(&self) -> usize { self.size }
    fn get_max_transfer_size(&self) -> usize {
        self.provider.client.max_transfer_size(self.ptr).unwrap_or(usize::MAX)
    }

    fn prepare_put(&self, offset: usize, size: usize) -> Result<RdmaPutHandle, RdmaError> {
        let token = do_prepare_put(self.provider, self.ptr as *const u8, self.size, offset)
            .map_err(|e| RdmaError::TokenGenerationFailed {
                key: String::from_utf8_lossy(&self.key).into_owned(),
                reason: format!("{e:?}"),
            })?;
        Ok(RdmaPutHandle::new(token, offset, size))
    }

    fn prepare_get(&self, offset: usize, size: usize) -> Result<RdmaGetHandle, RdmaError> {
        let token = do_prepare_get(self.provider, self.ptr, self.size, offset)
            .map_err(|e| RdmaError::TokenGenerationFailed {
                key: String::from_utf8_lossy(&self.key).into_owned(),
                reason: format!("{e:?}"),
            })?;
        Ok(RdmaGetHandle::new(token, offset, size))
    }

    fn complete_put(&self, handle: RdmaPutHandle, rdma_status: u16, _transferred: usize) -> Result<(), RdmaError> {
        self.provider.pending.lock().unwrap().remove(handle.token());
        if rdma_status_ok(rdma_status) { Ok(()) }
        else { Err(RdmaError::TransferFailed(format!("RDMA PUT reply status={rdma_status}"))) }
    }

    fn complete_get(&self, handle: RdmaGetHandle, rdma_status: u16, _transferred: usize) -> Result<(), RdmaError> {
        self.provider.pending.lock().unwrap().remove(handle.token());
        if rdma_status_ok(rdma_status) { Ok(()) }
        else { Err(RdmaError::TransferFailed(format!("RDMA GET reply status={rdma_status}"))) }
    }
}

// ── Provider singleton ─────────────────────────────────────────────────────

fn get_provider() -> Option<&'static CuObjSdkClient> {
    static INSTANCE: OnceLock<Option<Box<CuObjSdkClient>>> = OnceLock::new();
    INSTANCE.get_or_init(|| {
        match CuObjClient::new() {
            Ok(client) => {
                if !client.is_connected() {
                    error!("[s3-rdma] CuObjClient created but not connected to RDMA fabric");
                    return None;
                }
                Some(Box::new(CuObjSdkClient {
                    client,
                    pending: Mutex::new(HashMap::new()),
                }))
            }
            Err(e) => {
                error!("[s3-rdma] CuObjClient::new failed: {e:?}");
                None
            }
        }
    }).as_deref()
}

// ── Helpers ────────────────────────────────────────────────────────────────

fn do_prepare_put(
    provider: &'static CuObjSdkClient, ptr: *const u8, size: usize, offset: usize,
) -> Result<Vec<u8>, CuObjClientError> {
    let mptr = ptr as *mut u8;
    if (mptr as usize) % RDMA_REQUIRED_ALIGNMENT != 0 {
        return Err(CuObjClientError::RegistrationFailed {
            ptr: mptr as usize, size,
            reason: format!("buffer not {RDMA_REQUIRED_ALIGNMENT}-byte aligned: {ptr:p}"),
        });
    }
    let buf: RegisteredBuffer<'static> = match provider.client.register_raii(mptr, size) {
        Ok(buf) => buf,
        Err(CuObjClientError::AlreadyRegistered { .. }) => {
            provider.pending.lock().unwrap().retain(|_, op| op._buf.ptr() != mptr);
            provider.client.register_raii(mptr, size)?
        }
        Err(e) => return Err(e),
    };
    let token = buf.put_token(offset, size)?;
    let key = token.as_bytes().to_vec();
    provider.pending.lock().unwrap().insert(key.clone(), PendingOp { _buf: buf, _token: token });
    Ok(key)
}

fn do_prepare_get(
    provider: &'static CuObjSdkClient, ptr: *mut u8, size: usize, offset: usize,
) -> Result<Vec<u8>, CuObjClientError> {
    if !(ptr as usize).is_multiple_of(RDMA_REQUIRED_ALIGNMENT) {
        return Err(CuObjClientError::RegistrationFailed {
            ptr: ptr as usize, size,
            reason: format!("buffer not {RDMA_REQUIRED_ALIGNMENT}-byte aligned: {ptr:p}"),
        });
    }
    let buf: RegisteredBuffer<'static> = provider.client.register_raii(ptr, size)?;
    let token = buf.get_token(offset, size)?;
    let key = token.as_bytes().to_vec();
    provider.pending.lock().unwrap().insert(key.clone(), PendingOp { _buf: buf, _token: token });
    Ok(key)
}

// ── C vtable handlers ──────────────────────────────────────────────────────

unsafe extern "C" fn c_init(_: *mut c_void, _: *mut *mut c_void) -> i32 {
    if get_provider().is_some() { 0 } else { -1 }
}

unsafe extern "C" fn c_cleanup(_: *mut c_void) {}

unsafe extern "C" fn c_bind_channel(
    _ctx: *mut c_void, size: usize, key: *const ByteCursor, out: *mut *mut c_void,
) -> i32 {
    if key.is_null() || out.is_null() { return -1; }
    let Some(p) = get_provider() else { return -1; };
    let key_slice = unsafe { (*key).as_slice() };

    // Allocate RDMA-suitable memory.
    let layout = std::alloc::Layout::from_size_align(size, RDMA_REQUIRED_ALIGNMENT).unwrap();
    let mptr = unsafe { std::alloc::alloc(layout) };
    if mptr.is_null() { return -1; }

    let channel: Box<dyn RdmaClientChannel> = Box::new(CuObjClientChannel {
        key: key_slice.to_vec(), ptr: mptr, size, provider: p,
    });
    let boxed: Box<Box<dyn RdmaClientChannel>> = Box::new(channel);
    unsafe { *out = Box::into_raw(boxed) as *mut c_void; }
    0
}

// ── Telemetry ──────────────────────────────────────────────────────────────

unsafe extern "C" fn c_supports_telemetry(_: *mut c_void) -> i32 { 1 }

fn level_to_cuobj_flags(level: u32) -> u32 {
    match level {
        0 => 0,
        1 => 0x0004,
        2 => 0x0005,
        _ => 0x0007,
    }
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

pub fn vtable() -> *const RdmaProviderVtable {
    &VTABLE
}
