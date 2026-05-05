//! cuObject RDMA client backend — NVIDIA SDK hardware implementation.

use std::collections::HashMap;
use std::ffi::c_void;
use std::os::raw::c_char;
use std::sync::{Mutex, OnceLock};

use tracing::error;

use cuobj::client::RegisteredBuffer;
use cuobj::{CuObjClient, CuObjClientError, CuObjRdmaToken};
use s3_rdma::ffi::{ByteCursor, CTelemetryConfig, RdmaCompletionFn, RdmaProviderVtable};
use s3_rdma::provider::{RDMA_BYTES_HEADER, RDMA_REPLY_HEADER, RDMA_REQUIRED_ALIGNMENT, RDMA_TOKEN_HEADER};

static PROVIDER_NAME: &[u8] = b"s3-rdma\0";

/// Keeps an RDMA-registered buffer and its outstanding descriptor token alive
/// for the duration of one RDMA transfer.
///
/// Both fields are dropped together when [`c_process_reply_token`] confirms
/// that the server has completed its RDMA READ or WRITE.
struct PendingOp {
    _buf:   RegisteredBuffer<'static>,
    _token: CuObjRdmaToken,
}

// SAFETY: both fields wrap opaque handles into the thread-safe cuObj SDK.
unsafe impl Send for PendingOp {}

struct CuObjSdkClient {
    client:  CuObjClient,
    /// Pending RDMA ops keyed by their RDMA descriptor bytes.
    pending: Mutex<HashMap<Vec<u8>, PendingOp>>,
}

// SAFETY: CuObjClient is Send+Sync; Mutex<HashMap> provides interior
// mutability across threads.
unsafe impl Send for CuObjSdkClient {}
unsafe impl Sync for CuObjSdkClient {}

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

/// Register `ptr[0..size]` and obtain a PUT RDMA descriptor.
///
/// Calling through a `&'static CuObjSdkClient` gives
/// `RegisteredBuffer<'static>`, so no lifetime transmute is required.
fn do_prepare_put(
    provider: &'static CuObjSdkClient,
    ptr:      *const u8,
    size:     usize,
    offset:   usize,
) -> Result<Vec<u8>, CuObjClientError> {
    // PUT: server will RDMA-READ from the client buffer; the cast from
    // *const to *mut is safe — the buffer is writable; we just expose it
    // as const to signal that the provider won't modify it.
    let mptr = ptr as *mut u8;
    if (mptr as usize) % RDMA_REQUIRED_ALIGNMENT != 0 {
        return Err(CuObjClientError::RegistrationFailed {
            ptr: mptr as usize,
            size,
            reason: format!(
                "buffer not {RDMA_REQUIRED_ALIGNMENT}-byte aligned: {ptr:p}"
            ),
        });
    }
    let buf: RegisteredBuffer<'static> = match provider.client.register_raii(mptr, size) {
        Ok(buf) => buf,
        Err(CuObjClientError::AlreadyRegistered { .. }) => {
            // The buffer is still registered from a previous multipart part
            // whose RDMA transfer was not confirmed via x-amz-rdma-reply
            // (server ignored or didn't support RDMA).  The HTTP response for
            // that part has been received, so any RDMA transfer has completed.
            // Drop the stale PendingOp(s) so the RegisteredBuffer inside them
            // deregisters the MR and clears the entry in client.registered.
            {
                let mut pending = provider.pending.lock().unwrap();
                pending.retain(|_, op| op._buf.ptr() != mptr);
                // pending lock dropped here — RegisteredBuffer::drop (which
                // calls client.deregister) runs after the lock is released to
                // avoid a potential re-entrant lock.
            }
            // Now the MR is deregistered; register fresh.
            provider.client.register_raii(mptr, size)?
        }
        Err(e) => return Err(e),
    };
    let token = buf.put_token(offset, size)?;
    let key = token.as_bytes().to_vec();
    provider.pending.lock().unwrap().insert(key.clone(), PendingOp { _buf: buf, _token: token });
    Ok(key)
}

/// Register `ptr[0..size]` and obtain a GET RDMA descriptor.
fn do_prepare_get(
    provider: &'static CuObjSdkClient,
    ptr:      *mut u8,
    size:     usize,
    offset:   usize,
) -> Result<Vec<u8>, CuObjClientError> {
    if !(ptr as usize).is_multiple_of(RDMA_REQUIRED_ALIGNMENT) {
        return Err(CuObjClientError::RegistrationFailed {
            ptr: ptr as usize,
            size,
            reason: format!(
                "buffer not {RDMA_REQUIRED_ALIGNMENT}-byte aligned: {ptr:p}"
            ),
        });
    }
    let buf: RegisteredBuffer<'static> =
        provider.client.register_raii(ptr, size)?;
    let token = buf.get_token(offset, size)?;
    let key = token.as_bytes().to_vec();
    provider.pending.lock().unwrap().insert(key.clone(), PendingOp { _buf: buf, _token: token });
    Ok(key)
}

unsafe extern "C" fn c_init(_: *mut c_void, _: *mut *mut c_void) -> i32 {
    if get_provider().is_some() { 0 } else { -1 }
}

unsafe extern "C" fn c_cleanup(_: *mut c_void) {}

unsafe extern "C" fn c_is_memory_suitable(
    _: *mut c_void, ptr: *const c_void, size: usize,
) -> bool {
    if ptr.is_null() || size == 0 { return false; }
    if !(ptr as usize).is_multiple_of(RDMA_REQUIRED_ALIGNMENT) { return false; }
    CuObjClient::is_rdma_suitable(ptr as *const u8)
}

/// Registration is deferred to `prepare_put_token` / `prepare_get_token`.
unsafe extern "C" fn c_register_memory(
    _: *mut c_void, _ptr: *mut c_void, _size: usize,
) -> i32 { 0 }

/// Deregistration is deferred to `process_reply_token`.
unsafe extern "C" fn c_deregister_memory(
    _: *mut c_void, _ptr: *mut c_void,
) -> i32 { 0 }

unsafe extern "C" fn c_get_max_transfer_size(
    _: *mut c_void, ptr: *const c_void,
) -> usize {
    let Some(p) = get_provider() else { return usize::MAX; };
    match p.client.max_transfer_size(ptr as *const u8) {
        None | Some(0) => usize::MAX,
        Some(n) => n,
    }
}

unsafe extern "C" fn c_prepare_put_token(
    _: *mut c_void,
    s3_key: *const ByteCursor,
    buffer: *const c_void,
    size:   usize,
    offset: usize,
    out:    *mut ByteCursor,
) -> i32 {
    if s3_key.is_null() || out.is_null() { return -1; }
    let Some(p) = get_provider() else { return -1; };
    match do_prepare_put(p, buffer as *const u8, size, offset) {
        Ok(token) => {
            let len = token.len();
            let ptr = Box::into_raw(token.into_boxed_slice()) as *const u8;
            unsafe { *out = ByteCursor { ptr, len }; }
            0
        }
        Err(e) => {
            let key = unsafe { (*s3_key).as_slice() };
            error!("[s3-rdma] prepare_put_token({}): {e:?}",
                      String::from_utf8_lossy(key));
            -1
        }
    }
}

unsafe extern "C" fn c_prepare_get_token(
    _: *mut c_void,
    s3_key: *const ByteCursor,
    buffer: *mut c_void,
    size:   usize,
    offset: usize,
    out:    *mut ByteCursor,
) -> i32 {
    if s3_key.is_null() || out.is_null() { return -1; }
    let Some(p) = get_provider() else { return -1; };
    match do_prepare_get(p, buffer as *mut u8, size, offset) {
        Ok(token) => {
            let len = token.len();
            let ptr = Box::into_raw(token.into_boxed_slice()) as *const u8;
            unsafe { *out = ByteCursor { ptr, len }; }
            0
        }
        Err(e) => {
            let key = unsafe { (*s3_key).as_slice() };
            error!("[s3-rdma] prepare_get_token({}): {e:?}",
                      String::from_utf8_lossy(key));
            -1
        }
    }
}

/// Release the pending RDMA op (MR + descriptor) and fire the callback.
///
/// `reply` is the token echoed back by the server; it is used as the key to
/// find and drop the corresponding [`PendingOp`].  If no matching op is found
/// (e.g., the server sends a different reply), the callback is still fired
/// with success so the caller is not blocked.
unsafe extern "C" fn c_process_reply_token(
    _: *mut c_void,
    reply:     *const ByteCursor,
    user_data: *mut c_void,
    callback:  Option<RdmaCompletionFn>,
) -> i32 {
    if reply.is_null() { return -1; }
    let reply_bytes = unsafe { (*reply).as_slice() };

    if let Some(p) = get_provider() {
        p.pending.lock().unwrap().remove(reply_bytes);
    }

    if let Some(f) = callback {
        let cursor = ByteCursor::from_slice(reply_bytes);
        unsafe { f(user_data, 0, &cursor); }
    }
    0
}

unsafe extern "C" fn c_rdma_token_header_name(_: *mut c_void) -> ByteCursor {
    ByteCursor::from_slice(RDMA_TOKEN_HEADER.as_bytes())
}

unsafe extern "C" fn c_rdma_reply_header_name(_: *mut c_void) -> ByteCursor {
    ByteCursor::from_slice(RDMA_REPLY_HEADER.as_bytes())
}

unsafe extern "C" fn c_rdma_bytes_header_name(_: *mut c_void) -> ByteCursor {
    ByteCursor::from_slice(RDMA_BYTES_HEADER.as_bytes())
}

// ── Telemetry callbacks ──────────────────────────────────────────────────────

unsafe extern "C" fn c_supports_telemetry(_: *mut c_void) -> i32 { 1 }

fn level_to_cuobj_flags(level: u32) -> u32 {
    match level {
        0 => 0,
        1 => 0x0004,                       // CUOBJ_LOG_PATH_ERROR
        2 => 0x0005,                       // CUOBJ_LOG_PATH_ERROR | CUOBJ_LOG_PATH_INFO
        _ => 0x0007,                       // all three
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
    provider_name:          PROVIDER_NAME.as_ptr() as *const c_char,
    provider_version:       1,
    init:                   Some(c_init),
    cleanup:                Some(c_cleanup),
    is_memory_suitable:     Some(c_is_memory_suitable),
    register_memory:        Some(c_register_memory),
    deregister_memory:      Some(c_deregister_memory),
    get_max_transfer_size:  Some(c_get_max_transfer_size),
    prepare_put_token:      Some(c_prepare_put_token),
    prepare_get_token:      Some(c_prepare_get_token),
    process_reply_token:    Some(c_process_reply_token),
    rdma_token_header_name: Some(c_rdma_token_header_name),
    rdma_reply_header_name: Some(c_rdma_reply_header_name),
    rdma_bytes_header_name: Some(c_rdma_bytes_header_name),
    supports_telemetry:    Some(c_supports_telemetry),
    setup_telemetry:       Some(c_setup_telemetry),
    shutdown_telemetry:    Some(c_shutdown_telemetry),
    set_telem_level:       Some(c_set_telem_level),
};

pub fn vtable() -> *const RdmaProviderVtable {
    &VTABLE
}
