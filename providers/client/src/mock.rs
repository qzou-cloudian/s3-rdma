//! Mock RDMA client backend — shared-memory simulation, no hardware required.
//!
//! Shims delegate to [`s3_rdma::MockRdmaProvider`] via the [`RdmaClientProvider`]
//! trait.

use std::ffi::c_void;
use std::os::raw::c_char;
use std::sync::OnceLock;

use tracing::error;

use s3_rdma::ffi::{ByteCursor, RdmaCompletionFn, RdmaProviderVtable};
use s3_rdma::{MockRdmaProvider, RdmaClientProvider};

static PROVIDER_NAME: &[u8] = b"s3-rdma-mock\0";

fn get_provider() -> &'static dyn RdmaClientProvider {
    static INSTANCE: OnceLock<Box<dyn RdmaClientProvider + Send + Sync>> = OnceLock::new();
    INSTANCE.get_or_init(|| Box::new(MockRdmaProvider::with_debug(false))).as_ref()
}

unsafe extern "C" fn c_init(_: *mut c_void, _: *mut *mut c_void) -> i32 { 0 }
unsafe extern "C" fn c_cleanup(_: *mut c_void) {}

unsafe extern "C" fn c_is_memory_suitable(
    _: *mut c_void, ptr: *const c_void, size: usize,
) -> bool {
    get_provider().is_memory_suitable(ptr as *const u8, size)
}

unsafe extern "C" fn c_register_memory(
    _: *mut c_void, ptr: *mut c_void, size: usize,
) -> i32 {
    match get_provider().register_memory(ptr as *mut u8, size) {
        Ok(()) => 0,
        Err(e) => { error!("[s3-rdma-mock] register_memory: {e}"); -1 }
    }
}

unsafe extern "C" fn c_deregister_memory(
    _: *mut c_void, ptr: *mut c_void,
) -> i32 {
    match get_provider().deregister_memory(ptr as *mut u8) {
        Ok(()) => 0,
        Err(e) => { error!("[s3-rdma-mock] deregister_memory: {e}"); -1 }
    }
}

unsafe extern "C" fn c_get_max_transfer_size(
    _: *mut c_void, ptr: *const c_void,
) -> usize {
    get_provider().get_max_transfer_size(ptr as *const u8)
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
    let key = unsafe { (*s3_key).as_slice() };
    match get_provider().prepare_put_token(key, buffer as *const u8, size, offset) {
        Ok(token) => {
            let len = token.len();
            let ptr = Box::into_raw(token.into_boxed_slice()) as *const u8;
            unsafe { *out = ByteCursor { ptr, len }; }
            0
        }
        Err(e) => { error!("[s3-rdma-mock] prepare_put_token: {e}"); -1 }
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
    let key = unsafe { (*s3_key).as_slice() };
    match get_provider().prepare_get_token(key, buffer as *mut u8, size, offset) {
        Ok(token) => {
            let len = token.len();
            let ptr = Box::into_raw(token.into_boxed_slice()) as *const u8;
            unsafe { *out = ByteCursor { ptr, len }; }
            0
        }
        Err(e) => { error!("[s3-rdma-mock] prepare_get_token: {e}"); -1 }
    }
}

/// Process a reply token for cleanup.
///
/// The mock provider handles cleanup in its own `process_rdma_reply` method on
/// the Rust trait path.  This vtable entry exists for C-ABI callers that go
/// through the plugin layer (where `RdmaPluginClient` passes `callback=None`
/// and handles the status itself).  For direct vtable callers, we fire the
/// callback with success — the caller is responsible for translating the HTTP
/// status before calling here.
unsafe extern "C" fn c_process_reply_token(
    _: *mut c_void,
    reply:     *const ByteCursor,
    user_data: *mut c_void,
    callback:  Option<RdmaCompletionFn>,
) -> i32 {
    if reply.is_null() { return -1; }
    if let Some(f) = callback {
        let reply_bytes = unsafe { (*reply).as_slice() };
        let cursor = ByteCursor::from_slice(reply_bytes);
        unsafe { f(user_data, 0, &cursor); }
    }
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
    supports_telemetry:    None,
    setup_telemetry:       None,
    shutdown_telemetry:    None,
    set_telem_level:       None,
};

pub fn vtable() -> *const RdmaProviderVtable {
    &VTABLE
}
