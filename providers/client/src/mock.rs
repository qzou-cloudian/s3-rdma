//! Mock RDMA client backend — shared-memory simulation, no hardware required.

use std::ffi::c_void;
use std::os::raw::c_char;
use std::sync::OnceLock;

use tracing::error;

use s3_rdma::ffi::{ByteCursor, RdmaProviderVtable};
use s3_rdma::provider::RdmaClientChannel;
use s3_rdma::{MockRdmaProvider, RdmaClientProvider};

static PROVIDER_NAME: &[u8] = b"s3-rdma-mock\0";

fn get_provider() -> &'static (dyn RdmaClientProvider + Send + Sync) {
    static INSTANCE: OnceLock<Box<dyn RdmaClientProvider + Send + Sync>> = OnceLock::new();
    INSTANCE.get_or_init(|| Box::new(MockRdmaProvider::with_debug(false))).as_ref()
}

unsafe extern "C" fn c_init(_: *mut c_void, _: *mut *mut c_void) -> i32 { 0 }
unsafe extern "C" fn c_cleanup(_: *mut c_void) {}

unsafe extern "C" fn c_bind_channel(
    _ctx: *mut c_void,
    size: usize,
    key:  *const ByteCursor,
    out:  *mut *mut c_void,
) -> i32 {
    if key.is_null() || out.is_null() { return -1; }
    let key_slice = unsafe { (*key).as_slice() };
    match get_provider().bind(size, key_slice) {
        Ok(channel) => {
            let boxed: Box<Box<dyn RdmaClientChannel>> = Box::new(channel);
            unsafe { *out = Box::into_raw(boxed) as *mut c_void; }
            0
        }
        Err(e) => {
            error!("[s3-rdma-mock] bind_channel: {e}");
            -1
        }
    }
}

static VTABLE: RdmaProviderVtable = RdmaProviderVtable {
    provider_name:      PROVIDER_NAME.as_ptr() as *const c_char,
    provider_version:   1,
    init:               Some(c_init),
    cleanup:            Some(c_cleanup),
    bind_channel:       Some(c_bind_channel),
    supports_telemetry: Some(crate::telem::supports),
    setup_telemetry:    Some(crate::telem::setup),
    shutdown_telemetry: Some(crate::telem::shutdown),
    set_telem_level:    Some(crate::telem::set_level),
};

pub fn vtable() -> *const RdmaProviderVtable {
    &VTABLE
}
