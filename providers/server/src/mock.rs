//! Mock RDMA server backend — shared-memory simulation, no hardware required.

use std::ffi::c_void;
use std::os::raw::c_char;
use s3_rdma::ffi::{ByteCursor, RdmaIoReadFn, RdmaIoWriteFn, RdmaServerInitParams, RdmaServerVtable};

static PROVIDER_NAME: &[u8] = b"s3-rdma-server-mock\0";

unsafe extern "C" fn c_init(_: *const RdmaServerInitParams, _: *mut *mut c_void) -> i32 { 0 }
unsafe extern "C" fn c_cleanup(_: *mut c_void) {}

unsafe extern "C" fn c_handle_put(
    _ctx: *mut c_void,
    _s3_key: *const ByteCursor,
    _token: *const ByteCursor,
    _offset: usize,
    _size: usize,
    _write_fn: RdmaIoWriteFn,
    _write_fn_data: *mut c_void,
    out_transferred: *mut usize,
) -> i32 {
    if !out_transferred.is_null() {
        unsafe { *out_transferred = 0; }
    }
    0
}

unsafe extern "C" fn c_handle_get(
    _ctx: *mut c_void,
    _s3_key: *const ByteCursor,
    _token: *const ByteCursor,
    _offset: usize,
    _read_fn: RdmaIoReadFn,
    _read_fn_data: *mut c_void,
    out_written: *mut usize,
) -> i32 {
    if !out_written.is_null() {
        unsafe { *out_written = 0; }
    }
    0
}

static VTABLE: RdmaServerVtable = RdmaServerVtable {
    provider_name:    PROVIDER_NAME.as_ptr() as *const c_char,
    provider_version: 1,
    init:             Some(c_init),
    cleanup:          Some(c_cleanup),
    handle_put:       Some(c_handle_put),
    handle_get:       Some(c_handle_get),
    supports_telemetry: None,
    setup_telemetry:    None,
    shutdown_telemetry: None,
    set_telem_level:    None,
    open_channel:       None,
};

pub fn vtable() -> *const RdmaServerVtable {
    &VTABLE
}
