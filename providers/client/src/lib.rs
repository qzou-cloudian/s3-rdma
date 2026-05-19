//! RDMA client provider plugin.
//!
//! Builds as a `cdylib` (`libs3rdmaclient.so`) and exports a single named
//! entry point:
//! ```c
//! const RdmaProviderVtable *s3_rdma_get_client_vtable(const char *provider_name);
//! ```
//!
//! Supported provider names:
//! - `"mock"`   — shared-memory simulation, always available (feature `mock`)
//! - `"cuobj"` — cuObject RDMA hardware backend (feature `cuobj`)
//! - anything else → returns `NULL`
//!
//! Adding a new backend (e.g. UCX): add a Cargo feature, implement shims in a
//! new module, add a match arm in `s3_rdma_get_client_vtable`.

use s3_rdma::ffi::RdmaProviderVtable;
use std::os::raw::c_char;

mod telem;

// ── Mock backend ──────────────────────────────────────────────────────────────

#[cfg(feature = "mock")]
mod mock;

// ── cuObject (rdma) backend ─────────────────────────────────────────────────

#[cfg(feature = "cuobj")]
mod rdma;

// ── Named entry point ─────────────────────────────────────────────────────────

/// Returns the vtable for the named RDMA client provider.
///
/// Returns `NULL` if `provider_name` is not recognised or the corresponding
/// backend was not compiled in.
#[no_mangle]
pub unsafe extern "C" fn s3_rdma_get_client_vtable(
    provider_name: *const c_char,
) -> *const RdmaProviderVtable {
    let name = if provider_name.is_null() {
        ""
    } else {
        match unsafe { std::ffi::CStr::from_ptr(provider_name) }.to_str() {
            Ok(s) => s,
            Err(_) => return std::ptr::null(),
        }
    };

    match name {
        #[cfg(feature = "mock")]
        "mock" => mock::vtable(),
        #[cfg(feature = "cuobj")]
        "cuobj" => rdma::vtable(),
        _ => std::ptr::null(),
    }
}
