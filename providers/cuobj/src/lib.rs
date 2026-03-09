//! cuObject RDMA provider plugin for S3 clients.
//!
//! This crate compiles `s3-rdma` (with `cuobj` + `c-abi` features) into a
//! shared library that exposes the provider vtable with the standard C ABI.
//!
//! Exported symbol:
//! ```c
//! const RdmaProviderVtable *s3_rdma_provider_get_vtable(void);
//! ```

use s3_rdma::ffi::RdmaProviderVtable;

/// C ABI entry point: returns the statically allocated RDMA provider vtable.
#[no_mangle]
pub extern "C" fn s3_rdma_provider_get_vtable() -> *const RdmaProviderVtable {
    s3_rdma::get_vtable_ptr()
}
