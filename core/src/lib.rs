//! RDMA provider infrastructure for hsc.
//!
//! This module provides SDK-agnostic RDMA provider implementations (mock and
//! cuObject) and a C ABI vtable for use from C consumers.  The S3-specific
//! AWS Smithy interceptor lives in the root `hsc` crate.
//!
//! ## How it works
//!
//! ```text
//!   hsc upload/download
//!         │
//!         ▼
//!   Register buffer with cuObject (RdmaProvider::register_memory)
//!         │
//!         ▼
//!   Generate RDMA token  (RdmaProvider::prepare_put_token / prepare_get_token)
//!         │
//!         ▼
//!   aws-sdk-s3 PUT / GET  +  RdmaInterceptor
//!         │  → adds x-amz-rdma-token request header
//!         │  ← reads x-amz-rdma-reply response header
//!         │
//!         ▼
//!   S3 server performs RDMA READ/WRITE on registered buffer
//!         │
//!         ▼
//!   Deregister buffer  (RdmaProvider::deregister_memory)
//! ```
//!
//! ## Feature flags
//!
//! | Feature     | Effect                                               |
//! |-------------|------------------------------------------------------|
//! | `cuobj`     | Enable real [`CuObjProvider`] (needs NVIDIA SDK)  |
//! | (default)   | Only [`MockRdmaProvider`] is available               |

pub mod cuobj;
pub mod error;
pub mod ffi;
pub mod mock;
pub mod provider;
#[cfg(feature = "c-abi")]
mod c_abi;
#[cfg(feature = "pyo3")]
mod python;

/// Returns a pointer to the static C vtable for use by provider cdylibs.
///
/// Called by `s3-rdma-cuobj`'s `lib.rs` which re-exports it as
/// `#[no_mangle] s3_rdma_provider_get_vtable`.
#[cfg(feature = "c-abi")]
pub fn get_vtable_ptr() -> *const ffi::RdmaProviderVtable {
    c_abi::get_vtable()
}

pub use mock::MockRdmaProvider;
pub use provider::RdmaProvider;

#[cfg(feature = "cuobj")]
pub use cuobj::CuObjProvider;
#[cfg(feature = "cuobj")]
pub use cuobj::cuobj_available;

use std::sync::Arc;

/// Minimum buffer size (bytes) considered suitable for RDMA by the mock provider.
#[allow(dead_code)]
pub const RDMA_MIN_BUFFER_SIZE: usize = 1024 * 1024; // 1 MiB

/// Returns a one-line summary of compiled-in RDMA providers and their runtime
/// availability, e.g. `"RDMA providers: cuobj (available), mock"`.
pub fn rdma_provider_info() -> String {
    let mut entries: Vec<String> = Vec::new();

    #[cfg(feature = "cuobj")]
    {
        let status = if cuobj_available() { "available" } else { "unavailable" };
        entries.push(format!("cuobj ({status})"));
    }

    entries.push("mock".into());

    format!("RDMA providers: {}", entries.join(", "))
}

/// Build an `Arc<dyn RdmaProvider>` based on the requested mode.
///
/// - `use_mock = true`  → always use [`MockRdmaProvider`].
/// - `use_mock = false` → try [`CuObjProvider`] (requires `cuobj` feature),
///   falling back to [`MockRdmaProvider`] if the SDK is unavailable.
pub fn create_provider(use_mock: bool, debug: bool) -> Arc<dyn RdmaProvider> {
    if use_mock {
        if debug {
            eprintln!("[rdma] Using MockRdmaProvider (mock mode)");
        }
        return Arc::new(MockRdmaProvider::new(debug));
    }

    #[cfg(feature = "cuobj")]
    {
        match CuObjProvider::new() {
            Ok(p) => {
                if debug {
                    eprintln!("[rdma] Using CuObjProvider");
                }
                return Arc::new(p);
            }
            Err(e) => {
                eprintln!("[rdma] CuObjProvider unavailable ({e}); falling back to mock");
            }
        }
    }

    if debug {
        eprintln!("[rdma] Using MockRdmaProvider (fallback)");
    }
    Arc::new(MockRdmaProvider::new(debug))
}
