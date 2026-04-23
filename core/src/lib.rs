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
pub mod cuobj_server;
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
pub use mock::MockRdmaServerProvider;
pub use provider::RdmaProvider;
pub use provider::RdmaServerProvider;

#[cfg(feature = "cuobj")]
pub use cuobj::CuObjProvider;
#[cfg(feature = "cuobj")]
pub use cuobj::cuobj_available;

#[cfg(feature = "cuobj-server")]
pub use cuobj_server::CuObjServerProvider;
pub use cuobj_server::RdmaServerConfig;

use std::sync::Arc;

pub use error::RdmaError;

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

/// Build an `Arc<dyn RdmaProvider>` for the named provider.
///
/// `provider` must be one of:
/// - `"mock"`  → [`MockRdmaProvider`]
/// - `"cuobj"` → [`CuObjProvider`] (requires the `cuobj` feature)
///
/// Returns [`RdmaError::ProviderUnavailable`] if the name is unrecognised,
/// the required feature is not compiled in, or the underlying SDK fails to
/// initialise.
pub fn create_provider(provider: &str, debug: bool) -> Result<Arc<dyn RdmaProvider>, RdmaError> {
    match provider {
        "mock" => {
            if debug {
                eprintln!("[rdma] Using MockRdmaProvider");
            }
            Ok(Arc::new(MockRdmaProvider::new(debug)))
        }
        "cuobj" => {
            let result: Result<Arc<dyn RdmaProvider>, RdmaError>;
            #[cfg(feature = "cuobj")]
            {
                result = match CuObjProvider::new() {
                    Ok(p) => {
                        if debug {
                            eprintln!("[rdma] Using CuObjProvider");
                        }
                        Ok(Arc::new(p))
                    }
                    Err(e) => Err(RdmaError::ProviderUnavailable {
                        provider: provider.to_owned(),
                        reason: e.to_string(),
                    }),
                };
            }
            #[cfg(not(feature = "cuobj"))]
            {
                result = Err(RdmaError::ProviderUnavailable {
                    provider: provider.to_owned(),
                    reason: "compiled without the `cuobj` feature".to_owned(),
                });
            }
            result
        }
        other => Err(RdmaError::ProviderUnavailable {
            provider: other.to_owned(),
            reason: "unknown provider".to_owned(),
        }),
    }
}

/// Build an `Arc<dyn RdmaServerProvider>` for the named server-side provider.
///
/// `provider` must be one of:
/// - `"mock"`  → [`MockRdmaServerProvider`] — reads/writes files under
///               `cfg.shm_path` (default `/dev/shm`)
/// - `"cuobj"` → [`CuObjServerProvider`] loaded from `libs3_rdma_cuobj_server.so`
///              at runtime; requires the `cuobj-server` feature and a non-empty
///              `cfg.listen_addr`.
///
/// Returns [`RdmaError::ProviderUnavailable`] if the name is unrecognised,
/// the required feature is not compiled in, or the plugin fails to initialise.
pub fn create_server_provider(
    provider: &str,
    debug: bool,
    cfg: &RdmaServerConfig,
) -> Result<Arc<dyn RdmaServerProvider>, RdmaError> {
    match provider {
        "mock" => {
            if debug {
                eprintln!("[rdma] Using MockRdmaServerProvider (shm: {})", cfg.shm_path);
            }
            Ok(Arc::new(MockRdmaServerProvider::new(debug, &cfg.shm_path)))
        }
        "cuobj" => {
            let result: Result<Arc<dyn RdmaServerProvider>, RdmaError>;
            #[cfg(feature = "cuobj-server")]
            {
                result = match CuObjServerProvider::new(cfg) {
                    Ok(p) => {
                        if debug {
                            eprintln!("[rdma] Using CuObjServerProvider");
                        }
                        Ok(Arc::new(p))
                    }
                    Err(e) => Err(RdmaError::ProviderUnavailable {
                        provider: provider.to_owned(),
                        reason: e.to_string(),
                    }),
                };
            }
            #[cfg(not(feature = "cuobj-server"))]
            {
                let _ = cfg;
                result = Err(RdmaError::ProviderUnavailable {
                    provider: provider.to_owned(),
                    reason: "compiled without the `cuobj-server` feature".to_owned(),
                });
            }
            result
        }
        other => Err(RdmaError::ProviderUnavailable {
            provider: other.to_owned(),
            reason: "unknown server provider".to_owned(),
        }),
    }
}
