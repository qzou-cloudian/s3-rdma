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
//!   Register buffer with cuObject (RdmaEndpoint::register_memory)
//!         │
//!         ▼
//!   Generate RDMA token  (RdmaEndpoint::prepare_put_token / prepare_get_token)
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
//!   Deregister buffer  (RdmaEndpoint::deregister_memory)
//! ```
//!
//! ## Feature flags
//!
//! | Feature        | Effect                                                    |
//! |----------------|-----------------------------------------------------------|
//! | `cuobj-client` | Enable real [`CuObjEndpoint`] client role (loads plugin at runtime) |
//! | `cuobj-server` | Enable real [`CuObjEndpoint`] server role (loads plugin at runtime) |
//! | (default)      | Only [`MockRdmaEndpoint`]                                |

pub mod cuobj_client;
pub mod cuobj_server;
pub mod endpoint;
pub mod error;
pub mod ffi;
pub mod mock;
#[cfg(feature = "buffer-pool")]
pub mod pool;
pub mod provider;
pub mod token;
#[cfg(feature = "c-abi")]
mod c_abi;
#[cfg(feature = "pyo3")]
mod python;

/// Returns a pointer to the static C vtable for use by provider cdylibs.
///
/// Called by `s3-rdma-cuobj-client`'s `lib.rs` which re-exports it as
/// `#[no_mangle] s3_rdma_provider_get_vtable`.
#[cfg(feature = "c-abi")]
pub fn get_vtable_ptr() -> *const ffi::RdmaProviderVtable {
    c_abi::get_vtable()
}

// ── Primary public API ────────────────────────────────────────────────────────

pub use endpoint::{EndpointRole, RdmaEndpoint, RdmaEndpointConfig};
pub use mock::MockRdmaEndpoint;
pub use cuobj_client::CuObjEndpoint;

// ── Supporting types ──────────────────────────────────────────────────────────

pub use provider::RdmaServerConfig;
pub use token::RdmaToken;

#[cfg(feature = "buffer-pool")]
pub use pool::{BufferPool, BufferPoolBuilder, BufferPoolError, OwnedBufferGuard};

#[cfg(feature = "cuobj-client")]
pub use cuobj_client::CuObjClientProvider;
#[cfg(feature = "cuobj-client")]
pub use cuobj_client::cuobj_available;

#[cfg(feature = "cuobj-server")]
pub use cuobj_server::CuObjServerProvider;

use std::sync::Arc;

pub use error::RdmaError;

/// Returns a one-line summary of compiled-in RDMA providers and their runtime
/// availability, e.g. `"RDMA providers: cuobj (available), mock"`.
pub fn rdma_provider_info() -> String {
    let mut entries: Vec<String> = Vec::new();

    #[cfg(feature = "cuobj-client")]
    {
        let status = if cuobj_available() { "available" } else { "unavailable" };
        entries.push(format!("cuobj ({status})"));
    }

    entries.push("mock".into());

    format!("RDMA providers: {}", entries.join(", "))
}

/// Build an `Arc<dyn RdmaEndpoint>` for the named provider and role(s).
///
/// `provider` must be one of:
/// - `"mock"`  → [`MockRdmaEndpoint`]
/// - `"cuobj"` → [`CuObjEndpoint`] (requires `cuobj-client`/`cuobj-server` features)
/// - `"auto"`  → tries `"cuobj"` first, falls back to `"mock"`
///
/// `role` specifies which capabilities to initialise:
/// - [`EndpointRole::Client`]  — client methods (register/token/reply) only
/// - [`EndpointRole::Server`]  — server methods (handle_put/handle_get) only
/// - [`EndpointRole::Both`]    — both (typical for a gateway node)
///
/// Returns [`RdmaError::ProviderUnavailable`] if the name is unrecognised,
/// the required feature is not compiled in, or the underlying SDK fails to
/// initialise.
pub fn create_endpoint(
    provider: &str,
    debug: bool,
    role: EndpointRole,
    cfg: &RdmaServerConfig,
) -> Result<Arc<dyn RdmaEndpoint>, RdmaError> {
    match provider {
        "mock" => {
            if debug {
                eprintln!("[rdma] Using MockRdmaEndpoint (role={role:?})");
            }
            Ok(Arc::new(MockRdmaEndpoint::new(debug, &cfg.shm_path, cfg.buf_size)))
        }
        "cuobj" => {
            let result: Result<Arc<dyn RdmaEndpoint>, RdmaError>;
            #[cfg(any(feature = "cuobj-client", feature = "cuobj-server"))]
            {
                let ep = match role {
                    EndpointRole::Client => CuObjEndpoint::new_client(),
                    EndpointRole::Server => CuObjEndpoint::new_server(cfg),
                    EndpointRole::Both   => CuObjEndpoint::new_both(cfg),
                };
                result = ep.map(|e| {
                    if debug {
                        eprintln!("[rdma] Using CuObjEndpoint (role={role:?})");
                    }
                    Arc::new(e) as Arc<dyn RdmaEndpoint>
                }).map_err(|e| RdmaError::ProviderUnavailable {
                    provider: provider.to_owned(),
                    reason: e.to_string(),
                });
            }
            #[cfg(not(any(feature = "cuobj-client", feature = "cuobj-server")))]
            {
                let _ = (debug, role, cfg);
                result = Err(RdmaError::ProviderUnavailable {
                    provider: provider.to_owned(),
                    reason: "compiled without `cuobj-client` or `cuobj-server` features".into(),
                });
            }
            result
        }
        "auto" => {
            // Try cuobj first; fall back to mock on any error.
            match create_endpoint("cuobj", debug, role, cfg) {
                Ok(ep) => Ok(ep),
                Err(_) => create_endpoint("mock", debug, role, cfg),
            }
        }
        other => Err(RdmaError::ProviderUnavailable {
            provider: other.to_owned(),
            reason: "unknown provider (valid: mock, cuobj, auto)".to_owned(),
        }),
    }
}
