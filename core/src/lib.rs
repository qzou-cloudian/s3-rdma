//! RDMA provider infrastructure for S3-compatible gateways.
//!
//! Provides SDK-agnostic RDMA abstractions used by both client-side S3 tools
//! and server-side gateways.  The central abstraction is the [`RdmaProvider`]
//! trait family:
//!
//! ```text
//! RdmaProvider: RdmaClientProvider + RdmaServerProvider
//!   ├── RdmaClientProvider  — register memory, generate tokens, process replies
//!   └── RdmaServerProvider  — handle PUT/GET transfers from clients
//! ```
//!
//! Concrete implementations:
//!
//! - [`MockRdmaProvider`] — SHM-based simulation, no hardware required; both roles
//! - [`CuObjProvider`] — real NVIDIA cuObj RDMA (loads plugin `.so` at runtime);
//!   both roles from one instance
//! - [`CuObjClientProvider`] / [`CuObjServerProvider`] — focused single-role wrappers
//!   (require `cuobj-client` / `cuobj-server` feature respectively)
//! - [`RdmaEngine`] — fixed-size worker pool wrapping any provider (feature `engine`)
//!
//! ## Choosing the right trait
//!
//! | Scenario | Use |
//! |---|---|
//! | S3 client node (register memory, generate tokens) | `Arc<dyn RdmaClientProvider>` |
//! | S3 server / gateway node (handle PUT/GET) | `Arc<dyn RdmaServerProvider>` |
//! | Both roles in the same node / bindings | `Arc<dyn RdmaProvider>` |
//!
//! ## Flow
//!
//! ```text
//!   Client (RdmaClientProvider)
//!     │  register_memory(ptr, size)
//!     │  prepare_put_token(key, ptr, len, offset) → RdmaToken
//!     │  PUT /bucket/key  +  x-amz-rdma-token: <token>
//!     │                          ↓
//!     │          Server (RdmaServerProvider): RDMA_READ into registered buffer
//!     │                          ↓
//!     │  ← x-amz-rdma-reply: <reply>
//!     │  process_rdma_reply(200, reply_token, ...)
//!     │  deregister_memory(ptr)
//! ```
//!
//! ## Feature flags
//!
//! | Feature        | Effect                                                              |
//! |----------------|---------------------------------------------------------------------|
//! | `cuobj-client` | Enable [`CuObjClientProvider`] / [`CuObjProvider`] client role     |
//! | `cuobj-server` | Enable [`CuObjServerProvider`] / [`CuObjProvider`] server role     |
//! | `buffer-pool`  | Enable [`BufferPool`] for pre-registered RDMA buffers               |
//! | `engine`       | Enable [`RdmaEngine`] fixed-size worker pool                        |
//! | (none)         | Only [`MockRdmaProvider`] available                                 |

pub(crate) mod cuobj_client;
pub(crate) mod cuobj_server;
#[cfg(feature = "engine")]
pub mod engine;
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

pub use provider::{RdmaRole, RdmaProvider, RdmaProviderConfig};
pub use provider::{RdmaClientProvider, RdmaServerProvider};
pub use mock::MockRdmaProvider;
#[cfg(any(feature = "cuobj-client", feature = "cuobj-server"))]
pub use cuobj_client::CuObjProvider;

// ── Supporting types ──────────────────────────────────────────────────────────

pub use provider::RdmaServerConfig;
pub use token::RdmaToken;

#[cfg(feature = "buffer-pool")]
pub use pool::{BufferPool, BufferPoolBuilder, BufferPoolError, OwnedBufferGuard};

#[cfg(feature = "engine")]
pub use engine::{RdmaEngine, RdmaEngineConfig};

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

/// Build an `Arc<dyn RdmaProvider>` for the named provider and role(s).
///
/// `provider` must be one of:
/// - `"mock"`  → [`MockRdmaProvider`]
/// - `"cuobj"` → [`CuObjProvider`] (requires `cuobj-client`/`cuobj-server` features)
/// - `"auto"`  → tries `"cuobj"` first, falls back to `"mock"`
///
/// `role` specifies which capabilities to initialise:
/// - [`RdmaRole::Client`]  — client methods (register/token/reply) only
/// - [`RdmaRole::Server`]  — server methods (handle_put/handle_get) only
/// - [`RdmaRole::Both`]    — both (typical for a gateway node)
///
/// Returns [`RdmaError::ProviderUnavailable`] if the name is unrecognised,
/// the required feature is not compiled in, or the underlying SDK fails to
/// initialise.
pub fn create_provider(
    provider: &str,
    debug: bool,
    role: RdmaRole,
    cfg: &RdmaServerConfig,
) -> Result<Arc<dyn RdmaProvider>, RdmaError> {
    match provider {
        "mock" => {
            if debug {
                eprintln!("[rdma] Using MockRdmaProvider (role={role:?})");
            }
            Ok(Arc::new(MockRdmaProvider::new(debug, &cfg.shm_path, cfg.buf_size)))
        }
        "cuobj" => {
            let result: Result<Arc<dyn RdmaProvider>, RdmaError>;
            #[cfg(any(feature = "cuobj-client", feature = "cuobj-server"))]
            {
                let ep = match role {
                    RdmaRole::Client => CuObjProvider::new_client(),
                    RdmaRole::Server => CuObjProvider::new_server(cfg),
                    RdmaRole::Both   => CuObjProvider::new_both(cfg),
                };
                result = ep.map(|e| {
                    if debug {
                        eprintln!("[rdma] Using CuObjProvider (role={role:?})");
                    }
                    Arc::new(e) as Arc<dyn RdmaProvider>
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
            match create_provider("cuobj", debug, role, cfg) {
                Ok(ep) => Ok(ep),
                Err(_) => create_provider("mock", debug, role, cfg),
            }
        }
        other => Err(RdmaError::ProviderUnavailable {
            provider: other.to_owned(),
            reason: "unknown provider (valid: mock, cuobj, auto)".to_owned(),
        }),
    }
}

/// Build an `Arc<dyn RdmaClientProvider>` for the named provider.
///
/// Convenience wrapper around [`create_provider`] with [`RdmaRole::Client`].
/// Use this in code that only needs client-side operations (register memory,
/// generate tokens, process replies).
pub fn create_client_provider(
    provider: &str,
    debug: bool,
    cfg: &RdmaServerConfig,
) -> Result<Arc<dyn RdmaClientProvider>, RdmaError> {
    match provider {
        "mock" => {
            if debug {
                eprintln!("[rdma] Using MockRdmaProvider (role=Client)");
            }
            Ok(Arc::new(MockRdmaProvider::new(debug, &cfg.shm_path, cfg.buf_size)))
        }
        #[cfg(any(feature = "cuobj-client", feature = "cuobj-server"))]
        "cuobj" => {
            CuObjProvider::new_client()
                .map(|e| {
                    if debug { eprintln!("[rdma] Using CuObjProvider (role=Client)"); }
                    Arc::new(e) as Arc<dyn RdmaClientProvider>
                })
                .map_err(|e| RdmaError::ProviderUnavailable {
                    provider: provider.to_owned(),
                    reason: e.to_string(),
                })
        }
        #[cfg(not(any(feature = "cuobj-client", feature = "cuobj-server")))]
        "cuobj" => Err(RdmaError::ProviderUnavailable {
            provider: provider.to_owned(),
            reason: "compiled without `cuobj-client` or `cuobj-server` features".into(),
        }),
        "auto" => create_client_provider("cuobj", debug, cfg)
            .or_else(|_| create_client_provider("mock", debug, cfg)),
        other => Err(RdmaError::ProviderUnavailable {
            provider: other.to_owned(),
            reason: "unknown provider (valid: mock, cuobj, auto)".to_owned(),
        }),
    }
}

/// Build an `Arc<dyn RdmaServerProvider>` for the named provider.
///
/// Convenience wrapper around [`create_provider`] with [`RdmaRole::Server`].
/// Use this in code that only needs server-side operations (handle PUT/GET).
pub fn create_server_provider(
    provider: &str,
    debug: bool,
    cfg: &RdmaServerConfig,
) -> Result<Arc<dyn RdmaServerProvider>, RdmaError> {
    match provider {
        "mock" => {
            if debug {
                eprintln!("[rdma] Using MockRdmaProvider (role=Server)");
            }
            Ok(Arc::new(MockRdmaProvider::new(debug, &cfg.shm_path, cfg.buf_size)))
        }
        #[cfg(any(feature = "cuobj-client", feature = "cuobj-server"))]
        "cuobj" => {
            CuObjProvider::new_server(cfg)
                .map(|e| {
                    if debug { eprintln!("[rdma] Using CuObjProvider (role=Server)"); }
                    Arc::new(e) as Arc<dyn RdmaServerProvider>
                })
                .map_err(|e| RdmaError::ProviderUnavailable {
                    provider: provider.to_owned(),
                    reason: e.to_string(),
                })
        }
        #[cfg(not(any(feature = "cuobj-client", feature = "cuobj-server")))]
        "cuobj" => Err(RdmaError::ProviderUnavailable {
            provider: provider.to_owned(),
            reason: "compiled without `cuobj-client` or `cuobj-server` features".into(),
        }),
        "auto" => create_server_provider("cuobj", debug, cfg)
            .or_else(|_| create_server_provider("mock", debug, cfg)),
        other => Err(RdmaError::ProviderUnavailable {
            provider: other.to_owned(),
            reason: "unknown provider (valid: mock, cuobj, auto)".to_owned(),
        }),
    }
}
