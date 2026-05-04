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
//! - [`RdmaPlugin`] — generic RDMA plugin (loads `.so` at runtime); both roles
//!   from one instance
//! - [`RdmaPluginClient`] / [`RdmaPluginServer`] — focused single-role
//!   wrappers (require `rdma-client` / `rdma-server` feature respectively)
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
//! | Feature         | Effect                                                              |
//! |-----------------|---------------------------------------------------------------------|
//! | `rdma-client` | Enable [`RdmaPluginClient`] / [`RdmaPlugin`] client role   |
//! | `rdma-server` | Enable [`RdmaPluginServer`] / [`RdmaPlugin`] server role   |
//! | `buffer-pool`   | Enable [`BufferPool`] for pre-registered RDMA buffers               |
//! | `engine`        | Enable [`RdmaEngine`] fixed-size worker pool                        |
//! | (none)          | Only [`MockRdmaProvider`] available                                 |

pub(crate) mod rdma_client;
pub(crate) mod rdma_server;
#[cfg(feature = "engine")]
pub mod engine;
pub mod error;
pub mod ffi;
pub mod mock;
#[cfg(feature = "buffer-pool")]
pub mod pool;
pub mod provider;
pub mod token;
#[cfg(feature = "pyo3")]
mod python;

// ── Primary public API ────────────────────────────────────────────────────────

pub use provider::{RdmaRole, RdmaProvider, RdmaProviderConfig};
pub use provider::{RdmaClientProvider, RdmaServerProvider};
pub use provider::{TelemetryConfig, TelemLevel};
pub use provider::DEFAULT_RDMA_BUF_SIZE;
pub use mock::MockRdmaProvider;
#[cfg(any(feature = "rdma-client", feature = "rdma-server"))]
pub use rdma_client::RdmaPlugin;

// ── Supporting types ──────────────────────────────────────────────────────────

pub use provider::RdmaServerConfig;
pub use token::RdmaToken;

#[cfg(feature = "buffer-pool")]
pub use pool::{BufferPool, BufferPoolBuilder, BufferPoolError, OwnedBufferGuard};

#[cfg(feature = "engine")]
pub use engine::{RdmaEngine, RdmaEngineConfig};

#[cfg(feature = "rdma-client")]
pub use rdma_client::RdmaPluginClient;
#[cfg(feature = "rdma-client")]
pub use rdma_client::rdma_available;

#[cfg(feature = "rdma-server")]
pub use rdma_server::RdmaPluginServer;

use std::sync::Arc;

use tracing::info;

pub use error::RdmaError;

/// Returns a one-line summary of compiled-in RDMA providers and their runtime
/// availability, e.g. `"RDMA providers: rdma (available), mock"`.
pub fn rdma_provider_info() -> String {
    let mut entries: Vec<String> = Vec::new();

    #[cfg(feature = "rdma-client")]
    {
        let status = if rdma_available() { "available" } else { "unavailable" };
        entries.push(format!("rdma ({status})"));
    }

    entries.push("mock".into());

    format!("RDMA providers: {}", entries.join(", "))
}

/// Build an `Arc<dyn RdmaProvider>` for the named provider and role(s).
///
/// `provider` must be one of:
/// - `"mock"`   → [`MockRdmaProvider`]
/// - `"rdma"`  → [`RdmaPlugin`] (requires `rdma-client`/`rdma-server` features)
/// - `"auto"`  → tries `"rdma"` first, falls back to `"mock"`
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
    let ep: Arc<dyn RdmaProvider> = match provider {
        "mock" => {
            if debug {
                info!("[rdma] Using MockRdmaProvider (role={role:?})");
            }
            Arc::new(MockRdmaProvider::new(debug, &cfg.shm_path, cfg.buf_size))
        }
        "rdma" => {
            let result: Result<Arc<dyn RdmaProvider>, RdmaError>;
            #[cfg(any(feature = "rdma-client", feature = "rdma-server"))]
            {
                let ep = match role {
                    RdmaRole::Client => RdmaPlugin::new_client(),
                    RdmaRole::Server => RdmaPlugin::new_server(cfg),
                    RdmaRole::Both   => RdmaPlugin::new_both(cfg),
                };
                result = ep.map(|e| {
                    if debug {
                        info!("[rdma] Using RdmaPlugin (role={role:?})");
                    }
                    Arc::new(e) as Arc<dyn RdmaProvider>
                }).map_err(|e| RdmaError::ProviderUnavailable {
                    provider: provider.to_owned(),
                    reason: e.to_string(),
                });
            }
            #[cfg(not(any(feature = "rdma-client", feature = "rdma-server")))]
            {
                let _ = (debug, role, cfg);
                result = Err(RdmaError::ProviderUnavailable {
                    provider: provider.to_owned(),
                    reason: "compiled without `rdma-client` or `rdma-server` features".into(),
                });
            }
            result?
        }
        "auto" => {
            // Try rdma first; fall back to mock on any error.
            match create_provider("rdma", debug, role, cfg) {
                Ok(ep) => return Ok(ep),
                Err(_) => return create_provider("mock", debug, role, cfg),
            }
        }
        other => return Err(RdmaError::ProviderUnavailable {
            provider: other.to_owned(),
            reason: "unknown provider (valid: mock, rdma, auto)".to_owned(),
        }),
    };
    Ok(ep)
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
                info!("[rdma] Using MockRdmaProvider (role=Client)");
            }
            Ok(Arc::new(MockRdmaProvider::new(debug, &cfg.shm_path, cfg.buf_size)))
        }
        #[cfg(any(feature = "rdma-client", feature = "rdma-server"))]
        "rdma" => {
            RdmaPlugin::new_client()
                .map(|e| {
                    if debug { info!("[rdma] Using RdmaPlugin (role=Client)"); }
                    Arc::new(e) as Arc<dyn RdmaClientProvider>
                })
                .map_err(|e| RdmaError::ProviderUnavailable {
                    provider: provider.to_owned(),
                    reason: e.to_string(),
                })
        }
        #[cfg(not(any(feature = "rdma-client", feature = "rdma-server")))]
        "rdma" => Err(RdmaError::ProviderUnavailable {
            provider: provider.to_owned(),
            reason: "compiled without `rdma-client` or `rdma-server` features".into(),
        }),
        "auto" => create_client_provider("rdma", debug, cfg)
            .or_else(|_| create_client_provider("mock", debug, cfg)),
        other => Err(RdmaError::ProviderUnavailable {
            provider: other.to_owned(),
            reason: "unknown provider (valid: mock, rdma, auto)".to_owned(),
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
                info!("[rdma] Using MockRdmaProvider (role=Server)");
            }
            Ok(Arc::new(MockRdmaProvider::new(debug, &cfg.shm_path, cfg.buf_size)))
        }
        #[cfg(any(feature = "rdma-client", feature = "rdma-server"))]
        "rdma" => {
            RdmaPlugin::new_server(cfg)
                .map(|e| {
                    if debug { info!("[rdma] Using RdmaPlugin (role=Server)"); }
                    Arc::new(e) as Arc<dyn RdmaServerProvider>
                })
                .map_err(|e| RdmaError::ProviderUnavailable {
                    provider: provider.to_owned(),
                    reason: e.to_string(),
                })
        }
        #[cfg(not(any(feature = "rdma-client", feature = "rdma-server")))]
        "rdma" => Err(RdmaError::ProviderUnavailable {
            provider: provider.to_owned(),
            reason: "compiled without `rdma-client` or `rdma-server` features".into(),
        }),
        "auto" => create_server_provider("rdma", debug, cfg)
            .or_else(|_| create_server_provider("mock", debug, cfg)),
        other => Err(RdmaError::ProviderUnavailable {
            provider: other.to_owned(),
            reason: "unknown provider (valid: mock, rdma, auto)".to_owned(),
        }),
    }
}

