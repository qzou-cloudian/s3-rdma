//! RDMA provider infrastructure for S3-compatible gateways.
//!
//! Provides SDK-agnostic RDMA abstractions used by both client-side S3 tools
//! and server-side gateways.  The central abstraction is the [`RdmaProvider`]
//! trait family:
//!
//! ```text
//! RdmaProvider: RdmaClientProvider + RdmaServerProvider
//!   ├── RdmaClientProvider  — bind buffers, create RdmaClientChannel
//!   │   └── RdmaClientChannel — generate tokens, process replies
//!   └── RdmaServerProvider  — open channels for key+token pairs
//!       └── RdmaServerChannel  — handle PUT/GET transfers
//! ```
//!
//! ## Channel-based API (preferred)
//!
//! | Side   | Provider method | Returns | Channel methods |
//! |--------|----------------|---------|-----------------|
//! | Client | `bind(ptr, size, key)` | `RdmaClientChannel` | `prepare_put_token`, `prepare_get_token`, `process_rdma_reply` |
//! | Server | `open_channel(key, token)` | `RdmaServerChannel` | `handle_put`, `handle_get` (+ ZC variants) |
//!
//! The channel owns the binding (key, token, buffer registration) so callers
//! don't pass the same parameters repeatedly.  Drop deregisters / cleans up.
//!
//! Concrete implementations:
//!
//! - [`MockRdmaProvider`] — SHM-based simulation, no hardware required; both roles
//! - [`RdmaPlugin`] — generic RDMA plugin (loads `.so` at runtime); both roles
//! - [`RdmaPlugin`] — loads `.so` at runtime via the plugin system
//!
//! ## Choosing the right trait
//!
//! | Scenario | Use |
//! |---|---|
//! | S3 client node (bind buffers, generate tokens) | `Arc<dyn RdmaClientProvider>` |
//! | S3 server / gateway node (open channels, handle PUT/GET) | `Arc<dyn RdmaServerProvider>` |
//! | Both roles in the same node / bindings | `Arc<dyn RdmaProvider>` |
//!
//! ## Flow
//!
//! ```text
//!   Client                                    Server
//!   ──────                                    ──────
//!   ch = provider.bind(ptr, size, key)
//!   token = ch.prepare_put_token(offset, size)
//!     │  PUT /bucket/key  +  x-amz-rdma-token: <token>
//!     │                          ↓             ch = provider.open_channel(key, token)
//!     │                          ↓             n = ch.handle_put(buf, offset, size)
//!     │  ← x-amz-rdma-reply: <reply>
//!   ch.process_rdma_reply(200, token, n, cb)
//!   drop(ch)  →  deregister memory
//! ```
//!
//! ## Feature flags
//!
//! | Feature         | Effect                                                              |
//! |-----------------|---------------------------------------------------------------------|
//! | `rdma-client` | Enable [`RdmaPluginClient`] / [`RdmaPlugin`] client role   |
//! | `rdma-server` | Enable [`RdmaPluginServer`] / [`RdmaPlugin`] server role   |
//! | `buffer-pool`   | Enable [`BufferPool`] for pre-registered RDMA buffers               |
//! | (none)          | Only [`MockRdmaProvider`] available                                 |

pub(crate) mod rdma_client;
pub(crate) mod rdma_server;
pub mod error;
pub mod ffi;
pub mod mock;
#[cfg(feature = "buffer-pool")]
pub mod pool;
pub mod provider;
pub mod token;
pub mod worker_pool;
#[cfg(feature = "pyo3")]
mod python;

// ── Primary public API ────────────────────────────────────────────────────────

pub use provider::{RdmaRole, RdmaProvider, RdmaProviderConfig};
pub use provider::{RdmaServerChannel, RdmaClientChannel, RdmaClientProvider, RdmaServerProvider};
pub use provider::{TelemetryConfig, TelemLevel, TelemState};
pub use provider::{DEFAULT_RDMA_BUF_SIZE, RDMA_REQUIRED_ALIGNMENT};
pub use mock::MockRdmaProvider;
#[cfg(any(feature = "rdma-client", feature = "rdma-server"))]
pub use rdma_client::RdmaPlugin;

// ── Supporting types ──────────────────────────────────────────────────────────

pub use provider::RdmaServerConfig;
pub use token::RdmaToken;

pub use worker_pool::RdmaWorkerPool;

#[cfg(feature = "buffer-pool")]
pub use pool::{BufferPool, BufferPoolBuilder, BufferPoolError, BufferRegistrar, OwnedBufferGuard};


#[cfg(feature = "rdma-client")]
pub use rdma_client::RdmaPluginClient;
#[cfg(feature = "rdma-client")]
pub use rdma_client::rdma_available;

#[cfg(feature = "rdma-server")]
pub use rdma_server::RdmaPluginServer;

use std::sync::Arc;

use tracing::info;

pub use error::RdmaError;

/// Provider name constants for [`create_provider`].
pub const PROVIDER_AUTO: &str = "auto";
pub const PROVIDER_CUOBJ: &str = "cuobj";
pub const PROVIDER_MOCK: &str = "mock";

/// Returns a one-line summary of compiled-in RDMA providers and their runtime
/// availability, e.g. `"RDMA providers: cuobj (available), mock"`.
pub fn rdma_provider_info() -> String {
    let mut entries: Vec<String> = Vec::new();

    #[cfg(feature = "rdma-client")]
    {
        let status = if rdma_available() { "available" } else { "unavailable" };
        entries.push(format!("cuobj ({status})"));
    }

    entries.push("mock".into());

    format!("RDMA providers: {}", entries.join(", "))
}

/// Build an `Arc<dyn RdmaProvider>` for the named provider and role(s).
///
/// `provider` must be one of:
/// - `"mock"`   → [`MockRdmaProvider`]
/// - `"cuobj"`  → [`RdmaPlugin`] (requires `rdma-client`/`rdma-server` features)
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
    let ep: Arc<dyn RdmaProvider> = match provider {
        "mock" => {
            if debug {
                info!("[rdma] Using MockRdmaProvider (role={role:?})");
            }
            Arc::new(MockRdmaProvider::new(debug, &cfg.shm_path, cfg.buf_size))
        }
        "cuobj" => {
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
            // Try cuobj first; fall back to mock on any error.
            match create_provider("cuobj", debug, role, cfg) {
                Ok(ep) => return Ok(ep),
                Err(_) => return create_provider("mock", debug, role, cfg),
            }
        }
        other => return Err(RdmaError::ProviderUnavailable {
            provider: other.to_owned(),
            reason: "unknown provider (valid: mock, cuobj, auto)".to_owned(),
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
    create_provider(provider, debug, RdmaRole::Client, cfg)
        .map(|p| p as Arc<dyn RdmaClientProvider>)
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
    create_provider(provider, debug, RdmaRole::Server, cfg)
        .map(|p| p as Arc<dyn RdmaServerProvider>)
}

/// Build a [`RdmaWorkerPool`] wrapping the named provider.
///
/// Returns a bounded-concurrency worker pool.  Each of `workers` threads
/// calls `open_channel` on the underlying provider, bounding concurrent
/// RDMA transfers to `workers`.
///
/// `provider` must be one of:
/// - `"mock"`   → pool around [`MockRdmaProvider`] (requires `engine` feature)
/// - `"cuobj"`  → pool around [`RdmaPlugin`] (requires `rdma-server` feature)
/// - `"auto"`  → tries `"cuobj"` first, falls back to `"mock"`
///
/// Returned as `Box<dyn RdmaServerProvider>` — use the trait for all operations.
pub fn create_worker_pool(
    provider: &str,
    debug: bool,
    cfg: &RdmaServerConfig,
    workers: usize,
) -> Result<Box<dyn RdmaServerProvider>, RdmaError> {
    let inner: Arc<dyn RdmaServerProvider> = match provider {
        "mock" => {
            if debug { info!("[rdma] MockRdmaProvider in pool ({workers} workers)"); }
            Arc::new(MockRdmaProvider::new(debug, &cfg.shm_path, cfg.buf_size))
        }
        "cuobj" => {
            #[cfg(feature = "rdma-server")]
            {
                let plugin = RdmaPlugin::new_server(cfg)?;
                if debug { info!("[rdma] RdmaPlugin in pool ({workers} workers)"); }
                Arc::new(plugin) as Arc<dyn RdmaServerProvider>
            }
            #[cfg(not(feature = "rdma-server"))]
            {
                let _ = (debug, cfg, workers);
                return Err(RdmaError::ProviderUnavailable {
                    provider: provider.to_owned(),
                    reason: "compiled without `rdma-server` feature".into(),
                });
            }
        }
        "auto" => {
            match create_worker_pool("cuobj", debug, cfg, workers) {
                Ok(p) => return Ok(p),
                Err(_) => return create_worker_pool("mock", debug, cfg, workers),
            }
        }
        other => return Err(RdmaError::ProviderUnavailable {
            provider: other.to_owned(),
            reason: "unknown provider (valid: mock, cuobj, auto)".to_owned(),
        }),
    };
    Ok(Box::new(worker_pool::RdmaWorkerPool::new(inner, workers)))
}
