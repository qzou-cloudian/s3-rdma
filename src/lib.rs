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
//! | Client | `bind(size, key)` | `RdmaClientChannel` | `prepare_put`, `prepare_get`, `complete_put`, `complete_get` |
//! | Server | `open_channel(key, token)` | `RdmaServerChannel` | `handle_put`, `handle_get` (+ ZC variants) |
//!
//! The channel owns the binding (key, token, buffer registration) so callers
//! don't pass the same parameters repeatedly.  Drop deregisters / cleans up.
//!
//! Concrete implementations:
//!
//! - [`MockRdmaProvider`] — SHM-based simulation, no hardware required; both roles
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
//!   ch = provider.bind(size, key)
//!   put = ch.prepare_put(offset, size)
//!     │  PUT /bucket/key  +  x-amz-rdma-token: <put.token()>
//!     │                          ↓             ch = provider.open_channel(key, put.token())
//!     │                          ↓             n = ch.handle_put(buf, offset, size)
//!     │  ← x-amz-rdma-reply: <reply>
//!   ch.complete_put(put, 200, n)?
//!   drop(ch)  →  deregister memory
//! ```
//!
//! ## Feature flags
//!
//! | Feature         | Effect                                                              |
//! |-----------------|---------------------------------------------------------------------|
//! | `rdma-client` | Enable [`RdmaPluginClient`] / [`RdmaPlugin`] client role   |
//! | `rdma-server` | Enable [`RdmaPluginServer`] / [`RdmaPlugin`] server role   |
//! | (none)          | Only [`MockRdmaProvider`] available                                 |

pub mod error;
pub mod ffi;
pub mod mock;
pub mod provider;
#[cfg(feature = "pyo3")]
mod python;
pub(crate) mod rdma_client;
pub(crate) mod rdma_server;
pub mod token;

// ── Primary public API ────────────────────────────────────────────────────────

pub use mock::MockRdmaProvider;
pub use provider::{
    RdmaClientChannel, RdmaClientChannelExt, RdmaClientProvider, RdmaServerChannel,
    RdmaServerChannelExt, RdmaServerProvider, RdmaTransferHandle,
};
pub use provider::{
    RdmaProvider, RdmaProviderBase, RdmaProviderConfig, RdmaProviderConfigBuilder, RdmaRole,
};
pub use provider::{TelemLevel, TelemState, TelemetryConfig};
pub use provider::{DEFAULT_RDMA_BUF_SIZE, RDMA_REQUIRED_ALIGNMENT};
pub use provider::{
    RDMA_BYTES_HEADER, RDMA_REPLY_HEADER, RDMA_STREAM_HEADER, RDMA_TOKEN_HEADER,
    RDMA_TOKEN_SEPARATOR,
};
#[cfg(any(feature = "rdma-client", feature = "rdma-server"))]
pub use rdma_client::RdmaPlugin;

// ── Supporting types ──────────────────────────────────────────────────────────

pub use provider::{InflightTransfers, PendingTransfer, RdmaServerConfig};
pub use token::RdmaToken;

#[cfg(feature = "rdma-client")]
pub use rdma_client::rdma_available;
#[cfg(feature = "rdma-client")]
pub use rdma_client::RdmaPluginClient;

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
        let status = if rdma_available() {
            "available"
        } else {
            "unavailable"
        };
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
/// - [`RdmaRole::Client`]  — client methods (bind/token/reply) only
/// - [`RdmaRole::Server`]  — server methods (handle_put/handle_get) only
/// - [`RdmaRole::Both`]    — both (typical for a gateway node)
///
/// `cfg` is required when `role` includes a server component (`Server` or
/// `Both`), and ignored for `Client`-only providers.
///
/// Returns [`RdmaError::ProviderUnavailable`] if the name is unrecognised,
/// the required feature is not compiled in, or the underlying SDK fails to
/// initialise.
#[deprecated(since = "0.4.0", note = "use `RdmaProviderConfig::builder()` instead")]
#[allow(deprecated)]
pub fn create_provider(
    provider: &str,
    debug: bool,
    role: RdmaRole,
    cfg: Option<&RdmaServerConfig>,
) -> Result<Arc<dyn RdmaProvider>, RdmaError> {
    let ep: Arc<dyn RdmaProvider> = match provider {
        "mock" => {
            if debug {
                info!("[rdma] Using MockRdmaProvider (role={role:?})");
            }
            let shm = cfg.map_or("/dev/shm", |c| c.shm_path.as_str());
            Arc::new(MockRdmaProvider::new(debug, shm, 0))
        }
        "cuobj" => {
            let result: Result<Arc<dyn RdmaProvider>, RdmaError>;
            #[cfg(any(feature = "rdma-client", feature = "rdma-server"))]
            {
                let ep = match role {
                    RdmaRole::Client => RdmaPlugin::new_client(),
                    RdmaRole::Server => {
                        let c = cfg.ok_or_else(|| RdmaError::ProviderUnavailable {
                            provider: "cuobj".into(),
                            reason: "server config required for server role".into(),
                        })?;
                        RdmaPlugin::new_server(c)
                    }
                    RdmaRole::Both => {
                        let c = cfg.ok_or_else(|| RdmaError::ProviderUnavailable {
                            provider: "cuobj".into(),
                            reason: "server config required for both role".into(),
                        })?;
                        RdmaPlugin::new_both(c)
                    }
                };
                result = ep
                    .map(|e| {
                        if debug {
                            info!("[rdma] Using RdmaPlugin (role={role:?})");
                        }
                        Arc::new(e) as Arc<dyn RdmaProvider>
                    })
                    .map_err(|e| RdmaError::ProviderUnavailable {
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
        other => {
            return Err(RdmaError::ProviderUnavailable {
                provider: other.to_owned(),
                reason: "unknown provider (valid: mock, cuobj, auto)".to_owned(),
            })
        }
    };
    Ok(ep)
}

/// Build an `Arc<dyn RdmaClientProvider>` for the named provider.
///
/// Convenience wrapper around [`create_provider`] with [`RdmaRole::Client`].
/// No server configuration is needed — the provider is client-only.
#[deprecated(
    since = "0.4.0",
    note = "use `RdmaProviderConfig::builder().build_client()` instead"
)]
#[allow(deprecated)]
pub fn create_client_provider(
    provider: &str,
    debug: bool,
) -> Result<Arc<dyn RdmaClientProvider>, RdmaError> {
    create_provider(provider, debug, RdmaRole::Client, None)
        .map(|p| p as Arc<dyn RdmaClientProvider>)
}

/// Build an `Arc<dyn RdmaServerProvider>` for the named provider.
///
/// Convenience wrapper around [`create_provider`] with [`RdmaRole::Server`].
/// The `cfg` provides listen address, port, and buffer size for the server.
#[deprecated(
    since = "0.4.0",
    note = "use `RdmaProviderConfig::builder().build_server()` instead"
)]
#[allow(deprecated)]
pub fn create_server_provider(
    provider: &str,
    debug: bool,
    cfg: &RdmaServerConfig,
) -> Result<Arc<dyn RdmaServerProvider>, RdmaError> {
    create_provider(provider, debug, RdmaRole::Server, Some(cfg))
        .map(|p| p as Arc<dyn RdmaServerProvider>)
}
