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

pub use error::RdmaError;

/// Provider name constants for [`RdmaProviderConfig::builder`].
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
