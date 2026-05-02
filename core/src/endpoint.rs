//! Unified RDMA endpoint trait combining client and server roles.
//!
//! [`RdmaEndpoint`] replaces the separate [`RdmaProvider`] (client) and
//! [`RdmaServerProvider`] (server) traits with a single type that a node can
//! use for both initiating RDMA transfers (client) and accepting them (server).
//!
//! ## Role model
//!
//! | Role   | Methods used                                                 |
//! |--------|--------------------------------------------------------------|
//! | Client | `register_memory`, `prepare_{put,get}_token`, `process_rdma_reply`, `deregister_memory` |
//! | Server | `handle_put`, `handle_get`, `buf_size`                       |
//! | Both   | All of the above (typical for a gateway node)                |
//!
//! An endpoint reports which roles it supports via [`supports_client`] and
//! [`supports_server`].  Calling a server method on a client-only endpoint
//! (or vice-versa) returns `Err(RdmaError::OperationNotSupported)`.
//!
//! ## Implementations
//!
//! - [`MockRdmaEndpoint`](crate::mock::MockRdmaEndpoint) – `/dev/shm`-backed,
//!   same-host only; supports both client and server roles.
//! - [`CuObjEndpoint`](crate::cuobj_client::CuObjEndpoint) – real NVIDIA cuObject;
//!   each role requires the corresponding plugin to be loadable at runtime.
//!
//! [`supports_client`]: RdmaEndpoint::supports_client
//! [`supports_server`]: RdmaEndpoint::supports_server

use crate::error::RdmaError;
use crate::provider::{
    RdmaCompletionCallback, RdmaCompletionResult, RdmaServerConfig,
    RDMA_BYTES_HEADER, RDMA_REPLY_HEADER, RDMA_TOKEN_HEADER,
    rdma_status_to_error_code,
};

// ── EndpointRole ─────────────────────────────────────────────────────────────

/// Requested role(s) for a new RDMA endpoint.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EndpointRole {
    /// Client only — can initiate RDMA transfers (register memory, generate tokens).
    Client,
    /// Server only — can accept RDMA transfers (handle_put / handle_get).
    Server,
    /// Both client and server roles — typical for a gateway node.
    Both,
}

// ── RdmaEndpoint trait ────────────────────────────────────────────────────────

/// Unified RDMA abstraction covering both client-side token generation and
/// server-side data movement.
///
/// See the [module documentation](self) for an overview.
pub trait RdmaEndpoint: Send + Sync {
    /// Human-readable endpoint name (e.g. `"MockRdmaEndpoint"`).
    fn name(&self) -> &str;

    /// Transport protocol string: `"mock"` or `"cuobj"`.
    fn protocol(&self) -> &str;

    /// Returns `true` if this endpoint supports client-side operations
    /// (`register_memory`, `prepare_*_token`, `process_rdma_reply`).
    fn supports_client(&self) -> bool;

    /// Returns `true` if this endpoint supports server-side operations
    /// (`handle_put`, `handle_get`).
    fn supports_server(&self) -> bool;

    // ── Client-side methods ───────────────────────────────────────────────────

    /// Returns `true` if the memory at `ptr..ptr+size` is eligible for RDMA.
    fn is_memory_suitable(&self, _ptr: *const u8, _size: usize) -> bool {
        false
    }

    /// Register a memory region so the RDMA subsystem can access it.
    ///
    /// # Safety
    /// `ptr` must remain valid until [`deregister_memory`](Self::deregister_memory).
    fn register_memory(&self, _ptr: *mut u8, _size: usize) -> Result<(), RdmaError> {
        Err(RdmaError::OperationNotSupported(
            "register_memory: endpoint is server-only".into(),
        ))
    }

    /// Deregister a previously registered memory region.
    fn deregister_memory(&self, _ptr: *mut u8) -> Result<(), RdmaError> {
        Err(RdmaError::OperationNotSupported(
            "deregister_memory: endpoint is server-only".into(),
        ))
    }

    /// Maximum single-transfer size for the given buffer.
    /// Default: `usize::MAX` (no provider-imposed limit).
    fn get_max_transfer_size(&self, _ptr: *const u8) -> usize {
        usize::MAX
    }

    /// Generate an RDMA descriptor token for a PUT (upload) sub-request.
    ///
    /// The returned bytes are used as the value of the `x-amz-rdma-token`
    /// request header.
    fn prepare_put_token(
        &self,
        _s3_key: &[u8],
        _buffer: *const u8,
        _size: usize,
        _offset: usize,
    ) -> Result<Vec<u8>, RdmaError> {
        Err(RdmaError::OperationNotSupported(
            "prepare_put_token: endpoint is server-only".into(),
        ))
    }

    /// Generate an RDMA descriptor token for a GET (download) sub-request.
    fn prepare_get_token(
        &self,
        _s3_key: &[u8],
        _buffer: *mut u8,
        _size: usize,
        _offset: usize,
    ) -> Result<Vec<u8>, RdmaError> {
        Err(RdmaError::OperationNotSupported(
            "prepare_get_token: endpoint is server-only".into(),
        ))
    }

    /// Process the `x-amz-rdma-reply` numeric status returned by the S3 server.
    ///
    /// The default implementation fires `callback` immediately based on whether
    /// `rdma_status` is a 2xx success or not.  Provider overrides (e.g. mock)
    /// use this to copy SHM data into the buffer before calling back.
    fn process_rdma_reply(
        &self,
        rdma_status: u16,
        _request_token: &[u8],
        _transferred: usize,
        callback: RdmaCompletionCallback,
    ) -> Result<(), RdmaError> {
        callback(RdmaCompletionResult {
            error_code: rdma_status_to_error_code(rdma_status),
            reply_token: vec![],
        });
        Ok(())
    }

    /// Header name for the outbound RDMA token (default: `x-amz-rdma-token`).
    fn rdma_token_header_name(&self) -> &[u8] {
        RDMA_TOKEN_HEADER.as_bytes()
    }

    /// Header name for the server RDMA reply status (default: `x-amz-rdma-reply`).
    fn rdma_reply_header_name(&self) -> &[u8] {
        RDMA_REPLY_HEADER.as_bytes()
    }

    /// Header name for the RDMA byte-count (default: `x-amz-rdma-bytes`).
    fn rdma_bytes_header_name(&self) -> &[u8] {
        RDMA_BYTES_HEADER.as_bytes()
    }

    // ── Server-side methods ───────────────────────────────────────────────────

    /// Size in bytes of the server's per-slot transfer buffer.
    fn buf_size(&self) -> usize {
        0
    }

    /// Handle an RDMA PUT from a client: read `size` bytes from the client's
    /// registered buffer identified by `token` (semantics: RDMA_READ).
    ///
    /// Returns the number of bytes written into `buf`.
    fn handle_put(
        &self,
        _s3_key: &[u8],
        _token: &[u8],
        _buf: &mut [u8],
        _offset: usize,
        _size: usize,
    ) -> Result<usize, RdmaError> {
        Err(RdmaError::OperationNotSupported(
            "handle_put: endpoint is client-only".into(),
        ))
    }

    /// Handle an RDMA GET from a client: write `data` into the client's
    /// registered buffer identified by `token` (semantics: RDMA_WRITE).
    ///
    /// Returns the number of bytes actually written.
    fn handle_get(
        &self,
        _s3_key: &[u8],
        _data: &[u8],
        _token: &[u8],
        _offset: usize,
    ) -> Result<usize, RdmaError> {
        Err(RdmaError::OperationNotSupported(
            "handle_get: endpoint is client-only".into(),
        ))
    }
}

// ── RdmaEndpointConfig ────────────────────────────────────────────────────────

/// Unified configuration for constructing an [`RdmaEndpoint`].
///
/// The `server_cfg` fields are only used when `role` includes the server role.
#[derive(Debug, Clone)]
pub struct RdmaEndpointConfig {
    /// Transport protocol: `"mock"`, `"cuobj"`, or `"auto"`.
    pub provider: String,
    /// Role(s) this endpoint should support.
    pub role: EndpointRole,
    /// Emit debug messages to stderr.
    pub debug: bool,
    /// Server-side configuration (used when `role` includes `Server`).
    pub server: RdmaServerConfig,
}

impl Default for RdmaEndpointConfig {
    fn default() -> Self {
        Self {
            provider: "mock".to_owned(),
            role: EndpointRole::Both,
            debug: false,
            server: RdmaServerConfig::default(),
        }
    }
}
