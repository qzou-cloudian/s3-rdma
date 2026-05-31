use std::fmt;

/// Errors from RDMA provider operations.
#[derive(Debug)]
pub enum RdmaError {
    /// Memory registration failed.
    MemoryRegistrationFailed {
        ptr: usize,
        size: usize,
        reason: String,
    },
    /// Memory deregistration failed.
    MemoryDeregistrationFailed { ptr: usize, reason: String },
    /// Memory is not suitable for RDMA.
    MemoryNotSuitable { ptr: usize, size: usize },
    /// All client buffer-pool slots are leased and the wait timed out.
    /// Benign capacity pressure — the caller should fall back to plain HTTP
    /// for this transfer and retry RDMA later.
    PoolExhausted { slots: usize, wait_ms: u64 },
    /// The requested transfer is larger than a single client buffer-pool slot.
    /// The caller should fall back to plain HTTP for this transfer.
    RequestTooLarge { requested: usize, slot_size: usize },
    /// Token generation failed.
    TokenGenerationFailed { key: String, reason: String },
    /// Reply token processing failed.
    ReplyTokenProcessingFailed { reason: String },
    /// RDMA plugin failed to load or connect.
    RdmaPluginLoadFailed { reason: String },
    /// Requested provider is unknown or could not be initialised.
    ProviderUnavailable { provider: String, reason: String },
    /// RDMA data transfer failed (server side).
    TransferFailed { reason: String },
    /// Server buffer is smaller than the size the client requested to transfer.
    /// The caller should signal `x-amz-rdma-reply: 501` so the client retries
    /// with a standard HTTP body.
    BufferTooSmall {
        server_capacity: usize,
        requested: usize,
    },
    /// I/O error.
    Io(std::io::Error),
    /// The called method is not supported by this provider in its current role
    /// (e.g. calling `handle_put` on a client-only provider).
    OperationNotSupported { reason: String },
    /// Telemetry operation failed.
    TelemetryError { reason: String },
}

impl RdmaError {
    /// Returns `true` for errors where falling back to plain (non-RDMA) HTTP
    /// is safe — e.g. memory not suitable, token failure, library unavailable,
    /// or the server buffer was too small for the requested transfer.
    ///
    /// Returns `false` for hard I/O or transfer errors where the caller
    /// should propagate the failure.
    pub fn is_fallback_eligible(&self) -> bool {
        matches!(
            self,
            RdmaError::PoolExhausted { .. }
                | RdmaError::RequestTooLarge { .. }
                | RdmaError::MemoryNotSuitable { .. }
                | RdmaError::TokenGenerationFailed { .. }
                | RdmaError::RdmaPluginLoadFailed { .. }
                | RdmaError::ProviderUnavailable { .. }
                | RdmaError::BufferTooSmall { .. }
                | RdmaError::TelemetryError { .. }
        )
    }
}

impl fmt::Display for RdmaError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RdmaError::MemoryRegistrationFailed { ptr, size, reason } => {
                write!(
                    f,
                    "Failed to register memory at {ptr:#x} (size={size}): {reason}"
                )
            }
            RdmaError::MemoryDeregistrationFailed { ptr, reason } => {
                write!(f, "Failed to deregister memory at {ptr:#x}: {reason}")
            }
            RdmaError::MemoryNotSuitable { ptr, size } => {
                write!(
                    f,
                    "Memory at {ptr:#x} (size={size}) not suitable for RDMA \
                           (must be non-null, {}-byte aligned, and pinnable)",
                    crate::provider::RDMA_REQUIRED_ALIGNMENT
                )
            }
            RdmaError::PoolExhausted { slots, wait_ms } => {
                write!(
                    f,
                    "RDMA client buffer pool exhausted (slots={slots}, waited {wait_ms}ms)"
                )
            }
            RdmaError::RequestTooLarge {
                requested,
                slot_size,
            } => {
                write!(
                    f,
                    "requested transfer ({requested}) exceeds client pool slot size ({slot_size})"
                )
            }
            RdmaError::TokenGenerationFailed { key, reason } => {
                write!(f, "Failed to generate RDMA token for key '{key}': {reason}")
            }
            RdmaError::ReplyTokenProcessingFailed { reason } => {
                write!(f, "Failed to process RDMA reply token: {reason}")
            }
            RdmaError::RdmaPluginLoadFailed { reason } => {
                write!(f, "RDMA plugin load failed: {reason}")
            }
            RdmaError::ProviderUnavailable { provider, reason } => {
                write!(f, "RDMA provider '{provider}' unavailable: {reason}")
            }
            RdmaError::TransferFailed { reason } => {
                write!(f, "RDMA transfer failed: {reason}")
            }
            RdmaError::BufferTooSmall {
                server_capacity,
                requested,
            } => {
                write!(
                    f,
                    "server buffer too small: capacity={server_capacity}, transfer_size={requested}"
                )
            }
            RdmaError::Io(e) => write!(f, "I/O error: {e}"),
            RdmaError::OperationNotSupported { reason } => {
                write!(f, "Operation not supported: {reason}")
            }
            RdmaError::TelemetryError { reason } => write!(f, "Telemetry error: {reason}"),
        }
    }
}

impl std::error::Error for RdmaError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            RdmaError::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl From<std::io::Error> for RdmaError {
    fn from(e: std::io::Error) -> Self {
        RdmaError::Io(e)
    }
}
