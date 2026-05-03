use std::fmt;

/// Errors from RDMA provider operations.
#[derive(Debug)]
pub enum RdmaError {
    /// Memory registration failed.
    MemoryRegistrationFailed { ptr: usize, size: usize, reason: String },
    /// Memory deregistration failed.
    MemoryDeregistrationFailed { ptr: usize, reason: String },
    /// Memory is not suitable for RDMA.
    MemoryNotSuitable { ptr: usize, size: usize },
    /// Token generation failed.
    TokenGenerationFailed { key: String, reason: String },
    /// Reply token processing failed.
    ReplyTokenProcessingFailed(String),
    /// cuObject client failed to connect.
    CuObjConnectionFailed(String),
    /// Requested provider is unknown or could not be initialised.
    ProviderUnavailable { provider: String, reason: String },
    /// RDMA data transfer failed (server side).
    TransferFailed(String),
    /// Server buffer is smaller than the size the client requested to transfer.
    /// The caller should signal `x-amz-rdma-reply: 501` so the client retries
    /// with a standard HTTP body.
    BufferTooSmall { server_capacity: usize, requested: usize },
    /// I/O error.
    Io(std::io::Error),
    /// The called method is not supported by this provider in its current role
    /// (e.g. calling `handle_put` on a client-only provider).
    OperationNotSupported(String),
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
            RdmaError::MemoryNotSuitable { .. }
                | RdmaError::TokenGenerationFailed { .. }
                | RdmaError::CuObjConnectionFailed(_)
                | RdmaError::ProviderUnavailable { .. }
                | RdmaError::BufferTooSmall { .. }
        )
    }
}

impl fmt::Display for RdmaError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RdmaError::MemoryRegistrationFailed { ptr, size, reason } => {
                write!(f, "Failed to register memory at {ptr:#x} (size={size}): {reason}")
            }
            RdmaError::MemoryDeregistrationFailed { ptr, reason } => {
                write!(f, "Failed to deregister memory at {ptr:#x}: {reason}")
            }
            RdmaError::MemoryNotSuitable { ptr, size } => {
                write!(f, "Memory at {ptr:#x} (size={size}) not suitable for RDMA")
            }
            RdmaError::TokenGenerationFailed { key, reason } => {
                write!(f, "Failed to generate RDMA token for key '{key}': {reason}")
            }
            RdmaError::ReplyTokenProcessingFailed(msg) => {
                write!(f, "Failed to process RDMA reply token: {msg}")
            }
            RdmaError::CuObjConnectionFailed(msg) => {
                write!(f, "cuObject client failed to connect: {msg}")
            }
            RdmaError::ProviderUnavailable { provider, reason } => {
                write!(f, "RDMA provider '{provider}' unavailable: {reason}")
            }
            RdmaError::TransferFailed(msg) => {
                write!(f, "RDMA transfer failed: {msg}")
            }
            RdmaError::BufferTooSmall { server_capacity, requested } => {
                write!(
                    f,
                    "server buffer too small: capacity={server_capacity}, transfer_size={requested}"
                )
            }
            RdmaError::Io(e) => write!(f, "I/O error: {e}"),
            RdmaError::OperationNotSupported(msg) => write!(f, "Operation not supported: {msg}"),
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
