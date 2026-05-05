//! Generic RDMA engine trait.
//!
//! An engine is a bounded worker pool for server-side RDMA operations.
//! Each provider implements its own engine, optimized for its backend —
//! mock uses a simple thread pool, cuobj pre-allocates channels and buffers
//! per worker.
//!
//! The trait extends [`RdmaServerProvider`] so any engine can be used
//! wherever server-side RDMA operations are needed.

use crate::provider::RdmaServerProvider;

/// Generic RDMA engine: a bounded worker pool for server-side RDMA transfers.
///
/// Each provider implements its own engine.  The trait extends
/// [`RdmaServerProvider`] with engine-specific queries.
///
/// Concrete engine types (`MockRdmaEngine`, `CuObjEngine`) are not part of
/// the public API — use this trait to work with any engine generically.
pub trait RdmaEngine: RdmaServerProvider {
    /// Number of worker threads in the pool.
    fn worker_count(&self) -> usize;
}
