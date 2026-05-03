// SPDX-FileCopyrightText: Copyright (c) 2025 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Generic pre-registered buffer pool for RDMA transfers.
//!
//! Provides a fixed-size pool of 4 KiB-aligned host buffers that are
//! optionally pre-registered with an [`RdmaClientProvider`] at construction
//! time.  The three usage modes are:
//!
//! | Mode      | Builder method                 | Behaviour                               |
//! |-----------|--------------------------------|-----------------------------------------|
//! | **bytes** | neither called (default)       | Plain heap; no RDMA operations          |
//! | **mock**  | `with_client(mock_provider)`   | Each slot registered via SHM at startup |
//! | **cuobj** | `with_client(cuobj_provider)`  | Each slot pinned with IOMMU at startup  |
//!
//! Use `with_client(Arc<dyn RdmaClientProvider>)` when you already have a
//! client provider.  Use `with_rdma(Arc<dyn RdmaProvider>)` when you only
//! have the combined trait — it wraps the provider in a thin bridge.
//!
//! In all modes the per-transfer cost is `acquire → write/read → release`.
//! Registration overhead (`register_memory` / `deregister_memory`) is paid
//! once at pool construction and never again.
//!
//! # Quick start
//!
//! ```no_run
//! # use std::sync::Arc;
//! # use s3_rdma::pool::BufferPool;
//! // Plain-bytes pool (no RDMA provider):
//! let pool = Arc::new(BufferPool::builder().count(8).size(4 * 1024 * 1024).build().unwrap());
//! let guard = pool.try_acquire_owned().unwrap();
//! guard.as_slice(); // &[u8]
//! ```

use std::alloc::{Layout, alloc, dealloc};
use std::sync::{Arc, Condvar, Mutex};

use crossbeam_queue::ArrayQueue;
use tokio::sync::Semaphore;

use crate::error::RdmaError;
use crate::provider::{RdmaClientProvider, RdmaProvider};

// ─────────────────────────────────────────────────────────────────────────────
// Error type
// ─────────────────────────────────────────────────────────────────────────────

/// Errors returned by buffer-pool operations.
#[derive(Debug)]
pub enum BufferPoolError {
    /// Allocation or RDMA registration failed during pool construction.
    ConstructionFailed { slot: usize, reason: String },
    /// `register_memory` failed for a slot during construction.
    RegistrationFailed { slot: usize, source: RdmaError },
}

impl std::fmt::Display for BufferPoolError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ConstructionFailed { slot, reason } =>
                write!(f, "pool construction failed at slot {slot}: {reason}"),
            Self::RegistrationFailed { slot, source } =>
                write!(f, "pool construction failed at slot {slot}: RDMA registration error: {source}"),
        }
    }
}

impl std::error::Error for BufferPoolError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::RegistrationFailed { source, .. } => Some(source),
            _ => None,
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Internal slot
// ─────────────────────────────────────────────────────────────────────────────

/// Alignment required for RDMA: 4 KiB.
const RDMA_ALIGN: usize = 4096;

struct Slot {
    ptr:    *mut u8,
    size:   usize,
    layout: Layout,
}

// SAFETY: Slot bytes are accessed only while a guard has exclusive ownership.
unsafe impl Send for Slot {}
unsafe impl Sync for Slot {}

// ─────────────────────────────────────────────────────────────────────────────
// ProviderClientBridge — adapts Arc<dyn RdmaProvider> to RdmaClientProvider
// ─────────────────────────────────────────────────────────────────────────────

/// Bridges an `Arc<dyn RdmaProvider>` into an `Arc<dyn RdmaClientProvider>`.
///
/// Forwards `name`, `protocol`, `register_memory`, and `deregister_memory` to
/// the inner provider.  All other client methods use their default implementations.
/// This is acceptable for the buffer pool, which only ever calls the forwarded methods.
struct ProviderClientBridge(Arc<dyn RdmaProvider>);

impl crate::provider::RdmaClientProvider for ProviderClientBridge {
    fn name(&self) -> &str { self.0.name() }
    fn protocol(&self) -> &str { self.0.protocol() }

    fn register_memory(&self, ptr: *mut u8, size: usize) -> Result<(), RdmaError> {
        self.0.register_memory(ptr, size)
    }
    fn deregister_memory(&self, ptr: *mut u8) -> Result<(), RdmaError> {
        self.0.deregister_memory(ptr)
    }
}

// SAFETY: the inner Arc<dyn RdmaProvider> is Send + Sync.
unsafe impl Send for ProviderClientBridge {}
unsafe impl Sync for ProviderClientBridge {}

// ─────────────────────────────────────────────────────────────────────────────
// BufferPool
// ─────────────────────────────────────────────────────────────────────────────

/// A thread-safe pool of pre-registered RDMA buffers.
///
/// Buffers are acquired with [`try_acquire_owned`][Self::try_acquire_owned]
/// (non-blocking) or [`acquire_owned`][Self::acquire_owned] (async) and
/// returned to the pool when the [`OwnedBufferGuard`] is dropped.
///
/// Create via [`BufferPool::builder()`].
pub struct BufferPool {
    slots:      Vec<Slot>,
    free_queue: ArrayQueue<usize>,
    sem:        Semaphore,
    parking_mu: Mutex<()>,
    parking_cv: Condvar,
    /// Provider used to register/deregister slots.  `None` → plain-bytes mode.
    rdma:       Option<Arc<dyn RdmaClientProvider>>,
}

// SAFETY: `free_queue` and `sem` provide lock-free/atomic synchronisation;
// raw pointers in `Slot` are only dereferenced by live guards with exclusive
// logical ownership.
unsafe impl Send for BufferPool {}
unsafe impl Sync for BufferPool {}

impl BufferPool {
    /// Return a [`BufferPoolBuilder`] for constructing a pool.
    pub fn builder() -> BufferPoolBuilder {
        BufferPoolBuilder::default()
    }

    // ── Acquire ──────────────────────────────────────────────────────────────

    /// Pop a free slot index after a semaphore permit has been consumed.
    fn pop_slot(self: &Arc<Self>) -> OwnedBufferGuard {
        let idx  = self.free_queue.pop().expect("sem/queue invariant violated");
        let ptr  = self.slots[idx].ptr;
        let size = self.slots[idx].size;
        OwnedBufferGuard { pool: Arc::clone(self), idx, ptr, size, len: size }
    }

    /// Try to acquire a free buffer without blocking.
    ///
    /// Returns `None` if all buffers are in use.
    pub fn try_acquire_owned(self: &Arc<Self>) -> Option<OwnedBufferGuard> {
        let permit = self.sem.try_acquire().ok()?;
        permit.forget();
        Some(self.pop_slot())
    }

    /// Try to acquire a free buffer, waiting up to `timeout` for one.
    ///
    /// Returns `None` if no buffer became available within the timeout.
    pub fn try_acquire_owned_timeout(
        self: &Arc<Self>,
        timeout: std::time::Duration,
    ) -> Option<OwnedBufferGuard> {
        let deadline = std::time::Instant::now() + timeout;
        loop {
            if let Ok(permit) = self.sem.try_acquire() {
                permit.forget();
                return Some(self.pop_slot());
            }
            let remaining = deadline.saturating_duration_since(std::time::Instant::now());
            if remaining.is_zero() {
                return None;
            }
            let lock = self.parking_mu.lock().unwrap();
            if let Ok(permit) = self.sem.try_acquire() {
                drop(lock);
                permit.forget();
                return Some(self.pop_slot());
            }
            let remaining = deadline.saturating_duration_since(std::time::Instant::now());
            if remaining.is_zero() {
                return None;
            }
            let (g, timed_out) = self.parking_cv.wait_timeout(lock, remaining).unwrap();
            drop(g);
            if timed_out.timed_out() {
                if let Ok(permit) = self.sem.try_acquire() {
                    permit.forget();
                    return Some(self.pop_slot());
                }
                return None;
            }
        }
    }

    /// Async-friendly acquire.  Awaits a semaphore permit (zero cost when slots
    /// are available) then pops a slot index from the lock-free queue.
    pub async fn acquire_owned(self: &Arc<Self>) -> OwnedBufferGuard {
        let permit = self.sem.acquire().await.expect("BufferPool semaphore closed");
        permit.forget();
        self.pop_slot()
    }

    /// Acquire a free buffer, **blocking the current thread** until one is
    /// available.  Prefer [`acquire_owned`][Self::acquire_owned] in async
    /// contexts; call this from `spawn_blocking` if needed.
    pub fn acquire_owned_blocking(self: &Arc<Self>) -> OwnedBufferGuard {
        loop {
            if let Ok(permit) = self.sem.try_acquire() {
                permit.forget();
                return self.pop_slot();
            }
            let lock = self.parking_mu.lock().unwrap();
            if let Ok(permit) = self.sem.try_acquire() {
                drop(lock);
                permit.forget();
                return self.pop_slot();
            }
            drop(self.parking_cv.wait(lock).unwrap());
        }
    }

    // ── Queries ──────────────────────────────────────────────────────────────

    /// Total number of buffer slots.
    pub fn capacity(&self) -> usize { self.slots.len() }

    /// Number of buffer slots currently not in use.
    pub fn available(&self) -> usize { self.free_queue.len() }

    /// Whether this pool has an RDMA provider attached.
    pub fn is_rdma_registered(&self) -> bool { self.rdma.is_some() }

    // ── Internal ─────────────────────────────────────────────────────────────

    pub(crate) fn release(&self, idx: usize) {
        self.free_queue.push(idx).ok().expect("double-release of buffer slot");
        self.sem.add_permits(1);
        let _g = self.parking_mu.lock().unwrap();
        self.parking_cv.notify_one();
    }
}

impl Drop for BufferPool {
    fn drop(&mut self) {
        for slot in &self.slots {
            if let Some(rdma) = &self.rdma {
                let _ = rdma.deregister_memory(slot.ptr);
            }
            // SAFETY: allocated via alloc(); not null; layout matches.
            unsafe { dealloc(slot.ptr, slot.layout) };
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// OwnedBufferGuard
// ─────────────────────────────────────────────────────────────────────────────

/// RAII guard that holds exclusive ownership of one pool slot via `Arc`.
///
/// - `'static` — can be held across `.await` points or sent to `spawn_blocking`.
/// - Implements `AsRef<[u8]>` so it can be wrapped in
///   `bytes::Bytes::from_owner(guard)` for a zero-copy hand-off.
/// - Released back to the pool on `Drop`.
pub struct OwnedBufferGuard {
    pool: Arc<BufferPool>,
    idx:  usize,
    ptr:  *mut u8,
    size: usize,
    /// Valid bytes from `ptr[0..len]`.  Starts == `size`; call
    /// [`set_len`][Self::set_len] after a partial write.
    len:  usize,
}

// SAFETY: the guard has exclusive access to the slot bytes.
unsafe impl Send for OwnedBufferGuard {}
unsafe impl Sync for OwnedBufferGuard {}

impl OwnedBufferGuard {
    /// Total capacity of the slot in bytes.
    #[inline] pub fn capacity(&self) -> usize { self.size }
    /// Number of valid bytes (see [`set_len`][Self::set_len]).
    #[inline] pub fn len(&self) -> usize { self.len }
    /// `true` if `len == 0`.
    #[inline] pub fn is_empty(&self) -> bool { self.len == 0 }
    /// Raw const pointer to the slot's memory.
    #[inline] pub fn as_ptr(&self) -> *const u8 { self.ptr }
    /// Raw mutable pointer to the slot's memory.
    #[inline] pub fn as_mut_ptr(&mut self) -> *mut u8 { self.ptr }

    /// Immutable view of the valid region (`ptr[..len]`).
    #[inline]
    pub fn as_slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
    }

    /// Mutable view of the **full** capacity (`ptr[..size]`).
    ///
    /// Use this for writing; call [`set_len`][Self::set_len] afterwards.
    #[inline]
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.ptr, self.size) }
    }

    /// Restrict the valid region to the first `n` bytes.
    ///
    /// # Panics
    ///
    /// Panics if `n > capacity()`.
    #[inline]
    pub fn set_len(&mut self, n: usize) {
        assert!(n <= self.size, "OwnedBufferGuard::set_len({n}) > capacity({})", self.size);
        self.len = n;
    }
}

impl AsRef<[u8]> for OwnedBufferGuard {
    #[inline]
    fn as_ref(&self) -> &[u8] { self.as_slice() }
}

impl AsMut<[u8]> for OwnedBufferGuard {
    #[inline]
    fn as_mut(&mut self) -> &mut [u8] { self.as_mut_slice() }
}

impl Drop for OwnedBufferGuard {
    fn drop(&mut self) {
        self.pool.release(self.idx);
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// BufferPoolBuilder
// ─────────────────────────────────────────────────────────────────────────────

/// Builder for [`BufferPool`].  Obtain via [`BufferPool::builder()`].
pub struct BufferPoolBuilder {
    count: usize,
    size:  usize,
    rdma:  Option<Arc<dyn RdmaClientProvider>>,
}

impl Default for BufferPoolBuilder {
    fn default() -> Self { Self { count: 0, size: 0, rdma: None } }
}

impl BufferPoolBuilder {
    /// Number of buffer slots (must be ≥ 1).
    pub fn count(mut self, n: usize) -> Self { self.count = n; self }

    /// Size of each buffer slot in bytes.
    pub fn size(mut self, bytes: usize) -> Self { self.size = bytes; self }

    /// Attach a client-role RDMA provider.  All slots are registered with it
    /// at build time so per-transfer `register_memory`/`deregister_memory` is
    /// avoided.
    ///
    /// Use [`with_rdma`][Self::with_rdma] if you have an `Arc<dyn RdmaProvider>`
    /// (the combined trait).
    pub fn with_client(mut self, rdma: Option<Arc<dyn RdmaClientProvider>>) -> Self {
        self.rdma = rdma;
        self
    }

    /// Attach a combined RDMA provider.
    ///
    /// Convenience for callers that hold an `Arc<dyn RdmaProvider>`.
    /// Only the `register_memory`/`deregister_memory` methods are called on
    /// the provider; the server-side methods are never invoked.
    pub fn with_rdma(mut self, rdma: Option<Arc<dyn RdmaProvider>>) -> Self {
        self.rdma = rdma.map(|r| Arc::new(ProviderClientBridge(r)) as Arc<dyn RdmaClientProvider>);
        self
    }

    /// Allocate slots and (if a provider is set) register them.
    ///
    /// # Errors
    ///
    /// Returns an error if any slot cannot be allocated or registered.
    pub fn build(self) -> Result<BufferPool, BufferPoolError> {
        if self.count < 1 {
            return Err(BufferPoolError::ConstructionFailed {
                slot: 0,
                reason: "pool count must be ≥ 1".into(),
            });
        }
        if self.size < 1 {
            return Err(BufferPoolError::ConstructionFailed {
                slot: 0,
                reason: "pool slot size must be ≥ 1".into(),
            });
        }

        let mut slots = Vec::with_capacity(self.count);
        let free_queue = ArrayQueue::new(self.count);

        for i in 0..self.count {
            let layout = Layout::from_size_align(self.size, RDMA_ALIGN).map_err(|_| {
                BufferPoolError::ConstructionFailed {
                    slot: i,
                    reason: format!("invalid layout: size={}, align={RDMA_ALIGN}", self.size),
                }
            })?;
            let ptr = unsafe { alloc(layout) };
            if ptr.is_null() {
                return Err(BufferPoolError::ConstructionFailed {
                    slot: i,
                    reason: "std::alloc::alloc returned null".into(),
                });
            }
            if let Some(rdma) = &self.rdma {
                rdma.register_memory(ptr, self.size)
                    .map_err(|e| BufferPoolError::RegistrationFailed { slot: i, source: e })?;
            }
            slots.push(Slot { ptr, size: self.size, layout });
            free_queue.push(i).ok().expect("ArrayQueue capacity exceeded during build");
        }

        Ok(BufferPool {
            slots,
            free_queue,
            sem:        Semaphore::new(self.count),
            parking_mu: Mutex::new(()),
            parking_cv: Condvar::new(),
            rdma:       self.rdma,
        })
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn plain_pool(count: usize, size: usize) -> Arc<BufferPool> {
        Arc::new(BufferPool::builder().count(count).size(size).build().unwrap())
    }

    #[test]
    fn basic_acquire_release() {
        let pool = plain_pool(2, 1024);
        let g1 = pool.try_acquire_owned().unwrap();
        let g2 = pool.try_acquire_owned().unwrap();
        assert!(pool.try_acquire_owned().is_none()); // pool exhausted
        drop(g1);
        let g3 = pool.try_acquire_owned().unwrap();
        assert!(pool.try_acquire_owned().is_none());
        drop(g2);
        drop(g3);
        assert_eq!(pool.available(), 2);
    }

    #[test]
    fn set_len_and_slice() {
        let pool = plain_pool(1, 4096);
        let mut g = pool.try_acquire_owned().unwrap();
        assert_eq!(g.capacity(), 4096);
        assert_eq!(g.len(), 4096);
        g.as_mut_slice()[..4].copy_from_slice(b"test");
        g.set_len(4);
        assert_eq!(g.len(), 4);
        assert_eq!(g.as_slice(), b"test");
    }

    #[test]
    fn slots_are_4k_aligned() {
        let pool = plain_pool(4, 8192);
        let mut guards = Vec::new();
        for _ in 0..4 {
            let g = pool.try_acquire_owned().unwrap();
            assert_eq!(g.as_ptr() as usize % RDMA_ALIGN, 0, "slot not 4 KiB aligned");
            guards.push(g);
        }
    }

    #[test]
    fn blocking_acquire_succeeds() {
        let pool = plain_pool(1, 256);
        let g = pool.acquire_owned_blocking();
        drop(g);
        let _ = pool.acquire_owned_blocking();
    }

    #[test]
    fn async_acquire_succeeds() {
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        rt.block_on(async {
            let pool = plain_pool(2, 256);
            let g1 = pool.acquire_owned().await;
            let g2 = pool.acquire_owned().await;
            drop(g1);
            drop(g2);
            assert_eq!(pool.available(), 2);
        });
    }

    #[test]
    fn timeout_returns_none_when_pool_exhausted() {
        let pool = plain_pool(1, 256);
        let _g = pool.try_acquire_owned().unwrap();
        let result = pool.try_acquire_owned_timeout(std::time::Duration::from_millis(10));
        assert!(result.is_none());
    }

    #[test]
    fn guard_as_ref_impl() {
        let pool = plain_pool(1, 64);
        let mut g = pool.try_acquire_owned().unwrap();
        g.as_mut_slice()[..3].copy_from_slice(b"abc");
        g.set_len(3);
        let r: &[u8] = g.as_ref();
        assert_eq!(r, b"abc");
    }
}
