//! cuObject RDMA client backend — NVIDIA SDK hardware implementation.
//!
//! Uses a process-global lazy pool of registered buffers and creates the
//! matching cuObj token lazily in `prepare_put` / `prepare_get`.  Token
//! generation is intentionally operation-specific; a PUT should not create an
//! unused GET token during bind.
//!
//! # Why jemalloc must be disabled
//!
//! `tikv-jemallocator` intercepts `malloc`/`free` globally.  cuobj's C++ code
//! uses glibc `new`/`delete` internally; with jemalloc intercepting `free`,
//! cuobj's deallocation corrupts the allocator after ~3 operations.
//! Build the relay binary with `--no-default-features --features rdma`.
//!
//! # Buffer
//!
//! Pool slots are mmap'd and registered once, then reused for later channels.
//! Registered buffers are kept for the process lifetime (`cuMemObjPutDescriptor`
//! also triggers the same corruption pattern).

use std::ffi::c_void;
use std::os::raw::c_char;
use std::panic::{self, AssertUnwindSafe};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, OnceLock};
use std::time::{Duration, Instant};

use tracing::{error, info, warn};

use cuobj::CuObjClient;
use cuobj_sys::{self, CUOBJ_GET, CUOBJ_PUT, CU_OBJ_SUCCESS};
use s3_rdma::error::RdmaError;
use s3_rdma::ffi::{
    ByteCursor, CTelemetryConfig, RdmaClientInitParams, RdmaProviderVtable, RDMA_BIND_ERR_EXHAUSTED,
    RDMA_BIND_ERR_GENERIC, RDMA_BIND_ERR_NOT_READY, RDMA_BIND_ERR_REGISTER, RDMA_BIND_ERR_TOO_LARGE,
    RDMA_BIND_OK,
};
use s3_rdma::provider::{
    RdmaClientChannel, RdmaTransferHandle, DEFAULT_RDMA_BUF_SIZE, DEFAULT_RDMA_CLIENT_POOL_SLOTS,
    DEFAULT_RDMA_CLIENT_POOL_WAIT_MS,
};

static PROVIDER_NAME: &[u8] = b"s3-rdma-cuobj\0";

/// Page size for mmap allocations (4 KiB).
const PAGE_SIZE: usize = 4096;

fn mmap_len(size: usize) -> usize {
    size.div_ceil(PAGE_SIZE) * PAGE_SIZE
}

fn mmap_alloc(size: usize) -> *mut u8 {
    let pages = mmap_len(size);
    if pages == 0 {
        return std::ptr::null_mut();
    }
    let ptr = unsafe {
        libc::mmap(
            std::ptr::null_mut(),
            pages,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_ANONYMOUS | libc::MAP_PRIVATE,
            -1,
            0,
        )
    };
    if ptr == libc::MAP_FAILED {
        std::ptr::null_mut()
    } else {
        ptr as *mut u8
    }
}

fn mmap_free(ptr: *mut u8, size: usize) {
    if !ptr.is_null() {
        let _ = unsafe { libc::munmap(ptr as *mut c_void, mmap_len(size)) };
    }
}

// ── Global lazy bounded buffer pool ───────────────────────────────────────────

/// Upper bound on the pool wait, in milliseconds (1 hour).  Caps a misconfigured
/// or sign-flipped `client_pool_wait_ms` (e.g. a negative Java `jlong` cast to
/// `u64`) so the wait math can never overflow or hang indefinitely.
const WAIT_MS_CAP: u64 = 60 * 60 * 1000;

/// Caller-requested pool configuration (carried from `init` / first bind).  The
/// live pool applies these on first init and afterwards only *grows* its caps
/// towards them (see [`ClientPool::grow_caps`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct PoolConfig {
    slot_size: usize,
    max_slots: usize,
    wait_ms: u64,
}

impl PoolConfig {
    fn default() -> Self {
        Self {
            slot_size: DEFAULT_RDMA_BUF_SIZE,
            max_slots: DEFAULT_RDMA_CLIENT_POOL_SLOTS,
            wait_ms: DEFAULT_RDMA_CLIENT_POOL_WAIT_MS,
        }
    }

    unsafe fn from_raw(params: *mut c_void) -> Self {
        if params.is_null() {
            return Self::default();
        }
        let p = unsafe { &*(params as *const RdmaClientInitParams) };
        Self {
            slot_size: if p.client_buf_size == 0 {
                DEFAULT_RDMA_BUF_SIZE
            } else {
                p.client_buf_size
            },
            max_slots: p.client_pool_slots.max(1),
            wait_ms: p.client_pool_wait_ms.min(WAIT_MS_CAP),
        }
    }
}

/// State of a single pool slot.  Slots are never removed or moved once pushed,
/// so a slot's index is stable for the pool's lifetime (a `Ready` buffer can't
/// be freed anyway — the cuObj allocator corrupts on deregistration).
enum SlotState {
    /// No buffer yet (never allocated, or a prior allocation failed).  Eligible
    /// for a (re)allocation attempt.
    Empty,
    /// Index reserved; buffer allocation in progress with the lock released.
    Installing,
    /// Buffer allocated and registered.
    Ready { ptr: usize, size: usize },
}

struct PoolSlot {
    state: SlotState,
    leased: bool,
}

struct PoolInner {
    slots: Vec<PoolSlot>,
    /// Current slot size; grows towards the largest requested `slot_size`.
    slot_size: usize,
    /// Current slot cap; grows towards the largest requested `max_slots`.
    max_slots: usize,
}

struct ClientPool {
    client_raw: usize,
    inner: Mutex<PoolInner>,
    available: Condvar,
    /// Wait budget for a free slot; fixed at first init.
    wait_ms: u64,
}

unsafe impl Send for ClientPool {}
unsafe impl Sync for ClientPool {}

impl ClientPool {
    /// Lock the inner state, recovering from a poisoned mutex rather than
    /// panicking — a panic must never unwind across the C FFI boundary.
    fn lock(&self) -> MutexGuard<'_, PoolInner> {
        self.inner.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// Raise `slot_size` / `max_slots` towards `config`; never shrink.
    fn grow_caps(&self, config: PoolConfig) {
        let mut g = self.lock();
        if config.slot_size > g.slot_size {
            g.slot_size = config.slot_size;
        }
        if config.max_slots > g.max_slots {
            g.max_slots = config.max_slots;
        }
        if config.slot_size < g.slot_size || config.max_slots < g.max_slots {
            warn!(
                "[s3-rdma] client pool already initialised with larger caps \
                 (slot_size={} max_slots={}); ignoring smaller request \
                 (slot_size={} max_slots={})",
                g.slot_size, g.max_slots, config.slot_size, config.max_slots
            );
        }
    }

    fn lease(self: &Arc<Self>, requested_size: usize) -> Result<PoolLease, RdmaError> {
        let start = Instant::now();
        let mut g = self.lock();

        loop {
            if requested_size > g.slot_size {
                return Err(RdmaError::RequestTooLarge {
                    requested: requested_size,
                    slot_size: g.slot_size,
                });
            }

            // Reuse a ready, unleased slot.  `Installing` slots are never
            // selected, so an in-flight allocation can't be handed out twice.
            if let Some(slot_index) = g
                .slots
                .iter()
                .position(|s| !s.leased && matches!(s.state, SlotState::Ready { .. }))
            {
                let slot = &mut g.slots[slot_index];
                slot.leased = true;
                let (ptr, size) = match slot.state {
                    SlotState::Ready { ptr, size } => (ptr, size),
                    _ => unreachable!("scan only selects Ready slots"),
                };
                return Ok(PoolLease {
                    pool: Arc::clone(self),
                    slot_index,
                    ptr,
                    size,
                });
            }

            // Reuse an `Empty` slot, or grow.  Reserve a stable index, mark it
            // `Installing+leased` (so other threads skip it), allocate UNLOCKED,
            // then install.  On failure reset that index to `Empty` in place —
            // never remove a slot, so concurrent reservations keep their index.
            let reserved = g
                .slots
                .iter()
                .position(|s| !s.leased && matches!(s.state, SlotState::Empty));
            let slot_index = match reserved {
                Some(i) => {
                    g.slots[i].state = SlotState::Installing;
                    g.slots[i].leased = true;
                    Some(i)
                }
                None if g.slots.len() < g.max_slots => {
                    g.slots.push(PoolSlot {
                        state: SlotState::Installing,
                        leased: true,
                    });
                    Some(g.slots.len() - 1)
                }
                None => None,
            };
            if let Some(slot_index) = slot_index {
                let slot_size = g.slot_size;
                drop(g);

                let alloc = allocate_buffer(self.client_raw, slot_size);
                g = self.lock();
                match alloc {
                    Ok((ptr, size)) => {
                        g.slots[slot_index].state = SlotState::Ready { ptr, size };
                        return Ok(PoolLease {
                            pool: Arc::clone(self),
                            slot_index,
                            ptr,
                            size,
                        });
                    }
                    Err(e) => {
                        g.slots[slot_index].state = SlotState::Empty;
                        g.slots[slot_index].leased = false;
                        drop(g);
                        self.available.notify_all();
                        return Err(e);
                    }
                }
            }

            // Pool full: wait for a release, bounded by `wait_ms` with no
            // `Instant + Duration` (which panics on overflow).
            if self.wait_ms == 0 {
                return Err(self.exhausted_error(g.max_slots));
            }
            let elapsed = start.elapsed().as_millis().min(u128::from(u64::MAX)) as u64;
            if elapsed >= self.wait_ms {
                return Err(self.exhausted_error(g.max_slots));
            }
            let remaining = Duration::from_millis(self.wait_ms - elapsed);
            g = self
                .available
                .wait_timeout(g, remaining)
                .unwrap_or_else(|p| p.into_inner())
                .0;
        }
    }

    fn release(&self, slot_index: usize) {
        let mut g = self.lock();
        if let Some(slot) = g.slots.get_mut(slot_index) {
            slot.leased = false;
            self.available.notify_one();
        }
    }

    fn exhausted_error(&self, slots: usize) -> RdmaError {
        RdmaError::PoolExhausted {
            slots,
            wait_ms: self.wait_ms,
        }
    }
}

/// mmap + cuObj-register one slot buffer.  On register failure the mmap is
/// released and a (hard) [`RdmaError::MemoryRegistrationFailed`] is returned.
fn allocate_buffer(client_raw: usize, slot_size: usize) -> Result<(usize, usize), RdmaError> {
    let ptr = mmap_alloc(slot_size);
    if ptr.is_null() {
        return Err(RdmaError::MemoryRegistrationFailed {
            ptr: 0,
            size: slot_size,
            reason: "mmap returned null".into(),
        });
    }

    let rc = unsafe {
        cuobj_sys::cuobj_client_mem_get_descriptor(
            client_raw as *mut c_void,
            ptr as *mut c_void,
            slot_size,
        )
    };
    if rc != CU_OBJ_SUCCESS {
        mmap_free(ptr, slot_size);
        return Err(RdmaError::MemoryRegistrationFailed {
            ptr: ptr as usize,
            size: slot_size,
            reason: format!("cuMemObjGetDescriptor failed rc={rc}"),
        });
    }

    info!("[s3-rdma] client pool: registered slot ptr={ptr:?} size={slot_size}");
    Ok((ptr as usize, slot_size))
}

struct PoolLease {
    pool: Arc<ClientPool>,
    slot_index: usize,
    ptr: usize,
    size: usize,
}

impl Drop for PoolLease {
    fn drop(&mut self) {
        self.pool.release(self.slot_index);
    }
}

/// The one cuObjClient for the process.  Created at most once; never destroyed
/// (deregistration corrupts the allocator).  A creation/connection failure is
/// *not* cached here, so a transient fabric outage heals on the next attempt.
static CLIENT: Mutex<Option<usize>> = Mutex::new(None);

/// The live pool — cached on success only.  Never holds an error, so a failed
/// init can be retried (unlike a cached `Err`, which would poison forever).
static GLOBAL: OnceLock<Arc<ClientPool>> = OnceLock::new();

/// Return the process-wide cuObjClient, creating it once and re-checking
/// connectivity on every call.  Retryable: failures are never cached, and the
/// client is never recreated (so no client leak across retries).
fn get_client() -> Result<usize, RdmaError> {
    let mut guard = CLIENT.lock().unwrap_or_else(|p| p.into_inner());
    let client_raw = match *guard {
        Some(c) => c,
        None => {
            let raw = unsafe { cuobj_sys::cuobj_client_create() };
            if raw.is_null() {
                return Err(RdmaError::RdmaPluginLoadFailed {
                    reason: "cuobj_client_create returned null".into(),
                });
            }
            let raw = raw as usize;
            *guard = Some(raw);
            raw
        }
    };
    if unsafe { cuobj_sys::cuobj_client_is_connected(client_raw as *mut c_void) } == 0 {
        return Err(RdmaError::RdmaPluginLoadFailed {
            reason: "cuObjClient not connected to RDMA fabric".into(),
        });
    }
    Ok(client_raw)
}

fn global_init(config: PoolConfig) -> Result<Arc<ClientPool>, RdmaError> {
    if let Some(pool) = GLOBAL.get() {
        pool.grow_caps(config);
        return Ok(Arc::clone(pool));
    }

    let client_raw = get_client()?;
    let pool = Arc::new(ClientPool {
        client_raw,
        inner: Mutex::new(PoolInner {
            slots: Vec::new(),
            slot_size: config.slot_size,
            max_slots: config.max_slots,
        }),
        available: Condvar::new(),
        wait_ms: config.wait_ms,
    });
    info!(
        "[s3-rdma] client pool init: slot_size={} slots={} wait_ms={}",
        config.slot_size, config.max_slots, config.wait_ms
    );
    // A concurrent caller may have won the race; use whichever pool is stored.
    let _ = GLOBAL.set(Arc::clone(&pool));
    Ok(GLOBAL.get().map(Arc::clone).unwrap_or(pool))
}

unsafe fn clone_pool_from_ctx(ctx: *mut c_void) -> Option<Arc<ClientPool>> {
    if ctx.is_null() {
        return None;
    }
    let ptr = ctx as *const ClientPool;
    unsafe {
        Arc::increment_strong_count(ptr);
        Some(Arc::from_raw(ptr))
    }
}

// ── CuObjClientChannel ────────────────────────────────────────────────────────

/// One RDMA channel — a lease on one registered pool slot.
struct CuObjClientChannel {
    key: Vec<u8>,
    client_raw: usize,
    lease: PoolLease,
}

unsafe impl Send for CuObjClientChannel {}
unsafe impl Sync for CuObjClientChannel {}

impl RdmaClientChannel for CuObjClientChannel {
    fn key(&self) -> &[u8] {
        &self.key
    }
    fn ptr(&self) -> *mut u8 {
        self.lease.ptr as *mut u8
    }
    fn size(&self) -> usize {
        self.lease.size
    }

    fn get_max_transfer_size(&self) -> usize {
        self.size()
    }

    /// Return a PUT DCT token — the server will RDMA_READ from
    /// our buffer using the token; data is already in place from the caller's
    /// copy via `ptr()` after `bind()`.
    ///
    /// Appends `:offset:size` (hex-encoded) to the base cuObj token so the gateway
    /// can determine the exact transfer size without relying on Content-Length.
    /// The gateway's `RdmaToken::parse` strips this extension before passing
    /// the base token to the cuObj C SDK.
    fn prepare_put(&self, offset: usize, size: usize) -> Result<RdmaTransferHandle, RdmaError> {
        let token_put = self.token(CUOBJ_PUT, offset, size)?;
        info!(
            "[s3-rdma] prepare_put: token_len={} offset={offset} size={size}",
            token_put.len()
        );
        let base = String::from_utf8_lossy(&token_put);
        let token = format!("{base}:{offset:x}:{size:x}").into_bytes();
        Ok(RdmaTransferHandle::new(token, offset, size))
    }

    /// Return a GET DCT token.  The upstream will
    /// RDMA_WRITE into our buffer using the token; we read the buffer after `complete_get`.
    ///
    /// Appends `:offset:size` (hex-encoded) so the server knows how many bytes to RDMA_WRITE.
    fn prepare_get(&self, offset: usize, size: usize) -> Result<RdmaTransferHandle, RdmaError> {
        let token_get = self.token(CUOBJ_GET, offset, size)?;
        info!(
            "[s3-rdma] prepare_get: token_len={} offset={offset} size={size}",
            token_get.len()
        );
        let base = String::from_utf8_lossy(&token_get);
        let token = format!("{base}:{offset:x}:{size:x}").into_bytes();
        Ok(RdmaTransferHandle::new(token, offset, size))
    }

    /// The server has finished RDMA_READ from our buffer. Confirm the reply status.
    fn complete_put(
        &self,
        _handle: RdmaTransferHandle,
        rdma_status: u16,
        _transferred: usize,
    ) -> Result<(), RdmaError> {
        info!("[s3-rdma] complete_put: status={rdma_status}");
        if (200..300).contains(&(rdma_status as u32)) {
            Ok(())
        } else {
            Err(RdmaError::TransferFailed {
                reason: format!("RDMA PUT reply status={rdma_status}"),
            })
        }
    }

    /// The upstream has already written data into our buffer via RDMA_WRITE
    /// by the time the HTTP 200 arrives.  Nothing to signal.
    fn complete_get(
        &self,
        _handle: RdmaTransferHandle,
        rdma_status: u16,
        _transferred: usize,
    ) -> Result<(), RdmaError> {
        info!("[s3-rdma] complete_get: status={rdma_status}");
        if (200..300).contains(&(rdma_status as u32)) {
            Ok(())
        } else {
            Err(RdmaError::TransferFailed {
                reason: format!("RDMA GET reply status={rdma_status}"),
            })
        }
    }
}

impl CuObjClientChannel {
    fn token(&self, op: i32, _offset: usize, _size: usize) -> Result<Vec<u8>, RdmaError> {
        let mut desc: *mut c_char = std::ptr::null_mut();
        let rc = unsafe {
            cuobj_sys::cuobj_client_mem_get_rdma_token(
                self.client_raw as *mut c_void,
                self.ptr() as *mut c_void,
                self.size(),
                0,
                op,
                &mut desc,
            )
        };
        if rc != CU_OBJ_SUCCESS || desc.is_null() {
            return Err(RdmaError::TokenGenerationFailed {
                key: String::from_utf8_lossy(&self.key).into_owned(),
                reason: format!("cuMemObjGetRDMAToken op={op} failed rc={rc}"),
            });
        }
        Ok(unsafe { std::ffi::CStr::from_ptr(desc).to_bytes().to_vec() })
    }
}

// ── C vtable handlers ──────────────────────────────────────────────────────────

/// Map a lease/init failure to a `bind_channel` reason code so the loader can
/// reconstruct the right [`RdmaError`] variant across the `i32` boundary.
fn bind_err_code(e: &RdmaError) -> i32 {
    match e {
        RdmaError::RequestTooLarge { .. } => RDMA_BIND_ERR_TOO_LARGE,
        RdmaError::PoolExhausted { .. } => RDMA_BIND_ERR_EXHAUSTED,
        RdmaError::MemoryRegistrationFailed { .. } => RDMA_BIND_ERR_REGISTER,
        RdmaError::RdmaPluginLoadFailed { .. } => RDMA_BIND_ERR_NOT_READY,
        _ => RDMA_BIND_ERR_GENERIC,
    }
}

unsafe extern "C" fn c_init(params: *mut c_void, out_ctx: *mut *mut c_void) -> i32 {
    panic::catch_unwind(AssertUnwindSafe(|| {
        let config = unsafe { PoolConfig::from_raw(params) };
        match global_init(config) {
            Ok(pool) => {
                if !out_ctx.is_null() {
                    unsafe {
                        *out_ctx = Arc::into_raw(pool) as *mut c_void;
                    }
                }
                RDMA_BIND_OK
            }
            Err(e) => {
                error!("[s3-rdma] c_init: {e}");
                bind_err_code(&e)
            }
        }
    }))
    .unwrap_or_else(|_| {
        error!("[s3-rdma] c_init: panicked");
        RDMA_BIND_ERR_GENERIC
    })
}

unsafe extern "C" fn c_cleanup(ctx: *mut c_void) {
    let _ = panic::catch_unwind(AssertUnwindSafe(|| {
        if !ctx.is_null() {
            unsafe {
                drop(Arc::from_raw(ctx as *const ClientPool));
            }
        }
    }));
}

unsafe extern "C" fn c_bind_channel(
    ctx: *mut c_void,
    size: usize,
    key: *const ByteCursor,
    out: *mut *mut c_void,
) -> i32 {
    panic::catch_unwind(AssertUnwindSafe(|| {
        if key.is_null() || out.is_null() {
            return RDMA_BIND_ERR_GENERIC;
        }
        let key_slice = unsafe { (*key).as_slice() };

        info!("[s3-rdma] bind_channel: size={size}");

        let pool = match unsafe { clone_pool_from_ctx(ctx) } {
            Some(pool) => pool,
            None => match global_init(PoolConfig::default()) {
                Ok(pool) => pool,
                Err(e) => {
                    error!("[s3-rdma] bind_channel: {e}");
                    return bind_err_code(&e);
                }
            },
        };

        let lease = match pool.lease(size) {
            Ok(lease) => lease,
            Err(e) => {
                let code = bind_err_code(&e);
                warn!("[s3-rdma] bind_channel: {e}");
                return code;
            }
        };

        let channel: Box<dyn RdmaClientChannel> = Box::new(CuObjClientChannel {
            key: key_slice.to_vec(),
            client_raw: pool.client_raw,
            lease,
        });
        let boxed: Box<Box<dyn RdmaClientChannel>> = Box::new(channel);
        unsafe {
            *out = Box::into_raw(boxed) as *mut c_void;
        }
        RDMA_BIND_OK
    }))
    .unwrap_or_else(|_| {
        error!("[s3-rdma] bind_channel: panicked");
        RDMA_BIND_ERR_GENERIC
    })
}

// ── Telemetry ──────────────────────────────────────────────────────────────────
// Map our 0-3 level to the cuobj SDK flag bits (CUOBJ_LOG_PATH_*).
// Kept in sync with providers/server/src/rdma.rs.
fn level_to_cuobj_flags(level: u32) -> u32 {
    match level {
        0 => 0,
        1 => 0x0004,
        2 => 0x0005,
        _ => 0x0007,
    }
}

unsafe extern "C" fn c_setup_telemetry(ctx: *mut c_void, cfg: *const CTelemetryConfig) -> i32 {
    let rc = crate::telem::setup(ctx, cfg);
    if rc != 0 || cfg.is_null() {
        return rc;
    }
    let c = unsafe { &*cfg };
    let log_file = if c.log_file.is_null() {
        None
    } else {
        unsafe { std::ffi::CStr::from_ptr(c.log_file) }
            .to_str()
            .ok()
            .map(str::to_owned)
    };
    CuObjClient::setup_telemetry(c.use_otel != 0, log_file.as_deref());
    CuObjClient::set_telem_flags(level_to_cuobj_flags(c.level));
    0
}

unsafe extern "C" fn c_shutdown_telemetry(ctx: *mut c_void) -> i32 {
    crate::telem::shutdown(ctx);
    CuObjClient::shutdown_telemetry();
    0
}

unsafe extern "C" fn c_set_telem_level(ctx: *mut c_void, level: u32) -> i32 {
    crate::telem::set_level(ctx, level);
    CuObjClient::set_telem_flags(level_to_cuobj_flags(level));
    0
}

static VTABLE: RdmaProviderVtable = RdmaProviderVtable {
    provider_name: PROVIDER_NAME.as_ptr() as *const c_char,
    provider_version: 1,
    init: Some(c_init),
    cleanup: Some(c_cleanup),
    bind_channel: Some(c_bind_channel),
    supports_telemetry: Some(crate::telem::supports),
    setup_telemetry: Some(c_setup_telemetry),
    shutdown_telemetry: Some(c_shutdown_telemetry),
    set_telem_level: Some(c_set_telem_level),
};

pub fn vtable() -> *const RdmaProviderVtable {
    &VTABLE
}

#[cfg(test)]
mod tests {
    //! Pool accounting tests.  They pre-populate `Ready` slots with a dummy
    //! `client_raw`/`ptr`, so `lease` never reaches `allocate_buffer` (which
    //! would call into the cuObj SDK) — only the too-large / reuse / exhausted
    //! / wait / grow paths are exercised, all hardware-free.
    use super::*;

    /// Build a pool with `n` pre-registered slots (no cuObj), each leased or
    /// free per `leased`.  `client_raw` is a dummy never dereferenced on these
    /// paths.
    fn pool_with_slots(
        slot_size: usize,
        max_slots: usize,
        wait_ms: u64,
        n: usize,
        leased: bool,
    ) -> Arc<ClientPool> {
        let slots = (0..n)
            .map(|_| PoolSlot {
                state: SlotState::Ready {
                    ptr: 0x1000,
                    size: slot_size,
                },
                leased,
            })
            .collect();
        Arc::new(ClientPool {
            client_raw: 0,
            inner: Mutex::new(PoolInner {
                slots,
                slot_size,
                max_slots,
            }),
            available: Condvar::new(),
            wait_ms,
        })
    }

    #[test]
    fn lease_rejects_too_large() {
        let pool = pool_with_slots(1024, 4, 0, 0, false);
        match pool.lease(2048) {
            Err(RdmaError::RequestTooLarge {
                requested,
                slot_size,
            }) => {
                assert_eq!(requested, 2048);
                assert_eq!(slot_size, 1024);
            }
            Err(other) => panic!("expected RequestTooLarge, got {other:?}"),
            Ok(_) => panic!("expected RequestTooLarge, got Ok"),
        }
    }

    #[test]
    fn lease_exhausted_immediately_when_full_and_zero_wait() {
        // max_slots ready+leased == full; wait_ms 0 -> immediate exhaustion.
        let pool = pool_with_slots(1024, 2, 0, 2, true);
        match pool.lease(512) {
            Err(RdmaError::PoolExhausted { slots, wait_ms }) => {
                assert_eq!(slots, 2);
                assert_eq!(wait_ms, 0);
            }
            Err(other) => panic!("expected PoolExhausted, got {other:?}"),
            Ok(_) => panic!("expected PoolExhausted, got Ok"),
        }
    }

    #[test]
    fn lease_times_out_without_panic() {
        // Full pool, small wait, nobody releases -> PoolExhausted after ~wait_ms.
        let pool = pool_with_slots(1024, 1, 5, 1, true);
        assert!(matches!(
            pool.lease(512),
            Err(RdmaError::PoolExhausted { .. })
        ));
    }

    #[test]
    fn huge_wait_ms_does_not_panic() {
        // A free slot is available, so lease returns via the fast path; the key
        // point is that constructing the pool with wait_ms == u64::MAX and
        // calling lease never evaluates `Instant + Duration` (which would panic).
        let pool = pool_with_slots(1024, 1, u64::MAX, 1, false);
        assert!(pool.lease(512).is_ok());
    }

    #[test]
    fn release_lets_next_lease_reuse_slot() {
        let pool = pool_with_slots(1024, 1, 0, 1, true); // full
        pool.release(0);
        let lease = pool.lease(512).expect("freed slot should be reusable");
        assert_eq!(lease.ptr, 0x1000);
        assert_eq!(lease.size, 1024);
    }

    #[test]
    fn grow_caps_raises_but_never_shrinks() {
        let pool = pool_with_slots(1024, 2, 0, 0, false);
        pool.grow_caps(PoolConfig {
            slot_size: 4096,
            max_slots: 8,
            wait_ms: 999,
        });
        {
            let g = pool.lock();
            assert_eq!(g.slot_size, 4096);
            assert_eq!(g.max_slots, 8);
        }
        // Smaller request is ignored.
        pool.grow_caps(PoolConfig {
            slot_size: 512,
            max_slots: 1,
            wait_ms: 1,
        });
        {
            let g = pool.lock();
            assert_eq!(g.slot_size, 4096);
            assert_eq!(g.max_slots, 8);
        }
        // grow_caps never touches wait_ms.
        assert_eq!(pool.wait_ms, 0);
    }

    #[test]
    fn concurrent_lease_release_never_exceeds_capacity() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let max = 4usize;
        // All slots pre-registered and free; threads reuse them (no allocation),
        // so the pool never grows past `max`.
        let pool = pool_with_slots(1024, max, 50, max, false);
        let live = Arc::new(AtomicUsize::new(0));

        let handles: Vec<_> = (0..16)
            .map(|_| {
                let pool = Arc::clone(&pool);
                let live = Arc::clone(&live);
                std::thread::spawn(move || {
                    for _ in 0..50 {
                        if let Ok(lease) = pool.lease(512) {
                            let n = live.fetch_add(1, Ordering::SeqCst) + 1;
                            assert!(n <= max, "live leases {n} exceeded capacity {max}");
                            live.fetch_sub(1, Ordering::SeqCst);
                            drop(lease);
                        }
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }

        let g = pool.lock();
        assert_eq!(g.slots.len(), max, "pool must not grow beyond max_slots");
        assert!(g.slots.iter().all(|s| !s.leased), "all slots released");
    }
}
