//! Generic RDMA engine: fixed-size worker pool wrapping any
//! [`RdmaProvider`](crate::provider::RdmaProvider).
//!
//! [`RdmaEngine`] dispatches all server-side operations (`handle_put`,
//! `handle_get`, and their zero-copy variants) to a bounded pool of worker
//! threads.  Client-side operations (token generation, memory registration)
//! are forwarded directly to the inner provider without queuing.
//!
//! # Why workers?
//!
//! Each server-side call occupies one slot in the inner provider's internal
//! buffer pool for the duration of the RDMA transfer.  Bounding the number of
//! concurrent callers prevents the inner provider from being overwhelmed while
//! still allowing the Tokio thread pool to stay busy on other work.
//!
//! # Thread safety
//!
//! All methods on [`RdmaEngine`] are safe to call concurrently from any
//! number of threads.  Workers are joined when the engine is dropped.

use std::ffi::c_void;
use std::sync::{Arc, Mutex};
use std::sync::mpsc::{Receiver, SyncSender};

use crate::error::RdmaError;
use crate::ffi::{RdmaIoReadFn, RdmaIoWriteFn};
use crate::provider::{RdmaCompletionCallback, RdmaClientProvider, RdmaServerProvider, RdmaProvider};

// ── SendPtr ───────────────────────────────────────────────────────────────────

/// Wrapper that makes a raw `*mut c_void` `Send`.
///
/// # Safety
/// Callers must ensure the underlying data is live for the duration of the
/// cross-thread transmission (typically by blocking on the result channel).
struct SendPtr(*mut c_void);
unsafe impl Send for SendPtr {}

// ── WorkItem ────────────────────────────────────────────────────────────────

enum WorkItem {
    Put {
        key:       Vec<u8>,
        token:     Vec<u8>,
        buf_ptr:   usize,
        buf_len:   usize,
        offset:    usize,
        size:      usize,
        result_tx: SyncSender<Result<usize, String>>,
    },
    Get {
        key:       Vec<u8>,
        data_ptr:  usize,
        data_len:  usize,
        token:     Vec<u8>,
        offset:    usize,
        result_tx: SyncSender<Result<usize, String>>,
    },
    PutZc {
        key:           Vec<u8>,
        token:         Vec<u8>,
        offset:        usize,
        size:          usize,
        write_fn:      RdmaIoWriteFn,
        write_fn_data: SendPtr,
        result_tx:     SyncSender<Result<usize, String>>,
    },
    GetZc {
        key:          Vec<u8>,
        token:        Vec<u8>,
        offset:       usize,
        read_fn:      RdmaIoReadFn,
        read_fn_data: SendPtr,
        result_tx:    SyncSender<Result<usize, String>>,
    },
}

// ── Worker main loop ──────────────────────────────────────────────────────────

fn worker_main(provider: Arc<dyn RdmaProvider>, rx: Arc<Mutex<Receiver<WorkItem>>>) {
    loop {
        let work = match rx.lock().unwrap().recv() {
            Ok(w)  => w,
            Err(_) => break, // sender dropped → shutdown
        };
        match work {
            WorkItem::Put { key, token, buf_ptr, buf_len, offset, size, result_tx } => {
                // SAFETY: caller holds `buf` alive by blocking on `result_rx`.
                let buf = unsafe { std::slice::from_raw_parts_mut(buf_ptr as *mut u8, buf_len) };
                let r = provider.handle_put(&key, &token, buf, offset, size)
                    .map_err(|e| e.to_string());
                let _ = result_tx.send(r);
            }
            WorkItem::Get { key, data_ptr, data_len, token, offset, result_tx } => {
                // SAFETY: caller holds `data` alive by blocking on `result_rx`.
                let data = unsafe { std::slice::from_raw_parts(data_ptr as *const u8, data_len) };
                let r = provider.handle_get(&key, data, &token, offset)
                    .map_err(|e| e.to_string());
                let _ = result_tx.send(r);
            }
            WorkItem::PutZc { key, token, offset, size, write_fn, write_fn_data, result_tx } => {
                // SAFETY: caller guaranteed `write_fn_data` is alive (blocked).
                let r = unsafe {
                    provider.handle_put_zc(&key, &token, offset, size, write_fn, write_fn_data.0)
                }.map_err(|e| e.to_string());
                let _ = result_tx.send(r);
            }
            WorkItem::GetZc { key, token, offset, read_fn, read_fn_data, result_tx } => {
                // SAFETY: caller guaranteed `read_fn_data` is alive (blocked).
                let r = unsafe {
                    provider.handle_get_zc(&key, &token, offset, read_fn, read_fn_data.0)
                }.map_err(|e| e.to_string());
                let _ = result_tx.send(r);
            }
        }
    }
}

// ── RdmaEngineConfig ──────────────────────────────────────────────────────────

/// Configuration for [`RdmaEngine`].
#[derive(Debug, Clone)]
pub struct RdmaEngineConfig {
    /// Number of worker threads (= max concurrent server-side RDMA ops).
    ///
    /// Values of `0` are clamped to `1`.  Default: `4`.
    pub worker_count: usize,
}

impl Default for RdmaEngineConfig {
    fn default() -> Self {
        Self { worker_count: 4 }
    }
}

// ── RdmaEngine ────────────────────────────────────────────────────────────────

/// Fixed-size worker pool that wraps any [`RdmaProvider`] and dispatches
/// server-side RDMA operations to a bounded set of threads.
///
/// `RdmaEngine` implements [`RdmaProvider`] so it can replace any
/// `Arc<dyn RdmaProvider>` transparently.
///
/// # Drop
///
/// Dropping an `RdmaEngine` closes the work channel and joins all worker
/// threads, ensuring in-flight operations complete cleanly.
pub struct RdmaEngine {
    inner:   Arc<dyn RdmaProvider>,
    sender:  Option<std::sync::mpsc::Sender<WorkItem>>,
    workers: Vec<std::thread::JoinHandle<()>>,
}

// SAFETY: all mutable state is encapsulated in mpsc channels (Sender is Send)
// and the workers themselves are fully independent threads.
unsafe impl Send for RdmaEngine {}
unsafe impl Sync for RdmaEngine {}

impl RdmaEngine {
    /// Build an engine with `cfg.worker_count` threads bound to `provider`.
    pub fn new(provider: Arc<dyn RdmaProvider>, cfg: &RdmaEngineConfig) -> Self {
        let count = cfg.worker_count.max(1);
        let (tx, rx) = std::sync::mpsc::channel::<WorkItem>();
        let rx = Arc::new(Mutex::new(rx));
        let workers = (0..count)
            .map(|i| {
                let p  = Arc::clone(&provider);
                let rx = Arc::clone(&rx);
                std::thread::Builder::new()
                    .name(format!("rdma-engine-worker-{i}"))
                    .spawn(move || worker_main(p, rx))
                    .expect("failed to spawn rdma-engine worker")
            })
            .collect();
        Self { inner: provider, sender: Some(tx), workers }
    }

    fn dispatch(&self, work: WorkItem) -> Result<(), RdmaError> {
        self.sender
            .as_ref()
            .ok_or_else(|| RdmaError::TransferFailed("engine shut down".into()))?
            .send(work)
            .map_err(|e| RdmaError::TransferFailed(e.to_string()))
    }
}

impl Drop for RdmaEngine {
    fn drop(&mut self) {
        drop(self.sender.take());
        for h in self.workers.drain(..) {
            let _ = h.join();
        }
    }
}

impl RdmaClientProvider for RdmaEngine {
    fn name(&self)     -> &str  { self.inner.name() }
    fn protocol(&self) -> &str  { self.inner.protocol() }

    fn is_memory_suitable(&self, ptr: *const u8, size: usize) -> bool {
        self.inner.is_memory_suitable(ptr, size)
    }

    fn register_memory(&self, ptr: *mut u8, size: usize) -> Result<(), RdmaError> {
        self.inner.register_memory(ptr, size)
    }

    fn deregister_memory(&self, ptr: *mut u8) -> Result<(), RdmaError> {
        self.inner.deregister_memory(ptr)
    }

    fn get_max_transfer_size(&self, ptr: *const u8) -> usize {
        self.inner.get_max_transfer_size(ptr)
    }

    fn prepare_put_token(
        &self, s3_key: &[u8], buffer: *const u8, size: usize, offset: usize,
    ) -> Result<Vec<u8>, RdmaError> {
        self.inner.prepare_put_token(s3_key, buffer, size, offset)
    }

    fn prepare_get_token(
        &self, s3_key: &[u8], buffer: *mut u8, size: usize, offset: usize,
    ) -> Result<Vec<u8>, RdmaError> {
        self.inner.prepare_get_token(s3_key, buffer, size, offset)
    }

    fn process_rdma_reply(
        &self,
        rdma_status:   u16,
        request_token: &[u8],
        transferred:   usize,
        callback:      RdmaCompletionCallback,
    ) -> Result<(), RdmaError> {
        self.inner.process_rdma_reply(rdma_status, request_token, transferred, callback)
    }

    fn rdma_token_header_name(&self) -> &[u8] { self.inner.rdma_token_header_name() }
    fn rdma_reply_header_name(&self) -> &[u8] { self.inner.rdma_reply_header_name() }
    fn rdma_bytes_header_name(&self) -> &[u8] { self.inner.rdma_bytes_header_name() }
}

impl RdmaServerProvider for RdmaEngine {
    fn buf_size(&self) -> usize { self.inner.buf_size() }

    fn handle_put(
        &self,
        s3_key: &[u8],
        token:  &[u8],
        buf:    &mut [u8],
        offset: usize,
        size:   usize,
    ) -> Result<usize, RdmaError> {
        let (result_tx, result_rx) = std::sync::mpsc::sync_channel(1);
        self.dispatch(WorkItem::Put {
            key:     s3_key.to_vec(),
            token:   token.to_vec(),
            buf_ptr: buf.as_mut_ptr() as usize,
            buf_len: buf.len(),
            offset,
            size,
            result_tx,
        })?;
        result_rx.recv()
            .map_err(|e| RdmaError::TransferFailed(e.to_string()))?
            .map_err(RdmaError::TransferFailed)
    }

    fn handle_get(
        &self,
        s3_key: &[u8],
        data:   &[u8],
        token:  &[u8],
        offset: usize,
    ) -> Result<usize, RdmaError> {
        let (result_tx, result_rx) = std::sync::mpsc::sync_channel(1);
        self.dispatch(WorkItem::Get {
            key:      s3_key.to_vec(),
            data_ptr: data.as_ptr() as usize,
            data_len: data.len(),
            token:    token.to_vec(),
            offset,
            result_tx,
        })?;
        result_rx.recv()
            .map_err(|e| RdmaError::TransferFailed(e.to_string()))?
            .map_err(RdmaError::TransferFailed)
    }

    unsafe fn handle_put_zc(
        &self,
        s3_key:        &[u8],
        token:         &[u8],
        offset:        usize,
        size:          usize,
        write_fn:      RdmaIoWriteFn,
        write_fn_data: *mut c_void,
    ) -> Result<usize, RdmaError> {
        let (result_tx, result_rx) = std::sync::mpsc::sync_channel(1);
        self.dispatch(WorkItem::PutZc {
            key:           s3_key.to_vec(),
            token:         token.to_vec(),
            offset,
            size,
            write_fn,
            write_fn_data: SendPtr(write_fn_data),
            result_tx,
        })?;
        result_rx.recv()
            .map_err(|e| RdmaError::TransferFailed(e.to_string()))?
            .map_err(RdmaError::TransferFailed)
    }

    unsafe fn handle_get_zc(
        &self,
        s3_key:       &[u8],
        token:        &[u8],
        offset:       usize,
        read_fn:      RdmaIoReadFn,
        read_fn_data: *mut c_void,
    ) -> Result<usize, RdmaError> {
        let (result_tx, result_rx) = std::sync::mpsc::sync_channel(1);
        self.dispatch(WorkItem::GetZc {
            key:          s3_key.to_vec(),
            token:        token.to_vec(),
            offset,
            read_fn,
            read_fn_data: SendPtr(read_fn_data),
            result_tx,
        })?;
        result_rx.recv()
            .map_err(|e| RdmaError::TransferFailed(e.to_string()))?
            .map_err(RdmaError::TransferFailed)
    }
}

impl RdmaProvider for RdmaEngine {
    fn supports_client(&self) -> bool { self.inner.supports_client() }
    fn supports_server(&self) -> bool { self.inner.supports_server() }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mock::MockRdmaProvider;

    fn mock_engine(workers: usize) -> RdmaEngine {
        let provider: Arc<dyn RdmaProvider> =
            Arc::new(MockRdmaProvider::new(false, "/dev/shm", 4096));
        RdmaEngine::new(provider, &RdmaEngineConfig { worker_count: workers })
    }

    #[test]
    fn supports_server() {
        let e = mock_engine(2);
        assert!(e.supports_server());
    }

    #[test]
    fn name_delegates_to_inner() {
        let e = mock_engine(1);
        assert_eq!(e.name(), "MockRdmaProvider");
    }

    #[test]
    fn drop_joins_workers() {
        drop(mock_engine(4));
    }

    #[test]
    fn worker_count_clamped_to_one() {
        drop(mock_engine(0));
    }
}
