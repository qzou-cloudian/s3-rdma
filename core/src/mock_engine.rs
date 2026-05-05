//! Mock RDMA engine — fixed-size worker pool for the mock provider.
//!
//! Each worker thread calls through to [`MockRdmaProvider`] methods directly.
//! No channels or hardware resources — just a bounded thread pool to limit
//! concurrent server-side operations.

use std::ffi::c_void;
use std::sync::{Arc, Mutex};
use std::sync::mpsc::{Receiver, SyncSender};

use crate::error::RdmaError;
use crate::ffi::{RdmaIoReadFn, RdmaIoWriteFn};
use crate::mock::MockRdmaProvider;
use crate::engine::RdmaEngine;
use crate::provider::{RdmaClientProvider, RdmaServerProvider, RdmaProvider, RdmaCompletionCallback};

// ── SendPtr ───────────────────────────────────────────────────────────────────

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

fn worker_main(provider: Arc<MockRdmaProvider>, rx: Arc<Mutex<Receiver<WorkItem>>>) {
    loop {
        let work = match rx.lock().unwrap().recv() {
            Ok(w)  => w,
            Err(_) => break,
        };
        match work {
            WorkItem::Put { key, token, buf_ptr, buf_len, offset, size, result_tx } => {
                let buf = unsafe { std::slice::from_raw_parts_mut(buf_ptr as *mut u8, buf_len) };
                let r = provider.handle_put(&key, &token, buf, offset, size)
                    .map_err(|e| e.to_string());
                let _ = result_tx.send(r);
            }
            WorkItem::Get { key, data_ptr, data_len, token, offset, result_tx } => {
                let data = unsafe { std::slice::from_raw_parts(data_ptr as *const u8, data_len) };
                let r = provider.handle_get(&key, data, &token, offset)
                    .map_err(|e| e.to_string());
                let _ = result_tx.send(r);
            }
            WorkItem::PutZc { key, token, offset, size, write_fn, write_fn_data, result_tx } => {
                let r = unsafe {
                    provider.handle_put_zc(&key, &token, offset, size, write_fn, write_fn_data.0)
                }.map_err(|e| e.to_string());
                let _ = result_tx.send(r);
            }
            WorkItem::GetZc { key, token, offset, read_fn, read_fn_data, result_tx } => {
                let r = unsafe {
                    provider.handle_get_zc(&key, &token, offset, read_fn, read_fn_data.0)
                }.map_err(|e| e.to_string());
                let _ = result_tx.send(r);
            }
        }
    }
}

// ── MockRdmaEngine ───────────────────────────────────────────────────────────

/// Fixed-size worker pool wrapping a [`MockRdmaProvider`].
///
/// Implements [`RdmaProvider`] so it can replace any provider transparently.
///
/// # Drop
///
/// Closes the work channel and joins all workers.
pub struct MockRdmaEngine {
    inner:   Arc<MockRdmaProvider>,
    sender:  Option<std::sync::mpsc::Sender<WorkItem>>,
    workers: Vec<std::thread::JoinHandle<()>>,
}

unsafe impl Send for MockRdmaEngine {}
unsafe impl Sync for MockRdmaEngine {}

impl MockRdmaEngine {
    /// Build an engine with `worker_count` threads bound to `provider`.
    pub fn new(provider: Arc<MockRdmaProvider>, worker_count: usize) -> Self {
        let count = worker_count.max(1);
        let (tx, rx) = std::sync::mpsc::channel::<WorkItem>();
        let rx = Arc::new(Mutex::new(rx));
        let workers = (0..count)
            .map(|i| {
                let p  = Arc::clone(&provider);
                let rx = Arc::clone(&rx);
                std::thread::Builder::new()
                    .name(format!("mock-engine-worker-{i}"))
                    .spawn(move || worker_main(p, rx))
                    .expect("failed to spawn mock engine worker")
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

impl Drop for MockRdmaEngine {
    fn drop(&mut self) {
        drop(self.sender.take());
        for h in self.workers.drain(..) {
            let _ = h.join();
        }
    }
}

impl RdmaClientProvider for MockRdmaEngine {
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
    fn prepare_put_token(&self, key: &[u8], buf: *const u8, size: usize, offset: usize,
    ) -> Result<Vec<u8>, RdmaError> {
        self.inner.prepare_put_token(key, buf, size, offset)
    }
    fn prepare_get_token(&self, key: &[u8], buf: *mut u8, size: usize, offset: usize,
    ) -> Result<Vec<u8>, RdmaError> {
        self.inner.prepare_get_token(key, buf, size, offset)
    }
    fn process_rdma_reply(&self, status: u16, token: &[u8], transferred: usize,
        cb: RdmaCompletionCallback,
    ) -> Result<(), RdmaError> {
        self.inner.process_rdma_reply(status, token, transferred, cb)
    }
    fn rdma_token_header_name(&self) -> &[u8] { self.inner.rdma_token_header_name() }
    fn rdma_reply_header_name(&self) -> &[u8] { self.inner.rdma_reply_header_name() }
    fn rdma_bytes_header_name(&self) -> &[u8] { self.inner.rdma_bytes_header_name() }
}

impl RdmaServerProvider for MockRdmaEngine {
    fn buf_size(&self) -> usize { self.inner.buf_size() }

    fn handle_put(&self, key: &[u8], token: &[u8], buf: &mut [u8],
        offset: usize, size: usize,
    ) -> Result<usize, RdmaError> {
        let (tx, rx) = std::sync::mpsc::sync_channel(1);
        self.dispatch(WorkItem::Put {
            key: key.to_vec(), token: token.to_vec(),
            buf_ptr: buf.as_mut_ptr() as usize, buf_len: buf.len(),
            offset, size, result_tx: tx,
        })?;
        rx.recv().map_err(|e| RdmaError::TransferFailed(e.to_string()))?
            .map_err(RdmaError::TransferFailed)
    }

    fn handle_get(&self, key: &[u8], data: &[u8], token: &[u8],
        offset: usize,
    ) -> Result<usize, RdmaError> {
        let (tx, rx) = std::sync::mpsc::sync_channel(1);
        self.dispatch(WorkItem::Get {
            key: key.to_vec(), token: token.to_vec(),
            data_ptr: data.as_ptr() as usize, data_len: data.len(),
            offset, result_tx: tx,
        })?;
        rx.recv().map_err(|e| RdmaError::TransferFailed(e.to_string()))?
            .map_err(RdmaError::TransferFailed)
    }

    unsafe fn handle_put_zc(&self, key: &[u8], token: &[u8], offset: usize, size: usize,
        write_fn: RdmaIoWriteFn, write_fn_data: *mut c_void,
    ) -> Result<usize, RdmaError> {
        let (tx, rx) = std::sync::mpsc::sync_channel(1);
        self.dispatch(WorkItem::PutZc {
            key: key.to_vec(), token: token.to_vec(),
            offset, size, write_fn, write_fn_data: SendPtr(write_fn_data),
            result_tx: tx,
        })?;
        rx.recv().map_err(|e| RdmaError::TransferFailed(e.to_string()))?
            .map_err(RdmaError::TransferFailed)
    }

    unsafe fn handle_get_zc(&self, key: &[u8], token: &[u8], offset: usize,
        read_fn: RdmaIoReadFn, read_fn_data: *mut c_void,
    ) -> Result<usize, RdmaError> {
        let (tx, rx) = std::sync::mpsc::sync_channel(1);
        self.dispatch(WorkItem::GetZc {
            key: key.to_vec(), token: token.to_vec(),
            offset, read_fn, read_fn_data: SendPtr(read_fn_data),
            result_tx: tx,
        })?;
        rx.recv().map_err(|e| RdmaError::TransferFailed(e.to_string()))?
            .map_err(RdmaError::TransferFailed)
    }
}

impl RdmaEngine for MockRdmaEngine {
    fn worker_count(&self) -> usize { self.workers.len() }
}

impl RdmaProvider for MockRdmaEngine {
    fn supports_client(&self) -> bool { true }
    fn supports_server(&self) -> bool { true }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn mock_engine(workers: usize) -> MockRdmaEngine {
        let provider = Arc::new(
            MockRdmaProvider::new(false, "/dev/shm", 4096)
        );
        MockRdmaEngine::new(provider, workers)
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
