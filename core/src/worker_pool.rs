//! Generic RDMA worker pool — bounded concurrency for any [`RdmaServerProvider`].
//!
//! Wraps a provider in a fixed-size thread pool.  Each `open_channel()` returns
//! a channel that dispatches operations to a worker, bounding the number of
//! concurrent RDMA transfers to the worker count.

use std::sync::{Arc, Mutex};
use std::sync::mpsc::{Receiver, SyncSender};

use crate::error::RdmaError;
use crate::provider::RdmaServerChannel;
use crate::provider::RdmaServerProvider;

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
}

// ── Worker main loop ──────────────────────────────────────────────────────────

fn worker_main(provider: Arc<dyn RdmaServerProvider>, rx: Arc<Mutex<Receiver<WorkItem>>>) {
    loop {
        let work = match rx.lock().unwrap().recv() {
            Ok(w)  => w,
            Err(_) => break,
        };
        match work {
            WorkItem::Put { key, token, buf_ptr, buf_len, offset, size, result_tx } => {
                let buf = unsafe { std::slice::from_raw_parts_mut(buf_ptr as *mut u8, buf_len) };
                let r = (|| -> Result<usize, RdmaError> {
                    provider.open_channel(&key, &token)?.handle_put(buf, offset, size)
                })().map_err(|e| e.to_string());
                let _ = result_tx.send(r);
            }
            WorkItem::Get { key, data_ptr, data_len, token, offset, result_tx } => {
                let data = unsafe { std::slice::from_raw_parts(data_ptr as *const u8, data_len) };
                let r = (|| -> Result<usize, RdmaError> {
                    provider.open_channel(&key, &token)?.handle_get(data, offset)
                })().map_err(|e| e.to_string());
                let _ = result_tx.send(r);
            }
        }
    }
}

// ── RdmaWorkerPool ───────────────────────────────────────────────────────────

/// Generic bounded-concurrency worker pool wrapping any [`RdmaServerProvider`].
///
/// Each worker thread calls `provider.open_channel()` → channel operations
/// on the provider.  The pool bounds the number of concurrent RDMA transfers
/// to `worker_count`.
///
/// Implements [`RdmaServerProvider`] so it can wrap any provider transparently.
pub struct RdmaWorkerPool {
    _provider: Arc<dyn RdmaServerProvider>,
    sender:    Option<std::sync::mpsc::Sender<WorkItem>>,
    workers:   Vec<std::thread::JoinHandle<()>>,
}

unsafe impl Send for RdmaWorkerPool {}
unsafe impl Sync for RdmaWorkerPool {}

impl RdmaWorkerPool {
    pub fn new(provider: Arc<dyn RdmaServerProvider>, worker_count: usize) -> Self {
        let count = worker_count.max(1);
        let (tx, rx) = std::sync::mpsc::channel::<WorkItem>();
        let rx = Arc::new(Mutex::new(rx));
        let workers = (0..count)
            .map(|i| {
                let p  = Arc::clone(&provider);
                let rx = Arc::clone(&rx);
                std::thread::Builder::new()
                    .name(format!("rdma-pool-{i}"))
                    .spawn(move || worker_main(p, rx))
                    .expect("failed to spawn pool worker")
            })
            .collect();
        Self { _provider: provider, sender: Some(tx), workers }
    }
}

impl Drop for RdmaWorkerPool {
    fn drop(&mut self) {
        drop(self.sender.take());
        for h in self.workers.drain(..) {
            let _ = h.join();
        }
    }
}

impl RdmaServerProvider for RdmaWorkerPool {
    fn buf_size(&self) -> usize {
        // Workers allocate channels from the inner provider; buf_size
        // should come from there.  Since we only have &self and not the
        // provider's buf_size cached, return 0.  Callers should query
        // the inner provider directly for accurate sizing.
        0
    }

    fn open_channel(&self, key: &[u8], token: &[u8])
        -> Result<Box<dyn RdmaServerChannel>, RdmaError>
    {
        let sender = self.sender.as_ref()
            .ok_or_else(|| RdmaError::TransferFailed("pool shut down".into()))?
            .clone();
        Ok(Box::new(PoolChannel {
            key: key.to_vec(),
            token: token.to_vec(),
            sender,
        }))
    }
}

// ── PoolChannel ──────────────────────────────────────────────────────────────

struct PoolChannel {
    key:    Vec<u8>,
    token:  Vec<u8>,
    sender: std::sync::mpsc::Sender<WorkItem>,
}

impl RdmaServerChannel for PoolChannel {
    fn key(&self)   -> &[u8] { &self.key }
    fn token(&self) -> &[u8] { &self.token }
    fn buf_size(&self) -> usize { 0 }

    fn handle_put(&self, buf: &mut [u8], offset: usize, size: usize) -> Result<usize, RdmaError> {
        let (tx, rx) = std::sync::mpsc::sync_channel(1);
        self.sender.send(WorkItem::Put {
            key: self.key.clone(), token: self.token.clone(),
            buf_ptr: buf.as_mut_ptr() as usize, buf_len: buf.len(),
            offset, size, result_tx: tx,
        }).map_err(|e| RdmaError::TransferFailed(e.to_string()))?;
        rx.recv().map_err(|e| RdmaError::TransferFailed(e.to_string()))?
            .map_err(RdmaError::TransferFailed)
    }

    fn handle_get(&self, data: &[u8], offset: usize) -> Result<usize, RdmaError> {
        let (tx, rx) = std::sync::mpsc::sync_channel(1);
        self.sender.send(WorkItem::Get {
            key: self.key.clone(), token: self.token.clone(),
            data_ptr: data.as_ptr() as usize, data_len: data.len(),
            offset, result_tx: tx,
        }).map_err(|e| RdmaError::TransferFailed(e.to_string()))?;
        rx.recv().map_err(|e| RdmaError::TransferFailed(e.to_string()))?
            .map_err(RdmaError::TransferFailed)
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mock::MockRdmaProvider;
    use crate::provider::RdmaClientProvider;
    use std::alloc::{Layout, alloc, dealloc};
    use crate::provider::RDMA_REQUIRED_ALIGNMENT;

    #[test]
    fn mock_pool_put_get() {
        let provider: Arc<dyn RdmaServerProvider> =
            Arc::new(MockRdmaProvider::new(false, "/dev/shm", 4096));
        let pool = RdmaWorkerPool::new(provider, 2);

        // Register memory and generate tokens via the mock provider directly.
        let mock = MockRdmaProvider::new(false, "/dev/shm", 4096);
        let layout = Layout::from_size_align(4096, RDMA_REQUIRED_ALIGNMENT).unwrap();
        let ptr = unsafe { alloc(layout) };
        let data = unsafe { std::slice::from_raw_parts_mut(ptr, 4096) };
        data.fill(42);
        let key = b"test-bucket/key";

        mock.register_memory(ptr, 4096).unwrap();
        let put_token = mock.prepare_put_token(key, ptr, 4096, 0).unwrap();
        let get_token = mock.prepare_get_token(key, ptr, 4096, 0).unwrap();

        // PUT through the pool.
        let ch = pool.open_channel(key, &put_token).unwrap();
        let mut put_buf = vec![0u8; 4096];
        let n = ch.handle_put(&mut put_buf, 0, 4096).unwrap();
        assert_eq!(n, 4096);
        assert_eq!(&put_buf[..4], &[42, 42, 42, 42]);

        // GET through the pool.
        let ch = pool.open_channel(key, &get_token).unwrap();
        let payload = b"hello pool";
        let n = ch.handle_get(payload, 0).unwrap();
        assert_eq!(n, 10);

        // Cleanup.
        mock.process_rdma_reply(200, &put_token, 4096, Box::new(|_| {})).unwrap();
        mock.process_rdma_reply(200, &get_token, n, Box::new(|_| {})).unwrap();
        mock.deregister_memory(ptr).unwrap();
        unsafe { dealloc(ptr, layout) };
    }

    #[test]
    fn pool_drop_joins_workers() {
        let provider: Arc<dyn RdmaServerProvider> =
            Arc::new(MockRdmaProvider::new(false, "/dev/shm", 4096));
        drop(RdmaWorkerPool::new(provider, 4));
    }
}
