//! CuObj RDMA engine — fixed-size worker pool for the cuObject server provider.
//!
//! Each worker thread pre-allocates a channel (via [`CuObjServer::alloc_channel`])
//! and a [`PersistentBuffer`], then reuses them for every operation.  Channels are
//! freed when the worker exits.  Channels are **not** shared between threads —
//! only the owning worker may use its channel.
//!
//! # Sync-only (Phase 1)
//!
//! Operations are synchronous — the worker processes one `WorkItem` at a time.
//! Async batch submission + polling will be added in a follow-up for higher
//! throughput.

use std::ffi::c_void;
use std::sync::{Arc, Mutex};
use std::sync::mpsc::{Receiver, SyncSender};

use cuobj::server::CuObjServer;
use cuobj::PersistentBuffer;
use s3_rdma::engine::RdmaEngine;
use s3_rdma::error::RdmaError;
use s3_rdma::ffi::{RdmaIoReadFn, RdmaIoWriteFn};
use s3_rdma::provider::RdmaServerProvider;

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

fn cuobj_worker_main(
    server:   Arc<CuObjServer>,
    buf_size: usize,
    rx:       Arc<Mutex<Receiver<WorkItem>>>,
) {
    // Allocate a persistent channel for this worker's exclusive use.
    let channel_id = match server.alloc_channel() {
        Ok(ch) => {
            let id = ch.id();
            std::mem::forget(ch); // bypass RAII — we free manually on exit
            id
        }
        Err(e) => {
            panic!("CuObjEngine worker: alloc_channel failed: {e:?}");
        }
    };

    // Pre-allocate a persistent RDMA-registered buffer.
    let mut buffer = match PersistentBuffer::new(Arc::clone(&server), buf_size) {
        Ok(b)  => b,
        Err(e) => {
            server.free_channel_by_id(channel_id);
            panic!("CuObjEngine worker: PersistentBuffer::new({buf_size}) failed: {e:?}");
        }
    };

    loop {
        let work = match rx.lock().unwrap().recv() {
            Ok(w)  => w,
            Err(_) => break, // sender dropped → shutdown
        };
        match work {
            WorkItem::Put { key, token, buf_ptr, buf_len, offset, size, result_tx } => {
                let (key_s, token_s) = match parse_strs(&key, &token) {
                    Ok(p) => p,
                    Err(e) => { let _ = result_tx.send(Err(e)); continue; }
                };
                let r = buffer.handle_put(key_s, offset, size, token_s, Some(channel_id))
                    .map_err(|e| format!("{e:?}"));
                if let Ok(n) = &r {
                    let copy_len = (*n).min(buf_len);
                    unsafe {
                        std::ptr::copy_nonoverlapping(
                            buffer.as_ptr(), buf_ptr as *mut u8, copy_len,
                        );
                    }
                }
                let _ = result_tx.send(r.map(|n| n.min(buf_len)));
            }
            WorkItem::Get { key, data_ptr, data_len, token, offset, result_tx } => {
                let (key_s, token_s) = match parse_strs(&key, &token) {
                    Ok(p) => p,
                    Err(e) => { let _ = result_tx.send(Err(e)); continue; }
                };
                let copy_len = data_len.min(buffer.len());
                unsafe {
                    std::ptr::copy_nonoverlapping(
                        data_ptr as *const u8, buffer.as_mut_ptr(), copy_len,
                    );
                }
                let r = buffer.handle_get(key_s, offset, copy_len, token_s, Some(channel_id))
                    .map_err(|e| format!("{e:?}"));
                let _ = result_tx.send(r);
            }
            WorkItem::PutZc { key, token, offset, size, write_fn, write_fn_data, result_tx } => {
                let (key_s, token_s) = match parse_strs(&key, &token) {
                    Ok(p) => p,
                    Err(e) => { let _ = result_tx.send(Err(e)); continue; }
                };
                let r = buffer.handle_put(key_s, offset, size, token_s, Some(channel_id))
                    .map_err(|e| format!("{e:?}"));
                if let Ok(n) = &r {
                    unsafe {
                        write_fn(write_fn_data.0, buffer.as_ptr() as *const c_void, *n);
                    }
                }
                let _ = result_tx.send(r);
            }
            WorkItem::GetZc { key, token, offset, read_fn, read_fn_data, result_tx } => {
                let (key_s, token_s) = match parse_strs(&key, &token) {
                    Ok(p) => p,
                    Err(e) => { let _ = result_tx.send(Err(e)); continue; }
                };
                let n = unsafe {
                    read_fn(read_fn_data.0, buffer.as_mut_ptr() as *mut c_void, buffer.len())
                };
                if n < 0 {
                    let _ = result_tx.send(Err(format!("read_fn returned {n}")));
                    continue;
                }
                let write_n = n as usize;
                let r = buffer.handle_get(key_s, offset, write_n, token_s, Some(channel_id))
                    .map_err(|e| format!("{e:?}"));
                let _ = result_tx.send(r);
            }
        }
    }

    // Free the channel before exiting.  `buffer` drops here → deregisters memory.
    server.free_channel_by_id(channel_id);
}

fn parse_strs<'a>(key: &'a [u8], token: &'a [u8]) -> Result<(&'a str, &'a str), String> {
    let ks = std::str::from_utf8(key).map_err(|e| format!("invalid key utf-8: {e}"))?;
    let ts = std::str::from_utf8(token).map_err(|e| format!("invalid token utf-8: {e}"))?;
    Ok((ks, ts))
}

// ── CuObjEngine ──────────────────────────────────────────────────────────────

/// Fixed-size worker pool for the cuObject RDMA server provider.
///
/// Each worker holds a private channel and pre-allocated RDMA buffer.  Channels
/// are never shared between threads — only the owning worker may use its
/// channel.
///
/// Implements [`RdmaServerProvider`] so it can be used anywhere the trait is
/// expected.
pub struct CuObjEngine {
    #[allow(dead_code)]
    server:  Arc<CuObjServer>,
    buf_size: usize,
    sender:  Option<std::sync::mpsc::Sender<WorkItem>>,
    workers: Vec<std::thread::JoinHandle<()>>,
}

// SAFETY: mutable state in mpsc channels; workers are fully independent.
unsafe impl Send for CuObjEngine {}
unsafe impl Sync for CuObjEngine {}

impl CuObjEngine {
    pub fn new(server: Arc<CuObjServer>, worker_count: usize, buf_size: usize) -> Self {
        let count = worker_count.max(1);
        let (tx, rx) = std::sync::mpsc::channel::<WorkItem>();
        let rx = Arc::new(Mutex::new(rx));
        let workers = (0..count)
            .map(|i| {
                let srv = Arc::clone(&server);
                let r   = Arc::clone(&rx);
                std::thread::Builder::new()
                    .name(format!("cuobj-engine-worker-{i}"))
                    .spawn(move || cuobj_worker_main(srv, buf_size, r))
                    .expect("failed to spawn cuobj engine worker")
            })
            .collect();
        Self { server, buf_size, sender: Some(tx), workers }
    }

    fn dispatch(&self, work: WorkItem) -> Result<(), RdmaError> {
        self.sender
            .as_ref()
            .ok_or_else(|| RdmaError::TransferFailed("engine shut down".into()))?
            .send(work)
            .map_err(|e| RdmaError::TransferFailed(e.to_string()))
    }
}

impl Drop for CuObjEngine {
    fn drop(&mut self) {
        drop(self.sender.take());
        for h in self.workers.drain(..) {
            let _ = h.join();
        }
    }
}

impl RdmaEngine for CuObjEngine {
    fn worker_count(&self) -> usize { self.workers.len() }
}

impl RdmaServerProvider for CuObjEngine {
    fn buf_size(&self) -> usize { self.buf_size }

    fn handle_put(
        &self, s3_key: &[u8], token: &[u8], buf: &mut [u8],
        offset: usize, size: usize,
    ) -> Result<usize, RdmaError> {
        let (result_tx, result_rx) = std::sync::mpsc::sync_channel(1);
        self.dispatch(WorkItem::Put {
            key: s3_key.to_vec(), token: token.to_vec(),
            buf_ptr: buf.as_mut_ptr() as usize, buf_len: buf.len(),
            offset, size, result_tx,
        })?;
        result_rx.recv()
            .map_err(|e| RdmaError::TransferFailed(e.to_string()))?
            .map_err(RdmaError::TransferFailed)
    }

    fn handle_get(
        &self, s3_key: &[u8], data: &[u8], token: &[u8], offset: usize,
    ) -> Result<usize, RdmaError> {
        let (result_tx, result_rx) = std::sync::mpsc::sync_channel(1);
        self.dispatch(WorkItem::Get {
            key: s3_key.to_vec(), token: token.to_vec(),
            data_ptr: data.as_ptr() as usize, data_len: data.len(),
            offset, result_tx,
        })?;
        result_rx.recv()
            .map_err(|e| RdmaError::TransferFailed(e.to_string()))?
            .map_err(RdmaError::TransferFailed)
    }

    unsafe fn handle_put_zc(
        &self, s3_key: &[u8], token: &[u8], offset: usize, size: usize,
        write_fn: RdmaIoWriteFn, write_fn_data: *mut c_void,
    ) -> Result<usize, RdmaError> {
        let (result_tx, result_rx) = std::sync::mpsc::sync_channel(1);
        self.dispatch(WorkItem::PutZc {
            key: s3_key.to_vec(), token: token.to_vec(),
            offset, size, write_fn, write_fn_data: SendPtr(write_fn_data),
            result_tx,
        })?;
        result_rx.recv()
            .map_err(|e| RdmaError::TransferFailed(e.to_string()))?
            .map_err(RdmaError::TransferFailed)
    }

    unsafe fn handle_get_zc(
        &self, s3_key: &[u8], token: &[u8], offset: usize,
        read_fn: RdmaIoReadFn, read_fn_data: *mut c_void,
    ) -> Result<usize, RdmaError> {
        let (result_tx, result_rx) = std::sync::mpsc::sync_channel(1);
        self.dispatch(WorkItem::GetZc {
            key: s3_key.to_vec(), token: token.to_vec(),
            offset, read_fn, read_fn_data: SendPtr(read_fn_data),
            result_tx,
        })?;
        result_rx.recv()
            .map_err(|e| RdmaError::TransferFailed(e.to_string()))?
            .map_err(RdmaError::TransferFailed)
    }
}
