//! CuObjWorkerPool — multi-threaded channel-based engine for cuobj.
//!
//! Each worker thread owns one cuobj channel.  Buffers come from a shared
//! [`BufferPool`] — acquired per-operation, released on completion.
//! Multiple async operations can be in-flight on a single channel.
//! Channels are never shared between threads.  Free before server drop.

use std::ffi::c_void;
use std::sync::{Arc, Mutex};
use std::sync::mpsc::{Receiver, SyncSender, TryRecvError, RecvTimeoutError};
use std::time::Duration;

use cuobj::server::CuObjServer;
use cuobj::CuObjAsyncEvent;
use s3_rdma::error::RdmaError;
use s3_rdma::pool::OwnedBufferGuard;
use s3_rdma::provider::{RdmaServerChannel, RdmaServerProvider};

const POLL_DELAY_US: u64 = 5;

// ── FFI — raw rbuf async helpers ───────────────────────────────────────────

extern "C" {
    fn cuobj_srv_handle_put_async_rbuf(
        handle: *mut c_void, key: *const i8, rbuf: *mut c_void,
        remote_start: u64, size: usize, descr: *const i8,
        channel: u16, async_handle: *mut c_void,
    ) -> isize;

    fn cuobj_srv_handle_get_async_rbuf(
        handle: *mut c_void, key: *const i8, rbuf: *mut c_void,
        remote_start: u64, size: usize, descr: *const i8,
        channel: u16, async_handle: *mut c_void,
    ) -> isize;

    fn cuobj_srv_poll(
        handle: *mut c_void, events: *mut CuObjAsyncEvent,
        max_events: i32, channel: u16,
    ) -> i32;
}

struct SendPtr(*mut c_void);
unsafe impl Send for SendPtr {}

// ── WorkItem ────────────────────────────────────────────────────────────────

enum WorkItem {
    Put {
        key: Vec<u8>, token: Vec<u8>,
        buf_ptr: usize, buf_len: usize,
        offset: usize, size: usize,
        result_tx: SyncSender<Result<usize, String>>,
    },
    Get {
        key: Vec<u8>, data_ptr: usize, data_len: usize,
        token: Vec<u8>, offset: usize,
        result_tx: SyncSender<Result<usize, String>>,
    },
    PutZc {
        key: Vec<u8>, token: Vec<u8>,
        offset: usize, size: usize,
        write_fn: s3_rdma::ffi::RdmaIoWriteFn, write_fn_data: SendPtr,
        result_tx: SyncSender<Result<usize, String>>,
    },
    GetZc {
        key: Vec<u8>, token: Vec<u8>, offset: usize,
        read_fn: s3_rdma::ffi::RdmaIoReadFn, read_fn_data: SendPtr,
        result_tx: SyncSender<Result<usize, String>>,
    },
}

struct InFlight {
    guard: OwnedBufferGuard,
    result_tx: SyncSender<Result<usize, String>>,
    zc: Option<(s3_rdma::ffi::RdmaIoWriteFn, *mut c_void)>,
    put_buf_ptr: usize,
    put_buf_len: usize,
}

// ── Worker ────────────────────────────────────────────────────────────────────

fn worker_main(
    pool: Arc<s3_rdma::pool::BufferPool>,
    srv:  Arc<CuObjServer>,
    rx:   Arc<Mutex<Receiver<WorkItem>>>,
) {
    let channel_id = match srv.alloc_channel() {
        Ok(ch) => { let id = ch.id(); std::mem::forget(ch); id }
        Err(e) => panic!("CuObjWorkerPool: alloc_channel failed: {e:?}"),
    };
    let handle = srv.handle();
    let mut inflight: Vec<InFlight> = Vec::with_capacity(pool.capacity());
    let mut events: Vec<CuObjAsyncEvent> = Vec::with_capacity(pool.capacity());

    loop {
        // Accept work while pool has free buffers.
        loop {
            let work = if inflight.is_empty() {
                match rx.lock().unwrap().recv() { Ok(w) => Some(w), Err(_) => None }
            } else {
                match rx.lock().unwrap().recv_timeout(Duration::from_micros(POLL_DELAY_US)) {
                    Ok(w) => Some(w),
                    Err(RecvTimeoutError::Timeout) => None,
                    Err(RecvTimeoutError::Disconnected) => None,
                }
            };
            let work = match work {
                Some(w) => w,
                None => {
                    if inflight.is_empty() { break; }
                    // Timeout with in-flight ops — go poll below.
                    break;
                }
            };

            let mut batch: Vec<WorkItem> = Vec::new();
            batch.push(work);
            {
                let rx = rx.lock().unwrap();
                while batch.len() < pool.available() {
                    match rx.try_recv() {
                        Ok(w) => batch.push(w),
                        Err(TryRecvError::Empty) => break,
                        Err(TryRecvError::Disconnected) => break,
                    }
                }
            }

            for work in batch.into_iter() {
                let guard = pool.acquire_owned_blocking();
                let rbuf = guard.reg_handle();
                let buf_idx = guard.as_ptr() as usize; // unique-per-op cookie
                let (result_tx, zc, put_buf_ptr, put_buf_len) =
                    submit_one(handle, rbuf, channel_id, buf_idx, guard.as_ptr(), guard.len(), &work);
                inflight.push(InFlight { guard, result_tx, zc, put_buf_ptr, put_buf_len });
            }

            // If pool exhausted, stop accepting — go poll.
            if pool.available() == 0 { break; }
        }

        if inflight.is_empty() { break; } // pool shut down AND no work

        // Poll for completions.  As completions arrive, guards drop → buffers
        // return to pool → we loop back and can accept more work.
        events.resize(inflight.len(), CuObjAsyncEvent { async_handle: std::ptr::null_mut(), status: 0 });
        let n = unsafe { cuobj_srv_poll(handle, events.as_mut_ptr(), events.len() as i32, channel_id) };
        match n {
            0 => {
                if pool.available() == 0 {
                    std::thread::sleep(Duration::from_micros(POLL_DELAY_US));
                }
                continue;
            }
            n if n > 0 => {
                for i in 0..n as usize {
                    let ev = &events[i];
                    let buf_ptr = ev.async_handle as usize;
                    if let Some(pos) = inflight.iter().position(|f| f.guard.as_ptr() as usize == buf_ptr) {
                        let f = inflight.swap_remove(pos);
                        let r = if ev.status >= 0 {
                            let nb = ev.status as usize;
                            if let Some((write_fn, data)) = f.zc {
                                unsafe { write_fn(data, f.guard.as_ptr() as *const c_void, nb); }
                                Ok(nb)
                            } else if f.put_buf_len > 0 {
                                let cl = nb.min(f.put_buf_len);
                                unsafe { std::ptr::copy_nonoverlapping(
                                    f.guard.as_ptr(), f.put_buf_ptr as *mut u8, cl,
                                ); }
                                Ok(cl)
                            } else { Ok(nb) }
                        } else { Err(format!("RDMA failed: status={}", ev.status)) };
                        let _ = f.result_tx.send(r);
                    }
                }
            }
            _ => {
                tracing::error!("cuobj_srv_poll: error {n}");
                for f in inflight.drain(..) {
                    let _ = f.result_tx.send(Err(format!("poll error: {n}")));
                }
            }
        }
    }

    srv.free_channel_by_id(channel_id);
}

fn submit_one(
    handle: *mut c_void, rbuf: *mut c_void, channel_id: u16,
    buf_ptr: usize, host_ptr: *const u8, buf_len: usize,
    work: &WorkItem,
) -> (SyncSender<Result<usize, String>>, Option<(s3_rdma::ffi::RdmaIoWriteFn, *mut c_void)>, usize, usize) {
    let h = buf_ptr as *mut c_void;
    match work {
        WorkItem::Put { key, token, offset, size, buf_ptr: pb, buf_len: bl, result_tx, .. } => {
            let ks = std::ffi::CString::new(key.as_slice()).unwrap_or_default();
            let ts = std::ffi::CString::new(token.as_slice()).unwrap_or_default();
            let rc = unsafe { cuobj_srv_handle_put_async_rbuf(handle, ks.as_ptr(), rbuf, 0, *size, ts.as_ptr(), channel_id, h) };
            if rc != 0 { let _ = result_tx.send(Err(format!("submit err {rc}"))); }
            (result_tx.clone(), None, *pb, *bl)
        }
        WorkItem::Get { key, data_ptr, data_len, token, offset, result_tx, .. } => {
            let cl = (*data_len).min(buf_len);
            unsafe { std::ptr::copy_nonoverlapping(*data_ptr as *const u8, host_ptr as *mut u8, cl); }
            let ks = std::ffi::CString::new(key.as_slice()).unwrap_or_default();
            let ts = std::ffi::CString::new(token.as_slice()).unwrap_or_default();
            let rc = unsafe { cuobj_srv_handle_get_async_rbuf(handle, ks.as_ptr(), rbuf, 0, cl, ts.as_ptr(), channel_id, h) };
            if rc != 0 { let _ = result_tx.send(Err(format!("submit err {rc}"))); }
            (result_tx.clone(), None, 0, 0)
        }
        WorkItem::PutZc { key, token, offset, size, write_fn, write_fn_data, result_tx } => {
            let ks = std::ffi::CString::new(key.as_slice()).unwrap_or_default();
            let ts = std::ffi::CString::new(token.as_slice()).unwrap_or_default();
            let rc = unsafe { cuobj_srv_handle_put_async_rbuf(handle, ks.as_ptr(), rbuf, 0, *size, ts.as_ptr(), channel_id, h) };
            if rc != 0 { let _ = result_tx.send(Err(format!("submit err {rc}"))); }
            (result_tx.clone(), Some((*write_fn, write_fn_data.0)), 0, 0)
        }
        WorkItem::GetZc { key, token, offset, read_fn, read_fn_data, result_tx } => {
            let n = unsafe { read_fn(read_fn_data.0, host_ptr as *mut c_void, buf_len) };
            if n < 0 { let _ = result_tx.send(Err(format!("read_fn returned {n}"))); }
            let wn = n as usize;
            let ks = std::ffi::CString::new(key.as_slice()).unwrap_or_default();
            let ts = std::ffi::CString::new(token.as_slice()).unwrap_or_default();
            let rc = unsafe { cuobj_srv_handle_get_async_rbuf(handle, ks.as_ptr(), rbuf, 0, wn, ts.as_ptr(), channel_id, h) };
            if rc != 0 { let _ = result_tx.send(Err(format!("submit err {rc}"))); }
            (result_tx.clone(), None, 0, 0)
        }
    }
}

// ── CuObjWorkerPool ─────────────────────────────────────────────────────────

pub struct CuObjWorkerPool {
    pool:    Arc<s3_rdma::pool::BufferPool>,
    buf_size: usize,
    sender:  Option<std::sync::mpsc::Sender<WorkItem>>,
    workers: Vec<std::thread::JoinHandle<()>>,
}

unsafe impl Send for CuObjWorkerPool {}
unsafe impl Sync for CuObjWorkerPool {}

impl CuObjWorkerPool {
    pub fn new(
        server: Arc<CuObjServer>, pool: Arc<s3_rdma::pool::BufferPool>,
        worker_count: usize, buf_size: usize,
    ) -> Self {
        let (tx, rx) = std::sync::mpsc::channel();
        let rx = Arc::new(Mutex::new(rx));
        let workers = (0..worker_count.max(1)).map(|i| {
            let p = Arc::clone(&pool);
            let s = Arc::clone(&server);
            let r = Arc::clone(&rx);
            std::thread::Builder::new().name(format!("cuobj-w-{i}"))
                .spawn(move || worker_main(p, s, r)).expect("spawn")
        }).collect();
        Self { pool, buf_size, sender: Some(tx), workers }
    }
}

impl Drop for CuObjWorkerPool {
    fn drop(&mut self) {
        drop(self.sender.take());
        for h in self.workers.drain(..) { let _ = h.join(); }
    }
}

impl RdmaServerProvider for CuObjWorkerPool {
    fn buf_size(&self) -> usize { self.buf_size }
    fn open_channel(&self, key: &[u8], token: &[u8]) -> Result<Box<dyn RdmaServerChannel>, RdmaError> {
        let s = self.sender.as_ref().ok_or_else(|| RdmaError::TransferFailed("pool shut down".into()))?;
        Ok(Box::new(PoolChannel { key: key.to_vec(), token: token.to_vec(), buf_size: self.buf_size, sender: s.clone() }))
    }
}

// ── PoolChannel ─────────────────────────────────────────────────────────────

struct PoolChannel { key: Vec<u8>, token: Vec<u8>, buf_size: usize, sender: std::sync::mpsc::Sender<WorkItem> }

fn dispatch(sender: &std::sync::mpsc::Sender<WorkItem>, work: WorkItem) -> Result<usize, RdmaError> {
    let (tx, rx) = std::sync::mpsc::sync_channel(1);
    let w = match work {
        WorkItem::Put { key, token, buf_ptr, buf_len, offset, size, .. } =>
            WorkItem::Put { key, token, buf_ptr, buf_len, offset, size, result_tx: tx },
        WorkItem::Get { key, data_ptr, data_len, token, offset, .. } =>
            WorkItem::Get { key, data_ptr, data_len, token, offset, result_tx: tx },
        WorkItem::PutZc { key, token, offset, size, write_fn, write_fn_data, .. } =>
            WorkItem::PutZc { key, token, offset, size, write_fn, write_fn_data, result_tx: tx },
        WorkItem::GetZc { key, token, offset, read_fn, read_fn_data, .. } =>
            WorkItem::GetZc { key, token, offset, read_fn, read_fn_data, result_tx: tx },
    };
    sender.send(w).map_err(|e| RdmaError::TransferFailed(e.to_string()))?;
    rx.recv().map_err(|_| RdmaError::TransferFailed("worker disconnected".into()))?
        .map_err(RdmaError::TransferFailed)
}

impl RdmaServerChannel for PoolChannel {
    fn key(&self) -> &[u8] { &self.key }
    fn token(&self) -> &[u8] { &self.token }
    fn buf_size(&self) -> usize { self.buf_size }
    fn handle_put(&self, b: &mut [u8], o: usize, s: usize) -> Result<usize, RdmaError> {
        dispatch(&self.sender, WorkItem::Put{key:self.key.clone(),token:self.token.clone(),buf_ptr:b.as_mut_ptr()as usize,buf_len:b.len(),offset:o,size:s,result_tx:unsafe{std::mem::zeroed()}})
    }
    fn handle_get(&self, d: &[u8], o: usize) -> Result<usize, RdmaError> {
        dispatch(&self.sender, WorkItem::Get{key:self.key.clone(),data_ptr:d.as_ptr()as usize,data_len:d.len(),token:self.token.clone(),offset:o,result_tx:unsafe{std::mem::zeroed()}})
    }
    unsafe fn handle_put_zc(&self, o: usize, s: usize, wf: s3_rdma::ffi::RdmaIoWriteFn, wd: *mut c_void) -> Result<usize, RdmaError> {
        dispatch(&self.sender, WorkItem::PutZc{key:self.key.clone(),token:self.token.clone(),offset:o,size:s,write_fn:wf,write_fn_data:SendPtr(wd),result_tx:unsafe{std::mem::zeroed()}})
    }
    unsafe fn handle_get_zc(&self, o: usize, rf: s3_rdma::ffi::RdmaIoReadFn, rd: *mut c_void) -> Result<usize, RdmaError> {
        dispatch(&self.sender, WorkItem::GetZc{key:self.key.clone(),token:self.token.clone(),offset:o,read_fn:rf,read_fn_data:SendPtr(rd),result_tx:unsafe{std::mem::zeroed()}})
    }
}
