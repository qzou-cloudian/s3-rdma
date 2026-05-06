//! Generic RDMA worker pool — bounded concurrency for client and server ops.
//!
//! Wraps a unified [`RdmaProvider`] in separate client and server thread pools.

use std::sync::{Arc, Mutex};
use std::sync::mpsc::{Receiver, SyncSender};

use crate::error::RdmaError;
use crate::ffi::{RdmaIoReadFn, RdmaIoWriteFn};
use crate::provider::{RdmaClientChannel, RdmaClientProvider, RdmaPutHandle, RdmaGetHandle};
use crate::provider::{RdmaServerChannel, RdmaServerProvider, RdmaProvider};

// ── Client work items ───────────────────────────────────────────────────────

type ClientPutResult = Result<(RdmaPutHandle, Box<dyn RdmaClientChannel>), String>;
type ClientGetResult = Result<(RdmaGetHandle, Box<dyn RdmaClientChannel>), String>;

pub(crate) enum ClientWorkItem {
    /// Bind a buffer, fill it via write_fn, prepare PUT token.
    /// Returns the token + the channel for later completion.
    Put {
        key:           Vec<u8>,
        size:          usize,
        offset:        usize,
        write_fn:      usize,       // RdmaIoWriteFn cast to usize for Send
        write_fn_data: usize,
        result_tx:     SyncSender<ClientPutResult>,
    },
    /// Bind a buffer, prepare GET token.
    /// Returns the token + the channel.
    Get {
        key:           Vec<u8>,
        size:          usize,
        offset:        usize,
        result_tx:     SyncSender<ClientGetResult>,
    },
}

// ── Server work items ──────────────────────────────────────────────────────

enum ServerWorkItem {
    Put {
        key:           Vec<u8>,
        token:         Vec<u8>,
        offset:        usize,
        size:          usize,
        write_fn:      usize,
        write_fn_data: usize,
        result_tx:     SyncSender<Result<usize, String>>,
    },
    Get {
        key:           Vec<u8>,
        token:         Vec<u8>,
        offset:        usize,
        read_fn:       usize,
        read_fn_data:  usize,
        result_tx:     SyncSender<Result<usize, String>>,
    },
}

// ── Client worker ──────────────────────────────────────────────────────────

fn client_worker_main(
    provider: Arc<dyn RdmaProvider>,
    rx:       Arc<Mutex<Receiver<ClientWorkItem>>>,
) {
    loop {
        let work = match rx.lock().unwrap().recv() {
            Ok(w)  => w,
            Err(_) => break,
        };
        match work {
            ClientWorkItem::Put { key, size, offset, write_fn, write_fn_data, result_tx } => {
                let r = (|| {
                    let ch = RdmaClientProvider::bind(provider.as_ref(), size, &key)?;
                    let write_fn: RdmaIoWriteFn = unsafe { std::mem::transmute(write_fn) };
                    let write_fn_data = write_fn_data as *mut std::ffi::c_void;
                    let wr = unsafe { write_fn(write_fn_data, ch.ptr() as *const std::ffi::c_void, size) };
                    if wr < 0 {
                        return Err(RdmaError::TransferFailed(format!("write_fn returned {wr}")));
                    }
                    let handle = ch.prepare_put(offset, size)?;
                    Ok((handle, ch))
                })().map_err(|e: RdmaError| e.to_string());
                let _ = result_tx.send(r);
            }
            ClientWorkItem::Get { key, size, offset, result_tx } => {
                let r = (|| {
                    let ch = RdmaClientProvider::bind(provider.as_ref(), size, &key)?;
                    let handle = ch.prepare_get(offset, size)?;
                    Ok((handle, ch))
                })().map_err(|e: RdmaError| e.to_string());
                let _ = result_tx.send(r);
            }
        }
    }
}

// ── Server worker ──────────────────────────────────────────────────────────

fn server_worker_main(
    provider: Arc<dyn RdmaProvider>,
    rx:       Arc<Mutex<Receiver<ServerWorkItem>>>,
) {
    loop {
        let work = match rx.lock().unwrap().recv() {
            Ok(w)  => w,
            Err(_) => break,
        };
        match work {
            ServerWorkItem::Put { key, token, offset, size, write_fn, write_fn_data, result_tx } => {
                let write_fn: RdmaIoWriteFn = unsafe { std::mem::transmute(write_fn) };
                let write_fn_data = write_fn_data as *mut std::ffi::c_void;
                let r = (|| -> Result<usize, RdmaError> {
                    let ch = RdmaServerProvider::open_channel(provider.as_ref(), &key, &token)?;
                    unsafe { ch.handle_put(offset, size, write_fn, write_fn_data) }
                })().map_err(|e| e.to_string());
                let _ = result_tx.send(r);
            }
            ServerWorkItem::Get { key, token, offset, read_fn, read_fn_data, result_tx } => {
                let read_fn: RdmaIoReadFn = unsafe { std::mem::transmute(read_fn) };
                let read_fn_data = read_fn_data as *mut std::ffi::c_void;
                let r = (|| -> Result<usize, RdmaError> {
                    let ch = RdmaServerProvider::open_channel(provider.as_ref(), &key, &token)?;
                    unsafe { ch.handle_get(offset, read_fn, read_fn_data) }
                })().map_err(|e| e.to_string());
                let _ = result_tx.send(r);
            }
        }
    }
}

// ── RdmaWorkerPool ─────────────────────────────────────────────────────────

/// Bounded-concurrency pool with separate client and server thread pools.
pub struct RdmaWorkerPool {
    pub(crate) client_sender: Option<std::sync::mpsc::Sender<ClientWorkItem>>,
    server_sender:    Option<std::sync::mpsc::Sender<ServerWorkItem>>,
    _provider:        Arc<dyn RdmaProvider>,
    client_workers:   Vec<std::thread::JoinHandle<()>>,
    server_workers:   Vec<std::thread::JoinHandle<()>>,
}

unsafe impl Send for RdmaWorkerPool {}
unsafe impl Sync for RdmaWorkerPool {}

impl RdmaWorkerPool {
    pub fn builder() -> RdmaWorkerPoolBuilder {
        RdmaWorkerPoolBuilder::default()
    }
}

impl Drop for RdmaWorkerPool {
    fn drop(&mut self) {
        drop(self.client_sender.take());
        drop(self.server_sender.take());
        for h in self.client_workers.drain(..) { let _ = h.join(); }
        for h in self.server_workers.drain(..) { let _ = h.join(); }
    }
}

// ── RdmaServerProvider impl ────────────────────────────────────────────────

impl RdmaServerProvider for RdmaWorkerPool {
    fn buf_size(&self) -> usize { 0 }

    fn open_channel(&self, key: &[u8], token: &[u8])
        -> Result<Box<dyn RdmaServerChannel>, RdmaError>
    {
        let sender = self.server_sender.as_ref()
            .ok_or_else(|| RdmaError::TransferFailed("pool shut down".into()))?
            .clone();
        Ok(Box::new(PoolServerChannel {
            key: key.to_vec(),
            token: token.to_vec(),
            sender,
        }))
    }
}

// ── PoolServerChannel ──────────────────────────────────────────────────────

struct PoolServerChannel {
    key:    Vec<u8>,
    token:  Vec<u8>,
    sender: std::sync::mpsc::Sender<ServerWorkItem>,
}

impl RdmaServerChannel for PoolServerChannel {
    fn key(&self)   -> &[u8] { &self.key }
    fn token(&self) -> &[u8] { &self.token }
    fn buf_size(&self) -> usize { 0 }

    unsafe fn handle_put(
        &self, offset: usize, size: usize,
        write_fn: RdmaIoWriteFn, write_fn_data: *mut std::ffi::c_void,
    ) -> Result<usize, RdmaError> {
        let (tx, rx) = std::sync::mpsc::sync_channel(1);
        self.sender.send(ServerWorkItem::Put {
            key: self.key.clone(), token: self.token.clone(),
            offset, size,
            write_fn: write_fn as usize,
            write_fn_data: write_fn_data as usize,
            result_tx: tx,
        }).map_err(|e| RdmaError::TransferFailed(e.to_string()))?;
        rx.recv().map_err(|e| RdmaError::TransferFailed(e.to_string()))?
            .map_err(RdmaError::TransferFailed)
    }

    unsafe fn handle_get(
        &self, offset: usize,
        read_fn: RdmaIoReadFn, read_fn_data: *mut std::ffi::c_void,
    ) -> Result<usize, RdmaError> {
        let (tx, rx) = std::sync::mpsc::sync_channel(1);
        self.sender.send(ServerWorkItem::Get {
            key: self.key.clone(), token: self.token.clone(),
            offset,
            read_fn: read_fn as usize,
            read_fn_data: read_fn_data as usize,
            result_tx: tx,
        }).map_err(|e| RdmaError::TransferFailed(e.to_string()))?;
        rx.recv().map_err(|e| RdmaError::TransferFailed(e.to_string()))?
            .map_err(RdmaError::TransferFailed)
    }
}

// ── Builder ────────────────────────────────────────────────────────────────

pub struct RdmaWorkerPoolBuilder {
    client_workers: usize,
    server_workers: usize,
    provider:       Option<Arc<dyn RdmaProvider>>,
}

impl Default for RdmaWorkerPoolBuilder {
    fn default() -> Self {
        Self { client_workers: 1, server_workers: 2, provider: None }
    }
}

impl RdmaWorkerPoolBuilder {
    pub fn client_workers(mut self, n: usize) -> Self { self.client_workers = n; self }
    pub fn server_workers(mut self, n: usize) -> Self { self.server_workers = n; self }

    pub fn provider(mut self, p: Arc<dyn RdmaProvider>) -> Self {
        self.provider = Some(p);
        self
    }

    pub fn build(self) -> Result<RdmaWorkerPool, RdmaError> {
        let provider = self.provider
            .ok_or_else(|| RdmaError::OperationNotSupported("provider required".into()))?;

        // Client workers
        let (client_tx, client_rx) = std::sync::mpsc::channel::<ClientWorkItem>();
        let client_rx = Arc::new(Mutex::new(client_rx));
        let client_workers: Vec<_> = (0..self.client_workers.max(1))
            .map(|i| {
                let p  = Arc::clone(&provider);
                let rx = Arc::clone(&client_rx);
                std::thread::Builder::new()
                    .name(format!("rdma-client-{i}"))
                    .spawn(move || client_worker_main(p, rx))
                    .expect("failed to spawn client worker")
            })
            .collect();

        // Server workers
        let (server_tx, server_rx) = std::sync::mpsc::channel::<ServerWorkItem>();
        let server_rx = Arc::new(Mutex::new(server_rx));
        let server_workers: Vec<_> = (0..self.server_workers.max(1))
            .map(|i| {
                let p  = Arc::clone(&provider);
                let rx = Arc::clone(&server_rx);
                std::thread::Builder::new()
                    .name(format!("rdma-server-{i}"))
                    .spawn(move || server_worker_main(p, rx))
                    .expect("failed to spawn server worker")
            })
            .collect();

        Ok(RdmaWorkerPool {
            client_sender: Some(client_tx),
            server_sender: Some(server_tx),
            _provider: provider,
            client_workers,
            server_workers,
        })
    }
}

// ── Tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mock::MockRdmaProvider;
    use std::sync::Arc;

    #[test]
    fn mock_pool_client_and_server() {
        let mock = Arc::new(MockRdmaProvider::new(false, "/dev/shm", 4096));
        let pool = RdmaWorkerPool::builder()
            .client_workers(2)
            .server_workers(2)
            .provider(Arc::clone(&mock) as Arc<dyn RdmaProvider>)
            .build().unwrap();

        let key = b"test-bucket/key";
        let size = 4096;
        let sender = pool.client_sender.as_ref().unwrap().clone();

        // Client PUT through pool.
        let payload = b"ping".to_vec();
        unsafe extern "C" fn put_write(user_data: *mut std::ffi::c_void, buf: *const std::ffi::c_void, n: usize) -> isize {
            let src_data = &*(user_data as *const Vec<u8>);
            let dst = std::slice::from_raw_parts_mut(buf as *mut u8, n);
            let len = src_data.len().min(n);
            dst[..len].copy_from_slice(&src_data[..len]);
            len as isize
        }
        let (tx, rx) = std::sync::mpsc::sync_channel(1);
        sender.send(ClientWorkItem::Put {
            key: key.to_vec(), size, offset: 0,
            write_fn: put_write as usize,
            write_fn_data: &payload as *const Vec<u8> as usize,
            result_tx: tx,
        }).unwrap();
        let (put_handle, client_ch) = rx.recv().unwrap().unwrap();

        // Server PUT through pool.
        let mut put_buf: Vec<u8> = Vec::with_capacity(size);
        unsafe extern "C" fn srv_put_write(user_data: *mut std::ffi::c_void, buf: *const std::ffi::c_void, n: usize) -> isize {
            let out = &mut *(user_data as *mut Vec<u8>);
            let src = std::slice::from_raw_parts(buf as *const u8, n);
            out.extend_from_slice(src);
            n as isize
        }
        {
            let srv_ch = pool.open_channel(key, put_handle.token()).unwrap();
            let n = unsafe { srv_ch.handle_put(0, size, srv_put_write, &mut put_buf as *mut Vec<u8> as *mut std::ffi::c_void) }.unwrap();
            assert_eq!(n, 4096);
            assert_eq!(&put_buf[..4], b"ping");
        }

        // Complete PUT.
        client_ch.complete_put(put_handle, 200, 4096).unwrap();
        drop(client_ch);

        // Client GET through pool.
        let (tx, rx) = std::sync::mpsc::sync_channel(1);
        sender.send(ClientWorkItem::Get {
            key: key.to_vec(), size, offset: 0,
            result_tx: tx,
        }).unwrap();
        let (get_handle, client_ch) = rx.recv().unwrap().unwrap();

        // Server GET through pool.
        let get_data = b"hello pool".to_vec();
        unsafe extern "C" fn srv_get_read(user_data: *mut std::ffi::c_void, buf: *mut std::ffi::c_void, capacity: usize) -> isize {
            let src = &*(user_data as *const Vec<u8>);
            let dst = std::slice::from_raw_parts_mut(buf as *mut u8, capacity);
            let n = src.len().min(capacity);
            dst[..n].copy_from_slice(&src[..n]);
            n as isize
        }
        {
            let srv_ch = pool.open_channel(key, get_handle.token()).unwrap();
            let n = unsafe { srv_ch.handle_get(0, srv_get_read, &get_data as *const Vec<u8> as *mut std::ffi::c_void) }.unwrap();
            assert_eq!(n, 10);
        }

        // Complete GET — data lands in the channel buffer.
        client_ch.complete_get(get_handle, 200, 10).unwrap();
        let data = unsafe { std::slice::from_raw_parts(client_ch.ptr() as *const u8, 10) };
        assert_eq!(data, b"hello pool");
        drop(client_ch);
    }

    #[test]
    fn pool_drop_joins_workers() {
        let mock = Arc::new(MockRdmaProvider::new(false, "/dev/shm", 4096));
        let pool = RdmaWorkerPool::builder()
            .client_workers(2)
            .server_workers(2)
            .provider(mock as Arc<dyn RdmaProvider>)
            .build().unwrap();
        drop(pool);
    }
}
