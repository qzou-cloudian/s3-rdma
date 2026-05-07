//! Generic RDMA worker pool — bounded concurrency for client and server ops.
//!
//! Wraps a unified [`RdmaProvider`] in separate client and server thread pools.

use std::sync::{Arc, Mutex};
use std::sync::mpsc::{Receiver, SyncSender};

use crate::error::RdmaError;
use crate::provider::{RdmaClientProvider, RdmaServerChannel, RdmaServerProvider, RdmaProvider};

// ── Client work items ───────────────────────────────────────────────────────

pub enum ClientOp {
    /// Bind, write, prepare, send, complete — full PUT via `ch.put()`.
    Put {
        key:       Vec<u8>,
        size:      usize,
        offset:    usize,
        write:     usize,  // Box<Box<dyn FnOnce(&mut [u8]) -> usize + Send>> as *mut _ as usize
        send:      usize,  // Box<Box<dyn FnOnce(&[u8]) -> Result<(u16, usize), RdmaError> + Send>>
        result_tx: SyncSender<Result<usize, String>>,
    },
    /// Bind, prepare, send, complete, read — full GET via `ch.get()`.
    Get {
        key:       Vec<u8>,
        size:      usize,
        offset:    usize,
        send:      usize,  // Box<Box<dyn FnOnce(&[u8]) -> Result<(u16, usize), RdmaError> + Send>>
        read:      usize,  // Box<Box<dyn FnOnce(&[u8]) + Send>>
        result_tx: SyncSender<Result<usize, String>>,
    },
}

// ── Server work items ──────────────────────────────────────────────────────

enum ServerOp {
    Put {
        key:       Vec<u8>,
        token:     Vec<u8>,
        offset:    usize,
        size:      usize,
        write:     usize,  // Box<Box<dyn FnOnce(&[u8]) -> Result<usize, RdmaError> + Send>> as *mut _ as usize
        result_tx: SyncSender<Result<usize, String>>,
    },
    Get {
        key:       Vec<u8>,
        token:     Vec<u8>,
        offset:    usize,
        size:      usize,
        read:      usize,  // Box<Box<dyn FnOnce(&mut [u8]) -> usize + Send>> as *mut _ as usize
        result_tx: SyncSender<Result<usize, String>>,
    },
}

// ── Client worker ──────────────────────────────────────────────────────────

fn client_worker_main(
    provider: Arc<dyn RdmaProvider>,
    rx:       Arc<Mutex<Receiver<ClientOp>>>,
) {
    loop {
        let work = match rx.lock().unwrap().recv() {
            Ok(w)  => w,
            Err(_) => break,
        };
        match work {
            ClientOp::Put { key, size, offset, write, send, result_tx } => {
                let write = *unsafe { Box::from_raw(write as *mut Box<dyn FnOnce(&mut [u8]) -> usize + Send>) };
                let send  = *unsafe { Box::from_raw(send  as *mut Box<dyn FnOnce(&[u8]) -> Result<(u16, usize), RdmaError> + Send>) };
                let r = (|| {
                    let ch = RdmaClientProvider::bind(provider.as_ref(), size, &key)?;
                    ch.put(offset, size, write, send)
                })().map_err(|e: RdmaError| e.to_string());
                let _ = result_tx.send(r);
            }
            ClientOp::Get { key, size, offset, send, read, result_tx } => {
                let send = *unsafe { Box::from_raw(send as *mut Box<dyn FnOnce(&[u8]) -> Result<(u16, usize), RdmaError> + Send>) };
                let read = *unsafe { Box::from_raw(read as *mut Box<dyn FnOnce(&[u8]) + Send>) };
                let r = (|| {
                    let ch = RdmaClientProvider::bind(provider.as_ref(), size, &key)?;
                    ch.get(offset, size, send, read)
                })().map_err(|e: RdmaError| e.to_string());
                let _ = result_tx.send(r);
            }
        }
    }
}

// ── Server worker ──────────────────────────────────────────────────────────

fn server_worker_main(
    provider: Arc<dyn RdmaProvider>,
    rx:       Arc<Mutex<Receiver<ServerOp>>>,
) {
    loop {
        let work = match rx.lock().unwrap().recv() {
            Ok(w)  => w,
            Err(_) => break,
        };
        match work {
            ServerOp::Put { key, token, offset, size, write, result_tx } => {
                let write = *unsafe { Box::from_raw(write as *mut Box<dyn FnOnce(&[u8]) -> Result<usize, RdmaError> + Send>) };
                let r = (|| -> Result<usize, RdmaError> {
                    let ch = RdmaServerProvider::open_channel(provider.as_ref(), &key, &token)?;
                    ch.handle_put(offset, size, write)
                })().map_err(|e| e.to_string());
                let _ = result_tx.send(r);
            }
            ServerOp::Get { key, token, offset, size, read, result_tx } => {
                let read = *unsafe { Box::from_raw(read as *mut Box<dyn FnOnce(&mut [u8]) -> usize + Send>) };
                let r = (|| -> Result<usize, RdmaError> {
                    let ch = RdmaServerProvider::open_channel(provider.as_ref(), &key, &token)?;
                    ch.handle_get(offset, size, read)
                })().map_err(|e| e.to_string());
                let _ = result_tx.send(r);
            }
        }
    }
}

// ── RdmaWorkerPool ─────────────────────────────────────────────────────────

/// Bounded-concurrency pool with separate client and server thread pools.
pub struct RdmaWorkerPool {
    pub client_sender: Option<std::sync::mpsc::Sender<ClientOp>>,
    server_sender:    Option<std::sync::mpsc::Sender<ServerOp>>,
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
    sender: std::sync::mpsc::Sender<ServerOp>,
}

impl RdmaServerChannel for PoolServerChannel {
    fn key(&self)   -> &[u8] { &self.key }
    fn token(&self) -> &[u8] { &self.token }
    fn buf_size(&self) -> usize { 0 }

    fn handle_put(
        &self, offset: usize, size: usize,
        write: Box<dyn FnOnce(&[u8]) -> Result<usize, RdmaError> + Send + '_>,
    ) -> Result<usize, RdmaError> {
        let (tx, rx) = std::sync::mpsc::sync_channel(1);
        self.sender.send(ServerOp::Put {
            key: self.key.clone(), token: self.token.clone(),
            offset, size,
            write: Box::into_raw(Box::new(write)) as usize,
            result_tx: tx,
        }).map_err(|e| RdmaError::TransferFailed(e.to_string()))?;
        rx.recv().map_err(|e| RdmaError::TransferFailed(e.to_string()))?
            .map_err(RdmaError::TransferFailed)
    }

    fn handle_get(
        &self, offset: usize, size: usize,
        read: Box<dyn FnOnce(&mut [u8]) -> usize + Send + '_>,
    ) -> Result<usize, RdmaError> {
        let (tx, rx) = std::sync::mpsc::sync_channel(1);
        self.sender.send(ServerOp::Get {
            key: self.key.clone(), token: self.token.clone(),
            offset, size,
            read: Box::into_raw(Box::new(read)) as usize,
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
        let (client_tx, client_rx) = std::sync::mpsc::channel::<ClientOp>();
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
        let (server_tx, server_rx) = std::sync::mpsc::channel::<ServerOp>();
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

        // Client PUT through pool — write + send via `ch.put()`
        let (tx, rx) = std::sync::mpsc::sync_channel(1);
        let payload = b"ping".to_vec();
        let write: Box<dyn FnOnce(&mut [u8]) -> usize + Send> =
            Box::new(move |buf: &mut [u8]| {
                buf[..4].copy_from_slice(&payload);
                4
            });
        let send: Box<dyn FnOnce(&[u8]) -> Result<(u16, usize), RdmaError> + Send> =
            Box::new(|token: &[u8]| {
                let srv = MockRdmaProvider::new(false, "/dev/shm", 4096);
                let srv_ch = srv.open_channel(key, token).unwrap();
                let mut out: Vec<u8> = Vec::with_capacity(size);
                let n = srv_ch.handle_put(0, size, Box::new(|data: &[u8]| {
                    out.extend_from_slice(data);
                    Ok(data.len())
                })).unwrap();
                assert_eq!(&out[..4], b"ping");
                Ok((200, n))
            });
        sender.send(ClientOp::Put {
            key: key.to_vec(), size, offset: 0,
            write: Box::into_raw(Box::new(write)) as usize,
            send: Box::into_raw(Box::new(send)) as usize,
            result_tx: tx,
        }).unwrap();
        let n = rx.recv().unwrap().unwrap();
        assert_eq!(n, 4096);

        // Client GET through pool — send + read via `ch.get()`
        let (tx, rx) = std::sync::mpsc::sync_channel(1);
        let result = Arc::new(Mutex::new(Vec::new()));
        let r2 = Arc::clone(&result);
        let send: Box<dyn FnOnce(&[u8]) -> Result<(u16, usize), RdmaError> + Send> =
            Box::new(|token: &[u8]| {
                let srv = MockRdmaProvider::new(false, "/dev/shm", 4096);
                let srv_ch = srv.open_channel(key, token).unwrap();
                let payload: Vec<u8> = b"hello pool".to_vec();
                let n = srv_ch.handle_get(0, 0, Box::new(|buf: &mut [u8]| {
                    let n = payload.len().min(buf.len());
                    buf[..n].copy_from_slice(&payload[..n]);
                    n
                })).unwrap();
                Ok((200, n))
            });
        let read: Box<dyn FnOnce(&[u8]) + Send> =
            Box::new(move |data: &[u8]| r2.lock().unwrap().extend_from_slice(data));
        sender.send(ClientOp::Get {
            key: key.to_vec(), size, offset: 0,
            send: Box::into_raw(Box::new(send)) as usize,
            read: Box::into_raw(Box::new(read)) as usize,
            result_tx: tx,
        }).unwrap();
        let n = rx.recv().unwrap().unwrap();
        assert_eq!(n, 10);
        assert_eq!(&*result.lock().unwrap(), b"hello pool");
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
