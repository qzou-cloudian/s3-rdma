//! Multi-threaded ping-pong with RdmaWorkerPool.
//!
//!   cargo run --example pingpong_pool --manifest-path core/Cargo.toml

use std::sync::{Arc, Mutex};

use s3_rdma::mock::MockRdmaProvider;
use s3_rdma::provider::RdmaServerProvider;
use s3_rdma::worker_pool::{RdmaWorkerPool, ClientOp};

fn main() {
    println!("=== ping-pong (2-worker pool) ===\n");

    let size = 4096;
    let key = b"bucket/pingpong.dat";
    let mock = Arc::new(MockRdmaProvider::new(false, "/dev/shm", size));

    let pool = RdmaWorkerPool::builder()
        .client_workers(2)
        .server_workers(2)
        .provider(Arc::clone(&mock) as Arc<dyn s3_rdma::provider::RdmaProvider>)
        .build().unwrap();
    let sender = pool.client_sender.as_ref().unwrap().clone();

    // ── PUT through pool ────────────────────────────────────────────────────

    let (tx, rx) = std::sync::mpsc::sync_channel(1);
    let payload = b"ping".to_vec();
    sender.send(ClientOp::Put {
        key: key.to_vec(), size, offset: 0,
        write: Box::into_raw(Box::new(Box::new(move |buf: &mut [u8]| {
            buf[..4].copy_from_slice(&payload); 4usize
        }) as Box<dyn FnOnce(&mut [u8]) -> usize + Send>)) as usize,
        send: Box::into_raw(Box::new(Box::new(|token: &[u8]| {
            let srv = MockRdmaProvider::new(false, "/dev/shm", size);
            let ch = srv.open_channel(key, token).unwrap();
            let mut out = Vec::new();
            let n = ch.handle_put(0, size, Box::new(|data: &[u8]| {
                out.extend_from_slice(data);
                Ok(data.len())
            })).unwrap();
            println!("PUT:  received {} bytes: '{}'", n, std::str::from_utf8(&out[..4]).unwrap());
            Ok::<(u16, usize), s3_rdma::RdmaError>((200, n))
        }) as Box<dyn FnOnce(&[u8]) -> Result<(u16, usize), s3_rdma::RdmaError> + Send>)) as usize,
        result_tx: tx,
    }).unwrap();
    let n = rx.recv().unwrap().unwrap();
    assert_eq!(n, size);

    // ── GET through pool ────────────────────────────────────────────────────

    let (tx, rx) = std::sync::mpsc::sync_channel(1);
    let received = Arc::new(Mutex::new(Vec::new()));
    let r2 = Arc::clone(&received);
    sender.send(ClientOp::Get {
        key: key.to_vec(), size, offset: 0,
        send: Box::into_raw(Box::new(Box::new(|token: &[u8]| {
            let srv = MockRdmaProvider::new(false, "/dev/shm", size);
            let ch = srv.open_channel(key, token).unwrap();
            let pong: Vec<u8> = b"pong".to_vec();
            let n = ch.handle_get(0, 0, Box::new(move |buf: &mut [u8]| {
                let n = pong.len().min(buf.len());
                buf[..n].copy_from_slice(&pong[..n]);
                n
            })).unwrap();
            println!("GET:  written {} bytes", n);
            Ok::<(u16, usize), s3_rdma::RdmaError>((200, n))
        }) as Box<dyn FnOnce(&[u8]) -> Result<(u16, usize), s3_rdma::RdmaError> + Send>)) as usize,
        read: Box::into_raw(Box::new(Box::new(move |data: &[u8]| {
            r2.lock().unwrap().extend_from_slice(data)
        }) as Box<dyn FnOnce(&[u8]) + Send>)) as usize,
        result_tx: tx,
    }).unwrap();
    let n = rx.recv().unwrap().unwrap();
    assert_eq!(n, 4);
    assert_eq!(&*received.lock().unwrap(), b"pong");
    println!("      client buffer: '{}'", std::str::from_utf8(&received.lock().unwrap()).unwrap());

    println!("\nping-pong (pool) OK.");
}
