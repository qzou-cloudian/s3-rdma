//! Multi-threaded ping-pong with RdmaWorkerPool.
//!
//! Same flow as pingpong.rs, but server ops dispatch through a 2-worker pool.
//!
//!   cargo run --example pingpong_pool --manifest-path core/Cargo.toml

use std::alloc::{Layout, alloc, dealloc};
use std::sync::Arc;

use s3_rdma::mock::MockRdmaProvider;
use s3_rdma::provider::{RdmaClientProvider, RdmaServerProvider, RDMA_REQUIRED_ALIGNMENT};
use s3_rdma::worker_pool::RdmaWorkerPool;

fn main() {
    println!("=== ping-pong (2-worker pool) ===\n");

    let size = 4096;
    let layout = Layout::from_size_align(size, RDMA_REQUIRED_ALIGNMENT).unwrap();
    let ptr = unsafe { alloc(layout) };
    let data = unsafe { std::slice::from_raw_parts_mut(ptr, size) };
    let key = b"bucket/pingpong.dat";

    // Client: same as single-threaded.
    let client = MockRdmaProvider::new(false, "/dev/shm", 4096);
    let ch = client.bind(ptr, size, key).unwrap();
    println!("client: buffer {ptr:p}, {} bytes", size);

    // Server: wrapped in 2-worker pool.
    let server: Arc<dyn RdmaServerProvider> = Arc::new(MockRdmaProvider::new(false, "/dev/shm", 4096));
    let pool = RdmaWorkerPool::new(server, 2);

    // ── PUT through pool ────────────────────────────────────────────────────

    let payload = b"ping";
    unsafe { std::ptr::copy_nonoverlapping(payload.as_ptr(), ptr, payload.len()); }
    let put_token = ch.prepare_put_token(0, size).unwrap();

    let mut put_buf = vec![0u8; size];
    let srv_ch = pool.open_channel(key, &put_token).unwrap();
    let n = srv_ch.handle_put(&mut put_buf, 0, size).unwrap();
    println!("PUT:  received {} bytes: '{}'",
             n, std::str::from_utf8(&put_buf[..payload.len()]).unwrap());

    // ── GET through pool ────────────────────────────────────────────────────

    let get_token = ch.prepare_get_token(0, size).unwrap();
    let srv_ch = pool.open_channel(key, &get_token).unwrap();
    let n = srv_ch.handle_get(b"pong", 0).unwrap();
    println!("GET:  written {} bytes", n);

    ch.process_rdma_reply(200, &get_token, n, Box::new(|r| assert_eq!(r.error_code, 0))).unwrap();
    assert_eq!(&data[..4], b"pong");
    println!("      client buffer: '{}'", std::str::from_utf8(&data[..4]).unwrap());

    // ── Cleanup ─────────────────────────────────────────────────────────────

    ch.process_rdma_reply(200, &put_token, 4096, Box::new(|_| {})).unwrap();
    drop(ch);
    // pool drops here — joins workers
    unsafe { dealloc(ptr, layout); }
    println!("\nping-pong (pool) OK.");
}
