//! Example: s3-rdma worker pool with mock provider (no hardware required).
//!
//! Demonstrates wrapping a MockRdmaProvider in an RdmaWorkerPool for bounded
//! concurrency, then using the channel-based API.
//!
//! Run with:
//!   cargo run --example mock_engine --features engine --manifest-path core/Cargo.toml

use std::alloc::{Layout, alloc, dealloc};
use std::sync::Arc;

use s3_rdma::mock::MockRdmaProvider;
use s3_rdma::provider::{RdmaClientProvider, RdmaServerProvider, RDMA_REQUIRED_ALIGNMENT};
use s3_rdma::worker_pool::RdmaWorkerPool;
use s3_rdma::rdma_provider_info;

fn main() {
    println!("=== s3-rdma worker pool example ===\n");
    println!("Provider info: {}\n", rdma_provider_info());

    // Client ops: use the provider directly.
    let provider = MockRdmaProvider::new(true, "/dev/shm", 4096);

    // Server ops: wrap in a worker pool (2 threads) for bounded concurrency.
    let server_provider: Arc<dyn RdmaServerProvider> =
        Arc::new(MockRdmaProvider::new(true, "/dev/shm", 4096));
    let pool = RdmaWorkerPool::new(server_provider, 2);
    println!("Pool     : shared provider, 2 workers");
    println!("Buf size : {} bytes", pool.buf_size());

    // RDMA requires 4 KiB-aligned buffers.
    let buf_size = 4096;
    let layout = Layout::from_size_align(buf_size, RDMA_REQUIRED_ALIGNMENT).unwrap();
    let ptr = unsafe { alloc(layout) };
    let data = unsafe { std::slice::from_raw_parts_mut(ptr, buf_size) };
    data.fill(0);

    let key = b"my-bucket/my-object.bin";

    // ── Client-side ──────────────────────────────────────────────────────────

    println!("\n--- Client operations (direct) ---");

    provider.register_memory(data.as_mut_ptr(), data.len()).unwrap();
    println!("registered {} bytes at {:p}", data.len(), data.as_ptr());

    let put_token = provider.prepare_put_token(key, data.as_ptr(), data.len(), 0).unwrap();
    let get_token = provider.prepare_get_token(key, data.as_mut_ptr(), data.len(), 0).unwrap();
    println!("put_token: {} bytes", put_token.len());
    println!("get_token: {} bytes", get_token.len());

    // ── Server-side: through the worker pool ─────────────────────────────────

    println!("\n--- Server operations (dispatched to pool workers) ---");

    let mut put_buf = vec![0u8; 4096];
    let ch = pool.open_channel(key, &put_token).unwrap();
    let n = ch.handle_put(&mut put_buf, 0, 4096).unwrap();
    println!("handle_put: transferred {n} bytes");

    let payload = b"hello from worker pool!";
    let ch = pool.open_channel(key, &get_token).unwrap();
    let n = ch.handle_get(payload, 0).unwrap();
    println!("handle_get: written {n} bytes");

    provider.process_rdma_reply(200, &get_token, n, Box::new(|r| {
        println!("  reply (GET): error_code={}", r.error_code);
    })).unwrap();

    assert_eq!(&data[..n], payload);
    println!("verified: client buffer contains '{}'", String::from_utf8_lossy(&data[..n]));

    // ── Cleanup ──────────────────────────────────────────────────────────────

    println!("\n--- Cleanup ---");

    provider.process_rdma_reply(200, &put_token, 4096, Box::new(|r| {
        println!("  reply (PUT): error_code={}", r.error_code);
    })).unwrap();

    provider.deregister_memory(data.as_mut_ptr()).unwrap();
    println!("deregistered");

    drop(pool); // joins workers
    unsafe { dealloc(ptr, layout) };

    println!("\nDone.");
}
