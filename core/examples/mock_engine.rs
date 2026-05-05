//! Example: s3-rdma engine API via `create_engine` (no hardware required).
//!
//! Demonstrates constructing an engine through the public factory function and
//! using it through the [`RdmaEngine`] trait.  The concrete engine type is not
//! imported — only the trait and factory are needed.
//!
//! Run with:
//!   cargo run --example mock_engine --features engine --manifest-path core/Cargo.toml

use std::alloc::{Layout, alloc, dealloc};

use s3_rdma::mock::MockRdmaProvider;
use s3_rdma::provider::{RdmaClientProvider, RdmaServerConfig, RDMA_REQUIRED_ALIGNMENT};
use s3_rdma::{create_engine, rdma_provider_info};

fn main() {
    println!("=== s3-rdma engine example ===\n");
    println!("Provider info: {}\n", rdma_provider_info());

    // ── Setup ────────────────────────────────────────────────────────────────

    let cfg = RdmaServerConfig {
        shm_path: "/dev/shm".into(),
        buf_size: 4096,
        pool_count: 2,
        ..Default::default()
    };

    // Client ops: use the provider directly (no thread pool needed).
    let provider = MockRdmaProvider::new(true, "/dev/shm", 4096);

    // Server ops: dispatch through a bounded worker pool.
    let engine = create_engine("mock", true, &cfg).unwrap();
    println!("Engine   : {} workers", engine.worker_count());
    println!("Buf size : {} bytes", engine.buf_size());

    // RDMA requires 4 KiB-aligned buffers.
    let buf_size = 4096;
    let layout = Layout::from_size_align(buf_size, RDMA_REQUIRED_ALIGNMENT).unwrap();
    let ptr = unsafe { alloc(layout) };
    let data = unsafe { std::slice::from_raw_parts_mut(ptr, buf_size) };
    data.fill(0);
    assert_eq!(ptr as usize % RDMA_REQUIRED_ALIGNMENT, 0, "buffer must be aligned");

    let key = b"my-bucket/my-object.bin";

    // ── Client-side: register memory and generate tokens ─────────────────────

    println!("\n--- Client operations (direct) ---");

    println!("is_memory_suitable: {}", provider.is_memory_suitable(data.as_ptr(), data.len()));

    provider.register_memory(data.as_mut_ptr(), data.len()).unwrap();
    println!("registered {} bytes at {:p}", data.len(), data.as_ptr());

    let put_token = provider
        .prepare_put_token(key, data.as_ptr(), data.len(), 0)
        .unwrap();
    println!("put_token: {} bytes", put_token.len());

    let get_token = provider
        .prepare_get_token(key, data.as_mut_ptr(), data.len(), 0)
        .unwrap();
    println!("get_token: {} bytes", get_token.len());

    // ── Server-side: handle PUT and GET through the engine's worker pool ─────

    println!("\n--- Server operations (dispatched to engine workers) ---");

    let mut put_buf = vec![0u8; 4096];
    let n = engine.handle_put(key, &put_token, &mut put_buf, 0, 4096).unwrap();
    println!("handle_put: transferred {n} bytes");

    let payload = b"hello from rdma engine!";
    let n = engine.handle_get(key, payload, &get_token, 0).unwrap();
    println!("handle_get: written {n} bytes");

    provider
        .process_rdma_reply(
            200, &get_token, n,
            Box::new(|r| println!("  reply (GET): error_code={}", r.error_code)),
        )
        .unwrap();

    assert_eq!(&data[..n], payload);
    println!("verified: client buffer contains '{}'", String::from_utf8_lossy(&data[..n]));

    // ── Cleanup ──────────────────────────────────────────────────────────────

    println!("\n--- Cleanup ---");

    provider
        .process_rdma_reply(
            200, &put_token, 4096,
            Box::new(|r| println!("  reply (PUT): error_code={}", r.error_code)),
        )
        .unwrap();

    provider.deregister_memory(data.as_mut_ptr()).unwrap();
    println!("deregistered");

    drop(engine);

    unsafe { dealloc(ptr, layout); }

    println!("\nDone.");
}
