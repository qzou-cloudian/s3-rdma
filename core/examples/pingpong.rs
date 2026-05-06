//! Single-threaded ping-pong: client → server (PUT) → server → client (GET).
//!
//! Uses the public channel API (`RdmaClientChannel`, `RdmaServerChannel`).
//! No hardware required — runs against the mock provider.
//!
//!   cargo run --example pingpong --manifest-path core/Cargo.toml

use std::alloc::{Layout, alloc, dealloc};

use s3_rdma::mock::MockRdmaProvider;
use s3_rdma::provider::{RdmaClientProvider, RdmaServerProvider, RDMA_REQUIRED_ALIGNMENT};

fn main() {
    println!("=== ping-pong (single-threaded) ===\n");

    // Allocate 4 KiB-aligned buffer.
    let size = 4096;
    let layout = Layout::from_size_align(size, RDMA_REQUIRED_ALIGNMENT).unwrap();
    let ptr = unsafe { alloc(layout) };
    let data = unsafe { std::slice::from_raw_parts_mut(ptr, size) };
    let key = b"bucket/pingpong.dat";

    // Client: bind buffer, generate tokens.
    let client = MockRdmaProvider::new(false, "/dev/shm", 4096);
    let ch = client.bind(ptr, size, key).unwrap();
    println!("client: buffer {ptr:p}, {} bytes, key '{}'", size, std::str::from_utf8(key).unwrap());

    // ── PUT: client → server ────────────────────────────────────────────────

    let payload = b"ping";
    unsafe { std::ptr::copy_nonoverlapping(payload.as_ptr(), ptr, payload.len()); }
    let put_token = ch.prepare_put_token(0, size).unwrap();

    let server = MockRdmaProvider::new(false, "/dev/shm", 4096);
    let mut put_buf = vec![0u8; size];
    let srv_ch = server.open_channel(key, &put_token).unwrap();
    let n = srv_ch.handle_put(&mut put_buf, 0, size).unwrap();
    println!("PUT:  client sent 'ping' → server received {} bytes: '{}'",
             n, std::str::from_utf8(&put_buf[..payload.len()]).unwrap());

    // ── GET: server → client ────────────────────────────────────────────────

    let get_token = ch.prepare_get_token(0, size).unwrap();
    let pong = b"pong";
    let srv_ch = server.open_channel(key, &get_token).unwrap();
    let n = srv_ch.handle_get(pong, 0).unwrap();
    println!("GET:  server wrote {} bytes", n);

    ch.process_rdma_reply(200, &get_token, n, Box::new(|r| {
        assert_eq!(r.error_code, 0);
    })).unwrap();

    assert_eq!(&data[..pong.len()], b"pong");
    println!("      client buffer now: '{}'", std::str::from_utf8(&data[..pong.len()]).unwrap());

    // ── Cleanup ─────────────────────────────────────────────────────────────

    ch.process_rdma_reply(200, &put_token, 4096, Box::new(|r| assert_eq!(r.error_code, 0))).unwrap();
    drop(ch); // auto-deregisters
    unsafe { dealloc(ptr, layout); }

    println!("\nping-pong OK.");
}
