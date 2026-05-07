//! Single-threaded ping-pong using safe boxed-closure APIs.
//!
//!   cargo run --example pingpong --manifest-path core/Cargo.toml

use s3_rdma::mock::MockRdmaProvider;
use s3_rdma::provider::{RdmaClientProvider, RdmaServerProvider};

fn main() {
    println!("=== ping-pong (single-threaded) ===\n");

    let size = 4096;
    let key = b"bucket/pingpong.dat";

    let client = MockRdmaProvider::new(false, "/dev/shm", size);
    let ch = client.bind(size, key).unwrap();
    println!("client: buffer {:p}, {} bytes, key '{}'", ch.ptr(), size, std::str::from_utf8(key).unwrap());

    let server = MockRdmaProvider::new(false, "/dev/shm", size);

    // ── PUT ────────────────────────────────────────────────────────────────

    let n = ch.put(0, size,
        Box::new(|buf: &mut [u8]| { buf[..4].copy_from_slice(b"ping"); 4 }),
        Box::new(|token: &[u8]| {
            let srv = server.open_channel(key, token).unwrap();
            let mut out = Vec::new();
            let n = srv.handle_put(0, size, Box::new(|data: &[u8]| {
                out.extend_from_slice(data);
                Ok(data.len())
            })).unwrap();
            println!("PUT:  client sent 'ping' -> server received {} bytes: '{}'",
                     n, std::str::from_utf8(&out[..4]).unwrap());
            Ok((200, n))
        }),
    ).unwrap();
    assert_eq!(n, size);

    // ── GET ────────────────────────────────────────────────────────────────

    let mut received = Vec::new();
    let _n = ch.get(0, size,
        Box::new(|token: &[u8]| {
            let srv = server.open_channel(key, token).unwrap();
            let n = srv.handle_get(0, 0, Box::new(|buf: &mut [u8]| {
                buf[..4].copy_from_slice(b"pong");
                Ok(4)
            })).unwrap();
            println!("GET:  server wrote {} bytes", n);
            Ok((200, n))
        }),
        Box::new(|data: &[u8]| received.extend_from_slice(data)),
    ).unwrap();
    assert_eq!(received, b"pong");
    println!("      client received: '{}'", std::str::from_utf8(&received).unwrap());

    drop(ch);
    println!("\nping-pong OK.");
}
