//! Single-threaded ping-pong using the safe `put`/`get` API.
//!
//! `put` calls a `write` closure to fill the buffer, then the server
//! RDMA-reads from it — zero-copy.  `get` passes downloaded data to a
//! `read` closure.
//!
//!   cargo run --example pingpong --manifest-path core/Cargo.toml

use std::ffi::c_void;

use s3_rdma::mock::MockRdmaProvider;
use s3_rdma::provider::{RdmaClientProvider, RdmaServerProvider};

fn main() {
    println!("=== ping-pong (single-threaded) ===\n");

    let size = 4096;
    let key = b"bucket/pingpong.dat";

    let client = MockRdmaProvider::new(false, "/dev/shm", 4096);
    let ch = client.bind(size, key).unwrap();
    println!("client: buffer {:p}, {} bytes, key '{}'", ch.ptr(), size, std::str::from_utf8(key).unwrap());

    let server = MockRdmaProvider::new(false, "/dev/shm", 4096);

    // ── PUT ────────────────────────────────────────────────────────────────

    let n = ch.put(0, 4096,
        // write: fill the buffer directly (zero-copy)
        Box::new(|buf: &mut [u8]| {
            buf[..4].copy_from_slice(b"ping");
            4 // wrote 4 bytes
        }),
        // send: communicate token to server
        Box::new(|token: &[u8]| {
            let srv_ch = server.open_channel(key, token).unwrap();
            let mut out: Vec<u8> = Vec::with_capacity(size);
            let n = unsafe {
                extern "C" fn put_write(d: *mut c_void, b: *const c_void, n: usize) -> isize {
                    unsafe {
                        let out = &mut *(d as *mut Vec<u8>);
                        let src = std::slice::from_raw_parts(b as *const u8, n);
                        out.extend_from_slice(src);
                        n as isize
                    }
                }
                srv_ch.handle_put(0, size, put_write, &mut out as *mut Vec<u8> as *mut c_void)
            }.unwrap();
            println!("PUT:  client sent 'ping' -> server received {} bytes: '{}'",
                     n, std::str::from_utf8(&out[..4]).unwrap());
            Ok((200, n))
        }),
    ).unwrap();
    assert_eq!(n, 4096);

    // ── GET ────────────────────────────────────────────────────────────────

    let mut received = Vec::new();
    let n = ch.get(0, size,
        Box::new(|token: &[u8]| {
            let srv_ch = server.open_channel(key, token).unwrap();
            let pong: &[u8] = b"pong";
            let n = unsafe {
                extern "C" fn get_read(d: *mut c_void, b: *mut c_void, cap: usize) -> isize {
                    unsafe {
                        let src = &*(d as *const &[u8]);
                        let dst = std::slice::from_raw_parts_mut(b as *mut u8, cap);
                        let n = src.len().min(cap);
                        dst[..n].copy_from_slice(&src[..n]);
                        n as isize
                    }
                }
                srv_ch.handle_get(0, get_read, &pong as *const &[u8] as *mut c_void)
            }.unwrap();
            println!("GET:  server wrote {} bytes", n);
            Ok((200, n))
        }),
        Box::new(|data: &[u8]| received.extend_from_slice(data)),
    ).unwrap();
    assert_eq!(n, 4);
    assert_eq!(received, b"pong");
    println!("      client received: '{}'", std::str::from_utf8(&received).unwrap());

    drop(ch);
    println!("\nping-pong OK.");
}
