//! Multi-threaded ping-pong with RdmaWorkerPool.
//!
//! Same flow as pingpong.rs, but server ops dispatch through a 2-worker pool.
//!
//!   cargo run --example pingpong_pool --manifest-path core/Cargo.toml

use std::ffi::c_void;
use std::sync::Arc;

use s3_rdma::mock::MockRdmaProvider;
use s3_rdma::provider::{RdmaClientProvider, RdmaServerProvider};
use s3_rdma::worker_pool::RdmaWorkerPool;

fn main() {
    println!("=== ping-pong (2-worker pool) ===\n");

    let size = 4096;
    let key = b"bucket/pingpong.dat";

    let client = MockRdmaProvider::new(false, "/dev/shm", 4096);
    let ch = client.bind(size, key).unwrap();
    let data = unsafe { std::slice::from_raw_parts_mut(ch.ptr() as *mut u8, ch.size()) };
    println!("client: buffer {:p}, {} bytes", ch.ptr(), size);

    let provider = Arc::new(MockRdmaProvider::new(false, "/dev/shm", 4096));
    let pool = RdmaWorkerPool::builder()
        .client_workers(1)
        .server_workers(2)
        .provider(provider)
        .build().unwrap();

    // ── PUT through pool ────────────────────────────────────────────────────

    data[..4].copy_from_slice(b"ping");
    let put = ch.prepare_put(0, size).unwrap();

    let mut put_buf: Vec<u8> = Vec::with_capacity(size);
    let srv_ch = pool.open_channel(key, put.token()).unwrap();

    unsafe extern "C" fn put_write(user_data: *mut c_void, buf: *const c_void, n: usize) -> isize {
        let out = &mut *(user_data as *mut Vec<u8>);
        let src = std::slice::from_raw_parts(buf as *const u8, n);
        out.extend_from_slice(src);
        n as isize
    }
    let n = unsafe {
        srv_ch.handle_put(0, size, put_write, &mut put_buf as *mut Vec<u8> as *mut c_void)
    }.unwrap();
    println!("PUT:  received {} bytes: '{}'", n, std::str::from_utf8(&put_buf[..4]).unwrap());

    // ── GET through pool ────────────────────────────────────────────────────

    let get = ch.prepare_get(0, size).unwrap();
    let srv_ch = pool.open_channel(key, get.token()).unwrap();

    unsafe extern "C" fn get_read(user_data: *mut c_void, buf: *mut c_void, capacity: usize) -> isize {
        let src = &*(user_data as *const &[u8]);
        let dst = std::slice::from_raw_parts_mut(buf as *mut u8, capacity);
        let n = src.len().min(capacity);
        dst[..n].copy_from_slice(&src[..n]);
        n as isize
    }
    let pong: &[u8] = b"pong";
    let n = unsafe {
        srv_ch.handle_get(0, get_read, &pong as *const &[u8] as *mut c_void)
    }.unwrap();
    println!("GET:  written {} bytes", n);

    ch.complete_get(get, 200, n).unwrap();
    assert_eq!(&data[..4], b"pong");
    println!("      client buffer: '{}'", std::str::from_utf8(&data[..4]).unwrap());

    // ── Cleanup ─────────────────────────────────────────────────────────────

    ch.complete_put(put, 200, 4096).unwrap();
    drop(ch);
    println!("\nping-pong (pool) OK.");
}
