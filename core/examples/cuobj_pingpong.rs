//! cuobj RDMA ping-pong via plugin loading (separate client/server processes).
//!
//! The client loads `libs3rdmaclient.so` (which links `libcuobjclient.so`) and
//! the server loads `libs3rdmaserver.so` (which links `libcuobjserver.so`).
//! Running them in separate processes avoids glibc malloc conflicts.
//!
//! Tokens are exchanged over a TCP control channel, just like the cuobj example.
//!
//! Build plugins first:
//!   cargo build -p s3-rdma-client --features cuobj
//!   cargo build -p s3-rdma-server --features cuobj
//!
//! Then run (in separate terminals, or use `&`):
//!   LD_LIBRARY_PATH=target/debug:/usr/local/lib:/usr/local/cuda/targets/x86_64-linux/lib \
//!   cargo run --example cuobj_pingpong --features "rdma-client,rdma-server" -- server
//!
//!   LD_LIBRARY_PATH=target/debug:/usr/local/lib:/usr/local/cuda/targets/x86_64-linux/lib \
//!   cargo run --example cuobj_pingpong --features "rdma-client,rdma-server" -- client

// ── server ───────────────────────────────────────────────────────────────────

#[cfg(feature = "rdma-server")]
fn run_server() {
    use std::io::{BufRead, BufReader, Write};
    use std::net::TcpListener;
    use s3_rdma::provider::{RdmaServerConfig, RdmaServerProvider};
    use s3_rdma::RdmaPlugin;

    let rdma_ip   = std::env::var("RDMA_IP").unwrap_or_else(|_| "10.50.132.250".into());
    let rdma_port = std::env::var("RDMA_PORT").unwrap_or_else(|_| "4421".into()).parse().unwrap();
    let ctrl_port: u16 = std::env::var("CTRL_PORT").unwrap_or_else(|_| "7072".into()).parse().unwrap();

    let cfg = RdmaServerConfig { listen_addr: rdma_ip.clone(), listen_port: rdma_port, buf_size: 4096, shm_path: "/dev/shm".into() };
    let plugin = RdmaPlugin::new_server(&cfg).expect("[server] RdmaPlugin::new_server");
    println!("[server] RDMA ready at {rdma_ip}:{rdma_port}");

    let listener = TcpListener::bind(format!("0.0.0.0:{ctrl_port}")).unwrap();
    println!("[server] TCP ctrl on :{ctrl_port}");
    let (stream, peer) = listener.accept().unwrap();
    println!("[server] client from {peer}");
    let mut reader = BufReader::new(stream.try_clone().unwrap());
    let mut writer = stream;
    let key = b"pingpong";

    // Round 1: PUT
    let mut line = String::new();
    reader.read_line(&mut line).unwrap(); line.clear();
    reader.read_line(&mut line).unwrap();
    let put_token = line.trim().as_bytes().to_vec();
    line.clear();
    println!("[server] PUT token received");

    let mut put_buf = vec![0u8; 4096];
    {
        let ch = plugin.open_channel(key, &put_token).expect("open PUT");
        let n = ch.handle_put(&mut put_buf, 0, 4096).expect("handle_put");
        println!("[server] RDMA_READ {} bytes: {:?}", n, &put_buf[..4]);
    }

    // Round 2: GET
    reader.read_line(&mut line).unwrap(); line.clear();
    reader.read_line(&mut line).unwrap();
    let get_token = line.trim().as_bytes().to_vec();
    println!("[server] GET token received");

    {
        let ch = plugin.open_channel(key, &get_token).expect("open GET");
        let n = ch.handle_get(b"pong", 0).expect("handle_get");
        println!("[server] RDMA_WRITE {} bytes", n);
    }

    writeln!(writer, "done").unwrap();
    println!("[server] pingpong complete ✓");
}

#[cfg(not(feature = "rdma-server"))]
fn run_server() { eprintln!("Recompile with --features rdma-server"); }

// ── client ───────────────────────────────────────────────────────────────────

#[cfg(feature = "rdma-client")]
fn run_client(server_ip: &str) {
    use std::io::{BufRead, BufReader, Write};
    use std::net::TcpStream;
    use std::alloc::{Layout, alloc, dealloc};
    use s3_rdma::provider::{RdmaClientProvider, RDMA_REQUIRED_ALIGNMENT};
    use s3_rdma::RdmaPlugin;

    let ctrl_port: u16 = std::env::var("CTRL_PORT").unwrap_or_else(|_| "7071".into()).parse().unwrap();
    let client = RdmaPlugin::new_client().expect("[client] RdmaPlugin::new_client");
    println!("[client] plugin loaded");

    let buf_size = 4096usize;
    let key = b"pingpong";

    // Register ping buffer and write "ping".
    let layout = Layout::from_size_align(buf_size, RDMA_REQUIRED_ALIGNMENT).unwrap();
    let ping_ptr = unsafe { alloc(layout) };
    assert!(client.is_memory_suitable(ping_ptr, buf_size));
    client.register_memory(ping_ptr, buf_size).expect("register ping");
    unsafe { std::ptr::copy_nonoverlapping(b"ping".as_ptr(), ping_ptr, 4); }
    let put_token = client.prepare_put_token(key, ping_ptr, buf_size, 0)
        .expect("prepare_put_token");

    // Register pong buffer (empty).
    let pong_ptr = unsafe { alloc(layout) };
    client.register_memory(pong_ptr, buf_size).expect("register pong");
    let get_token = client.prepare_get_token(key, pong_ptr, buf_size, 0)
        .expect("prepare_get_token");

    // Connect to server and send tokens.
    let addr = format!("{server_ip}:{ctrl_port}");
    println!("[client] connecting to {addr}");
    let mut stream = TcpStream::connect(&addr).expect("connect");
    let mut reader = BufReader::new(stream.try_clone().unwrap());
    writeln!(stream, "{buf_size}").unwrap();
    writeln!(stream, "{}", String::from_utf8_lossy(&put_token)).unwrap();
    writeln!(stream, "{buf_size}").unwrap();
    writeln!(stream, "{}", String::from_utf8_lossy(&get_token)).unwrap();
    println!("[client] tokens sent, waiting for server …");

    // Wait for server done.
    let mut line = String::new();
    reader.read_line(&mut line).expect("read done");
    assert_eq!(line.trim(), "done");

    // Verify pong.
    let received = unsafe { std::slice::from_raw_parts(pong_ptr, 4) };
    assert_eq!(received, b"pong", "expected 'pong', got {received:?}");
    println!("[client] received: {:?}", std::str::from_utf8(received).unwrap());
    println!("[client] pingpong complete ✓");

    // Cleanup.
    client.process_rdma_reply(200, &put_token, buf_size, Box::new(|_| {})).ok();
    client.process_rdma_reply(200, &get_token, 4, Box::new(|_| {})).ok();
    client.deregister_memory(ping_ptr).ok();
    client.deregister_memory(pong_ptr).ok();
    unsafe { dealloc(ping_ptr, layout); dealloc(pong_ptr, layout); }
}

#[cfg(not(feature = "rdma-client"))]
fn run_client(_: &str) { eprintln!("Recompile with --features rdma-client"); }

// ── main ─────────────────────────────────────────────────────────────────────

fn main() {
    let args: Vec<String> = std::env::args().collect();
    match args.get(1).map(String::as_str) {
        Some("server") => run_server(),
        Some("client") => {
            let ip = args.get(2).map(String::as_str).unwrap_or("127.0.0.1");
            run_client(ip);
        }
        _ => eprintln!("Usage: cuobj_pingpong server | client [server-ip]"),
    }
}
