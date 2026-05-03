//! Example: s3-rdma provider API using MockRdmaProvider (no hardware required).
//!
//! Run with:
//!   cargo run --example mock_provider --manifest-path s3-rdma/Cargo.toml

use s3_rdma::{rdma_provider_info, MockRdmaProvider, RdmaProvider};

fn main() {
    println!("=== s3-rdma Rust example ===\n");

    let provider = MockRdmaProvider::new(true, "/dev/shm", 0);
    println!("Provider : {}", provider.name());

    // Allocate a buffer to act as the RDMA-registered memory region.
    let mut data = vec![42u8; 1024 * 1024]; // 1 MiB
    let key = b"my-bucket/my-object.bin";

    println!("\n[is_memory_suitable]");
    println!("  suitable: {}", provider.is_memory_suitable(data.as_ptr(), data.len()));

    println!("\n[register_memory]");
    provider.register_memory(data.as_mut_ptr(), data.len()).unwrap();

    println!("\n[prepare_put_token]  (S3 PUT / upload)");
    let put_token = provider
        .prepare_put_token(key, data.as_ptr(), data.len(), 0)
        .unwrap();
    println!("  token: {}", String::from_utf8_lossy(&put_token));

    println!("\n[prepare_get_token]  (S3 GET / download)");
    let get_token = provider
        .prepare_get_token(key, data.as_mut_ptr(), data.len(), 0)
        .unwrap();
    println!("  token: {}", String::from_utf8_lossy(&get_token));

    println!("\n[process_rdma_reply]");
    provider
        .process_rdma_reply(
            200,
            &put_token,
            data.len(),
            Box::new(|result| {
                println!(
                    "  callback: error_code={}, reply={}",
                    result.error_code,
                    String::from_utf8_lossy(&result.reply_token)
                );
            }),
        )
        .unwrap();

    println!("\n[deregister_memory]");
    provider.deregister_memory(data.as_mut_ptr()).unwrap();

    // Clean up the GET token's shm entry (no server to handle it).
    let _ = get_token;

    println!("\nProvider info: {}", rdma_provider_info());
    println!("\nDone.");
}
