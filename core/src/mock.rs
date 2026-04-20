//! Mock RDMA provider – no NVIDIA hardware required.
//!
//! Uses POSIX shared memory (`/dev/shm`) so that the client and an out-of-process
//! relay server on the **same host** can exchange object data without copying
//! through the HTTP body.
//!
//! ## Token format
//! ```text
//! RDMA_{PUT|GET}_{shm-name}_{size}_{offset}_key_{s3-key}
//! ```
//! `shm-name` is `rdma-{pid}-{buf_ptr_hex}` (hyphens only, no underscores).
//!
//! ## Client lifecycle
//! | Step | Client calls | Effect |
//! |------|-------------|--------|
//! | 1 | `register_memory(ptr, size)` | Creates `/dev/shm/{shm-name}`, `mmap`s it |
//! | 2 | `prepare_put_token(…)` | Copies `ptr[offset..offset+size]` → shm, returns token |
//! | 2 | `prepare_get_token(…)` | Returns token (shm left empty; relay will fill it) |
//! | 3 | *(HTTP exchange)* | Relay reads or writes the shm file |
//! | 4 | `process_reply_token(…)` | For GET: copies shm → `ptr`; cleans up shm |
//! | 5 | `deregister_memory(ptr)` | Cleans up if not already done by step 4 |
//!
//! ## Server lifecycle (`MockRdmaServerProvider`)
//! | Step | Server calls | Effect |
//! |------|-------------|--------|
//! | 1 | `handle_put(token, size)` | Opens `/dev/shm/{shm-name}`, reads `size` bytes |
//! | 2 | `handle_get(data, token)` | Opens `/dev/shm/{shm-name}`, writes `data` |

use std::collections::HashMap;
use std::fs::OpenOptions;
use std::os::unix::io::AsRawFd;
use std::sync::Mutex;

use crate::error::RdmaError;
use crate::provider::{RdmaCompletionCallback, RdmaCompletionResult, RdmaProvider, RdmaServerProvider};

/// Base directory for shared-memory files.
const SHM_DIR: &str = "/dev/shm";

fn env_or(var: &str, default: &'static str) -> &'static str {
    match std::env::var(var) {
        Ok(v) if !v.is_empty() => Box::leak(v.into_boxed_str()),
        _ => default,
    }
}

struct ShmEntry {
    /// Basename, e.g. `rdma-12345-7fab12340000`.
    name: String,
    /// Full path `/dev/shm/rdma-…`.
    path: String,
    /// `mmap`-ed pointer into the shared-memory region.
    shm_ptr: *mut libc::c_void,
    /// Original client buffer pointer (destination for GET copies).
    buf_ptr: *mut u8,
    size: usize,
}

// SAFETY: raw pointers are only accessed under the `registered` Mutex.
unsafe impl Send for ShmEntry {}
unsafe impl Sync for ShmEntry {}

impl ShmEntry {
    /// Unmap and delete the shared-memory file.
    ///
    /// # Safety
    /// Must be called at most once per entry.
    unsafe fn cleanup(&self) {
        libc::munmap(self.shm_ptr, self.size);
        let _ = std::fs::remove_file(&self.path);
    }
}

/// Pure-Rust mock RDMA provider backed by `/dev/shm` shared memory.
pub struct MockRdmaProvider {
    debug: bool,
    registered: Mutex<HashMap<usize, ShmEntry>>,
    token_header: &'static str,
    reply_header: &'static str,
}

impl std::fmt::Debug for MockRdmaProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MockRdmaProvider")
            .field("debug", &self.debug)
            .finish()
    }
}

impl MockRdmaProvider {
    /// Create a new mock provider.  Set `debug = true` for trace output.
    pub fn new(debug: bool) -> Self {
        Self {
            debug,
            registered: Mutex::new(HashMap::new()),
            token_header: env_or("CUOBJ_RDMA_TOKEN_HEADER_NAME", "x-amz-rdma-token"),
            reply_header: env_or("CUOBJ_RDMA_REPLY_HEADER_NAME", "x-amz-rdma-reply"),
        }
    }

    fn dbg(&self, msg: &str) {
        if self.debug {
            eprintln!("[MockRdmaProvider] {msg}");
        }
    }
}

impl RdmaProvider for MockRdmaProvider {
    fn name(&self) -> &str { "MockRdmaProvider" }
    fn version(&self) -> u32 { 1 }

    fn is_memory_suitable(&self, ptr: *const u8, size: usize) -> bool {
        let ok = !ptr.is_null() && size > 0;
        self.dbg(&format!("is_memory_suitable ptr={ptr:p} size={size} -> {ok}"));
        ok
    }

    /// Create a `/dev/shm` segment and `mmap` it, ready for PUT or GET.
    fn register_memory(&self, ptr: *mut u8, size: usize) -> Result<(), RdmaError> {
        self.dbg(&format!("register_memory ptr={ptr:p} size={size}"));

        let name = format!("rdma-{}-{:x}", std::process::id(), ptr as usize);
        let path = format!("{SHM_DIR}/{name}");

        let file = OpenOptions::new()
            .read(true).write(true).create(true).truncate(true)
            .open(&path)
            .map_err(|e| RdmaError::MemoryRegistrationFailed {
                ptr: ptr as usize, size,
                reason: format!("create {path}: {e}"),
            })?;
        file.set_len(size as u64)
            .map_err(|e| RdmaError::MemoryRegistrationFailed {
                ptr: ptr as usize, size,
                reason: format!("resize {path}: {e}"),
            })?;

        // SAFETY: fd is valid and file is correctly sized.
        let shm_ptr = unsafe {
            libc::mmap(
                std::ptr::null_mut(), size,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_SHARED, file.as_raw_fd(), 0,
            )
        };
        if shm_ptr == libc::MAP_FAILED {
            let _ = std::fs::remove_file(&path);
            return Err(RdmaError::MemoryRegistrationFailed {
                ptr: ptr as usize, size,
                reason: format!("mmap {path}: {}", std::io::Error::last_os_error()),
            });
        }
        // fd may be closed; the mapping persists until munmap.
        drop(file);

        self.registered.lock().unwrap().insert(
            ptr as usize,
            ShmEntry { name, path, shm_ptr, buf_ptr: ptr, size },
        );
        Ok(())
    }

    fn deregister_memory(&self, ptr: *mut u8) -> Result<(), RdmaError> {
        self.dbg(&format!("deregister_memory ptr={ptr:p}"));
        if let Some(entry) = self.registered.lock().unwrap().remove(&(ptr as usize)) {
            // SAFETY: entry is removed from the map; cleanup called exactly once.
            unsafe { entry.cleanup(); }
        }
        Ok(())
    }

    /// Copy client buffer → shm, then return a token the relay can use to open
    /// `/dev/shm/{shm-name}` and read the data.
    fn prepare_put_token(
        &self,
        s3_key: &[u8],
        buffer: *const u8,
        size: usize,
        offset: usize,
    ) -> Result<Vec<u8>, RdmaError> {
        let key = String::from_utf8_lossy(s3_key);
        let registered = self.registered.lock().unwrap();
        let entry = registered.get(&(buffer as usize)).ok_or_else(|| {
            RdmaError::TokenGenerationFailed {
                key: key.to_string(),
                reason: format!("buffer {buffer:p} not registered"),
            }
        })?;

        // SAFETY: buffer..+size is valid (caller's responsibility); shm_ptr is
        // a valid MAP_SHARED mapping of `entry.size` bytes.
        unsafe {
            std::ptr::copy_nonoverlapping(
                buffer.add(offset),
                entry.shm_ptr as *mut u8,
                size,
            );
            // Flush to page cache so the relay's read() syscall sees the data.
            libc::msync(entry.shm_ptr, size, libc::MS_SYNC);
        }

        let token = format!("RDMA_PUT_{}_{}_{}_key_{}", entry.name, size, offset, key)
            .into_bytes();
        self.dbg(&format!("prepare_put_token key={key} shm={}", entry.name));
        Ok(token)
    }

    /// Return a token encoding the (empty) shm region; the relay will fill it
    /// with fetched object data before replying.
    fn prepare_get_token(
        &self,
        s3_key: &[u8],
        buffer: *mut u8,
        size: usize,
        offset: usize,
    ) -> Result<Vec<u8>, RdmaError> {
        let key = String::from_utf8_lossy(s3_key);
        let registered = self.registered.lock().unwrap();
        let entry = registered.get(&(buffer as usize)).ok_or_else(|| {
            RdmaError::TokenGenerationFailed {
                key: key.to_string(),
                reason: format!("buffer {buffer:p} not registered"),
            }
        })?;

        let token = format!("RDMA_GET_{}_{}_{}_key_{}", entry.name, size, offset, key)
            .into_bytes();
        self.dbg(&format!("prepare_get_token key={key} shm={}", entry.name));
        Ok(token)
    }

    /// For GET: the relay has written data into the shm file; copy it into the
    /// client's original buffer.  Then clean up the shm entry.
    fn process_reply_token(
        &self,
        reply_token: &[u8],
        callback: RdmaCompletionCallback,
    ) -> Result<(), RdmaError> {
        let token_str = std::str::from_utf8(reply_token).unwrap_or("");
        self.dbg(&format!("process_reply_token token={token_str}"));

        // Parse: RDMA_{PUT|GET}_{shm-name}_{size}_{offset}_key_{s3-key}
        let parts: Vec<&str> = token_str.splitn(7, '_').collect();
        let entry_to_drop: Option<ShmEntry> = if parts.len() >= 5 && parts[0] == "RDMA" {
            let op = parts[1];
            let shm_name = parts[2];
            let size: usize = parts[3].parse().unwrap_or(0);
            let offset: usize = parts[4].parse().unwrap_or(0);

            let mut registered = self.registered.lock().unwrap();
            // Find the entry by shm name (linear scan; few entries in practice).
            let found_ptr = registered.iter()
                .find(|(_, e)| e.name == shm_name)
                .map(|(&k, _)| k);

            found_ptr.and_then(|ptr| {
                let entry = registered.remove(&ptr)?;
                if op == "GET" && size > 0 {
                    // The relay wrote `size` bytes to the shm file; copy into
                    // the client's buffer via the existing MAP_SHARED mapping.
                    // Clamp to avoid writing beyond the registered region.
                    let available = entry.size.saturating_sub(offset);
                    let copy_len = size.min(available);
                    if copy_len > 0 {
                        // SAFETY: shm_ptr is a valid MAP_SHARED mapping of entry.size
                        // bytes; buf_ptr+offset..+copy_len is within the registered
                        // region (offset + copy_len <= entry.size, enforced above).
                        unsafe {
                            std::ptr::copy_nonoverlapping(
                                entry.shm_ptr as *const u8,
                                entry.buf_ptr.add(offset),
                                copy_len,
                            );
                        }
                    }
                }
                Some(entry)
            })
        } else {
            None
        };

        // Clean up outside the lock.
        if let Some(entry) = entry_to_drop {
            // SAFETY: removed from map; cleanup called exactly once.
            unsafe { entry.cleanup(); }
        }

        callback(RdmaCompletionResult { error_code: 0, reply_token: reply_token.to_vec() });
        Ok(())
    }

    fn rdma_token_header_name(&self) -> &[u8] { self.token_header.as_bytes() }
    fn rdma_reply_header_name(&self) -> &[u8] { self.reply_header.as_bytes() }
}

// ─────────────────────────────────────────────────────────────────────────────
// MockRdmaServerProvider
// ─────────────────────────────────────────────────────────────────────────────

/// Server-side counterpart to [`MockRdmaProvider`].
///
/// Both sides must share the same `/dev/shm` directory (e.g. via Docker
/// `ipc: host`).  The server reads from / writes to the file that the client
/// created in [`MockRdmaProvider::register_memory`].
pub struct MockRdmaServerProvider {
    debug: bool,
}

impl MockRdmaServerProvider {
    pub fn new(debug: bool) -> Self {
        Self { debug }
    }

    fn dbg(&self, msg: &str) {
        if self.debug {
            eprintln!("[MockRdmaServerProvider] {msg}");
        }
    }

    /// Parse the mock token: `RDMA_{op}_{shm-name}_{size}_{offset}_key_{s3-key}`.
    ///
    /// Returns `(op, shm_name, token_size, offset)` on success.
    fn parse_token(token: &[u8]) -> Result<(&str, &str, usize, usize), RdmaError> {
        let s = std::str::from_utf8(token).map_err(|e| {
            RdmaError::TransferFailed(format!("token is not valid UTF-8: {e}"))
        })?;
        // Split into at most 7 fields; the last field (s3-key) may contain '_'.
        let parts: Vec<&str> = s.splitn(7, '_').collect();
        if parts.len() < 5 || parts[0] != "RDMA" {
            return Err(RdmaError::TransferFailed(format!(
                "invalid mock RDMA token: {s}"
            )));
        }
        let op = parts[1];
        let shm_name = parts[2];
        let size: usize = parts[3].parse().unwrap_or(0);
        let offset: usize = parts[4].parse().unwrap_or(0);
        Ok((op, shm_name, size, offset))
    }
}

impl RdmaServerProvider for MockRdmaServerProvider {
    fn name(&self) -> &str {
        "MockRdmaServerProvider"
    }

    /// RDMA PUT: read the data that the client wrote into `/dev/shm/{shm-name}`.
    ///
    /// Returns `(data, reply_token)`.  The reply token echoes the original
    /// request token so the client's [`MockRdmaProvider::process_reply_token`]
    /// can locate and clean up the shm entry.
    fn handle_put(
        &self,
        _s3_key: &[u8],
        token: &[u8],
        size: usize,
    ) -> Result<(Vec<u8>, Vec<u8>), RdmaError> {
        let (op, shm_name, token_size, offset) = Self::parse_token(token)?;
        if op != "PUT" {
            return Err(RdmaError::TransferFailed(format!(
                "expected PUT token, got {op}"
            )));
        }
        let read_size = if size > 0 { size } else { token_size };
        let path = format!("{SHM_DIR}/{shm_name}");
        self.dbg(&format!("handle_put shm={shm_name} size={read_size} offset={offset}"));

        let file_bytes = std::fs::read(&path).map_err(|e| {
            RdmaError::TransferFailed(format!("read {path}: {e}"))
        })?;

        let start = offset.min(file_bytes.len());
        let end = (offset + read_size).min(file_bytes.len());
        let data = file_bytes[start..end].to_vec();

        Ok((data, token.to_vec()))
    }

    /// RDMA GET: write `data` into `/dev/shm/{shm-name}` so the client can
    /// copy it to its buffer via [`MockRdmaProvider::process_reply_token`].
    ///
    /// Returns a reply token with the same format but with the actual byte
    /// count, which the client parses to determine how many bytes to copy.
    fn handle_get(
        &self,
        _s3_key: &[u8],
        data: &[u8],
        token: &[u8],
    ) -> Result<Vec<u8>, RdmaError> {
        let (op, shm_name, _token_size, offset) = Self::parse_token(token)?;
        if op != "GET" {
            return Err(RdmaError::TransferFailed(format!(
                "expected GET token, got {op}"
            )));
        }
        let path = format!("{SHM_DIR}/{shm_name}");
        self.dbg(&format!(
            "handle_get shm={shm_name} size={} offset={offset}",
            data.len()
        ));

        use std::io::{Seek, SeekFrom, Write};
        let mut file = OpenOptions::new()
            .write(true)
            .open(&path)
            .map_err(|e| RdmaError::TransferFailed(format!("open {path}: {e}")))?;
        file.seek(SeekFrom::Start(offset as u64))
            .map_err(|e| RdmaError::TransferFailed(format!("seek {path}: {e}")))?;
        file.write_all(data)
            .map_err(|e| RdmaError::TransferFailed(format!("write {path}: {e}")))?;

        // Extract the s3-key suffix (parts[5..]) and rebuild the reply token
        // with the actual byte count so process_reply_token copies the right amount.
        let token_str = std::str::from_utf8(token).unwrap_or("");
        let key_suffix = {
            // token = "RDMA_GET_{shm}_{size}_{offset}_key_{s3-key}"
            // splitn(6, '_') gives: ["RDMA","GET",shm,size,offset,"key_{s3-key}"]
            let parts6: Vec<&str> = token_str.splitn(6, '_').collect();
            if parts6.len() >= 6 { parts6[5] } else { "" }
        };
        let reply = format!("RDMA_GET_{shm_name}_{}_{}_{key_suffix}", data.len(), offset)
            .into_bytes();
        Ok(reply)
    }
}
