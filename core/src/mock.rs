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
//! | 4 | `process_rdma_reply(status, request_token, transferred, cb)` | For GET: copies shm → `ptr`; cleans up shm |
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
        // Box::leak is intentional: the value is read once at startup and lives
        // for the process lifetime — equivalent cost to a static string.
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
            if libc::msync(entry.shm_ptr, size, libc::MS_SYNC) != 0 {
                let err = std::io::Error::last_os_error();
                self.dbg(&format!("prepare_put_token: msync failed: {err}"));
            }
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

    /// Process the `x-amz-rdma-reply` numeric status code returned by the server.
    ///
    /// For GET operations: if `rdma_status` is 2xx, parses `request_token` to
    /// locate the shm region and copies `transferred` bytes into the client
    /// buffer.  For PUT operations: just cleans up the shm entry.
    /// If `rdma_status` is not 2xx, fires `callback` with a non-zero error code.
    fn process_rdma_reply(
        &self,
        rdma_status: u16,
        request_token: &[u8],
        transferred: usize,
        callback: RdmaCompletionCallback,
    ) -> Result<(), RdmaError> {
        self.dbg(&format!("process_rdma_reply status={rdma_status} transferred={transferred}"));

        if rdma_status == 0 || !(200..300).contains(&rdma_status) {
            // rdma_status == 0 means the reply header was absent; 0 as i32 == 0
            // would be wrongly interpreted as success, so map it to -1.
            let error_code = if rdma_status == 0 { -1i32 } else { rdma_status as i32 };
            callback(RdmaCompletionResult { error_code, reply_token: vec![] });
            return Ok(());
        }

        // Parse the original request token to find the shm entry.
        let token_str = match std::str::from_utf8(request_token) {
            Ok(s) => s,
            Err(_) => {
                callback(RdmaCompletionResult { error_code: -1, reply_token: vec![] });
                return Err(RdmaError::ReplyTokenProcessingFailed("token is not valid UTF-8".into()));
            }
        };
        let parts: Vec<&str> = token_str.splitn(7, '_').collect();
        let entry_to_drop: Option<ShmEntry> = if parts.len() >= 5 && parts[0] == "RDMA" {
            let op = parts[1];
            let shm_name = parts[2];
            // token_offset is the base position in the shm region where the
            // server wrote the first chunk (matches `offset` embedded in the token).
            let token_offset: usize = parts[4].parse().unwrap_or(0);

            let mut registered = self.registered.lock().unwrap();
            let found_ptr = registered.iter()
                .find(|(_, e)| e.name == shm_name)
                .map(|(&k, _)| k);

            found_ptr.and_then(|ptr| {
                let entry = registered.remove(&ptr)?;
                if op == "GET" && transferred > 0 {
                    // `handle_get` wrote `transferred` bytes into the shm file starting
                    // at `token_offset` (the base offset from `prepare_get_token`).
                    // Copy the data back to the START of the client's registered buffer
                    // (`buf_ptr + 0`), not to `buf_ptr + token_offset`, because callers
                    // (including the relay) always read from the buffer base after the
                    // transfer completes.
                    let available = entry.size.saturating_sub(token_offset);
                    let copy_len = transferred.min(available);
                    if copy_len > 0 {
                        // SAFETY: shm_ptr is a valid MAP_SHARED mapping of entry.size
                        // bytes; buf_ptr points to the registered client buffer of at
                        // least entry.size bytes.  copy_len <= available <= entry.size.
                        unsafe {
                            std::ptr::copy_nonoverlapping(
                                (entry.shm_ptr as *const u8).add(token_offset),
                                entry.buf_ptr, // always write to the buffer base
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

        if let Some(entry) = entry_to_drop {
            // SAFETY: removed from map; cleanup called exactly once.
            unsafe { entry.cleanup(); }
        }

        callback(RdmaCompletionResult { error_code: 0, reply_token: vec![] });
        Ok(())
    }

    /// Process the `x-amz-rdma-reply` token returned by the S3 server.
    ///
    /// **Deprecated**: use [`process_rdma_reply`](Self::process_rdma_reply)
    /// instead. The reply value is now a numeric status code (200/204/206/501),
    /// not an opaque echo token.
    #[allow(deprecated)]
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
            let found_ptr = registered.iter()
                .find(|(_, e)| e.name == shm_name)
                .map(|(&k, _)| k);

            found_ptr.and_then(|ptr| {
                let entry = registered.remove(&ptr)?;
                if op == "GET" && size > 0 {
                    let available = entry.size.saturating_sub(offset);
                    let copy_len = size.min(available);
                    if copy_len > 0 {
                        unsafe {
                            std::ptr::copy_nonoverlapping(
                                (entry.shm_ptr as *const u8).add(offset),
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

        if let Some(entry) = entry_to_drop {
            unsafe { entry.cleanup(); }
        }

        callback(RdmaCompletionResult { error_code: 0, reply_token: vec![] });
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
/// Both sides must share the same shared-memory directory (e.g. via Docker
/// `ipc: host`).  The server reads from / writes to the file that the client
/// created in [`MockRdmaProvider::register_memory`].
pub struct MockRdmaServerProvider {
    debug:    bool,
    shm_path: String,
    buf_size: usize,
}

impl MockRdmaServerProvider {
    pub fn new(debug: bool, shm_path: impl Into<String>, buf_size: usize) -> Self {
        Self { debug, shm_path: shm_path.into(), buf_size }
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

    fn buf_size(&self) -> usize {
        // Use configured buf_size as the RDMA chunk size for mock.
        // When 0 (unconfigured), return usize::MAX to disable chunking.
        if self.buf_size > 0 { self.buf_size } else { usize::MAX }
    }

    /// RDMA PUT: read `size` bytes from `/dev/shm/{shm-name}` starting at
    /// `offset` bytes into the client's buffer.
    ///
    /// `offset` overrides the offset encoded in the token (the token's offset
    /// always describes the base of the full client buffer; chunked callers
    /// supply the per-chunk byte offset explicitly).  If `size == 0` the
    /// whole token-encoded buffer is read from `offset`.
    fn handle_put(
        &self,
        _s3_key: &[u8],
        token: &[u8],
        offset: usize,
        size: usize,
    ) -> Result<Vec<u8>, RdmaError> {
        let (op, shm_name, token_size, _token_offset) = Self::parse_token(token)?;
        if op != "PUT" {
            return Err(RdmaError::TransferFailed(format!(
                "expected PUT token, got {op}"
            )));
        }
        let read_size = if size > 0 { size } else { token_size };
        let path = format!("{}/{shm_name}", self.shm_path);
        self.dbg(&format!("handle_put shm={shm_name} offset={offset} size={read_size}"));

        let file_bytes = std::fs::read(&path).map_err(|e| {
            RdmaError::TransferFailed(format!("read {path}: {e}"))
        })?;

        let start = offset.min(file_bytes.len());
        let end = (offset + read_size).min(file_bytes.len());
        Ok(file_bytes[start..end].to_vec())
    }

    /// RDMA GET: write `data` into `/dev/shm/{shm-name}` at `token_base + offset`.
    ///
    /// `offset` is the caller-supplied byte position within the client's buffer
    /// (0 = start, advances per chunk in streaming mode).  The token's embedded
    /// offset is added as the base so the shm file position is
    /// `token_offset + offset`.  Clips the write to the remaining capacity
    /// (`capacity - offset`) so the client buffer is never overfilled.
    fn handle_get(
        &self,
        _s3_key: &[u8],
        data: &[u8],
        token: &[u8],
        offset: usize,
    ) -> Result<usize, RdmaError> {
        let (op, shm_name, capacity, token_offset) = Self::parse_token(token)?;
        if op != "GET" {
            return Err(RdmaError::TransferFailed(format!(
                "expected GET token, got {op}"
            )));
        }
        // Remaining space in the client's buffer from the current chunk offset.
        let remaining_cap = capacity.saturating_sub(offset);
        let write_len = data.len().min(remaining_cap);
        let write_start = token_offset + offset;
        let path = format!("{}/{shm_name}", self.shm_path);
        self.dbg(&format!(
            "handle_get shm={shm_name} write={write_len}/{} write_start={write_start}",
            data.len()
        ));

        use std::io::{Seek, SeekFrom, Write};
        let mut file = OpenOptions::new()
            .write(true)
            .open(&path)
            .map_err(|e| RdmaError::TransferFailed(format!("open {path}: {e}")))?;
        file.seek(SeekFrom::Start(write_start as u64))
            .map_err(|e| RdmaError::TransferFailed(format!("seek {path}: {e}")))?;
        file.write_all(&data[..write_len])
            .map_err(|e| RdmaError::TransferFailed(format!("write {path}: {e}")))?;

        Ok(write_len)
    }
}
