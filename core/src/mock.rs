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
//! ## Client lifecycle ([`MockRdmaProvider`])
//! | Step | Client calls | Effect |
//! |------|-------------|--------|
//! | 1 | `register_memory(ptr, size)` | Creates `/dev/shm/{shm-name}`, `mmap`s it |
//! | 2 | `prepare_put_token(…)` | Copies `ptr[offset..offset+size]` → shm, returns token |
//! | 2 | `prepare_get_token(…)` | Returns token (shm left empty; relay will fill it) |
//! | 3 | *(HTTP exchange)* | Relay reads or writes the shm file |
//! | 4 | `process_rdma_reply(status, request_token, transferred, cb)` | For GET: copies shm → `ptr`; cleans up shm |
//! | 5 | `deregister_memory(ptr)` | Cleans up if not already done by step 4 |
//!
//! ## Server lifecycle ([`MockRdmaProvider`])
//! | Step | Server calls | Effect |
//! |------|-------------|--------|
//! | 1 | `handle_put(token, size)` | Opens `/dev/shm/{shm-name}`, reads `size` bytes |
//! | 2 | `handle_get(data, token)` | Opens `/dev/shm/{shm-name}`, writes `data` |

use std::collections::HashMap;
use std::fs::OpenOptions;
use std::os::unix::io::AsRawFd;
use std::sync::{Mutex, OnceLock};

use crate::error::RdmaError;
use crate::provider::{RdmaCompletionCallback, RdmaCompletionResult, RdmaClientProvider, RdmaServerProvider, RdmaProvider};

/// Base directory for shared-memory files.
const SHM_DIR: &str = "/dev/shm";

static TOKEN_HEADER: OnceLock<String> = OnceLock::new();
static REPLY_HEADER: OnceLock<String> = OnceLock::new();

fn env_header(lock: &'static OnceLock<String>, var: &str, default: &'static str) -> &'static str {
    lock.get_or_init(|| {
        std::env::var(var)
            .ok()
            .filter(|v| !v.is_empty())
            .unwrap_or_else(|| default.to_owned())
    })
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

// ─────────────────────────────────────────────────────────────────────────────
// MockRdmaProvider  (unified client + server)
// ─────────────────────────────────────────────────────────────────────────────

/// Unified mock RDMA provider that supports both client and server roles.
///
/// Replaces the separate [`MockRdmaProvider`] (client) and
/// [`MockRdmaServerProvider`] (server) with a single instance that can be
/// shared across an entire HSG gateway process.
///
/// Both roles use the same `/dev/shm` directory; the client creates SHM files
/// keyed by buffer pointer, and the server reads or writes those files using
/// the token embedded in the S3 request.
pub struct MockRdmaProvider {
    debug:    bool,
    shm_dir:  String,
    buf_size: usize,
    registered: Mutex<HashMap<usize, ShmEntry>>,
    token_header: &'static str,
    reply_header: &'static str,
}

impl std::fmt::Debug for MockRdmaProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MockRdmaProvider")
            .field("debug", &self.debug)
            .field("shm_dir", &self.shm_dir)
            .field("buf_size", &self.buf_size)
            .finish()
    }
}

impl MockRdmaProvider {
    /// Create a new provider.
    ///
    /// - `debug` — emit trace lines to stderr.
    /// - `shm_dir` — directory for shared-memory files (typically `"/dev/shm"`).
    /// - `buf_size` — maximum bytes per RDMA transfer on the server side
    ///   (`0` = unlimited / use `usize::MAX`).
    pub fn new(debug: bool, shm_dir: impl Into<String>, buf_size: usize) -> Self {
        Self {
            debug,
            shm_dir: shm_dir.into(),
            buf_size,
            registered: Mutex::new(HashMap::new()),
            token_header: env_header(&TOKEN_HEADER, "CUOBJ_RDMA_TOKEN_HEADER_NAME", "x-amz-rdma-token"),
            reply_header: env_header(&REPLY_HEADER, "CUOBJ_RDMA_REPLY_HEADER_NAME", "x-amz-rdma-reply"),
        }
    }

    /// Convenience constructor with default `/dev/shm` and unlimited buf_size.
    pub fn with_debug(debug: bool) -> Self {
        Self::new(debug, SHM_DIR, 0)
    }

    fn dbg(&self, msg: &str) {
        if self.debug {
            eprintln!("[MockRdmaProvider] {msg}");
        }
    }

    fn parse_token(token: &[u8]) -> Result<(&str, &str, usize, usize), RdmaError> {
        let s = std::str::from_utf8(token).map_err(|e| {
            RdmaError::TransferFailed(format!("token is not valid UTF-8: {e}"))
        })?;
        let parts: Vec<&str> = s.splitn(7, '_').collect();
        if parts.len() < 5 || parts[0] != "RDMA" {
            return Err(RdmaError::TransferFailed(format!("invalid mock RDMA token: {s}")));
        }
        let op = parts[1];
        let shm_name = parts[2];
        let size: usize = parts[3].parse().unwrap_or(0);
        let offset: usize = parts[4].parse().unwrap_or(0);
        Ok((op, shm_name, size, offset))
    }
}

impl RdmaClientProvider for MockRdmaProvider {
    fn name(&self) -> &str { "MockRdmaProvider" }
    fn protocol(&self) -> &str { "mock" }

    fn is_memory_suitable(&self, ptr: *const u8, size: usize) -> bool {
        let ok = !ptr.is_null() && size > 0;
        self.dbg(&format!("is_memory_suitable ptr={ptr:p} size={size} -> {ok}"));
        ok
    }

    fn register_memory(&self, ptr: *mut u8, size: usize) -> Result<(), RdmaError> {
        self.dbg(&format!("register_memory ptr={ptr:p} size={size}"));

        let name = format!("rdma-{}-{:x}", std::process::id(), ptr as usize);
        let path = format!("{}/{name}", self.shm_dir);

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
            unsafe { entry.cleanup(); }
        }
        Ok(())
    }

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

        unsafe {
            std::ptr::copy_nonoverlapping(
                buffer.add(offset),
                entry.shm_ptr as *mut u8,
                size,
            );
            if libc::msync(entry.shm_ptr, size, libc::MS_SYNC) != 0 {
                self.dbg(&format!("prepare_put_token: msync failed: {}", std::io::Error::last_os_error()));
            }
        }

        let token = format!("RDMA_PUT_{}_{}_{}_key_{}", entry.name, size, offset, key)
            .into_bytes();
        self.dbg(&format!("prepare_put_token key={key} shm={}", entry.name));
        Ok(token)
    }

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

    fn process_rdma_reply(
        &self,
        rdma_status: u16,
        request_token: &[u8],
        transferred: usize,
        callback: RdmaCompletionCallback,
    ) -> Result<(), RdmaError> {
        self.dbg(&format!("process_rdma_reply status={rdma_status} transferred={transferred}"));

        if rdma_status == 0 || !(200..300).contains(&rdma_status) {
            let error_code = if rdma_status == 0 { -1i32 } else { rdma_status as i32 };
            callback(RdmaCompletionResult { error_code, reply_token: vec![] });
            return Ok(());
        }

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
            let token_offset: usize = parts[4].parse().unwrap_or(0);

            let mut registered = self.registered.lock().unwrap();
            let found_ptr = registered.iter()
                .find(|(_, e)| e.name == shm_name)
                .map(|(&k, _)| k);

            found_ptr.and_then(|ptr| {
                let entry = registered.remove(&ptr)?;
                if op == "GET" && transferred > 0 {
                    let available = entry.size.saturating_sub(token_offset);
                    let copy_len = transferred.min(available);
                    if copy_len > 0 {
                        unsafe {
                            std::ptr::copy_nonoverlapping(
                                (entry.shm_ptr as *const u8).add(token_offset),
                                entry.buf_ptr,
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

impl RdmaServerProvider for MockRdmaProvider {
    fn buf_size(&self) -> usize {
        if self.buf_size > 0 { self.buf_size } else { usize::MAX }
    }

    fn handle_put(
        &self,
        _s3_key: &[u8],
        token: &[u8],
        buf: &mut [u8],
        offset: usize,
        size: usize,
    ) -> Result<usize, RdmaError> {
        let (op, shm_name, token_size, _token_offset) = Self::parse_token(token)?;
        if op != "PUT" {
            return Err(RdmaError::TransferFailed(format!("expected PUT token, got {op}")));
        }
        let read_size = if size > 0 { size } else { token_size };
        let path = format!("{}/{shm_name}", self.shm_dir);
        self.dbg(&format!("handle_put shm={shm_name} offset={offset} size={read_size}"));

        let file_bytes = std::fs::read(&path)
            .map_err(|e| RdmaError::TransferFailed(format!("read {path}: {e}")))?;

        let start = offset.min(file_bytes.len());
        let end = (offset + read_size).min(file_bytes.len());
        let slice = &file_bytes[start..end];
        let n = slice.len().min(buf.len());
        buf[..n].copy_from_slice(&slice[..n]);
        Ok(n)
    }

    fn handle_get(
        &self,
        _s3_key: &[u8],
        data: &[u8],
        token: &[u8],
        offset: usize,
    ) -> Result<usize, RdmaError> {
        let (op, shm_name, capacity, token_offset) = Self::parse_token(token)?;
        if op != "GET" {
            return Err(RdmaError::TransferFailed(format!("expected GET token, got {op}")));
        }
        let remaining_cap = capacity.saturating_sub(offset);
        let write_len = data.len().min(remaining_cap);
        let write_start = token_offset + offset;
        let path = format!("{}/{shm_name}", self.shm_dir);
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

    unsafe fn handle_put_zc(
        &self,
        _s3_key:       &[u8],
        token:         &[u8],
        offset:        usize,
        size:          usize,
        write_fn:      crate::ffi::RdmaIoWriteFn,
        write_fn_data: *mut std::ffi::c_void,
    ) -> Result<usize, RdmaError> {
        let (op, shm_name, token_size, _token_offset) = Self::parse_token(token)?;
        if op != "PUT" {
            return Err(RdmaError::TransferFailed(format!("expected PUT token, got {op}")));
        }
        let read_size = if size > 0 { size } else { token_size };
        let path = format!("{}/{shm_name}", self.shm_dir);
        self.dbg(&format!("handle_put_zc shm={shm_name} offset={offset} size={read_size}"));

        let file_bytes = std::fs::read(&path)
            .map_err(|e| RdmaError::TransferFailed(format!("read {path}: {e}")))?;
        let start = offset.min(file_bytes.len());
        let end = (offset + read_size).min(file_bytes.len());
        let slice = &file_bytes[start..end];

        let n = unsafe {
            write_fn(write_fn_data, slice.as_ptr() as *const std::ffi::c_void, slice.len())
        };
        if n < 0 {
            return Err(RdmaError::TransferFailed(format!("write_fn returned {n}")));
        }
        Ok(n as usize)
    }

    unsafe fn handle_get_zc(
        &self,
        _s3_key:      &[u8],
        token:        &[u8],
        offset:       usize,
        read_fn:      crate::ffi::RdmaIoReadFn,
        read_fn_data: *mut std::ffi::c_void,
    ) -> Result<usize, RdmaError> {
        let (op, shm_name, capacity, token_offset) = Self::parse_token(token)?;
        if op != "GET" {
            return Err(RdmaError::TransferFailed(format!("expected GET token, got {op}")));
        }
        let remaining_cap = capacity.saturating_sub(offset);
        let buf_cap = if self.buf_size > 0 { remaining_cap.min(self.buf_size) } else { remaining_cap };
        let mut buf = vec![0u8; buf_cap];

        let n = unsafe {
            read_fn(read_fn_data, buf.as_mut_ptr() as *mut std::ffi::c_void, buf_cap)
        };
        if n < 0 {
            return Err(RdmaError::TransferFailed(format!("read_fn returned {n}")));
        }
        let read_bytes = n as usize;
        let write_len = read_bytes.min(remaining_cap);
        let write_start = token_offset + offset;
        let path = format!("{}/{shm_name}", self.shm_dir);
        self.dbg(&format!(
            "handle_get_zc shm={shm_name} write={write_len} write_start={write_start}"
        ));

        use std::io::{Seek, SeekFrom, Write};
        let mut file = OpenOptions::new()
            .write(true)
            .open(&path)
            .map_err(|e| RdmaError::TransferFailed(format!("open {path}: {e}")))?;
        file.seek(SeekFrom::Start(write_start as u64))
            .map_err(|e| RdmaError::TransferFailed(format!("seek {path}: {e}")))?;
        file.write_all(&buf[..write_len])
            .map_err(|e| RdmaError::TransferFailed(format!("write {path}: {e}")))?;

        Ok(write_len)
    }
}

impl RdmaProvider for MockRdmaProvider {
    fn supports_client(&self) -> bool { true }
    fn supports_server(&self) -> bool { true }
}
