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

use tracing::debug;

use crate::error::RdmaError;
use crate::provider::{RdmaServerChannel, RdmaClientChannel, RdmaClientProvider, RdmaCompletionCallback, RdmaCompletionResult, RdmaServerProvider, RdmaProvider, rdma_status_to_error_code, RDMA_REQUIRED_ALIGNMENT, TelemState, TelemetryConfig, TelemLevel};

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
// MockChannel
// ─────────────────────────────────────────────────────────────────────────────

struct MockChannel {
    key:       Vec<u8>,
    token:     Vec<u8>,
    shm_dir:   String,
    shm_name:  String,
    op:        String,
    token_size: usize,
    token_offset: usize,
    buf_size:  usize,
    debug:     bool,
}

unsafe impl Send for MockChannel {}
unsafe impl Sync for MockChannel {}

impl MockChannel {
    fn dbg(&self, msg: &str) {
        if self.debug { tracing::debug!("[MockChannel] {msg}"); }
    }
}

impl RdmaServerChannel for MockChannel {
    fn key(&self)   -> &[u8] { &self.key }
    fn token(&self) -> &[u8] { &self.token }
    fn buf_size(&self) -> usize { self.buf_size }

    fn handle_put(&self, buf: &mut [u8], offset: usize, size: usize) -> Result<usize, RdmaError> {
        if self.op != "PUT" {
            return Err(RdmaError::TransferFailed(format!("expected PUT token, got {}", self.op)));
        }
        let read_size = if size > 0 { size } else { self.token_size };
        let path = format!("{}/{}", self.shm_dir, self.shm_name);
        self.dbg(&format!("handle_put shm={} offset={offset} size={read_size}", self.shm_name));
        let mut file = std::fs::File::open(&path)
            .map_err(|e| RdmaError::TransferFailed(format!("open {path}: {e}")))?;
        let file_len = file.metadata()
            .map_err(|e| RdmaError::TransferFailed(format!("metadata {path}: {e}")))?
            .len() as usize;
        use std::io::{Seek, SeekFrom, Read};
        let start = offset.min(file_len);
        let end = (offset + read_size).min(file_len);
        let want = end.saturating_sub(start);
        let n = want.min(buf.len());
        file.seek(SeekFrom::Start(start as u64)).map_err(|e| RdmaError::TransferFailed(format!("seek {path}: {e}")))?;
        file.read_exact(&mut buf[..n]).map_err(|e| RdmaError::TransferFailed(format!("read {path}: {e}")))?;
        Ok(n)
    }

    fn handle_get(&self, data: &[u8], offset: usize) -> Result<usize, RdmaError> {
        if self.op != "GET" {
            return Err(RdmaError::TransferFailed(format!("expected GET token, got {}", self.op)));
        }
        let remaining_cap = self.token_size.saturating_sub(offset);
        let write_len = data.len().min(remaining_cap);
        let write_start = self.token_offset + offset;
        let path = format!("{}/{}", self.shm_dir, self.shm_name);
        self.dbg(&format!("handle_get shm={} write={write_len}/{} write_start={write_start}", self.shm_name, data.len()));
        use std::io::{Seek, SeekFrom, Write};
        let mut file = OpenOptions::new().write(true).open(&path)
            .map_err(|e| RdmaError::TransferFailed(format!("open {path}: {e}")))?;
        file.seek(SeekFrom::Start(write_start as u64)).map_err(|e| RdmaError::TransferFailed(format!("seek {path}: {e}")))?;
        file.write_all(&data[..write_len]).map_err(|e| RdmaError::TransferFailed(format!("write {path}: {e}")))?;
        Ok(write_len)
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// MockClientChannel
// ─────────────────────────────────────────────────────────────────────────────

struct MockClientChannel {
    key:       Vec<u8>,
    ptr:       *mut u8,
    size:      usize,
    shm_dir:   String,
    shm_name:  String,
    shm_ptr:   *mut libc::c_void,
    debug:     bool,
    /// Whether the SHM has already been cleaned up by process_rdma_reply.
    cleaned:   std::sync::atomic::AtomicBool,
}

unsafe impl Send for MockClientChannel {}
unsafe impl Sync for MockClientChannel {}

impl MockClientChannel {
    fn dbg(&self, msg: &str) {
        if self.debug { tracing::debug!("[MockClientChannel] {msg}"); }
    }
}

impl RdmaClientChannel for MockClientChannel {
    fn key(&self)  -> &[u8] { &self.key }
    fn ptr(&self)  -> *const u8 { self.ptr }
    fn size(&self) -> usize { self.size }
    fn get_max_transfer_size(&self) -> usize { usize::MAX }

    fn prepare_put_token(&self, offset: usize, size: usize) -> Result<Vec<u8>, RdmaError> {
        self.dbg(&format!("prepare_put_token offset={offset} size={size}"));
        unsafe { std::ptr::copy_nonoverlapping(self.ptr.add(offset), self.shm_ptr as *mut u8, size); }
        if unsafe { libc::msync(self.shm_ptr, size, libc::MS_SYNC) } != 0 {
            let e = std::io::Error::last_os_error();
            return Err(RdmaError::TokenGenerationFailed {
                key: String::from_utf8_lossy(&self.key).into_owned(),
                reason: format!("msync shm={} failed: {e}", self.shm_name),
            });
        }
        let token = format!(
            "RDMA_PUT_{}_{size}_{offset}_key_{}",
            self.shm_name,
            String::from_utf8_lossy(&self.key),
        ).into_bytes();
        Ok(token)
    }

    fn prepare_get_token(&self, offset: usize, size: usize) -> Result<Vec<u8>, RdmaError> {
        self.dbg(&format!("prepare_get_token offset={offset} size={size}"));
        let token = format!(
            "RDMA_GET_{}_{size}_{offset}_key_{}",
            self.shm_name,
            String::from_utf8_lossy(&self.key),
        ).into_bytes();
        Ok(token)
    }

    fn process_rdma_reply(
        &self, status: u16, request_token: &[u8], transferred: usize, cb: RdmaCompletionCallback,
    ) -> Result<(), RdmaError> {
        let error_code = rdma_status_to_error_code(status);
        if error_code != 0 {
            cb(RdmaCompletionResult { error_code, reply_token: vec![] });
            return Ok(());
        }

        let (op, _shm_name, _size, token_offset) = match MockRdmaProvider::parse_token(request_token) {
            Ok(t) => t,
            Err(e) => {
                cb(RdmaCompletionResult { error_code: -1, reply_token: vec![] });
                return Err(e);
            }
        };

        if op == "GET" && transferred > 0 {
            let available = self.size.saturating_sub(token_offset);
            let copy_len = transferred.min(available);
            if copy_len > 0 {
                unsafe {
                    std::ptr::copy_nonoverlapping(
                        (self.shm_ptr as *const u8).add(token_offset),
                        self.ptr, copy_len,
                    );
                }
            }
        }

        self.cleanup_shm();
        cb(RdmaCompletionResult { error_code: 0, reply_token: vec![] });
        Ok(())
    }
}

impl MockClientChannel {
    fn cleanup_shm(&self) {
        if self.cleaned.swap(true, std::sync::atomic::Ordering::Relaxed) { return; }
        self.dbg(&format!("cleanup shm={}", self.shm_name));
        unsafe { libc::munmap(self.shm_ptr, self.size); }
        let path = format!("{}/{}", self.shm_dir, self.shm_name);
        let _ = std::fs::remove_file(&path);
    }
}

impl Drop for MockClientChannel {
    fn drop(&mut self) { self.cleanup_shm(); }
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
    telem: TelemState,
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
            telem: TelemState::new(),
        }
    }

    /// Convenience constructor with default `/dev/shm` and unlimited buf_size.
    pub fn with_debug(debug: bool) -> Self {
        Self::new(debug, SHM_DIR, 0)
    }

    fn dbg(&self, msg: &str) {
        if self.debug {
            debug!("[MockRdmaProvider] {msg}");
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
        let size: usize = parts[3].parse().map_err(|_| {
            RdmaError::TransferFailed(format!("invalid size field in mock token: {}", parts[3]))
        })?;
        let offset: usize = parts[4].parse().map_err(|_| {
            RdmaError::TransferFailed(format!("invalid offset field in mock token: {}", parts[4]))
        })?;
        Ok((op, shm_name, size, offset))
    }
}

impl RdmaClientProvider for MockRdmaProvider {
    fn name(&self) -> &str { "MockRdmaProvider" }
    fn protocol(&self) -> &str { "mock" }

    fn is_memory_suitable(&self, ptr: *const u8, size: usize) -> bool {
        let ok = !ptr.is_null() && size > 0 && (ptr as usize).is_multiple_of(RDMA_REQUIRED_ALIGNMENT);
        self.dbg(&format!("is_memory_suitable ptr={ptr:p} size={size} -> {ok}"));
        ok
    }

    fn bind(&self, ptr: *mut u8, size: usize, key: &[u8]) -> Result<Box<dyn RdmaClientChannel>, RdmaError> {
        self.dbg(&format!("bind ptr={ptr:p} size={size} key={}", String::from_utf8_lossy(key)));
        if !(ptr as usize).is_multiple_of(RDMA_REQUIRED_ALIGNMENT) {
            return Err(RdmaError::MemoryNotSuitable { ptr: ptr as usize, size });
        }
        let shm_name = format!("rdma-{}-{:x}", std::process::id(), ptr as usize);
        let path = format!("{}/{shm_name}", self.shm_dir);
        let file = OpenOptions::new().read(true).write(true).create(true).truncate(true)
            .open(&path)
            .map_err(|e| RdmaError::MemoryRegistrationFailed {
                ptr: ptr as usize, size,
                reason: format!("create {path}: {e}"),
            })?;
        file.set_len(size as u64).map_err(|e| RdmaError::MemoryRegistrationFailed {
            ptr: ptr as usize, size,
            reason: format!("resize {path}: {e}"),
        })?;
        let shm_ptr = unsafe {
            libc::mmap(std::ptr::null_mut(), size, libc::PROT_READ | libc::PROT_WRITE,
                       libc::MAP_SHARED, file.as_raw_fd(), 0)
        };
        drop(file);
        if shm_ptr == libc::MAP_FAILED {
            let _ = std::fs::remove_file(&path);
            return Err(RdmaError::MemoryRegistrationFailed {
                ptr: ptr as usize, size,
                reason: format!("mmap {path}: {}", std::io::Error::last_os_error()),
            });
        }
        Ok(Box::new(MockClientChannel {
            key: key.to_vec(), ptr, size,
            shm_dir: self.shm_dir.clone(), shm_name, shm_ptr,
            debug: self.debug,
            cleaned: std::sync::atomic::AtomicBool::new(false),
        }))
    }

    fn register_memory(&self, ptr: *mut u8, size: usize) -> Result<(), RdmaError> {
        self.dbg(&format!("register_memory ptr={ptr:p} size={size}"));

        if !(ptr as usize).is_multiple_of(RDMA_REQUIRED_ALIGNMENT) {
            return Err(RdmaError::MemoryNotSuitable {
                ptr: ptr as usize,
                size,
            });
        }

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

    #[allow(clippy::not_unsafe_ptr_arg_deref)]
    fn prepare_put_token(
        &self,
        s3_key: &[u8],
        buffer: *const u8,
        size: usize,
        offset: usize,
    ) -> Result<Vec<u8>, RdmaError> {
        let key = String::from_utf8_lossy(s3_key);
        let shm_name = {
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
                    let e = std::io::Error::last_os_error();
                    return Err(RdmaError::TokenGenerationFailed {
                        key: key.to_string(),
                        reason: format!("msync shm={} failed: {e}", entry.name),
                    });
                }
            }
            entry.name.clone()
        }; // lock released; string allocation below is outside the critical section

        let token = format!("RDMA_PUT_{shm_name}_{size}_{offset}_key_{key}")
            .into_bytes();
        self.dbg(&format!("prepare_put_token key={key} shm={shm_name}"));
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
        let shm_name = {
            let registered = self.registered.lock().unwrap();
            let entry = registered.get(&(buffer as usize)).ok_or_else(|| {
                RdmaError::TokenGenerationFailed {
                    key: key.to_string(),
                    reason: format!("buffer {buffer:p} not registered"),
                }
            })?;
            entry.name.clone()
        }; // lock released; string allocation below is outside the critical section

        let token = format!("RDMA_GET_{shm_name}_{size}_{offset}_key_{key}")
            .into_bytes();
        self.dbg(&format!("prepare_get_token key={key} shm={shm_name}"));
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

        let error_code = rdma_status_to_error_code(rdma_status);
        if error_code != 0 {
            callback(RdmaCompletionResult { error_code, reply_token: vec![] });
            return Ok(());
        }

        let (op, shm_name, _size, token_offset) = match Self::parse_token(request_token) {
            Ok(t) => t,
            Err(e) => {
                callback(RdmaCompletionResult { error_code: -1, reply_token: vec![] });
                return Err(e);
            }
        };

        let entry_to_drop: Option<ShmEntry> = {
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
        };

        if let Some(entry) = entry_to_drop {
            unsafe { entry.cleanup(); }
        }

        callback(RdmaCompletionResult { error_code: 0, reply_token: vec![] });
        Ok(())
    }

    fn rdma_token_header_name(&self) -> &[u8] { self.token_header.as_bytes() }
    fn rdma_reply_header_name(&self) -> &[u8] { self.reply_header.as_bytes() }

    // ── Telemetry (delegated to TelemState) ──────────────────────────────────

    fn supports_telemetry(&self) -> bool { self.telem.supports_telemetry() }

    fn setup_telemetry(&self, config: &TelemetryConfig) -> Result<(), RdmaError> {
        if self.debug {
            let dest = config.log_file.as_deref().unwrap_or("stdout");
            debug!("[MockRdmaProvider] telemetry: use_otel={} dest={dest} level={:?}", config.use_otel, config.level);
        }
        self.telem.setup_telemetry(config)
    }

    fn shutdown_telemetry(&self) -> Result<(), RdmaError> { self.telem.shutdown_telemetry() }

    fn set_telem_level(&self, level: TelemLevel) -> Result<(), RdmaError> { self.telem.set_telem_level(level) }
}

impl RdmaServerProvider for MockRdmaProvider {
    fn buf_size(&self) -> usize {
        if self.buf_size > 0 { self.buf_size } else { usize::MAX }
    }

    fn open_channel(&self, key: &[u8], token: &[u8]) -> Result<Box<dyn RdmaServerChannel>, RdmaError> {
        let (op, shm_name, token_size, token_offset) = Self::parse_token(token)?;
        self.dbg(&format!("open_channel key={} shm={shm_name}", String::from_utf8_lossy(key)));
        Ok(Box::new(MockChannel {
            key:       key.to_vec(),
            token:     token.to_vec(),
            shm_dir:   self.shm_dir.clone(),
            shm_name:  shm_name.to_owned(),
            op:        op.to_owned(),
            token_size,
            token_offset,
            buf_size:  if self.buf_size > 0 { self.buf_size } else { usize::MAX },
            debug:     self.debug,
        }))
    }

    // ── Telemetry (delegated to TelemState) ──────────────────────────────────

    fn supports_telemetry(&self) -> bool { self.telem.supports_telemetry() }

    fn setup_telemetry(&self, config: &TelemetryConfig) -> Result<(), RdmaError> {
        self.telem.setup_telemetry(config)
    }

    fn shutdown_telemetry(&self) -> Result<(), RdmaError> { self.telem.shutdown_telemetry() }

    fn set_telem_level(&self, level: TelemLevel) -> Result<(), RdmaError> { self.telem.set_telem_level(level) }
}

impl RdmaProvider for MockRdmaProvider {
    fn supports_client(&self) -> bool { true }
    fn supports_server(&self) -> bool { true }
}
