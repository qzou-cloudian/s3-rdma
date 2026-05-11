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
//! ## Client lifecycle (channel API)
//! | Step | Client calls | Effect |
//! |------|-------------|--------|
//! | 1 | `provider.bind(size, key)` | Creates channel; registers buffer and SHM |
//! | 2 | `ch.prepare_put(offset, size)` | Copies `ptr[offset..offset+size]` → shm, returns `RdmaTransferHandle` |
//! | 2 | `ch.prepare_get(offset, size)` | Returns `RdmaTransferHandle` (shm left empty; relay will fill it) |
//! | 3 | *(HTTP exchange)* | Relay reads or writes the shm file |
//! | 4 | `ch.complete_put(handle, status, n)` / `ch.complete_get(handle, status, n)` | For GET: copies shm → `ptr`; cleans up shm |
//! | 5 | `drop(ch)` | Deregisters; cleans up SHM if not already done |
//!
//! ## Server lifecycle
//! | Step | Server calls | Effect |
//! |------|-------------|--------|
//! | 1 | `provider.open_channel(key, token)` | Opens an `RdmaServerChannel` |
//! | 2 | `ch.handle_put(buf, offset, size)` | Reads from SHM into `buf` |
//! | 2 | `ch.handle_get(data, offset)` | Writes `data` into SHM |

use std::alloc::Layout;
use std::collections::HashMap;
use std::fs::OpenOptions;
use std::os::unix::io::AsRawFd;
use std::sync::{Arc, Mutex};

use tracing::debug;

use crate::error::RdmaError;
use crate::provider::{RdmaServerChannel, RdmaClientChannel, RdmaTransferHandle, RdmaClientProvider, RdmaProviderBase, RdmaServerProvider, RdmaProvider, RDMA_REQUIRED_ALIGNMENT, TelemState, TelemetryConfig, TelemLevel};

/// Base directory for shared-memory files.
const SHM_DIR: &str = "/dev/shm";

struct ShmEntry {
    name:    String,
    shm_ptr: *mut libc::c_void,
    size:    usize,
}

// SAFETY: raw pointers are only accessed under the `registered` Mutex.
unsafe impl Send for ShmEntry {}
unsafe impl Sync for ShmEntry {}

// ─────────────────────────────────────────────────────────────────────────────
// MockServerChannel
// ─────────────────────────────────────────────────────────────────────────────

struct MockServerChannel {
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

unsafe impl Send for MockServerChannel {}
unsafe impl Sync for MockServerChannel {}

impl MockServerChannel {
    fn dbg(&self, msg: &str) {
        if self.debug { tracing::debug!("[MockServerChannel] {msg}"); }
    }
}

impl RdmaServerChannel for MockServerChannel {
    fn key(&self)   -> &[u8] { &self.key }
    fn token(&self) -> &[u8] { &self.token }
    fn buf_size(&self) -> usize { self.buf_size }

    fn handle_put(
        &self, offset: usize, size: usize,
        write: Box<dyn FnOnce(&[u8]) -> Result<usize, RdmaError> + Send + '_>,
    ) -> Result<usize, RdmaError> {
        if self.op != "PUT" {
            return Err(RdmaError::TransferFailed { reason: format!("expected PUT token, got {}", self.op) });
        }
        let read_size = if size > 0 { size } else { self.token_size };
        let path = format!("{}/{}", self.shm_dir, self.shm_name);
        self.dbg(&format!("handle_put shm={} offset={offset} size={read_size}", self.shm_name));
        let mut file = std::fs::File::open(&path)
            .map_err(|e| RdmaError::TransferFailed { reason: format!("open {path}: {e}") })?;
        let file_len = file.metadata()
            .map_err(|e| RdmaError::TransferFailed { reason: format!("metadata {path}: {e}") })?
            .len() as usize;
        use std::io::{Seek, SeekFrom, Read};
        let start = offset.min(file_len);
        let end = (offset + read_size).min(file_len);
        let want = end.saturating_sub(start);
        let mut buf = vec![0u8; want];
        file.seek(SeekFrom::Start(start as u64)).map_err(|e| RdmaError::TransferFailed { reason: format!("seek {path}: {e}") })?;
        file.read_exact(&mut buf).map_err(|e| RdmaError::TransferFailed { reason: format!("read {path}: {e}") })?;
        write(&buf)
    }

    fn handle_get(
        &self, offset: usize, size: usize,
        read: Box<dyn FnOnce(&mut [u8]) -> Result<usize, RdmaError> + Send + '_>,
    ) -> Result<usize, RdmaError> {
        if self.op != "GET" {
            return Err(RdmaError::TransferFailed { reason: format!("expected GET token, got {}", self.op) });
        }
        let cap = if size > 0 { size } else { self.token_size };
        let capacity = cap.saturating_sub(offset);
        let mut buf = vec![0u8; capacity];
        let n = read(&mut buf)?;
        let write_start = self.token_offset + offset;
        let path = format!("{}/{}", self.shm_dir, self.shm_name);
        self.dbg(&format!("handle_get shm={} write={n}/{} write_start={write_start}", self.shm_name, capacity));
        use std::io::{Seek, SeekFrom, Write};
        let mut file = OpenOptions::new().write(true).open(&path)
            .map_err(|e| RdmaError::TransferFailed { reason: format!("open {path}: {e}") })?;
        file.seek(SeekFrom::Start(write_start as u64)).map_err(|e| RdmaError::TransferFailed { reason: format!("seek {path}: {e}") })?;
        file.write_all(&buf[..n]).map_err(|e| RdmaError::TransferFailed { reason: format!("write {path}: {e}") })?;
        Ok(n)
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
    cleaned:   std::sync::atomic::AtomicBool,
    owned_shm: bool,
    /// Layout for freeing `ptr` when owned (from `bind`).  `None` when
    /// the buffer is externally owned (from `bind_registered`).
    layout:    Option<std::alloc::Layout>,
    /// Shared provider registry for cleanup on drop.
    registered: Arc<Mutex<HashMap<usize, ShmEntry>>>,
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
    fn ptr(&self)  -> *mut u8 { self.ptr }
    fn size(&self) -> usize { self.size }
    fn get_max_transfer_size(&self) -> usize { usize::MAX }

    fn prepare_put(&self, offset: usize, size: usize) -> Result<RdmaTransferHandle, RdmaError> {
        self.dbg(&format!("prepare_put offset={offset} size={size}"));
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
        Ok(RdmaTransferHandle { token, offset, size })
    }

    fn prepare_get(&self, offset: usize, size: usize) -> Result<RdmaTransferHandle, RdmaError> {
        self.dbg(&format!("prepare_get offset={offset} size={size}"));
        let token = format!(
            "RDMA_GET_{}_{size}_{offset}_key_{}",
            self.shm_name,
            String::from_utf8_lossy(&self.key),
        ).into_bytes();
        Ok(RdmaTransferHandle { token, offset, size })
    }

    fn complete_put(
        &self, _handle: RdmaTransferHandle, rdma_status: u16, _transferred: usize,
    ) -> Result<(), RdmaError> {
        if (200..300).contains(&rdma_status) { Ok(()) }
        else { Err(RdmaError::TransferFailed { reason: format!("RDMA PUT reply status={rdma_status}") }) }
    }

    fn complete_get(
        &self, handle: RdmaTransferHandle, rdma_status: u16, transferred: usize,
    ) -> Result<(), RdmaError> {
        if !(200..300).contains(&rdma_status) {
            return Err(RdmaError::TransferFailed { reason: format!("RDMA GET reply status={rdma_status}") });
        }
        let token_offset = handle.offset();
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
        Ok(())
    }
}

impl MockClientChannel {
    fn cleanup_shm(&self) {
        if self.cleaned.swap(true, std::sync::atomic::Ordering::Relaxed) { return; }
        if !self.owned_shm { return; }
        self.dbg(&format!("cleanup shm={}", self.shm_name));
        unsafe { libc::munmap(self.shm_ptr, self.size); }
        let path = format!("{}/{}", self.shm_dir, self.shm_name);
        let _ = std::fs::remove_file(&path);
    }
}

impl Drop for MockClientChannel {
    fn drop(&mut self) {
        self.cleanup_shm();
        // Remove the SHM entry from the provider's registered map.
        self.registered.lock().unwrap().remove(&(self.ptr as usize));
        if let Some(layout) = self.layout {
            unsafe { std::alloc::dealloc(self.ptr, layout); }
        }
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
    registered: Arc<Mutex<HashMap<usize, ShmEntry>>>,
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
            registered: Arc::new(Mutex::new(HashMap::new())),
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

    /// Register a buffer by creating an SHM file and mmap'ing it.
    /// Internal helper used by `bind`.
    fn register_shm(&self, ptr: *mut u8, size: usize) -> Result<(), RdmaError> {
        let name = format!("rdma-{}-{:x}", std::process::id(), ptr as usize);
        let path = format!("{}/{name}", self.shm_dir);
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
        self.registered.lock().unwrap().insert(
            ptr as usize,
            ShmEntry { name, shm_ptr, size },
        );
        Ok(())
    }

    fn parse_token(token: &[u8]) -> Result<(&str, &str, usize, usize), RdmaError> {
        let s = std::str::from_utf8(token).map_err(|e| {
            RdmaError::TransferFailed { reason: format!("token is not valid UTF-8: {e}") }
        })?;
        let parts: Vec<&str> = s.splitn(7, '_').collect();
        if parts.len() < 5 || parts[0] != "RDMA" {
            return Err(RdmaError::TransferFailed { reason: format!("invalid mock RDMA token: {s}") });
        }
        let op = parts[1];
        let shm_name = parts[2];
        let size: usize = parts[3].parse().map_err(|_| {
            RdmaError::TransferFailed { reason: format!("invalid size field in mock token: {}", parts[3]) }
        })?;
        let offset: usize = parts[4].parse().map_err(|_| {
            RdmaError::TransferFailed { reason: format!("invalid offset field in mock token: {}", parts[4]) }
        })?;
        Ok((op, shm_name, size, offset))
    }
}

impl RdmaProviderBase for MockRdmaProvider {
    fn name(&self) -> &str { "MockRdmaProvider" }
    fn protocol(&self) -> &str { "mock" }

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

impl RdmaClientProvider for MockRdmaProvider {
    fn bind(&self, size: usize, key: &[u8]) -> Result<Box<dyn RdmaClientChannel>, RdmaError> {
        self.dbg(&format!("bind size={size} key={}", String::from_utf8_lossy(key)));
        let layout = Layout::from_size_align(size, RDMA_REQUIRED_ALIGNMENT)
            .map_err(|e| RdmaError::MemoryRegistrationFailed {
                ptr: 0, size,
                reason: format!("layout: {e}"),
            })?;
        let ptr = unsafe { std::alloc::alloc(layout) };
        if ptr.is_null() {
            return Err(RdmaError::MemoryRegistrationFailed {
                ptr: 0, size,
                reason: "alloc returned null".into(),
            });
        }
        self.register_shm(ptr, size)?;
        Ok(Box::new(MockClientChannel {
            key: key.to_vec(), ptr, size,
            shm_dir: self.shm_dir.clone(),
            shm_name: format!("rdma-{}-{:x}", std::process::id(), ptr as usize),
            shm_ptr: self.registered.lock().unwrap().get(&(ptr as usize)).unwrap().shm_ptr,
            debug: self.debug,
            cleaned: std::sync::atomic::AtomicBool::new(false),
            owned_shm: true,
            layout: Some(layout),
            registered: self.registered.clone(),
        }))
    }

    fn bind_registered(&self, key: &[u8]) -> Result<Box<dyn RdmaClientChannel>, RdmaError> {
        self.dbg(&format!("bind_registered key={}", String::from_utf8_lossy(key)));
        // Use the first available registered buffer from the pool.
        // If none are registered, return error.
        let reg = self.registered.lock().unwrap();
        let (&ptr_key, entry) = reg.iter().next().ok_or_else(|| {
            RdmaError::OperationNotSupported { reason: "no registered buffers available".into() }
        })?;
        Ok(Box::new(MockClientChannel {
            key: key.to_vec(),
            ptr: ptr_key as *mut u8,
            size: entry.size,
            shm_dir: self.shm_dir.clone(),
            shm_name: entry.name.clone(),
            shm_ptr:  entry.shm_ptr,
            debug: self.debug,
            cleaned: std::sync::atomic::AtomicBool::new(false),
            owned_shm: false,
            layout: None,
            registered: self.registered.clone(),
        }))
    }
}

impl RdmaServerProvider for MockRdmaProvider {
    fn buf_size(&self) -> usize {
        if self.buf_size > 0 { self.buf_size } else { usize::MAX }
    }

    fn open_channel(&self, key: &[u8], token: &[u8]) -> Result<Box<dyn RdmaServerChannel>, RdmaError> {
        let (op, shm_name, token_size, token_offset) = Self::parse_token(token)?;
        self.dbg(&format!("open_channel key={} shm={shm_name}", String::from_utf8_lossy(key)));
        Ok(Box::new(MockServerChannel {
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
}

impl RdmaProvider for MockRdmaProvider {
    fn supports_client(&self) -> bool { true }
    fn supports_server(&self) -> bool { true }
}

// ── Tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::provider::RdmaClientChannelExt;
    use crate::provider::RdmaServerChannelExt;

    #[test]
    fn put_then_get_roundtrip() {
        let client = MockRdmaProvider::new(false, "/dev/shm", 4096);
        let ch = client.bind(4096, b"key").unwrap();
        let server = MockRdmaProvider::new(false, "/dev/shm", 4096);

        // PUT "hello"
        let n = ch.put(0, 4096,
            |buf: &mut [u8]| {
                buf[..5].copy_from_slice(b"hello");
                5
            },
            |token: &[u8]| {
                let srv = server.open_channel(b"key", token).unwrap();
                let mut out: Vec<u8> = Vec::with_capacity(4096);
                let n = srv.put(0, 4096, |data: &[u8]| {
                    out.extend_from_slice(data);
                    Ok(data.len())
                }).unwrap();
                assert_eq!(&out[..5], b"hello");
                Ok((200, n))
            },
        ).unwrap();
        assert_eq!(n, 4096);

        // GET "world"
        let mut received = Vec::new();
        let n = ch.get(0, 4096,
            |token: &[u8]| {
                let srv = server.open_channel(b"key", token).unwrap();
                let payload: &[u8] = b"world";
                let n = srv.get(0, payload.len(), |buf: &mut [u8]| {
                    let n = payload.len().min(buf.len());
                    buf[..n].copy_from_slice(&payload[..n]);
                    Ok(n)
                }).unwrap();
                Ok((200, n))
            },
            |data: &[u8]| received.extend_from_slice(data),
        ).unwrap();
        assert_eq!(n, 5);
        assert_eq!(received, b"world");
    }

    #[test]
    fn put_empty_write() {
        let client = MockRdmaProvider::new(false, "/dev/shm", 4096);
        let ch = client.bind(4096, b"key").unwrap();
        let server = MockRdmaProvider::new(false, "/dev/shm", 4096);

        let n = ch.put(0, 4096,
            |_buf: &mut [u8]| 0,
            |token: &[u8]| {
                let srv = server.open_channel(b"key", token).unwrap();
                let mut out: Vec<u8> = Vec::with_capacity(4096);
                let n = srv.put(0, 4096, |data: &[u8]| {
                    out.extend_from_slice(data);
                    Ok(data.len())
                }).unwrap();
                Ok((200, n))
            },
        ).unwrap();
        assert_eq!(n, 4096);
    }

    #[test]
    fn put_then_get_offset() {
        let client = MockRdmaProvider::new(false, "/dev/shm", 4096);
        let ch = client.bind(4096, b"key").unwrap();
        let server = MockRdmaProvider::new(false, "/dev/shm", 4096);

        // PUT 3 bytes at offset 100
        let n = ch.put(100, 4096,
            |buf: &mut [u8]| {
                buf[..3].copy_from_slice(b"foo");
                3
            },
            |token: &[u8]| {
                let srv = server.open_channel(b"key", token).unwrap();
                let mut out: Vec<u8> = Vec::with_capacity(4096);
                let n = srv.put(0, 4096, |data: &[u8]| {
                    out.extend_from_slice(data);
                    Ok(data.len())
                }).unwrap();
                assert_eq!(&out[..3], b"foo");
                Ok((200, n))
            },
        ).unwrap();
        assert_eq!(n, 4096);

        // GET 3 bytes at offset 100
        let mut received = Vec::new();
        let n = ch.get(100, 3,
            |token: &[u8]| {
                let srv = server.open_channel(b"key", token).unwrap();
                let payload: &[u8] = b"bar";
                let n = srv.get(0, payload.len(), |buf: &mut [u8]| {
                    let n = payload.len().min(buf.len());
                    buf[..n].copy_from_slice(&payload[..n]);
                    Ok(n)
                }).unwrap();
                Ok((200, n))
            },
            |data: &[u8]| received.extend_from_slice(data),
        ).unwrap();
        assert_eq!(n, 3);
        assert_eq!(received, b"bar");
    }
}
