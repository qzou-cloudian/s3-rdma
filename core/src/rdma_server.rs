//! Generic RDMA server provider via lazy runtime loading.
//!
//! Enabled by the `rdma-server` Cargo feature.  At runtime the gateway
//! searches for the server plugin (`libs3rdmaserver.so`) and loads it on
//! demand, so the main binary has **no compile-time dependency** on any RDMA
//! SDK or server library symbols.
//!
//! Search order for `libs3rdmaserver.so`:
//! 1. Standard dynamic-linker paths (`LD_LIBRARY_PATH`, RPATH, ldconfig cache).
//! 2. Directory containing the current executable.
//! 3. `/lib64/`, `/usr/lib64/` (common CUDA/cuObject install locations).

#[cfg(feature = "rdma-server")]
mod real {
    use std::ffi::{c_void, CString};
    use libloading::{Library, Symbol};
    use crate::error::RdmaError;
    use crate::ffi::{ByteCursor, CTelemetryConfig, GetNamedServerVtableFn, RdmaServerInitParams, RdmaServerVtable};
    use crate::provider::{RdmaServerConfig, TelemetryConfig, TelemLevel};

    const LIB_NAME: &str = "libs3rdmaserver.so";

    fn try_load(path: impl AsRef<std::ffi::OsStr>) -> Option<Library> {
        unsafe { Library::new(path) }.ok()
    }

    fn find_library() -> Option<Library> {
        if let Some(lib) = try_load(LIB_NAME) {
            return Some(lib);
        }
        if let Ok(exe) = std::env::current_exe() {
            if let Some(dir) = exe.parent() {
                if let Some(lib) = try_load(dir.join(LIB_NAME)) {
                    return Some(lib);
                }
            }
        }
        for dir in &["/lib64", "/usr/lib64"] {
            if let Some(lib) = try_load(format!("{dir}/{LIB_NAME}")) {
                return Some(lib);
            }
        }
        None
    }

    // ── RdmaPluginServer ──────────────────────────────────────────────────

    /// RDMA server provider that loads `libs3rdmaserver.so` at runtime.
    ///
    /// Wraps a server + buffer pool internally (inside the plugin); the gateway
    /// has no compile-time dependency on any RDMA SDK.
    pub struct RdmaPluginServer {
        vtable:   *const RdmaServerVtable,
        ctx:      *mut c_void,
        buf_size: usize,
        _lib:     Library,
    }

    unsafe impl Send for RdmaPluginServer {}
    unsafe impl Sync for RdmaPluginServer {}

    impl RdmaPluginServer {
        /// Load the plugin and initialise it with the given config.
        pub fn new(cfg: &RdmaServerConfig) -> Result<Self, RdmaError> {
            let lib = find_library().ok_or_else(|| {
                RdmaError::RdmaPluginLoadFailed(
                    format!("{LIB_NAME} not found \
                             (searched LD_LIBRARY_PATH, exe dir, /lib64, /usr/lib64)")
                )
            })?;

            let provider_name = CString::new("rdma").unwrap();
            let vtable: *const RdmaServerVtable = unsafe {
                let get_vtable: Symbol<GetNamedServerVtableFn> = lib
                    .get(b"s3_rdma_get_server_vtable\0")
                    .map_err(|e| RdmaError::RdmaPluginLoadFailed(e.to_string()))?;
                let ptr = get_vtable(provider_name.as_ptr());
                if ptr.is_null() {
                    return Err(RdmaError::RdmaPluginLoadFailed(
                        format!("s3_rdma_get_server_vtable(\"rdma\") returned null")
                    ));
                }
                ptr
            };

            let addr_cstr = CString::new(cfg.listen_addr.as_str())
                .map_err(|e| RdmaError::RdmaPluginLoadFailed(e.to_string()))?;
            let params = RdmaServerInitParams {
                listen_addr:    addr_cstr.as_ptr(),
                listen_port:    cfg.listen_port,
                pool_count:     cfg.pool_count,
                buf_pool_count: cfg.buf_pool_count,
                buf_size:       cfg.buf_size,
            };

            let mut ctx = std::ptr::null_mut::<c_void>();
            if let Some(init_fn) = unsafe { (*vtable).init } {
                let rc = unsafe { init_fn(&params, &mut ctx) };
                if rc != 0 {
                    return Err(RdmaError::RdmaPluginLoadFailed(
                        format!("server vtable init() returned {rc}")
                    ));
                }
            }

            Ok(Self { vtable, ctx, buf_size: cfg.buf_size, _lib: lib })
        }

        fn vtable(&self) -> &RdmaServerVtable {
            unsafe { &*self.vtable }
        }
    }

    impl Drop for RdmaPluginServer {
        fn drop(&mut self) {
            if let Some(cleanup) = self.vtable().cleanup {
                unsafe { cleanup(self.ctx); }
            }
        }
    }

    impl RdmaPluginServer {
        pub fn name(&self) -> &str {
            let ptr = self.vtable().provider_name;
            if ptr.is_null() {
                return "RdmaPluginServer";
            }
            unsafe { std::ffi::CStr::from_ptr(ptr) }
                .to_str()
                .unwrap_or("RdmaPluginServer")
        }

        pub fn buf_size(&self) -> usize { self.buf_size }

        /// Delegates to the plugin's `handle_put` vtable entry.
        ///
        /// The plugin performs an RDMA_READ from the client's buffer into its
        /// internal pool slot, then copies the transferred bytes into the
        /// caller-provided `buf`.  Returns the number of bytes written.
        pub fn handle_put(
            &self,
            s3_key: &[u8],
            token: &[u8],
            buf: &mut [u8],
            offset: usize,
            size: usize,
        ) -> Result<usize, RdmaError> {
            let f = self.vtable().handle_put.ok_or_else(|| {
                RdmaError::TransferFailed("handle_put not implemented in rdma plugin".into())
            })?;
            let key_cursor   = ByteCursor::from_slice(s3_key);
            let token_cursor = ByteCursor::from_slice(token);
            let mut transferred: usize = 0;
            let rc = unsafe {
                f(self.ctx, &key_cursor, &token_cursor,
                  buf.as_mut_ptr(), offset, size, &mut transferred)
            };
            if rc != 0 {
                return Err(RdmaError::TransferFailed(
                    format!("rdma server handle_put returned {rc}")
                ));
            }
            Ok(transferred)
        }

        /// Delegates to the plugin's `handle_get` vtable entry.
        ///
        /// Copies `data` into a pool slot and performs an RDMA_WRITE into the
        /// client's registered buffer via `token`.  Returns the number of bytes
        /// successfully written.
        pub fn handle_get(
            &self,
            s3_key: &[u8],
            data: &[u8],
            token: &[u8],
            offset: usize,
        ) -> Result<usize, RdmaError> {
            let f = self.vtable().handle_get.ok_or_else(|| {
                RdmaError::TransferFailed("handle_get not implemented in rdma plugin".into())
            })?;
            let key_cursor   = ByteCursor::from_slice(s3_key);
            let data_cursor  = ByteCursor::from_slice(data);
            let token_cursor = ByteCursor::from_slice(token);
            let mut written: usize = 0;
            let rc = unsafe {
                f(self.ctx, &key_cursor, &data_cursor, &token_cursor, offset, &mut written)
            };
            if rc != 0 {
                return Err(RdmaError::TransferFailed(
                    format!("rdma server handle_get returned {rc}")
                ));
            }
            Ok(written)
        }

        /// Zero-copy RDMA PUT: delegates to the plugin's `handle_put_zc` entry.
        ///
        /// The plugin RDMA_READs from the client buffer into its internal
        /// RDMA-registered slot, then invokes `write_fn(write_fn_data, ptr, n)`
        /// to flush the data to storage without an intermediate copy.
        ///
        /// # Safety
        /// `write_fn_data` must remain valid for the duration of the call.
        /// `write_fn` must be thread-safe (called from a plugin worker thread).
        pub unsafe fn handle_put_zc(
            &self,
            s3_key:        &[u8],
            token:         &[u8],
            offset:        usize,
            size:          usize,
            write_fn:      crate::ffi::RdmaIoWriteFn,
            write_fn_data: *mut std::ffi::c_void,
        ) -> Result<usize, RdmaError> {
            let f = self.vtable().handle_put_zc.ok_or_else(|| {
                RdmaError::TransferFailed("handle_put_zc not implemented in rdma plugin".into())
            })?;
            let key_cursor   = ByteCursor::from_slice(s3_key);
            let token_cursor = ByteCursor::from_slice(token);
            let mut transferred: usize = 0;
            let rc = unsafe {
                f(self.ctx, &key_cursor, &token_cursor,
                  offset, size, write_fn, write_fn_data, &mut transferred)
            };
            if rc != 0 {
                return Err(RdmaError::TransferFailed(
                    format!("rdma server handle_put_zc returned {rc}")
                ));
            }
            Ok(transferred)
        }

        /// Zero-copy RDMA GET: delegates to the plugin's `handle_get_zc` entry.
        ///
        /// The plugin calls `read_fn(read_fn_data, ptr, capacity)` to fill its
        /// RDMA-registered slot directly from storage, then RDMA_WRITEs the
        /// filled buffer to the client without an intermediate copy.
        ///
        /// # Safety
        /// `read_fn_data` must remain valid for the duration of the call.
        /// `read_fn` must be thread-safe (called from a plugin worker thread).
        pub unsafe fn handle_get_zc(
            &self,
            s3_key:       &[u8],
            token:        &[u8],
            offset:       usize,
            read_fn:      crate::ffi::RdmaIoReadFn,
            read_fn_data: *mut std::ffi::c_void,
        ) -> Result<usize, RdmaError> {
            let f = self.vtable().handle_get_zc.ok_or_else(|| {
                RdmaError::TransferFailed("handle_get_zc not implemented in rdma plugin".into())
            })?;
            let key_cursor   = ByteCursor::from_slice(s3_key);
            let token_cursor = ByteCursor::from_slice(token);
            let mut written: usize = 0;
            let rc = unsafe {
                f(self.ctx, &key_cursor, &token_cursor,
                  offset, read_fn, read_fn_data, &mut written)
            };
            if rc != 0 {
                return Err(RdmaError::TransferFailed(
                    format!("rdma server handle_get_zc returned {rc}")
                ));
            }
            Ok(written)
        }

        // ── Telemetry ────────────────────────────────────────────────────────

        pub fn supports_telemetry(&self) -> bool {
            self.vtable().supports_telemetry.is_some()
        }

        pub fn setup_telemetry(&self, config: &TelemetryConfig) -> Result<(), RdmaError> {
            let Some(f) = self.vtable().setup_telemetry else {
                return Err(RdmaError::OperationNotSupported(
                    "telemetry not supported by rdma server plugin".into(),
                ));
            };
            let log_file = config.log_file.as_ref()
                .and_then(|s| std::ffi::CString::new(s.as_str()).ok());
            let c_cfg = CTelemetryConfig {
                use_otel: config.use_otel as i32,
                log_file: log_file.as_ref().map(|cs| cs.as_ptr()).unwrap_or(std::ptr::null()),
                level: config.level as u32,
            };
            let rc = unsafe { f(self.ctx, &c_cfg) };
            if rc != 0 {
                return Err(RdmaError::TelemetryError(format!("setup_telemetry returned {rc}")));
            }
            Ok(())
        }

        pub fn shutdown_telemetry(&self) -> Result<(), RdmaError> {
            let Some(f) = self.vtable().shutdown_telemetry else {
                return Err(RdmaError::OperationNotSupported(
                    "telemetry not supported by rdma server plugin".into(),
                ));
            };
            let rc = unsafe { f(self.ctx) };
            if rc != 0 {
                return Err(RdmaError::TelemetryError(format!("shutdown_telemetry returned {rc}")));
            }
            Ok(())
        }

        pub fn set_telem_level(&self, level: TelemLevel) -> Result<(), RdmaError> {
            let Some(f) = self.vtable().set_telem_level else {
                return Err(RdmaError::OperationNotSupported(
                    "telemetry not supported by rdma server plugin".into(),
                ));
            };
            let rc = unsafe { f(self.ctx, level as u32) };
            if rc != 0 {
                return Err(RdmaError::TelemetryError(format!("set_telem_level returned {rc}")));
            }
            Ok(())
        }

    }
}

#[cfg(feature = "rdma-server")]
pub use real::RdmaPluginServer;
