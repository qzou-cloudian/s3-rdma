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
    use crate::error::RdmaError;
    use crate::ffi::{
        ByteCursor, CTelemetryConfig, GetNamedServerVtableFn, RdmaServerInitParams,
        RdmaServerVtable,
    };
    use crate::provider::{RdmaServerConfig, TelemLevel, TelemetryConfig};
    use libloading::{Library, Symbol};
    use std::ffi::{c_void, CString};

    const LIB_NAMES: &[&str] = &["libs3rdmaserver.so", "libcuobjserver.so"];

    // ── RdmaPluginServer ──────────────────────────────────────────────────

    /// RDMA server provider that loads `libs3rdmaserver.so` (or `libcuobjserver.so`) at runtime.
    ///
    /// Wraps a server + buffer pool internally (inside the plugin); the gateway
    /// has no compile-time dependency on any RDMA SDK.
    pub struct RdmaPluginServer {
        vtable: *const RdmaServerVtable,
        ctx: *mut c_void,
        buf_size: usize,
        _lib: Library,
    }

    unsafe impl Send for RdmaPluginServer {}
    unsafe impl Sync for RdmaPluginServer {}

    impl RdmaPluginServer {
        /// Load the plugin and initialise it with the given config.
        pub fn new(cfg: &RdmaServerConfig) -> Result<Self, RdmaError> {
            let lib = LIB_NAMES
                .iter()
                .find_map(|name| crate::ffi::find_library(name))
                .ok_or_else(|| RdmaError::RdmaPluginLoadFailed {
                    reason: format!(
                        "{} not found \
                             (searched LD_LIBRARY_PATH, exe dir, /lib64, /usr/lib64, /usr/local/lib)",
                        LIB_NAMES.join(" / ")
                    ),
                })?;

            let provider_name = CString::new("cuobj").unwrap();
            let vtable: *const RdmaServerVtable = unsafe {
                let get_vtable: Symbol<GetNamedServerVtableFn> = lib
                    .get(b"s3_rdma_get_server_vtable\0")
                    .map_err(|e| RdmaError::RdmaPluginLoadFailed {
                        reason: e.to_string(),
                    })?;
                let ptr = get_vtable(provider_name.as_ptr());
                if ptr.is_null() {
                    return Err(RdmaError::RdmaPluginLoadFailed {
                        reason: "s3_rdma_get_server_vtable(\"cuobj\") returned null".into(),
                    });
                }
                ptr
            };

            let addr_cstr = CString::new(cfg.listen_addr.as_str()).map_err(|e| {
                RdmaError::RdmaPluginLoadFailed {
                    reason: e.to_string(),
                }
            })?;
            let params = RdmaServerInitParams {
                listen_addr: addr_cstr.as_ptr(),
                listen_port: cfg.listen_port,
                buf_size: cfg.buf_size,
            };

            let mut ctx = std::ptr::null_mut::<c_void>();
            if let Some(init_fn) = unsafe { (*vtable).init } {
                let rc = unsafe { init_fn(&params, &mut ctx) };
                if rc != 0 {
                    return Err(RdmaError::RdmaPluginLoadFailed {
                        reason: format!("server vtable init() returned {rc}"),
                    });
                }
            }

            Ok(Self {
                vtable,
                ctx,
                buf_size: cfg.buf_size,
                _lib: lib,
            })
        }

        fn vtable(&self) -> &RdmaServerVtable {
            unsafe { &*self.vtable }
        }
    }

    impl Drop for RdmaPluginServer {
        fn drop(&mut self) {
            if let Some(cleanup) = self.vtable().cleanup {
                unsafe {
                    cleanup(self.ctx);
                }
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

        pub fn buf_size(&self) -> usize {
            self.buf_size
        }

        pub fn open_channel(
            &self,
            key: &[u8],
            token: &[u8],
        ) -> Result<Box<dyn crate::provider::RdmaServerChannel>, RdmaError> {
            let Some(f) = self.vtable().open_channel else {
                return Err(RdmaError::OperationNotSupported {
                    reason: "open_channel: not supported by rdma server plugin".into(),
                });
            };
            let kc = ByteCursor::from_slice(key);
            let tc = ByteCursor::from_slice(token);
            let mut out: *mut c_void = std::ptr::null_mut();
            let rc = unsafe { f(self.ctx, &kc, &tc, &mut out) };
            if rc != 0 {
                return Err(RdmaError::TransferFailed {
                    reason: format!("open_channel returned {rc}"),
                });
            }
            // Unwrap double-box: C side returns Box<Box<dyn RdmaServerChannel>>.
            let boxed: Box<Box<dyn crate::provider::RdmaServerChannel>> =
                unsafe { Box::from_raw(out as *mut Box<dyn crate::provider::RdmaServerChannel>) };
            Ok(*boxed)
        }

        /// Low-level vtable entry point for an RDMA PUT.
        ///
        /// This is a direct call into the `handle_put` slot of the server vtable.
        /// Prefer [`open_channel`](Self::open_channel) + [`RdmaServerChannel::handle_put`]
        /// for new code — the channel API is safer and more composable.
        ///
        /// # Safety
        /// `write_fn_data` must remain valid for the duration of the call.
        /// `write_fn` must be thread-safe (called from a plugin worker thread).
        pub unsafe fn handle_put(
            &self,
            s3_key: &[u8],
            token: &[u8],
            offset: usize,
            size: usize,
            write_fn: crate::ffi::RdmaIoWriteFn,
            write_fn_data: *mut std::ffi::c_void,
        ) -> Result<usize, RdmaError> {
            let f = self
                .vtable()
                .handle_put
                .ok_or_else(|| RdmaError::TransferFailed {
                    reason: "handle_put not implemented in rdma plugin".into(),
                })?;
            let key_cursor = ByteCursor::from_slice(s3_key);
            let token_cursor = ByteCursor::from_slice(token);
            let mut transferred: usize = 0;
            let rc = unsafe {
                f(
                    self.ctx,
                    &key_cursor,
                    &token_cursor,
                    offset,
                    size,
                    write_fn,
                    write_fn_data,
                    &mut transferred,
                )
            };
            if rc != 0 {
                return Err(RdmaError::TransferFailed {
                    reason: format!("rdma server handle_put returned {rc}"),
                });
            }
            Ok(transferred)
        }

        /// Low-level vtable entry point for an RDMA GET (zero-copy).
        ///
        /// This is a direct call into the `handle_get` slot of the server vtable.
        /// Prefer [`open_channel`](Self::open_channel) + [`RdmaServerChannel::handle_get`]
        /// for new code.  The plugin calls `read_fn(read_fn_data, ptr, capacity)` to fill
        /// its RDMA-registered slot directly from storage, then RDMA_WRITEs to the client.
        ///
        /// # Safety
        /// `read_fn_data` must remain valid for the duration of the call.
        /// `read_fn` must be thread-safe (called from a plugin worker thread).
        pub unsafe fn handle_get(
            &self,
            s3_key: &[u8],
            token: &[u8],
            offset: usize,
            read_fn: crate::ffi::RdmaIoReadFn,
            read_fn_data: *mut std::ffi::c_void,
        ) -> Result<usize, RdmaError> {
            let f = self
                .vtable()
                .handle_get
                .ok_or_else(|| RdmaError::TransferFailed {
                    reason: "handle_get not implemented in rdma plugin".into(),
                })?;
            let key_cursor = ByteCursor::from_slice(s3_key);
            let token_cursor = ByteCursor::from_slice(token);
            let mut written: usize = 0;
            let rc = unsafe {
                f(
                    self.ctx,
                    &key_cursor,
                    &token_cursor,
                    offset,
                    read_fn,
                    read_fn_data,
                    &mut written,
                )
            };
            if rc != 0 {
                return Err(RdmaError::TransferFailed {
                    reason: format!("rdma server handle_get returned {rc}"),
                });
            }
            Ok(written)
        }

        // ── Telemetry ────────────────────────────────────────────────────────

        pub fn supports_telemetry(&self) -> bool {
            self.vtable().supports_telemetry.is_some()
        }

        pub fn setup_telemetry(&self, config: &TelemetryConfig) -> Result<(), RdmaError> {
            let Some(f) = self.vtable().setup_telemetry else {
                return Err(RdmaError::OperationNotSupported {
                    reason: "telemetry not supported by rdma server plugin".into(),
                });
            };
            let log_file = config
                .log_file
                .as_ref()
                .and_then(|s| std::ffi::CString::new(s.as_str()).ok());
            let c_cfg = CTelemetryConfig {
                use_otel: config.use_otel as i32,
                log_file: log_file
                    .as_ref()
                    .map(|cs| cs.as_ptr())
                    .unwrap_or(std::ptr::null()),
                level: config.level as u32,
            };
            let rc = unsafe { f(self.ctx, &c_cfg) };
            if rc != 0 {
                return Err(RdmaError::TelemetryError {
                    reason: format!("setup_telemetry returned {rc}"),
                });
            }
            Ok(())
        }

        pub fn shutdown_telemetry(&self) -> Result<(), RdmaError> {
            let Some(f) = self.vtable().shutdown_telemetry else {
                return Err(RdmaError::OperationNotSupported {
                    reason: "telemetry not supported by rdma server plugin".into(),
                });
            };
            let rc = unsafe { f(self.ctx) };
            if rc != 0 {
                return Err(RdmaError::TelemetryError {
                    reason: format!("shutdown_telemetry returned {rc}"),
                });
            }
            Ok(())
        }

        pub fn set_telem_level(&self, level: TelemLevel) -> Result<(), RdmaError> {
            let Some(f) = self.vtable().set_telem_level else {
                return Err(RdmaError::OperationNotSupported {
                    reason: "telemetry not supported by rdma server plugin".into(),
                });
            };
            let rc = unsafe { f(self.ctx, level as u32) };
            if rc != 0 {
                return Err(RdmaError::TelemetryError {
                    reason: format!("set_telem_level returned {rc}"),
                });
            }
            Ok(())
        }
    }
}

#[cfg(feature = "rdma-server")]
pub use real::RdmaPluginServer;
