//! Generic RDMA client provider via lazy runtime loading.
//!
//! Enabled by the `rdma-client` Cargo feature.  At runtime `hsc` searches
//! for the provider plugin (`libs3rdmaclient.so`) and loads it on demand.
//! The main `hsc` binary therefore has **no compile-time or ELF-level
//! dependency** on any RDMA SDK — the library is pulled in transitively when
//! the plugin is dlopened.
//!
//! Search order for `libs3rdmaclient.so`:
//! 1. Standard dynamic-linker paths (`LD_LIBRARY_PATH`, RPATH, ldconfig cache).
//! 2. Directory containing the current executable.
//! 3. `/lib64/`, `/usr/lib64/` (common CUDA/cuObject install locations).

#[cfg(any(feature = "rdma-client", feature = "rdma-server"))]
use crate::provider::{TelemetryConfig, TelemLevel};

#[cfg(feature = "rdma-client")]
mod real {
    use std::ffi::{c_void, CString};

    use libloading::{Library, Symbol};
    use tracing::error;

    use crate::error::RdmaError;
    use crate::ffi::{ByteCursor, CTelemetryConfig, GetNamedClientVtableFn, RdmaProviderVtable};
    use crate::provider::{
        RdmaCompletionCallback, RdmaCompletionResult,
        rdma_status_to_error_code, TelemetryConfig, TelemLevel,
    };

    // ── Library search ────────────────────────────────────────────────────────

    const LIB_NAME: &str = "libs3rdmaclient.so";

    fn try_load(path: impl AsRef<std::ffi::OsStr>) -> Option<Library> {
        unsafe { Library::new(path) }.ok()
    }

    fn find_library() -> Option<Library> {
        // Search order:
        //   1. Standard linker paths (LD_LIBRARY_PATH, RPATH, ldconfig)
        //   2. Directory alongside the running executable
        //   3. Common RDMA SDK install locations
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

    // ── RdmaPluginClient ──────────────────────────────────────────────────

    /// RDMA client provider that loads `libs3rdmaclient.so` at runtime and
    /// calls through the exported [`RdmaProviderVtable`].
    ///
    /// The plugin is queried for the `"cuobj"` backend name; the `.so` routes
    /// this to whichever hardware backend it was compiled with (currently cuObj).
    pub struct RdmaPluginClient {
        // Order matters: ctx/vtable must be dropped (logically) before _lib
        // unloads, so cleanup() is called in Drop::drop() before _lib drops.
        vtable: *const RdmaProviderVtable,
        ctx:    *mut c_void,
        _lib:   Library,  // keep the .so mapped; drops last
    }

    unsafe impl Send for RdmaPluginClient {}
    unsafe impl Sync for RdmaPluginClient {}

    impl std::fmt::Debug for RdmaPluginClient {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "RdmaPluginClient(vtable)")
        }
    }

    impl RdmaPluginClient {
        /// Try to load the provider plugin and initialise it.
        ///
        /// Returns `Err` if the plugin is not found, fails to load (e.g.
        /// the underlying RDMA SDK `.so` is missing), or the vtable's
        /// `init` fails.
        pub fn new() -> Result<Self, RdmaError> {
            let lib = find_library().ok_or_else(|| {
                RdmaError::RdmaPluginLoadFailed(
                    format!("{LIB_NAME} not found \
                             (searched LD_LIBRARY_PATH, exe dir, /lib64)")
                )
            })?;

            let provider_name = CString::new("cuobj").unwrap();
            let vtable: *const RdmaProviderVtable = unsafe {
                let get_vtable: Symbol<GetNamedClientVtableFn> = lib
                    .get(b"s3_rdma_get_client_vtable\0")
                    .map_err(|e| RdmaError::RdmaPluginLoadFailed(e.to_string()))?;
                let ptr = get_vtable(provider_name.as_ptr());
                if ptr.is_null() {
                    return Err(RdmaError::RdmaPluginLoadFailed(
                        "s3_rdma_get_client_vtable(\"cuobj\") returned null".into()
                    ));
                }
                ptr
            };

            let mut ctx = std::ptr::null_mut::<c_void>();
            if let Some(init_fn) = unsafe { (*vtable).init } {
                let rc = unsafe { init_fn(std::ptr::null_mut(), &mut ctx) };
                if rc != 0 {
                    return Err(RdmaError::RdmaPluginLoadFailed(
                        format!("vtable init() returned {rc}")
                    ));
                }
            }

            Ok(Self { vtable, ctx, _lib: lib })
        }

        fn vtable(&self) -> &RdmaProviderVtable {
            // SAFETY: vtable lives in the library's data segment which is
            // mapped for the lifetime of `self` (kept alive by `_lib`).
            unsafe { &*self.vtable }
        }
    }

    impl Drop for RdmaPluginClient {
        fn drop(&mut self) {
            if let Some(cleanup) = self.vtable().cleanup {
                unsafe { cleanup(self.ctx); }
            }
        }
    }

    impl RdmaPluginClient {
        pub fn name(&self)    -> &str { "RdmaPluginClient" }
        pub fn version(&self) -> u32  { unsafe { (*self.vtable).provider_version } }

        pub fn is_memory_suitable(&self, ptr: *const u8, size: usize) -> bool {
            if let Some(f) = self.vtable().is_memory_suitable {
                unsafe { f(self.ctx, ptr as *const std::ffi::c_void, size) }
            } else {
                true
            }
        }

        pub fn register_memory(&self, ptr: *mut u8, size: usize) -> Result<(), RdmaError> {
            if let Some(f) = self.vtable().register_memory {
                let rc = unsafe { f(self.ctx, ptr as *mut std::ffi::c_void, size) };
                if rc != 0 {
                    return Err(RdmaError::MemoryRegistrationFailed {
                        ptr: ptr as usize, size,
                        reason: format!("vtable register_memory returned {rc}"),
                    });
                }
            }
            Ok(())
        }

        pub fn deregister_memory(&self, ptr: *mut u8) -> Result<(), RdmaError> {
            if let Some(f) = self.vtable().deregister_memory {
                let rc = unsafe { f(self.ctx, ptr as *mut std::ffi::c_void) };
                if rc != 0 {
                    return Err(RdmaError::MemoryDeregistrationFailed {
                        ptr: ptr as usize,
                        reason: format!("vtable deregister_memory returned {rc}"),
                    });
                }
            }
            Ok(())
        }

        pub fn get_max_transfer_size(&self, ptr: *const u8) -> usize {
            if let Some(f) = self.vtable().get_max_transfer_size {
                unsafe { f(self.ctx, ptr as *const std::ffi::c_void) }
            } else {
                usize::MAX
            }
        }

        pub fn prepare_put_token(
            &self,
            s3_key: &[u8],
            buffer: *const u8,
            size:   usize,
            offset: usize,
        ) -> Result<Vec<u8>, RdmaError> {
            let f = self.vtable().prepare_put_token.ok_or_else(|| {
                RdmaError::TokenGenerationFailed {
                    key: String::from_utf8_lossy(s3_key).into_owned(),
                    reason: "prepare_put_token not implemented in rdma plugin".into(),
                }
            })?;
            let key_cursor = ByteCursor::from_slice(s3_key);
            let mut out = ByteCursor::null();
            let rc = unsafe {
                f(self.ctx, &key_cursor,
                  buffer as *const std::ffi::c_void, size, offset,
                  &mut out)
            };
            if rc != 0 {
                return Err(RdmaError::TokenGenerationFailed {
                    key: String::from_utf8_lossy(s3_key).into_owned(),
                    reason: format!("prepare_put_token returned {rc}"),
                });
            }
            let token = unsafe {
                let slice = std::slice::from_raw_parts(out.ptr, out.len);
                let v = slice.to_vec();
                drop(Box::from_raw(
                    std::ptr::slice_from_raw_parts_mut(out.ptr as *mut u8, out.len)
                ));
                v
            };
            Ok(token)
        }

        pub fn prepare_get_token(
            &self,
            s3_key: &[u8],
            buffer: *mut u8,
            size:   usize,
            offset: usize,
        ) -> Result<Vec<u8>, RdmaError> {
            let f = self.vtable().prepare_get_token.ok_or_else(|| {
                RdmaError::TokenGenerationFailed {
                    key: String::from_utf8_lossy(s3_key).into_owned(),
                    reason: "prepare_get_token not implemented in rdma plugin".into(),
                }
            })?;
            let key_cursor = ByteCursor::from_slice(s3_key);
            let mut out = ByteCursor::null();
            let rc = unsafe {
                f(self.ctx, &key_cursor,
                  buffer as *mut std::ffi::c_void, size, offset,
                  &mut out)
            };
            if rc != 0 {
                return Err(RdmaError::TokenGenerationFailed {
                    key: String::from_utf8_lossy(s3_key).into_owned(),
                    reason: format!("prepare_get_token returned {rc}"),
                });
            }
            let token = unsafe {
                let slice = std::slice::from_raw_parts(out.ptr, out.len);
                let v = slice.to_vec();
                drop(Box::from_raw(
                    std::ptr::slice_from_raw_parts_mut(out.ptr as *mut u8, out.len)
                ));
                v
            };
            Ok(token)
        }

        /// Process the `x-amz-rdma-reply` status code returned by the S3 server.
        ///
        /// Always deregisters the MR via `process_reply_token(request_token)`
        /// regardless of RDMA status, so the plugin's pending map entry is
        /// cleaned up when the server replies.
        pub fn process_rdma_reply(
            &self,
            rdma_status: u16,
            request_token: &[u8],
            _transferred: usize,
            callback: RdmaCompletionCallback,
        ) -> Result<(), RdmaError> {
            // Always deregister the MR, whether the server reports success or error.
            if let Some(f) = self.vtable().process_reply_token {
                let cursor = ByteCursor::from_slice(request_token);
                // Pass null user_data and no callback — we only need the cleanup.
                let rc = unsafe { f(self.ctx, &cursor, std::ptr::null_mut(), None) };
                if rc != 0 {
                    error!("[RdmaPluginClient] process_reply_token returned {rc}");
                }
            }
            callback(RdmaCompletionResult {
                error_code: rdma_status_to_error_code(rdma_status),
                reply_token: vec![],
            });
            Ok(())
        }

        // ── Telemetry ────────────────────────────────────────────────────────

        pub fn supports_telemetry(&self) -> bool {
            self.vtable().supports_telemetry.is_some()
        }

        pub fn setup_telemetry(&self, config: &TelemetryConfig) -> Result<(), RdmaError> {
            let Some(f) = self.vtable().setup_telemetry else {
                return Err(RdmaError::OperationNotSupported(
                    "telemetry not supported by rdma plugin".into(),
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
                    "telemetry not supported by rdma plugin".into(),
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
                    "telemetry not supported by rdma plugin".into(),
                ));
            };
            let rc = unsafe { f(self.ctx, level as u32) };
            if rc != 0 {
                return Err(RdmaError::TelemetryError(format!("set_telem_level returned {rc}")));
            }
            Ok(())
        }

    }

    // ── Availability probe ────────────────────────────────────────────────────

    /// Returns `true` if the RDMA client provider plugin can be loaded
    /// (i.e., `libs3rdmaclient.so` and its SDK dependencies are installed).
    ///
    /// This only checks that the shared library can be `dlopen`ed; it does
    /// not initialise the hardware backend (which may still fail at
    /// construction time if no GPU is available).
    pub fn is_available() -> bool {
        find_library().is_some()
    }
}

#[cfg(feature = "rdma-client")]
pub use real::RdmaPluginClient;
#[cfg(feature = "rdma-client")]
pub use real::is_available as rdma_available;

// ── RdmaPlugin ────────────────────────────────────────────────────────────

#[cfg(any(feature = "rdma-client", feature = "rdma-server"))]
/// Unified RDMA plugin provider
///
/// Each role is loaded on demand from the corresponding plugin:
/// - Client: `libs3rdmaclient.so`
///   (requires `rdma-client` feature)
/// - Server: `libs3rdmaserver.so`
///   (requires `rdma-server` feature)
///
/// Either or both roles may be absent (plugin not found, hardware unavailable).
pub struct RdmaPlugin {
    #[cfg(feature = "rdma-client")]
    client: Option<real::RdmaPluginClient>,
    #[cfg(feature = "rdma-server")]
    server: Option<crate::rdma_server::RdmaPluginServer>,
    buf_size:   usize,
}

#[cfg(any(feature = "rdma-client", feature = "rdma-server"))]
unsafe impl Send for RdmaPlugin {}
#[cfg(any(feature = "rdma-client", feature = "rdma-server"))]
unsafe impl Sync for RdmaPlugin {}

#[cfg(any(feature = "rdma-client", feature = "rdma-server"))]
impl std::fmt::Debug for RdmaPlugin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RdmaPlugin")
            .field("buf_size", &self.buf_size)
            .finish()
    }
}

#[cfg(any(feature = "rdma-client", feature = "rdma-server"))]
impl RdmaPlugin {
    ///
    /// Returns `Err` if the `rdma-client` feature is not compiled in or the
    /// plugin cannot be loaded.
    pub fn new_client() -> Result<Self, crate::RdmaError> {
        #[cfg(feature = "rdma-client")]
        {
            let client = real::RdmaPluginClient::new()?;
            Ok(Self {
                client: Some(client),
                #[cfg(feature = "rdma-server")]
                server: None,
                buf_size:   0,
            })
        }
        #[cfg(not(feature = "rdma-client"))]
        {
            Err(crate::RdmaError::ProviderUnavailable {
                provider: "cuobj".into(),
                reason: "compiled without the `rdma-client` feature".into(),
            })
        }
    }

    /// Create a server-only provider.
    pub fn new_server(cfg: &crate::provider::RdmaServerConfig) -> Result<Self, crate::RdmaError> {
        #[cfg(feature = "rdma-server")]
        {
            let server = crate::rdma_server::RdmaPluginServer::new(cfg)?;
            Ok(Self {
                #[cfg(feature = "rdma-client")]
                client: None,
                server: Some(server),
                buf_size:   cfg.buf_size,
            })
        }
        #[cfg(not(feature = "rdma-server"))]
        {
            let _ = cfg;
            Err(crate::RdmaError::ProviderUnavailable {
                provider: "cuobj".into(),
                reason: "compiled without the `rdma-server` feature".into(),
            })
        }
    }

    /// Create a provider carrying both client and server roles.
    ///
    /// If the client plugin fails to load, `supports_client()` returns `false`
    /// (not a hard error); the server role is still available.
    /// If the server plugin fails, `Err` is returned.
    pub fn new_both(cfg: &crate::provider::RdmaServerConfig) -> Result<Self, crate::RdmaError> {
        let mut ep = Self::new_server(cfg)?;
        #[cfg(feature = "rdma-client")]
        {
            ep.client = real::RdmaPluginClient::new().ok();
        }
        Ok(ep)
    }
}

#[cfg(any(feature = "rdma-client", feature = "rdma-server"))]
impl crate::provider::RdmaClientProvider for RdmaPlugin {
    fn name(&self) -> &str { "RdmaPlugin" }
    fn protocol(&self) -> &str { "cuobj" }

    fn is_memory_suitable(&self, ptr: *const u8, size: usize) -> bool {
        #[cfg(feature = "rdma-client")]
        if let Some(c) = &self.client {
            return c.is_memory_suitable(ptr, size);
        }
        let _ = (ptr, size);
        false
    }

    fn register_memory(&self, ptr: *mut u8, size: usize) -> Result<(), crate::RdmaError> {
        #[cfg(feature = "rdma-client")]
        if let Some(c) = &self.client {
            return c.register_memory(ptr, size);
        }
        let _ = (ptr, size);
        Err(crate::RdmaError::OperationNotSupported("rdma client not loaded".into()))
    }

    fn deregister_memory(&self, ptr: *mut u8) -> Result<(), crate::RdmaError> {
        #[cfg(feature = "rdma-client")]
        if let Some(c) = &self.client {
            return c.deregister_memory(ptr);
        }
        let _ = ptr;
        Err(crate::RdmaError::OperationNotSupported("rdma client not loaded".into()))
    }

    fn get_max_transfer_size(&self, ptr: *const u8) -> usize {
        #[cfg(feature = "rdma-client")]
        if let Some(c) = &self.client {
            return c.get_max_transfer_size(ptr);
        }
        let _ = ptr;
        0
    }

    fn prepare_put_token(
        &self,
        s3_key: &[u8],
        buffer: *const u8,
        size:   usize,
        offset: usize,
    ) -> Result<Vec<u8>, crate::RdmaError> {
        #[cfg(feature = "rdma-client")]
        if let Some(c) = &self.client {
            return c.prepare_put_token(s3_key, buffer, size, offset);
        }
        let _ = (s3_key, buffer, size, offset);
        Err(crate::RdmaError::OperationNotSupported("rdma client not loaded".into()))
    }

    fn prepare_get_token(
        &self,
        s3_key: &[u8],
        buffer: *mut u8,
        size:   usize,
        offset: usize,
    ) -> Result<Vec<u8>, crate::RdmaError> {
        #[cfg(feature = "rdma-client")]
        if let Some(c) = &self.client {
            return c.prepare_get_token(s3_key, buffer, size, offset);
        }
        let _ = (s3_key, buffer, size, offset);
        Err(crate::RdmaError::OperationNotSupported("rdma client not loaded".into()))
    }

    fn process_rdma_reply(
        &self,
        rdma_status: u16,
        request_token: &[u8],
        transferred: usize,
        callback: crate::provider::RdmaCompletionCallback,
    ) -> Result<(), crate::RdmaError> {
        #[cfg(feature = "rdma-client")]
        if let Some(c) = &self.client {
            return c.process_rdma_reply(rdma_status, request_token, transferred, callback);
        }
        let _ = (rdma_status, request_token, transferred, callback);
        Err(crate::RdmaError::OperationNotSupported("rdma client not loaded".into()))
    }

    // ── Telemetry ────────────────────────────────────────────────────────────

    fn supports_telemetry(&self) -> bool {
        #[cfg(feature = "rdma-client")]
        if let Some(c) = &self.client {
            return c.supports_telemetry();
        }
        false
    }

    fn setup_telemetry(&self, config: &TelemetryConfig) -> Result<(), crate::RdmaError> {
        #[cfg(feature = "rdma-client")]
        if let Some(c) = &self.client {
            return c.setup_telemetry(config);
        }
        let _ = config;
        Err(crate::RdmaError::OperationNotSupported("rdma client not loaded".into()))
    }

    fn shutdown_telemetry(&self) -> Result<(), crate::RdmaError> {
        #[cfg(feature = "rdma-client")]
        if let Some(c) = &self.client {
            return c.shutdown_telemetry();
        }
        Err(crate::RdmaError::OperationNotSupported("rdma client not loaded".into()))
    }

    fn set_telem_level(&self, level: TelemLevel) -> Result<(), crate::RdmaError> {
        #[cfg(feature = "rdma-client")]
        if let Some(c) = &self.client {
            return c.set_telem_level(level);
        }
        let _ = level;
        Err(crate::RdmaError::OperationNotSupported("rdma client not loaded".into()))
    }
}

#[cfg(any(feature = "rdma-client", feature = "rdma-server"))]
impl crate::provider::RdmaServerProvider for RdmaPlugin {
    fn buf_size(&self) -> usize {
        self.buf_size
    }

    fn open_channel(&self, key: &[u8], token: &[u8])
        -> Result<Box<dyn crate::provider::RdmaServerChannel>, crate::RdmaError>
    {
        #[cfg(feature = "rdma-server")]
        if let Some(s) = &self.server {
            return s.open_channel(key, token);
        }
        let _ = (key, token);
        Err(crate::RdmaError::OperationNotSupported("rdma server not loaded".into()))
    }

    // ── Telemetry ────────────────────────────────────────────────────────────

    fn supports_telemetry(&self) -> bool {
        #[cfg(feature = "rdma-server")]
        if let Some(s) = &self.server {
            return s.supports_telemetry();
        }
        false
    }

    fn setup_telemetry(&self, config: &TelemetryConfig) -> Result<(), crate::RdmaError> {
        #[cfg(feature = "rdma-server")]
        if let Some(s) = &self.server {
            return s.setup_telemetry(config);
        }
        let _ = config;
        Err(crate::RdmaError::OperationNotSupported("rdma server not loaded".into()))
    }

    fn shutdown_telemetry(&self) -> Result<(), crate::RdmaError> {
        #[cfg(feature = "rdma-server")]
        if let Some(s) = &self.server {
            return s.shutdown_telemetry();
        }
        Err(crate::RdmaError::OperationNotSupported("rdma server not loaded".into()))
    }

    fn set_telem_level(&self, level: TelemLevel) -> Result<(), crate::RdmaError> {
        #[cfg(feature = "rdma-server")]
        if let Some(s) = &self.server {
            return s.set_telem_level(level);
        }
        let _ = level;
        Err(crate::RdmaError::OperationNotSupported("rdma server not loaded".into()))
    }
}

#[cfg(any(feature = "rdma-client", feature = "rdma-server"))]
impl crate::provider::RdmaProvider for RdmaPlugin {
    fn supports_client(&self) -> bool {
        #[cfg(feature = "rdma-client")]
        { self.client.is_some() }
        #[cfg(not(feature = "rdma-client"))]
        { false }
    }

    fn supports_server(&self) -> bool {
        #[cfg(feature = "rdma-server")]
        { self.server.is_some() }
        #[cfg(not(feature = "rdma-server"))]
        { false }
    }
}
