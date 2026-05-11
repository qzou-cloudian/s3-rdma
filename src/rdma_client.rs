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
use crate::provider::{TelemetryConfig, TelemLevel, DEFAULT_RDMA_BUF_SIZE};

#[cfg(feature = "rdma-client")]
mod real {
    use std::ffi::{c_void, CString};

    use libloading::{Library, Symbol};

    use crate::error::RdmaError;
    use crate::ffi::{ByteCursor, CTelemetryConfig, GetNamedClientVtableFn, RdmaProviderVtable};
    use crate::provider::{RdmaClientChannel, TelemetryConfig, TelemLevel};

    const LIB_NAME: &str = "libs3rdmaclient.so";
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
            let lib = crate::ffi::find_library(LIB_NAME).ok_or_else(|| {
                RdmaError::RdmaPluginLoadFailed { reason: 
                    format!("{LIB_NAME} not found \
                             (searched LD_LIBRARY_PATH, exe dir, /lib64)")
                 }
            })?;

            let provider_name = CString::new("cuobj").unwrap();
            let vtable: *const RdmaProviderVtable = unsafe {
                let get_vtable: Symbol<GetNamedClientVtableFn> = lib
                    .get(b"s3_rdma_get_client_vtable\0")
                    .map_err(|e| RdmaError::RdmaPluginLoadFailed { reason: e.to_string() })?;
                let ptr = get_vtable(provider_name.as_ptr());
                if ptr.is_null() {
                    return Err(RdmaError::RdmaPluginLoadFailed { reason: 
                        "s3_rdma_get_client_vtable(\"cuobj\") returned null".into()
                     });
                }
                ptr
            };

            let mut ctx = std::ptr::null_mut::<c_void>();
            if let Some(init_fn) = unsafe { (*vtable).init } {
                let rc = unsafe { init_fn(std::ptr::null_mut(), &mut ctx) };
                if rc != 0 {
                    return Err(RdmaError::RdmaPluginLoadFailed { reason: 
                        format!("vtable init() returned {rc}")
                     });
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

        /// Allocate, register, and bind a buffer.  The plugin allocates memory
        /// internally; access it via `ch.ptr()` / `ch.size()`.
        pub fn bind(
            &self, size: usize, key: &[u8],
        ) -> Result<Box<dyn RdmaClientChannel>, RdmaError> {
            let f = self.vtable().bind_channel.ok_or_else(|| {
                RdmaError::OperationNotSupported { reason: 
                    "bind_channel not implemented".into(),
                 }
            })?;
            let key_cursor = ByteCursor::from_slice(key);
            let mut out: *mut std::ffi::c_void = std::ptr::null_mut();
            let rc = unsafe { f(self.ctx, size, &key_cursor, &mut out) };
            if rc != 0 || out.is_null() {
                return Err(RdmaError::MemoryRegistrationFailed {
                    ptr: 0, size,
                    reason: format!("bind_channel returned {rc}"),
                });
            }
            let boxed: Box<Box<dyn RdmaClientChannel>> =
                unsafe { Box::from_raw(out as *mut Box<dyn RdmaClientChannel>) };
            Ok(*boxed)
        }

        // ── Telemetry ────────────────────────────────────────────────────────

        pub fn supports_telemetry(&self) -> bool {
            self.vtable().supports_telemetry
                .map(|f| unsafe { f(self.ctx) } != 0)
                .unwrap_or(false)
        }

        pub fn setup_telemetry(&self, config: &TelemetryConfig) -> Result<(), RdmaError> {
            let f = self.vtable().setup_telemetry.ok_or_else(|| {
                RdmaError::TelemetryError { reason: "setup_telemetry not implemented".into() }
            })?;
            let log_file = config.log_file.as_ref()
                .and_then(|s| std::ffi::CString::new(s.as_str()).ok());
            let c_cfg = CTelemetryConfig {
                use_otel: config.use_otel as i32,
                log_file: log_file.as_ref().map(|cs| cs.as_ptr()).unwrap_or(std::ptr::null()),
                level: config.level as u32,
            };
            let rc = unsafe { f(self.ctx, &c_cfg) };
            if rc != 0 {
                Err(RdmaError::TelemetryError { reason: format!("setup_telemetry returned {rc}") })
            } else {
                Ok(())
            }
        }

        pub fn shutdown_telemetry(&self) -> Result<(), RdmaError> {
            let f = self.vtable().shutdown_telemetry.ok_or_else(|| {
                RdmaError::TelemetryError { reason: "shutdown_telemetry not implemented".into() }
            })?;
            let rc = unsafe { f(self.ctx) };
            if rc != 0 {
                Err(RdmaError::TelemetryError { reason: format!("shutdown_telemetry returned {rc}") })
            } else {
                Ok(())
            }
        }

        pub fn set_telem_level(&self, level: TelemLevel) -> Result<(), RdmaError> {
            let f = self.vtable().set_telem_level.ok_or_else(|| {
                RdmaError::TelemetryError { reason: "set_telem_level not implemented".into() }
            })?;
            let rc = unsafe { f(self.ctx, level as u32) };
            if rc != 0 {
                Err(RdmaError::TelemetryError { reason: format!("set_telem_level returned {rc}") })
            } else {
                Ok(())
            }
        }

        // ── Availability probe ────────────────────────────────────────────────

        /// Returns `true` if the RDMA client provider plugin can be loaded
        /// (i.e., `libs3rdmaclient.so` and its SDK dependencies are installed).
        ///
        /// This only checks that the shared library can be `dlopen`ed; it does
        /// not initialise the hardware backend (which may still fail at
        /// construction time if no GPU is available).
        pub fn is_available() -> bool {
            crate::ffi::find_library(LIB_NAME).is_some()
        }
    }

} // mod real

#[cfg(feature = "rdma-client")]
pub use real::RdmaPluginClient;

/// Returns `true` if the RDMA client plugin (`libs3rdmaclient.so`) can be
/// loaded at runtime.
#[cfg(feature = "rdma-client")]
pub fn rdma_available() -> bool {
    real::RdmaPluginClient::is_available()
}

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
    /// Create a new [`RdmaPlugin`] in client-only mode.
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
impl crate::provider::RdmaProviderBase for RdmaPlugin {
    fn name(&self) -> &str { "RdmaPlugin" }
    fn protocol(&self) -> &str { "cuobj" }

    fn supports_telemetry(&self) -> bool {
        #[cfg(feature = "rdma-client")]
        if let Some(c) = &self.client { if c.supports_telemetry() { return true; } }
        #[cfg(feature = "rdma-server")]
        if let Some(s) = &self.server { if s.supports_telemetry() { return true; } }
        false
    }

    fn setup_telemetry(&self, config: &TelemetryConfig) -> Result<(), crate::RdmaError> {
        #[cfg(feature = "rdma-client")]
        if let Some(c) = &self.client { return c.setup_telemetry(config); }
        #[cfg(feature = "rdma-server")]
        if let Some(s) = &self.server { return s.setup_telemetry(config); }
        let _ = config;
        Err(crate::RdmaError::OperationNotSupported { reason: "telemetry not supported".into() })
    }

    fn shutdown_telemetry(&self) -> Result<(), crate::RdmaError> {
        #[cfg(feature = "rdma-client")]
        if let Some(c) = &self.client { return c.shutdown_telemetry(); }
        #[cfg(feature = "rdma-server")]
        if let Some(s) = &self.server { return s.shutdown_telemetry(); }
        Err(crate::RdmaError::OperationNotSupported { reason: "telemetry not supported".into() })
    }

    fn set_telem_level(&self, level: TelemLevel) -> Result<(), crate::RdmaError> {
        #[cfg(feature = "rdma-client")]
        if let Some(c) = &self.client { return c.set_telem_level(level); }
        #[cfg(feature = "rdma-server")]
        if let Some(s) = &self.server { return s.set_telem_level(level); }
        let _ = level;
        Err(crate::RdmaError::OperationNotSupported { reason: "telemetry not supported".into() })
    }
}

#[cfg(any(feature = "rdma-client", feature = "rdma-server"))]
impl crate::provider::RdmaClientProvider for RdmaPlugin {
    fn bind(&self, size: usize, key: &[u8]) -> Result<Box<dyn crate::provider::RdmaClientChannel>, crate::RdmaError> {
        #[cfg(feature = "rdma-client")]
        if let Some(c) = &self.client {
            return c.bind(size, key);
        }
        let _ = (size, key);
        Err(crate::RdmaError::OperationNotSupported { reason: "rdma client not loaded".into() })
    }

    fn bind_registered(&self, key: &[u8]) -> Result<Box<dyn crate::provider::RdmaClientChannel>, crate::RdmaError> {
        #[cfg(feature = "rdma-client")]
        if let Some(c) = &self.client {
            return c.bind(DEFAULT_RDMA_BUF_SIZE, key);
        }
        let _ = key;
        Err(crate::RdmaError::OperationNotSupported { reason: "rdma client not loaded".into() })
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
        Err(crate::RdmaError::OperationNotSupported { reason: "rdma server not loaded".into() })
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
